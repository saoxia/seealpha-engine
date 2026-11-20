"""Engine-owned durable policy snapshots and job files (no business database)."""
import json
import os
from pathlib import Path
from uuid import uuid4


class StorageUnavailable(OSError):
    pass


class PolicyRepository:
    def __init__(self, path, tenant_id):
        self.path = Path(path)
        self.tenant_id = str(tenant_id)

    def list(self, tenant_id=None):
        if tenant_id is not None and str(tenant_id) != self.tenant_id:
            raise StorageUnavailable("租户行情配置范围不匹配")
        if not self.path.exists():
            return []
        try:
            body = json.loads(self.path.read_text())
            if not isinstance(body, list) or not all(isinstance(row, dict) for row in body):
                raise ValueError("invalid policy file")
            return body
        except (ValueError, OSError) as exc:
            raise StorageUnavailable("租户行情配置记录损坏，已保留原文件。") from exc

    def save(self, policies, *, tenant_id):
        if str(tenant_id) != self.tenant_id:
            raise StorageUnavailable("租户行情配置范围不匹配")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(f".{uuid4().hex}.tmp")
        try:
            with temporary.open("x") as stream:
                json.dump([p.model_dump(mode="json") for p in policies], stream, ensure_ascii=False)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)


class OperationFiles:
    """Compatibility HTTP operations keep their status in files, never business ORM."""

    def __init__(self, directory):
        self.directory = directory

    def list_jobs(self):
        try:
            return [json.loads(path.read_text()) for path in self.directory.glob("*.json")]
        except (OSError, ValueError):
            raise StorageUnavailable("数据操作记录无法读取；原文件已保留。") from None

    def save_job(self, job):
        self.directory.mkdir(parents=True, exist_ok=True)
        target = self.directory / f"{job['id']}.json"
        temporary = target.with_suffix(f".{uuid4().hex}.tmp")
        try:
            with temporary.open("x") as stream:
                json.dump(job, stream, ensure_ascii=False)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
