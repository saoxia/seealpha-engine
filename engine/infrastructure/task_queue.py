"""Tenant-local, durable asynchronous task coordinator.

Backtests run in isolated child processes managed by a bounded thread-based
supervisor. API requests never execute strategy code. Cancellation transitions
RUNNING -> CANCELLING -> CANCELLED only after the child is reaped.
Unconfirmed tasks remain INTERRUPTED on restart, never implicitly retried.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from fastapi import HTTPException
from pydantic import ValidationError

from engine.market_data.control.contracts import (
    DataJobRequest,
    PairDeleteRequest,
    PairHistoryJob,
    PolicyTrimRequest,
)
from engine.trading.store import Store

_ID = re.compile(r"^[0-9a-f]{32}$")
_TENANT = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED", "INTERRUPTED"}
_DATA_STATUS = {
    "running": "RUNNING",
    "succeeded": "SUCCEEDED",
    "failed": "FAILED",
    "cancelled": "CANCELLED",
    "interrupted": "INTERRUPTED",
    "unknown": "UNKNOWN",
}
_LOG = logging.getLogger(__name__)


def _now():
    return datetime.now(UTC).isoformat()


def _fingerprint(kind, parameters):
    canonical = json.dumps(
        {"kind": kind, "parameters": parameters},
        sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_tenant(tenant_id):
    tenant = str(tenant_id)
    if not _TENANT.fullmatch(tenant):
        raise HTTPException(422, "租户标识无效。")
    return tenant


def _validate_id(identity):
    if not _ID.fullmatch(identity):
        raise HTTPException(404, "任务不存在。")


class TaskJournal:
    """SQLite task envelopes in the Engine book; large results stay as files.

    A previous JSON task journal is imported once at startup. Legacy records
    are renamed only after the database commit; never replay interrupted work.
    """

    def __init__(self, root, *, store=None):
        self.root = Path(root)
        self.lock = threading.RLock()
        self.store = store if store is not None else Store(self.root.parent / "book.sqlite")
        self._migrate_legacy()

    def _directory(self, tenant_id):
        return self.root / _validate_tenant(tenant_id)

    def _path(self, tenant_id, identity, *, result=False):
        _validate_id(identity)
        ending = "result.json" if result else "record.json"
        return self._directory(tenant_id) / f"{identity}.{ending}"

    @staticmethod
    def _atomic(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f"{path.name}.{uuid4().hex}.tmp")
        try:
            with temporary.open("x", encoding="utf-8") as stream:
                json.dump(value, stream, ensure_ascii=False, allow_nan=False)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _checked(record, tenant_id, identity):
        if (not isinstance(record, dict) or record.get("id") != identity
                or record.get("tenant_id") != str(tenant_id)
                or record.get("kind") != "backtest"):
            raise HTTPException(503, "任务归属记录无效，已拒绝读取。")
        return record

    def _migrate_legacy(self):
        if not self.root.is_dir():
            return
        try:
            for folder in self.root.iterdir():
                if not folder.is_dir() or folder.is_symlink() or not _TENANT.fullmatch(folder.name):
                    continue
                for file in folder.glob("*.record.json"):
                    identity = file.name.removesuffix(".record.json")
                    if not _ID.fullmatch(identity):
                        continue
                    if file.is_symlink():
                        raise HTTPException(503, "任务记录路径不可信，已拒绝迁移。")
                    record = self._checked(
                        json.loads(file.read_text(encoding="utf-8")), folder.name, identity,
                    )
                    self.store.save_task_record(record, if_absent=True)
                    # Preserve the original audit bytes, but do not import again on restart.
                    file.rename(file.with_name(file.name + ".migrated"))
        except HTTPException:
            raise
        except (OSError, ValueError, sqlite3.Error) as exc:
            raise HTTPException(503, "旧任务记录迁移失败，原记录已保留。") from exc

    def save(self, record):
        tenant_id = _validate_tenant(record["tenant_id"])
        identity = record["id"]
        _validate_id(identity)
        self._checked(record, tenant_id, identity)
        with self.lock:
            try:
                self.store.save_task_record(record)
            except (sqlite3.Error, ValueError) as exc:
                raise HTTPException(503, "任务状态无法持久化。") from exc

    def result(self, tenant_id, identity, value):
        with self.lock:
            self._atomic(self._path(tenant_id, identity, result=True), value)

    def read_result(self, tenant_id, identity):
        path = self._path(tenant_id, identity, result=True)
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            raise HTTPException(409, "任务尚无可读取结果。") from None
        except (OSError, ValueError):
            raise HTTPException(503, "任务结果文件不可用。") from None

    def get(self, tenant_id, identity):
        tenant_id = _validate_tenant(tenant_id)
        _validate_id(identity)
        try:
            row = self.store.task_record(tenant_id, identity)
        except (sqlite3.Error, ValueError, TypeError) as exc:
            raise HTTPException(503, "任务记录无法读取。") from exc
        return None if row is None else self._checked(row, tenant_id, identity)

    def find_idempotent(self, tenant_id, key):
        tenant_id = _validate_tenant(tenant_id)
        try:
            row = self.store.task_for_key(tenant_id, key)
        except (sqlite3.Error, ValueError, TypeError) as exc:
            raise HTTPException(503, "任务幂等记录无法读取。") from exc
        return None if row is None else self._checked(row, tenant_id, row.get("id"))

    def list(self, tenant_id):
        tenant_id = _validate_tenant(tenant_id)
        try:
            records = self.store.task_records(tenant_id)
        except (sqlite3.Error, ValueError, TypeError) as exc:
            raise HTTPException(503, "任务列表无法读取。") from exc
        return [self._checked(row, tenant_id, row.get("id")) for row in records]

    def recover_interrupted(self):
        """Fail closed on restart; do not resend a possibly completed job."""
        try:
            records = self.store.task_records(unfinished=True)
        except (sqlite3.Error, ValueError, TypeError) as exc:
            raise HTTPException(503, "任务恢复记录无法读取。") from exc
        for record in records:
            self._checked(record, record.get("tenant_id"), record.get("id"))
            record.update(
                status="INTERRUPTED", finished_at=_now(),
                error="Engine 运行中断，执行结果未确认。可检查后显式重试。",
                progress="interrupted", cancellable=False, retryable=True,
            )
            self.save(record)


@dataclass
class _WorkerSlot:
    process: subprocess.Popen | None = None
    cancel_requested: bool = False


def _worker_environment():
    """Do not inherit Engine API keys, grants, or exchange credentials."""
    allowed = {
        "PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "TMPDIR",
        "VIRTUAL_ENV", "PYTHONUNBUFFERED", "PYTHONDONTWRITEBYTECODE",
        "LD_LIBRARY_PATH", "SSL_CERT_FILE",
    }
    return {name: value for name, value in os.environ.items() if name in allowed}


def _stop_process(process):
    """Terminate a worker process group, escalating only if it refuses to exit."""
    if process.poll() is not None:
        return True
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            return False
    return process.poll() is not None


class TaskCoordinator:
    """Bounded, process-isolated backtest pool plus the existing data-job adapter."""

    def __init__(
        self, runtime, market_data, state_root, *, capacity=16,
        max_workers=1, timeout_seconds=3600, command_factory=None,
    ):
        if type(max_workers) is not int or not 1 <= max_workers <= 8:
            raise ValueError("ENGINE_BACKTEST_MAX_WORKERS must be between 1 and 8")
        if (isinstance(timeout_seconds, bool)
                or not isinstance(timeout_seconds, (int, float))
                or not 0.1 <= timeout_seconds <= 86400):
            raise ValueError("ENGINE_BACKTEST_TIMEOUT_SECONDS must be between 0.1 and 86400")
        self.runtime = runtime
        self.market_data = market_data
        self.state_root = Path(state_root)
        # The task journal uses its own SQLite connection to the existing WAL DB.
        self.journal = TaskJournal(self.state_root / "tasks")
        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        self.pending = queue.Queue(maxsize=capacity)
        self.closed = False
        self.default_max_workers = max_workers
        self.default_timeout_seconds = timeout_seconds
        persisted = self.journal.store.backtest_worker_settings()
        self.max_workers = persisted["max_workers"] if persisted else max_workers
        self.timeout_seconds = persisted["timeout_seconds"] if persisted else timeout_seconds
        self.settings_source = "saved" if persisted else "default"
        self.active_slots = 0
        self.command_factory = command_factory or self._worker_command
        self.running: dict[tuple[str, str], _WorkerSlot] = {}
        self.journal.recover_interrupted()
        self.workers = []
        self._grow_workers(self.max_workers)

    def _grow_workers(self, target):
        """Caller holds the coordinator lock after initialization."""
        for index in range(len(self.workers), target):
            worker = threading.Thread(
                target=self._loop, name=f"engine-backtest-supervisor-{index}", daemon=True,
            )
            self.workers.append(worker)
            worker.start()

    def worker_settings(self, tenant_id):
        tenant = self._active(tenant_id)
        if tenant["purpose"] != "development":
            raise HTTPException(409, "只有开发租户可以配置回测 Worker。")
        with self.lock:
            return {
                "max_workers": self.max_workers,
                "timeout_seconds": self.timeout_seconds,
                "default_max_workers": self.default_max_workers,
                "default_timeout_seconds": self.default_timeout_seconds,
                "source": self.settings_source,
                "running": self.active_slots,
                "queued": sum(
                    row["status"] == "PENDING" for row in self.journal.list(tenant_id)
                ),
            }

    def update_worker_settings(self, tenant_id, *, max_workers, timeout_seconds):
        tenant = self._active(tenant_id)
        if tenant["purpose"] != "development":
            raise HTTPException(409, "只有开发租户可以配置回测 Worker。")
        if (type(max_workers) is not int or not 1 <= max_workers <= 8
                or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 86400):
            raise HTTPException(422, "Worker 数量须为 1–8，执行超时须为 1–86400 秒。")
        with self.condition:
            if self.closed:
                raise HTTPException(503, "任务服务正在关闭。")
            try:
                self.journal.store.save_backtest_worker_settings(max_workers, timeout_seconds)
            except sqlite3.Error as exc:
                raise HTTPException(503, "Worker 配置持久化失败。") from exc
            self.max_workers = max_workers
            self.timeout_seconds = timeout_seconds
            self.settings_source = "saved"
            self._grow_workers(max_workers)
            self.condition.notify_all()
        return self.worker_settings(tenant_id)

    def _worker_command(self, tenant_id, identity):
        return [
            sys.executable, "-m", "engine.backtesting.worker",
            "--state-root", str(self.state_root),
            "--project", str(self.runtime.project),
            "--tenant-id", tenant_id, "--task-id", identity,
        ]

    def _active(self, tenant_id):
        tenant_id = _validate_tenant(tenant_id)
        tenant = self.runtime.tenant_row(tenant_id)
        if tenant is None or tenant["status"] != "active":
            raise HTTPException(409, "租户不存在或已归档。")
        return tenant

    def _data(self, tenant_id):
        if str(tenant_id) != self.market_data.tenant_id:
            raise HTTPException(403, "当前 Engine 不管理该租户的行情任务。")
        return self.market_data

    @staticmethod
    def _public(row):
        view = {
            key: value for key, value in row.items()
            if key not in {"fingerprint", "idempotency_key"}
        }
        parameters = view.get("parameters")
        if isinstance(parameters, dict):
            candidate = parameters.get("_candidate")
            if isinstance(candidate, dict) and "archive" in candidate:
                view["parameters"] = {
                    **parameters,
                    "_candidate": {
                        key: value for key, value in candidate.items()
                        if key != "archive"
                    },
                }
        return view

    def _data_view(self, tenant_id, row):
        status = _DATA_STATUS.get(row["status"], "UNKNOWN")
        return {
            "id": row["id"], "tenant_id": str(tenant_id), "kind": "data",
            "action": row["action"], "status": status,
            "created_at": row["created_at"], "started_at": row["created_at"],
            "finished_at": row.get("finished_at"), "progress": row.get("progress", ""),
            "error": row.get("error"),
            "result_ref": (f"/tenants/{tenant_id}/jobs/{row['id']}/result"
                           if row.get("result") is not None else None),
            "cancellable": row.get("cancellable", False),
            "retryable": row["action"] == "backfill" and status in {
                "FAILED", "CANCELLED", "INTERRUPTED",
            },
        }

    def _decode_data(self, payload):
        action = payload.get("action")
        model = {
            "backfill": PairHistoryJob,
            "delete_pair": PairDeleteRequest,
            "trim": PolicyTrimRequest,
        }.get(action, DataJobRequest)
        try:
            return model.model_validate(payload)
        except (ValidationError, TypeError):
            raise HTTPException(422, "行情任务参数无效。") from None

    def submit(self, tenant_id, kind, parameters, *, idempotency_key=None, retry_of=None):
        tenant = self._active(tenant_id)
        if self.closed:
            raise HTTPException(503, "Engine 任务服务正在关闭。")
        if kind == "data":
            model = self._decode_data(parameters)
            normalized = model.model_dump(mode="json", exclude_none=True)
            fingerprint = _fingerprint("data", normalized)
            manager = self._data(tenant_id)
            job = manager.start(
                model, idempotency_key=idempotency_key, request_hash=fingerprint,
            )
            return self._data_view(tenant_id, job)
        if kind != "backtest":
            raise HTTPException(422, "未知任务类型。")
        if tenant["purpose"] != "development":
            raise HTTPException(409, "只有开发租户可以提交异步回测。")
        if not isinstance(parameters.get("account_id"), str):
            raise HTTPException(422, "缺少回测账户。")
        fingerprint = _fingerprint(kind, parameters)
        with self.lock:
            if idempotency_key:
                job = self.journal.find_idempotent(tenant_id, idempotency_key)
                if job is not None:
                    if job["fingerprint"] != fingerprint:
                        raise HTTPException(409, "相同幂等键不能用于不同任务参数。")
                    return self._public(job)
            candidate = parameters.get("_candidate")
            if candidate is None:
                if not self.runtime.store.strategy_loads(
                    tenant_id, parameters["account_id"], include_archive=True,
                ):
                    raise HTTPException(409, "该租户账户没有已下发的策略。")
            else:
                if (not isinstance(candidate, dict)
                        or not isinstance(candidate.get("candidate_id"), str)
                        or not re.fullmatch(r"[0-9a-f]{32}", candidate["candidate_id"])
                        or not isinstance(candidate.get("package_hash"), str)
                        or not re.fullmatch(r"[0-9a-f]{64}", candidate["package_hash"])):
                    raise HTTPException(422, "Candidate identity invalid")
                if candidate.get("mode") == "python":
                    from engine.research.python_candidate import inspect_python_candidate

                    if set(candidate) != {
                        "mode", "candidate_id", "package_hash", "archive",
                        "name", "version", "entry_point", "parameters",
                    }:
                        raise HTTPException(422, "Python candidate shape invalid")
                    try:
                        package = inspect_python_candidate(
                            candidate["archive"], package_hash=candidate["package_hash"],
                            symbol=candidate["parameters"]["symbol"],
                        )
                        if any(
                            package[field] != candidate[field]
                            for field in ("name", "version", "entry_point", "parameters")
                        ):
                            raise ValueError
                    except (TypeError, KeyError, ValueError):
                        raise HTTPException(422, "Python candidate package invalid") from None
                else:
                    from engine.research.candidate_template import validate_candidate

                    if set(candidate) != {"candidate_id", "package_hash", "recipe"}:
                        raise HTTPException(422, "Restricted candidate shape invalid")
                    try:
                        validate_candidate(candidate["recipe"])
                    except ValueError:
                        raise HTTPException(422, "Candidate recipe invalid") from None
            if self.pending.full():
                raise HTTPException(429, "该 Engine 的回测等待队列已满。")
            identity = uuid4().hex
            row = {
                "id": identity, "tenant_id": str(tenant_id),
                "kind": "backtest", "action": "run", "status": "PENDING",
                "parameters": parameters, "fingerprint": fingerprint,
                "idempotency_key": idempotency_key, "retry_of": retry_of,
                "created_at": _now(), "started_at": None, "finished_at": None,
                "progress": "queued", "error": None, "result_ref": None,
                "cancellable": True, "retryable": False,
            }
            self.journal.save(row)
            self.pending.put_nowait((str(tenant_id), identity))
            return self._public(row)

    def _execute(self, tenant_id, identity, slot):
        """Block only a supervisor thread; never execute strategy code in Engine."""
        with self.lock:
            if slot.cancel_requested:
                return "CANCELLED", "回测在启动前被取消。", None
            if self.closed:
                return "INTERRUPTED", "Engine 关闭时回测尚未开始。", None
        path = self.journal._directory(tenant_id) / f"{identity}.worker.log"
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as output:
            process = subprocess.Popen(
                self.command_factory(tenant_id, identity),
                stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                # The code root is fixed; a tenant's data project is only an input path.
                cwd=Path(__file__).resolve().parents[2],
                env=_worker_environment(), start_new_session=True, close_fds=True,
            )
            with self.lock:
                slot.process = process
            started = time.monotonic()
            with self.lock:
                task_timeout = self.timeout_seconds
            reason = None
            while process.poll() is None:
                with self.lock:
                    if self.closed:
                        reason = "shutdown"
                    elif slot.cancel_requested:
                        reason = "cancel"
                if reason is None and time.monotonic() - started >= task_timeout:
                    reason = "timeout"
                if reason is not None:
                    if not _stop_process(process):
                        return "INTERRUPTED", "无法确认 Worker 已终止，禁止自动重试。", None
                    break
                try:
                    process.wait(timeout=0.1)
                except subprocess.TimeoutExpired:
                    continue
        if reason == "shutdown":
            return "INTERRUPTED", "Engine 停止时回测中断。", None
        if reason == "cancel":
            return "CANCELLED", "已终止回测 Worker。", None
        if reason == "timeout":
            return "FAILED", "回测 Worker 超出运行时限。", None
        if process.returncode != 0:
            return "FAILED", "回测 Worker 异常退出，请检查 Engine 私有任务日志。", None
        result = self.journal.read_result(tenant_id, identity)
        if not isinstance(result, dict) or result.get("tenant_id") != tenant_id:
            return "FAILED", "回测 Worker 返回结果归属无效。", None
        return "SUCCEEDED", None, result

    def _loop(self):
        while True:
            try:
                work = self.pending.get(timeout=0.25)
            except queue.Empty:
                if self.closed:
                    return
                continue
            claimed = False
            try:
                tenant_id, identity = work
                key = (tenant_id, identity)
                with self.condition:
                    record = self.journal.get(tenant_id, identity)
                    if record is None or record["status"] != "PENDING":
                        continue
                    # Decreasing the configured cap never terminates active jobs.
                    # It blocks subsequent starts until concurrency falls below cap.
                    while self.active_slots >= self.max_workers and not self.closed:
                        self.condition.wait()
                    record = self.journal.get(tenant_id, identity)
                    if record is None or record["status"] != "PENDING":
                        continue
                    if self.closed:
                        record.update(
                            status="INTERRUPTED", finished_at=_now(),
                            progress="interrupted", cancellable=False, retryable=True,
                            error="Engine 正在关闭，排队任务未执行；可显式重试。",
                        )
                        self.journal.save(record)
                        continue
                    self.active_slots += 1
                    claimed = True
                    self.running[key] = slot = _WorkerSlot()
                    record.update(
                        status="RUNNING", started_at=_now(),
                        progress="running", cancellable=True,
                    )
                    self.journal.save(record)
                try:
                    status, error, result = self._execute(tenant_id, identity, slot)
                except Exception:  # noqa: BLE001 - contain child launch and output failures
                    _LOG.exception("Backtest worker failed (id=%s)", identity)
                    status, error, result = "FAILED", "回测 Worker 启动或读取失败。", None
                with self.lock:
                    self.running.pop(key, None)
                    record = self.journal.get(tenant_id, identity)
                    if record is None:
                        continue
                    if self.closed:
                        status, error = "INTERRUPTED", "Engine 停止时回测中断。"
                    elif status != "INTERRUPTED" and slot.cancel_requested:
                        status, error = "CANCELLED", "已终止回测 Worker。"
                    if status == "SUCCEEDED" and record["parameters"].get("_candidate") is None:
                        try:
                            self.journal.store.add_event(tenant_id, record["parameters"]["account_id"], "backtest", result)
                        except Exception:  # noqa: BLE001 - do not claim a partially committed result
                            _LOG.exception("Cannot publish completed backtest (id=%s)", identity)
                            status, error = "FAILED", "回测结果无法持久化。"
                    if status != "SUCCEEDED":
                        self.journal._path(tenant_id, identity, result=True).unlink(missing_ok=True)
                    record.update(
                        status=status, error=error, finished_at=_now(),
                        progress="completed" if status == "SUCCEEDED" else status.lower(),
                        result_ref=(f"/tenants/{tenant_id}/jobs/{identity}/result"
                                    if status == "SUCCEEDED" else None),
                        cancellable=False, retryable=status in {"FAILED", "CANCELLED", "INTERRUPTED"},
                    )
                    self.journal.save(record)
            except Exception:  # noqa: BLE001 - keep supervisor alive on state failures
                _LOG.exception("Engine backtest task state could not be persisted")
            finally:
                with self.condition:
                    if "claimed" in locals() and claimed:
                        self.active_slots -= 1
                        self.condition.notify_all()
                self.pending.task_done()

    def list(self, tenant_id):
        self._active(tenant_id)
        with self.lock:
            result = [self._public(item) for item in self.journal.list(tenant_id)]
            if str(tenant_id) == self.market_data.tenant_id:
                result.extend(self._data_view(tenant_id, row) for row in self.market_data.jobs())
            return sorted(result, key=lambda row: row["created_at"], reverse=True)[:100]

    def get(self, tenant_id, identity):
        self._active(tenant_id)
        _validate_id(identity)
        with self.lock:
            row = self.journal.get(tenant_id, identity)
            if row is not None:
                return self._public(row)
            if str(tenant_id) != self.market_data.tenant_id:
                raise HTTPException(404, "任务不存在。")
            try:
                return self._data_view(tenant_id, self.market_data.job(identity))
            except HTTPException as error:
                if error.status_code == 404:
                    raise HTTPException(404, "任务不存在。") from None
                raise

    def result(self, tenant_id, identity):
        record = self.get(tenant_id, identity)
        if record["status"] != "SUCCEEDED":
            raise HTTPException(409, "任务尚未成功完成。")
        if record["kind"] == "backtest":
            return self.journal.read_result(tenant_id, identity)
        return self._data(tenant_id).job(identity)["result"]

    def cancel(self, tenant_id, identity):
        row = self.get(tenant_id, identity)
        if row["kind"] == "data":
            return self._data_view(tenant_id, self._data(tenant_id).cancel(identity))
        with self.lock:
            record = self.journal.get(tenant_id, identity)
            if record["status"] == "PENDING":
                record.update(
                    status="CANCELLED", finished_at=_now(), progress="cancelled",
                    cancellable=False, retryable=True, error="任务在执行前被取消。",
                )
                self.journal.save(record)
            elif record["status"] == "RUNNING":
                slot = self.running.get((str(tenant_id), identity))
                if slot is None:
                    raise HTTPException(409, "回测 Worker 所有权未确认，无法保证安全取消。")
                slot.cancel_requested = True
                record.update(status="CANCELLING", progress="stopping", cancellable=False)
                self.journal.save(record)
            return self._public(record)

    def retry(self, tenant_id, identity):
        row = self.get(tenant_id, identity)
        if row["kind"] == "data":
            return self._data_view(tenant_id, self._data(tenant_id).retry(identity))
        if row["status"] not in {"FAILED", "CANCELLED", "INTERRUPTED"}:
            raise HTTPException(409, "只有失败、取消或中断的回测允许显式重试。")
        return self.submit(tenant_id, "backtest", row["parameters"], retry_of=identity)

    def close(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()
        # Supervisors observe shutdown and terminate their process groups.
        for worker in self.workers:
            worker.join(timeout=9)
        # A running process must not be abandoned if its supervisor failed.
        with self.lock:
            leftovers = [slot.process for slot in self.running.values() if slot.process is not None]
        for process in leftovers:
            _stop_process(process)
        self.journal.store.close()
