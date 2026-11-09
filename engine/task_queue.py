"""Tenant-local, durable asynchronous task coordinator.

Long-running backtests are queued inside the resident Engine process instead
of holding a request connection open. Existing market-data jobs retain their
subprocess owner and are exposed through the same task-view contract.

Important: A running in-process backtest cannot be forcibly interrupted
safely. Only queued backtests can be cancelled; active cancellation returns
409 rather than reporting a false CANCELLED state. A process restart marks
unknown unfinished attempts INTERRUPTED and requires an explicit retry.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import re
import threading
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from fastapi import HTTPException
from pydantic import ValidationError

from core.engine_contracts import EngineConflict
from engine.data_manager.contracts import (
    DataJobRequest,
    PairDeleteRequest,
    PairHistoryJob,
    PolicyTrimRequest,
)

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
    """Atomic JSON task envelopes with results kept in separate files."""

    def __init__(self, root):
        self.root = Path(root)
        self.lock = threading.RLock()

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

    def save(self, record):
        with self.lock:
            self._atomic(self._path(record["tenant_id"], record["id"]), record)

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
        path = self._path(tenant_id, identity)
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, ValueError):
            raise HTTPException(503, "任务记录损坏，已保留原文件。") from None
        if not isinstance(row, dict) or row.get("id") != identity or row.get("tenant_id") != str(tenant_id):
            raise HTTPException(503, "任务归属记录无效，已拒绝读取。")
        return row

    def list(self, tenant_id):
        folder = self._directory(tenant_id)
        if not folder.is_dir():
            return []
        items = []
        try:
            for file in folder.glob("*.record.json"):
                identity = file.name.removesuffix(".record.json")
                if not _ID.fullmatch(identity):
                    continue
                items.append(self.get(tenant_id, identity))
        except OSError:
            raise HTTPException(503, "任务目录不可用。") from None
        return [item for item in items if item is not None]

    def recover_interrupted(self):
        """Never replay uncertain work automatically after Engine restart."""
        if not self.root.is_dir():
            return
        for folder in self.root.iterdir():
            if not folder.is_dir() or not _TENANT.fullmatch(folder.name):
                continue
            for record in self.list(folder.name):
                if record["status"] in {"PENDING", "RUNNING"}:
                    record.update(
                        status="INTERRUPTED", finished_at=_now(),
                        error="Engine 运行中断，执行结果未确认。可检查后显式重试。",
                        progress="interrupted",
                    )
                    self.save(record)


class TaskCoordinator:
    """One bounded in-process backtest worker per Engine, plus data-job adapter."""

    def __init__(self, runtime, market_data, state_root, *, capacity=16):
        self.runtime = runtime
        self.market_data = market_data
        self.journal = TaskJournal(Path(state_root) / "tasks")
        self.lock = threading.RLock()
        self.pending = queue.Queue(maxsize=capacity)
        self.closed = False
        self.worker = threading.Thread(target=self._loop, name="engine-backtest-jobs", daemon=True)
        self.journal.recover_interrupted()
        self.worker.start()

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
        return {key: value for key, value in row.items()
                if key not in {"fingerprint", "idempotency_key"}}

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
                for job in self.journal.list(tenant_id):
                    if job.get("idempotency_key") != idempotency_key:
                        continue
                    if job["fingerprint"] != fingerprint:
                        raise HTTPException(409, "相同幂等键不能用于不同任务参数。")
                    return self._public(job)
            # A replay returns its original handle even if its strategy version
            # was archived after the first durable submission.
            if not self.runtime.store.strategy_loads(
                tenant_id, parameters["account_id"], include_archive=True,
            ):
                raise HTTPException(409, "该租户账户没有已下发的策略。")
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

    def _loop(self):
        while True:
            work = self.pending.get()
            try:
                if work is None:
                    return
                tenant_id, identity = work
                with self.lock:
                    record = self.journal.get(tenant_id, identity)
                    if record is None or record["status"] != "PENDING":
                        continue
                    if self.closed:
                        # Never start queued work while Docker/Engine is exiting.
                        record.update(
                            status="INTERRUPTED", finished_at=_now(),
                            progress="interrupted", cancellable=False, retryable=True,
                            error="Engine 正在关闭，排队任务未执行；可显式重试。",
                        )
                        self.journal.save(record)
                        continue
                    record.update(
                        status="RUNNING", started_at=_now(),
                        progress="running", cancellable=False,
                    )
                    self.journal.save(record)
                try:
                    parameters = record["parameters"]
                    result = self.runtime.run_backtest(
                        tenant_id, parameters["account_id"],
                        parameters["start"], parameters["end"], parameters["interval"],
                        parameters["starting_balance"], parameters["quote_currency"],
                    )
                    self.journal.result(tenant_id, identity, result)
                    record.update(
                        status="SUCCEEDED", progress="completed",
                        result_ref=f"/tenants/{tenant_id}/jobs/{identity}/result",
                    )
                except EngineConflict as error:
                    record.update(status="FAILED", progress="failed", error=error.message)
                except Exception:  # noqa: BLE001 - untrusted strategy errors must be contained
                    _LOG.exception("Engine backtest task failed (id=%s)", identity)
                    record.update(status="FAILED", progress="failed",
                                  error="回测执行失败，请检查 Engine 日志。")
                record.update(finished_at=_now(), cancellable=False,
                              retryable=record["status"] == "FAILED")
                with self.lock:
                    self.journal.save(record)
            except Exception:  # noqa: BLE001 - protect resident worker from task failures
                _LOG.exception("Engine task state could not be persisted")
            finally:
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
            if record["status"] == "RUNNING":
                raise HTTPException(
                    409, "运行中的回测不可安全强制取消；请等待结束。排队中的任务可取消。",
                )
            if record["status"] == "PENDING":
                record.update(
                    status="CANCELLED", finished_at=_now(), progress="cancelled",
                    cancellable=False, retryable=True, error="任务在执行前被取消。",
                )
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
        with self.lock:
            self.closed = True
            # Running work is not falsely marked cancelled; restart recovery
            # will mark any uncertain state INTERRUPTED after process death.
            try:
                self.pending.put_nowait(None)
            except queue.Full:
                pass
