"""API orchestration; market-data jobs run in the engine pipeline."""

import fcntl
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
from contextlib import ExitStack
from datetime import UTC, datetime
from uuid import uuid4

from fastapi import HTTPException

from core.paths import PROJECT_ROOT
from engine.pipeline.config import load_settings
from engine.pipeline.data_management import checked_request, coverage, deletion_plan
from engine.pipeline.sync_binance_batch import plan_daily_archives

from .repository import OperationFiles, PolicyRepository

PREFIX = "tick_data:"


class DataService:
    def __init__(self, tenant_id, *, project=PROJECT_ROOT, state_root=None):
        self.project = project
        self.tenant_id = str(tenant_id)
        # Engine alone owns task state in its isolated tenant volume.
        # Never adopt a stale Platform job directory when both happen to exist.
        self._directory = (state_root or self.project / ".local/state/engine") / "data_jobs"
        self.repository = OperationFiles(self.directory / "operations")
        self.policy_repository = PolicyRepository(self.directory / "policies.json", self.tenant_id)
        self.lock = threading.RLock()
        self.processes = {}
        self.threads = []
        self.closed = False
        from .subscriptions import Subscriptions
        self.subscriptions = Subscriptions(self)
        from .monitor import ServiceMonitor
        self.monitor = ServiceMonitor(self)

    @property
    def directory(self):
        return self._directory

    @property
    def log_directory(self):
        return self.project / "logs/api/data"

    def storage(self):
        checked_at = datetime.now(UTC).isoformat()
        data = self.project / "data"
        if not data.is_dir():
            return {"available": False, "writable": False, "free_bytes": None,
                    "checked_at": checked_at, "message": "数据目录或挂载盘不可用；不会自动创建替代目录。"}
        settings = load_settings()
        try:
            anchors = []
            for path in (settings.catalog_dir, settings.raw_dir, settings.state_dir, data):
                while not path.exists():
                    if path.is_symlink():
                        raise OSError("配置目录链接失效")
                    path = path.parent
                if not path.is_dir():
                    raise OSError("配置目录不是文件夹")
                anchors.append(path)
            return {"available": True, "writable": all(os.access(p, os.W_OK) for p in anchors),
                    "free_bytes": min(shutil.disk_usage(p).free for p in anchors),
                    "checked_at": checked_at, "message": None}
        except OSError:
            return {"available": False, "writable": False, "free_bytes": None,
                    "checked_at": checked_at, "message": "数据存储无法访问，请检查挂载与权限。"}

    def require_storage(self, *, write=False):
        state = self.storage()
        if not state["available"] or write and not state["writable"]:
            raise HTTPException(503, state["message"] or "数据存储不可写")

    def request(self, payload):
        settings = load_settings()
        request = checked_request(payload.symbol, payload.start, payload.end,
                                  payload.intervals, settings, market=payload.product)
        if request.market == "futures_um":
            plan_daily_archives(request)
        return request, settings

    def plan(self, payload):
        self.require_storage()
        request, settings = self.request(payload)
        if request.market == "options":
            from engine.pipeline.options_data import check_download
            check_download(request)
        result = coverage(request, settings)
        return {**result, "archive_days": len(result["days"]),
                "missing_days": sum(not item["catalog_present"] for item in result["days"]),
                "request": payload.model_dump(mode="json"),
                "message": ("期权 Tick 获取官方近期接口返回的最多 500 笔，实际时间以记录为准；不代表完整历史。"
                            if request.market == "options" and not request.bar_intervals else
                            "期权历史 K 线直接来自 Binance；不从 K 线反推 Tick，不证明历史逐笔完整。"
                            if request.market == "options" else
                            "按完整 UTC 日归档处理；只补缺失，不强制重建。下载体积与耗时未知。")}

    def delete_plan(self, payload):
        self.require_storage(write=True)
        request, settings = self.request(payload)
        with self.lock:
            lease = self._lease(payload.symbol, payload.product)
            try:
                return deletion_plan(request, settings, delete_archives=payload.delete_archives)
            finally:
                lease.close()

    def _lease(self, symbol, product="futures_um", *, access=False, shared=False):
        from .contracts import normalize_scope
        symbol = normalize_scope(symbol, product)
        kind = "access" if access else "job"
        # Engine and API operate on the *same* mounted data volume, not on
        # distinct /app/.local/run and host-side runtime directories.
        data = self.project / "data"
        if not data.is_dir():
            raise HTTPException(503, "数据存储目录不可用。")
        path = data / ".locks" / product / f"{symbol}.{kind}.lock"
        path.parent.mkdir(parents=True, exist_ok=True)
        target = path.open("a")
        try:
            fcntl.flock(target, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
        except BlockingIOError:
            target.close()
            message = ("该交易对正在删除，请稍后操作。" if shared else
                       "该交易对仍有采集或数据读写，请先停止后再删除。" if access else
                       "该交易对已有数据任务运行；其他交易对仍可操作。")
            raise HTTPException(409, message) from None
        return target

    def pair_delete_plan(self, payload):
        from engine.pipeline.pair_deletion import pair_deletion_plan
        self.require_storage(write=True)
        with self.lock, self.subscriptions.lock:
            self.subscriptions.pause_for_deletion(payload.symbol, payload.product)
            with self._lease(payload.symbol, payload.product), self._lease(payload.symbol, payload.product, access=True):
                return pair_deletion_plan(payload.symbol, payload.product, load_settings(), project=self.project)

    def configuration_plan(self, policy):
        """Resolve a desired start; later starts require an exclusive deletion plan."""
        from engine.pipeline.pair_deletion import history_trim_plan
        with self.lock, self.subscriptions.lock:
            self.subscriptions._claim()
            old = next((item for item in self.subscriptions._read(refresh=True)
                        if (item.product, item.symbol) == (policy.product, policy.symbol)), None)
            policy = self.subscriptions.resolve(policy, old)
            later = False
            if policy.product == "futures_um" and policy.history_start is not None:
                if policy.history_start >= datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0):
                    raise ValueError("数据开始日期须早于今天 UTC 零点")
                if old is not None and old.history_mode == "start" and old.history_start is not None:
                    later = policy.history_start > old.history_start
                else:
                    # Enabling history must not stop a collector when the desired
                    # start is earlier than all retained data, including raw archives.
                    from dataclasses import replace

                    from core.market.instruments import INTERVALS, bar_window
                    from engine.feed.pair_store import journal_inventory
                    from engine.pipeline.data_management import dataset_inventory
                    from engine.pipeline.pair_deletion import pair_deletion_plan
                    self.require_storage(write=True)
                    settings = load_settings()
                    items = dataset_inventory(replace(settings, intervals=tuple(INTERVALS)), all_levels=True) if settings.catalog_dir.is_dir() else []
                    items += [item for path in self.subscriptions.journal_paths(old or policy)
                              for item in journal_inventory(path, policy.symbol)]
                    for item in items:
                        if item["symbol"] != policy.symbol or item.get("product", "futures_um") != policy.product:
                            continue
                        for first, _ in item["event_ranges_ns"]:
                            stamp = int(first) // 1000000
                            opening = stamp if item["level"] == "tick" else bar_window(item["level"], stamp)[0]
                            later |= opening < int(policy.history_start.timestamp() * 1000)
                    for item in pair_deletion_plan(policy.symbol, policy.product, settings, project=self.project)["files"]:
                        match = re.search(r"-(\d{4}-\d{2}(?:-\d{2})?)\.zip", item["path"])
                        if item["group"] == "archives" and match:
                            value = match[1]
                            begin = datetime.fromisoformat(value + ("-01" if len(value) == 7 else "")).replace(tzinfo=UTC)
                            later |= begin < policy.history_start
                        elif item["group"] == "archives":
                            later = True  # Unknown archive dates require an explicit retention review.
            if not later:
                return {"policy": policy.model_dump(mode="json"), "requires_confirmation": False, "deletion": None}
            self.require_storage(write=True)
            self.subscriptions.pause_for_deletion(policy.symbol, policy.product)
            with self._lease(policy.symbol, policy.product), self._lease(policy.symbol, policy.product, access=True):
                plan = history_trim_plan(policy.symbol, policy.product, policy.history_start, load_settings(), project=self.project)
                paused = next((item.model_dump(mode="json") for item in self.subscriptions._read()
                               if (item.product, item.symbol) == (policy.product, policy.symbol)), None)
                file_snapshot = plan["snapshot"]
                plan["snapshot"] = hashlib.sha256(json.dumps({"files": file_snapshot,
                    "policy": policy.model_dump(mode="json"), "previous": paused}, sort_keys=True).encode()).hexdigest()
            return {"policy": policy.model_dump(mode="json"), "requires_confirmation": bool(plan["file_count"] or plan["retained_files"]),
                    "deletion": plan, "file_snapshot": file_snapshot}

    def save_configuration(self, policy, *, deletion_snapshot=None, confirmation=None):
        from .contracts import DataPolicy, PolicyTrimRequest
        with self.lock, self.subscriptions.lock:
            proposal = self.configuration_plan(policy)
            policy = DataPolicy.model_validate(proposal["policy"])
            if proposal["requires_confirmation"]:
                if deletion_snapshot != proposal["deletion"]["snapshot"] or confirmation != f"DELETE {policy.symbol}":
                    raise HTTPException(409, "开始日期调晚须先预览并确认删除；数据变化后须重新预览")
                return {"policy": policy.model_dump(mode="json"), "job": self.start(PolicyTrimRequest(
                    symbol=policy.symbol, product=policy.product, policy=policy,
                    deletion_snapshot=proposal["file_snapshot"], confirmation=confirmation))}
            if policy.product == "options" and policy.symbol == "ALL":
                # Scope parameters have no market files and must not acquire a contract lease.
                return self.subscriptions._save(policy)
            if (not (self.project / "data").is_dir()
                    and not policy.realtime_enabled and not policy.history_enabled
                    and proposal["deletion"] is None):
                # Disabling/configuring a stopped feed only updates the business
                # policy. It must be possible while the market-data disk is offline.
                return self.subscriptions._save(policy, allow_later_start=True)
            with ExitStack() as lease:
                if proposal["deletion"] is not None:
                    lease.enter_context(self._lease(policy.symbol, policy.product))
                lease.enter_context(self._lease(policy.symbol, policy.product, access=True, shared=True))
                return self.subscriptions._save(policy, allow_later_start=True)

    def _records(self):
        return [job for job in self.repository.list_jobs()
                if job.get("action", "").startswith(PREFIX)]

    def recover(self):
        records = self._records()
        for record in records:
            if record["status"] != "running":
                continue
            parameters = record.get("parameters", {})
            if not isinstance(parameters, dict) or not isinstance(parameters.get("symbol"), str):
                continue
            try:
                lease = self._lease(parameters.get("symbol", ""), parameters.get("product", "futures_um"))
            except HTTPException as exc:
                if exc.status_code == 409:
                    continue  # The owning worker still holds this pair's inherited lease.
                raise
            except ValueError:
                continue  # Unknown scope stays unconfirmed; never infer that it stopped.
            with lease:
                # Re-read after claiming the pair: a worker may have finished since
                # the first snapshot. Never overwrite its committed terminal state.
                job = next((item for item in self._records() if item["id"] == record["id"]), None)
                if job is not None and job["status"] == "running":
                    job.update(status="interrupted", finished_at=datetime.now(UTC).isoformat(),
                               result=self._result(job["id"]),
                               error=("上次删除任务中断，可能已删除部分文件；请查看日志并重新预览。"
                                      if job["action"] in {PREFIX + "delete", PREFIX + "delete_pair", PREFIX + "trim"} else
                                      "上次任务未正常结束；已保留数据和结果，请校验后重试。"))
                    self.repository.save_job(job)
                if job is not None:
                    record.update(job)
        return records

    def jobs(self, *, reconcile=True):
        # Reconcile an orphan only after its inherited worker lease has been released.
        records = self.recover() if reconcile else self._records()
        result = []
        for job in sorted(records, key=lambda j: j["created_at"], reverse=True)[:100]:
            result.append(self._public(job))
        return result

    def _public(self, job):
        value = dict(job)
        value.pop("idempotency_key", None)
        value.pop("request_hash", None)
        value["action"] = value["action"].removeprefix(PREFIX)
        owned = job["id"] in self.processes
        value["cancellable"] = job["status"] == "running" and owned
        if job["status"] == "running" and not owned:
            # A worker may outlive an interrupted API. Inspect its durable result,
            # but do not assume success or kill a PID from persisted metadata.
            value["status"] = "unknown"
            value["error"] = "任务所属 API 已变化，执行状态未确认；不会自动重试。"
        return value

    def job(self, identity):
        if not re.fullmatch(r"[0-9a-f]{32}", identity):
            raise HTTPException(404, "数据任务不存在")
        with self.lock:
            matches = [job for job in self._records() if job["id"] == identity]
            if not matches:
                raise HTTPException(404, "数据任务不存在")
            value = self._public(matches[0])
        return self._with_log(value)

    def task_snapshot(self, selected=""):
        # Publishing is read-only; startup/start/status perform durable recovery.
        tasks = self.jobs(reconcile=False)
        detail = None
        for index, job in enumerate(tasks):
            if job["status"] in {"running", "unknown"} or job["id"] == selected:
                value = self._with_log(job)
                tasks[index] = {**job, "progress": value["progress"]}
                if job["id"] == selected:
                    detail = value
        snapshot = {"type": "tasks", "tasks": tasks, "detail": detail}
        snapshot["subscriptions"] = self.subscriptions.items()
        return snapshot

    def _with_log(self, value):
        identity = value["id"]
        value = dict(value)
        try:
            with (self.log_directory / f"{identity}.log").open("rb") as log:
                size = log.seek(0, os.SEEK_END)
                log.seek(max(0, size - 32_768))
                value.update(log=log.read(32_768).decode("utf-8", errors="replace"),
                             log_truncated=size > 32_768)
        except FileNotFoundError:
            value.update(log="", log_truncated=False)
        lines = value["log"].splitlines()
        value["progress"] = next((line.strip()[-300:] for line in reversed(lines) if line.strip()), "")
        # Preserve a completed validation result even when the API owner was lost.
        if value["status"] == "unknown":
            value["result"] = self._result(identity)
        return value

    def _result(self, identity):
        try:
            return json.loads((self.directory / f"{identity}.json").read_text())
        except (OSError, ValueError):
            return None

    def start(self, payload, *, resolved_history=None, idempotency_key=None, request_hash=None):
        if idempotency_key is not None:
            if not request_hash:
                raise ValueError("幂等提交必须提供请求摘要。")
            # Replay must return the previously accepted durable handle even
            # if storage is subsequently unavailable or request data changed.
            with self.lock:
                for prior in self._records():
                    if prior.get("idempotency_key") != idempotency_key:
                        continue
                    if prior.get("request_hash") != request_hash:
                        raise HTTPException(409, "同一幂等键已用于其他行情任务。")
                    return self._public(prior)
        self.require_storage(write=True)
        if payload.action not in {"backfill", "delete_pair", "trim"}:
            selection, _settings = self.request(payload)
        if payload.product == "options":
            if payload.action not in {"prepare", "validate", "delete", "delete_pair"}:
                raise ValueError("期权仅支持获取官方数据、本地来源校验和确认删除")
            if payload.action == "prepare":
                from engine.pipeline.options_data import check_download
                check_download(selection)
        if payload.action not in {"delete_pair", "delete", "trim"} and any(
                policy.symbol == payload.symbol and policy.product == payload.product and policy.history_enabled
                for policy in self.subscriptions._read()):
            raise HTTPException(409, "该交易对由历史补齐服务管理；请先暂停历史补齐，再执行兼容数据操作。")
        self.recover()
        with self.lock:
            if self.closed:
                raise HTTPException(503, "服务正在停止")
            if idempotency_key is not None:
                for prior in self._records():
                    if prior.get("idempotency_key") != idempotency_key:
                        continue
                    if prior.get("request_hash") != request_hash:
                        raise HTTPException(409, "同一幂等键已用于其他行情任务。")
                    return self._public(prior)
            lease = ExitStack()
            process = None
            marked_deleting = False
            try:
                job_lease = lease.enter_context(self._lease(payload.symbol, payload.product))
                if payload.action == "delete_pair":
                    from engine.pipeline.pair_deletion import pair_deletion_plan
                    with self.subscriptions.lock:
                        self.subscriptions.pause_for_deletion(payload.symbol, payload.product)
                        access_lease = lease.enter_context(self._lease(payload.symbol, payload.product, access=True))
                        plan = pair_deletion_plan(payload.symbol, payload.product, load_settings(), project=self.project)
                        if plan["snapshot"] != payload.deletion_snapshot:
                            raise HTTPException(409, "数据已变化，请重新预览删除交易对")
                        self.subscriptions.deleting.add((payload.product, payload.symbol))
                        marked_deleting = True
                if payload.action == "trim":
                    from engine.pipeline.pair_deletion import history_trim_plan
                    with self.subscriptions.lock:
                        self.subscriptions.pause_for_deletion(payload.symbol, payload.product)
                        access_lease = lease.enter_context(self._lease(payload.symbol, payload.product, access=True))
                        plan = history_trim_plan(payload.symbol, payload.product, payload.policy.history_start,
                                                 load_settings(), project=self.project)
                        if plan["snapshot"] != payload.deletion_snapshot:
                            raise HTTPException(409, "数据已变化，请重新预览配置删除范围")
                        self.subscriptions.deleting.add((payload.product, payload.symbol))
                        marked_deleting = True
                if payload.action == "delete":
                    if any(item["policy"]["symbol"] == payload.symbol
                           and item["policy"]["product"] == payload.product
                           and (item["policy"]["realtime_enabled"] or item["policy"]["history_enabled"])
                           for item in self.subscriptions.items()):
                        raise HTTPException(409, "请先暂停该交易对的实时接入和历史补齐，再删除历史数据")
                    access_lease = lease.enter_context(self._lease(payload.symbol, payload.product, access=True))
                    request, settings = self.request(payload)
                    plan = deletion_plan(request, settings, delete_archives=payload.delete_archives)
                    if plan["snapshot"] != payload.deletion_snapshot or not plan["file_count"]:
                        raise HTTPException(409, "数据已变化或没有可删除文件，请重新预览删除范围。")
                if payload.action not in {"delete", "delete_pair", "trim"}:
                    access_lease = lease.enter_context(self._lease(payload.symbol, payload.product, access=True, shared=True))
                identity = uuid4().hex
                parameters = payload.model_dump(mode="json", exclude={"action"}, exclude_none=True)
                if payload.action == "backfill":
                    if resolved_history is None:
                        from engine.pipeline.catalog import ParquetDataCatalog
                        from engine.pipeline.pair_data import history_bounds
                        settings = load_settings()
                        catalog = ParquetDataCatalog(settings.catalog_dir) if settings.catalog_dir.is_dir() else None
                        from engine.feed.pair_store import journal_inventory
                        starts = [int(item["event_ranges_ns"][0][0])
                                  for path in self.subscriptions.journal_paths(payload)
                                  for item in journal_inventory(path, payload.symbol)
                                  if item["level"] == payload.base_level and item["event_ranges_ns"]]
                        first, last = history_bounds(parameters, catalog, datetime.now(UTC), retained_start_ns=min(starts, default=None))
                        resolved_history = {"resolved_start": first.isoformat() if first else None, "resolved_end": last.isoformat()}
                    parameters.update(resolved_history)
                job = {"id": identity, "action": PREFIX + payload.action, "status": "running",
                       "created_at": datetime.now(UTC).isoformat(), "finished_at": None,
                       "parameters": parameters, "error": None, "result": None,
                       "idempotency_key": idempotency_key, "request_hash": request_hash}
                self.repository.save_job(job)
                self.directory.mkdir(parents=True, exist_ok=True)
                self.log_directory.mkdir(parents=True, exist_ok=True)
                self.threads = [thread for thread in self.threads if thread.is_alive()]
                command = ([sys.executable, "-m", "engine.pipeline.pair_data"] if payload.action == "backfill" else
                           [sys.executable, "-m", "engine.pipeline.data_management", payload.action]) + [
                    "--request", json.dumps(parameters), "--job-id", identity]
                engine = getattr(self, "engine", None)
                log_path = self.log_directory / f"{identity}.log"
                if engine is not None and engine.remote:
                    process = engine.spawn_worker(
                        command, log_path, tenant_id=self.subscriptions.bound_tenant_id(),
                        access=(payload.symbol, payload.product, True), log_mode="xb",
                    )
                else:
                    from engine.supervisor import popen as start_worker
                    with log_path.open("xb", buffering=0) as output:
                        process = start_worker(
                            command, cwd=self.project, stdin=subprocess.DEVNULL, stdout=output,
                            stderr=subprocess.STDOUT, start_new_session=True,
                            pass_fds=(job_lease.fileno(), access_lease.fileno()),
                            env={**os.environ, "PYTHONUNBUFFERED": "1",
                                 "ENGINE_DATA_JOBS": str(self.directory)},
                        )
                    if engine is not None and payload.action == "backfill":
                        engine.note_worker(command, tenant_id=self.subscriptions.bound_tenant_id())
                self.processes[identity] = (process, threading.Event())
                thread = threading.Thread(target=self._wait, args=(job, process, lease, access_lease), daemon=True)
                self.threads.append(thread)
                thread.start()
                return self._public(job)
            except Exception:
                if marked_deleting:
                    self.subscriptions.deleting.discard((payload.product, payload.symbol))
                if process is not None:
                    self._stop(process)
                lease.close()
                if "job" in locals():
                    job.update(status="failed", finished_at=datetime.now(UTC).isoformat(),
                               error="数据任务未能启动")
                    self.repository.save_job(job)
                raise

    def retry(self, identity):
        previous = self.job(identity)
        if previous["action"] != "backfill" or previous["status"] not in {"failed", "cancelled", "interrupted"}:
            raise HTTPException(409, "仅可重试已失败、取消或中断的历史追加任务")
        from .contracts import PairHistoryJob
        fields = PairHistoryJob.model_fields
        parameters = previous["parameters"]
        payload = PairHistoryJob(**{key: value for key, value in parameters.items() if key in fields})
        resolved = {key: parameters[key] for key in ("resolved_start", "resolved_end") if key in parameters}
        return self.start(payload, resolved_history=resolved or None)

    @staticmethod
    def _stop(process):
        try:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                pass
            # Kill surviving conversion descendants as well as the process leader.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)
        except ProcessLookupError:
            pass

    def _wait(self, job, process, lease, access_lease=None):
        try:
            try:
                process.wait(timeout=6 * 60 * 60)
                error = None
            except subprocess.TimeoutExpired:
                self._stop(process)
                error = ("删除任务超过六小时限制，已终止；可能已删除部分文件，请重新预览。"
                         if job["action"] in {PREFIX + "delete", PREFIX + "delete_pair", PREFIX + "trim"} else
                         "任务超过六小时限制，已终止；已完成数据保留。")
            result = self._result(job["id"])
            with self.lock:
                cancelled = self.processes[job["id"]][1].is_set()
                status = ("cancelled" if cancelled else "succeeded"
                          if process.returncode == 0 and result is not None and not error else "failed")
                if status == "succeeded" and job["action"] == PREFIX + "delete_pair":
                    try:
                        self.subscriptions.remove(job["parameters"]["symbol"], job["parameters"]["product"])
                    except (OSError, HTTPException):
                        status = "failed"
                        error = "行情文件已删除，但配置移除失败；请重新预览并重试删除交易对。"
                if status == "succeeded" and job["action"] == PREFIX + "trim":
                    from .contracts import DataPolicy
                    from .repository import StorageUnavailable
                    try:
                        with self.subscriptions.lock:
                            self.subscriptions.deleting.discard((job["parameters"]["product"], job["parameters"]["symbol"]))
                            access_lease.close()
                            self.subscriptions._save(DataPolicy.model_validate(job["parameters"]["policy"]), allow_later_start=True)
                    except (OSError, ValueError, HTTPException, StorageUnavailable):
                        status, error = "failed", "历史已裁剪，但新配置保存失败；服务保持暂停，请重新检查并保存配置。"
                job.update(status=status, finished_at=datetime.now(UTC).isoformat(), result=result,
                           error=error or (("删除已取消，可能已删除部分文件；请查看日志并重新预览。"
                                           if job["action"] in {PREFIX + "delete", PREFIX + "delete_pair", PREFIX + "trim"} else
                                           "已取消；部分数据可能已完成，下次先校验。") if cancelled else
                                           ("删除未完成，可能已删除部分文件；请查看日志并重新预览。"
                                            if job["action"] in {PREFIX + "delete", PREFIX + "delete_pair", PREFIX + "trim"} else
                                            "任务未完成，请查看日志与校验结果。") if status == "failed"
                                           else None))
                self.repository.save_job(job)
        finally:
            # Keep cross-process exclusion until the child has stopped and state is saved.
            with self.lock:
                self.processes.pop(job["id"], None)
                if job["action"] in {PREFIX + "delete_pair", PREFIX + "trim"}:
                    self.subscriptions.deleting.discard((job["parameters"]["product"], job["parameters"]["symbol"]))
            lease.close()

    def cancel(self, identity):
        self.job(identity)
        with self.lock:
            entry = self.processes.get(identity)
            if entry is None:
                if self.job(identity)["status"] == "unknown":
                    raise HTTPException(409, "执行状态未确认，不能从当前进程安全取消。")
                return self.job(identity)
            process, cancelled = entry
            cancelled.set()
            self._stop(process)
        return self.job(identity)

    def close(self):
        self.subscriptions.close()
        with self.lock:
            self.closed = True
            entries = list(self.processes.values())
            for _, cancelled in entries:
                cancelled.set()
        for process, _ in entries:
            self._stop(process)
        for thread in self.threads:
            thread.join(timeout=10)
