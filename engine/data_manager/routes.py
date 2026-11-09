"""Tenant-owned market-data control plane: jobs, feeds, configuration, and export."""
from __future__ import annotations

import json
import re
import time
from contextlib import ExitStack

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from engine.data_manager.contracts import (
    DataDeletePlanRequest,
    DataJobRequest,
    DataPolicy,
    PairDeletePlanRequest,
    PairDeleteRequest,
    PairHistoryJob,
    PolicyTrimRequest,
)
from engine.data_manager.repository import StorageUnavailable

router = APIRouter(prefix="/tenants/{tenant_id}/data/control", tags=["engine-data-control"])
_HEX = re.compile(r"^[0-9a-f]{32}$")


class PolicySet(BaseModel):
    model_config = ConfigDict(extra="forbid")
    policies: list[DataPolicy] = Field(max_length=2500)


class PolicyBody(BaseModel):
    policy: DataPolicy


class IdBody(BaseModel):
    id: str = Field(pattern=r"^[0-9a-f]{32}$")


class TaskSnapshot(BaseModel):
    selected: str = Field(default="", max_length=32)


class SubscriptionOptions(BaseModel):
    group_options: bool = False


class JobPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payload: dict


def get_manager(request: Request, tenant_id: str):
    runtime = request.app.state.runtime
    tenant = runtime.tenant_row(tenant_id)
    if tenant is None or tenant["status"] != "active":
        raise HTTPException(409, "租户不存在或已归档。")
    manager = request.app.state.market_data
    if manager.tenant_id != str(tenant_id):
        raise HTTPException(403, "行情管理器租户不匹配。")
    return manager


def call(operation):
    try:
        return operation()
    except StorageUnavailable:
        raise HTTPException(503, "市场数据状态存储不可用。") from None
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None


@router.post("/storage")
def storage(tenant_id: str, request: Request):
    return call(lambda: get_manager(request, tenant_id).storage())


@router.post("/sync")
def sync(tenant_id: str, body: PolicySet, request: Request):
    manager = get_manager(request, tenant_id)
    policies = body.policies
    if len({(p.product, p.symbol) for p in policies}) != len(policies):
        raise HTTPException(422, "行情配置包含重复交易对。")

    def update():
        with manager.subscriptions.lock:
            current = manager.subscriptions._read(refresh=True)
            before = {(p.product, p.symbol): p for p in current}
            after = {(p.product, p.symbol): p for p in policies}
            changed = sorted(key for key in before.keys() | after.keys()
                             if before.get(key) != after.get(key))
            # Shared access permits active collectors and previews, but not
            # an exclusive pair deletion. Hold leases through persistence and
            # process reconciliation so a deletion cannot race a policy edit.
            with ExitStack() as access:
                for product, symbol in changed:
                    if product == "options" and "-" not in symbol:
                        continue  # Scope entries do not own an individual data file.
                    access.enter_context(
                        manager._lease(symbol, product, access=True, shared=True),
                    )
                if current != policies:
                    manager.policy_repository.save(policies, tenant_id=tenant_id)
                manager.subscriptions.restore()
        return {"synced": len(policies), "last_error": manager.subscriptions.last_error}

    return call(update)


@router.post("/subscriptions")
def subscriptions(tenant_id: str, body: SubscriptionOptions, request: Request):
    manager = get_manager(request, tenant_id)
    return call(lambda: {"items": manager.subscriptions.items(group_options=body.group_options)})


@router.post("/services")
def services(tenant_id: str, request: Request):
    return call(lambda: get_manager(request, tenant_id).monitor.read())


@router.post("/snapshot")
def snapshot(tenant_id: str, body: TaskSnapshot, request: Request):
    manager = get_manager(request, tenant_id)
    if body.selected and not _HEX.fullmatch(body.selected):
        raise HTTPException(422, "数据任务标识无效。")
    return call(lambda: manager.task_snapshot(body.selected))


@router.post("/jobs")
def jobs(tenant_id: str, request: Request):
    return call(lambda: get_manager(request, tenant_id).jobs())


@router.post("/job")
def job(tenant_id: str, body: IdBody, request: Request):
    return call(lambda: get_manager(request, tenant_id).job(body.id))


@router.post("/job/start")
def start_job(tenant_id: str, body: JobPayload, request: Request):
    manager = get_manager(request, tenant_id)
    action = body.payload.get("action")
    model = {
        "backfill": PairHistoryJob,
        "delete_pair": PairDeleteRequest,
        "trim": PolicyTrimRequest,
    }.get(action, DataJobRequest)
    try:
        payload = model.model_validate(body.payload)
    except ValidationError:
        raise HTTPException(422, "行情任务参数无效。") from None
    return call(lambda: manager.start(payload))


@router.post("/job/retry")
def retry_job(tenant_id: str, body: IdBody, request: Request):
    return call(lambda: get_manager(request, tenant_id).retry(body.id))


@router.post("/job/cancel")
def cancel_job(tenant_id: str, body: IdBody, request: Request):
    return call(lambda: get_manager(request, tenant_id).cancel(body.id))


@router.get("/exports/{identity}")
def export(tenant_id: str, identity: str, request: Request):
    manager = get_manager(request, tenant_id)
    if not _HEX.fullmatch(identity):
        raise HTTPException(404, "数据任务不存在。")
    value = manager.job(identity)
    if value["status"] != "succeeded" or value["action"] != "export":
        raise HTTPException(409, "该任务没有已完成的导出文件。")
    manager.require_storage()
    root = manager.project / "data/exports/ticks"
    path = root / f"{identity}.csv"
    if (root.is_symlink() or root.parent.is_symlink() or path.is_symlink()
            or not path.is_file() or path.resolve().parent != root.resolve()):
        raise HTTPException(404, "导出文件不可用。")
    return FileResponse(path, media_type="text/csv", filename=f"ticks-{identity}.csv")


@router.post("/delete-plan")
def delete_plan(tenant_id: str, body: DataDeletePlanRequest, request: Request):
    manager = get_manager(request, tenant_id)
    return call(lambda: manager.delete_plan(body))


@router.post("/pair-delete-plan")
def pair_delete_plan(tenant_id: str, body: PairDeletePlanRequest, request: Request):
    manager = get_manager(request, tenant_id)
    return call(lambda: manager.pair_delete_plan(body))


@router.post("/configuration-plan")
def configuration_plan(tenant_id: str, body: PolicyBody, request: Request):
    manager = get_manager(request, tenant_id)
    return call(lambda: manager.configuration_plan(body.policy))


@router.post("/feed-status")
def feed_status(tenant_id: str, request: Request):
    manager = get_manager(request, tenant_id)
    manager.require_storage()
    path = manager.project / "data/market_data/binance_um_1m/status.json"
    default = {"state": "STOPPED", "data_ready": False, "reason": "not_started",
               "data_type": "minute_ohlcv", "checked_ms": None, "valid_until_ms": None}
    if not path.is_file():
        return default
    try:
        value = json.loads(path.read_text())
        if (not isinstance(value, dict) or type(value.get("valid_until_ms")) is not int
                or type(value.get("checked_ms")) is not int
                or value.get("state") not in {"READY", "DEGRADED", "WARMING_UP", "STOPPED"}):
            raise ValueError("Invalid feed status")
        expired = int(time.time() * 1000) > value["valid_until_ms"]
        raw_symbols = value.get("symbols", {})
        if not isinstance(raw_symbols, dict):
            raise TypeError("Invalid symbols")
        symbols = {}
        for symbol, progress in raw_symbols.items():
            if not isinstance(progress, dict):
                raise TypeError("Invalid symbol progress")
            symbols[symbol] = {}
            for field in ("next_ms", "last_websocket_ms"):
                stamp = progress.get(field)
                if stamp is not None and (type(stamp) is not int or stamp < 0):
                    raise ValueError("Invalid timestamp")
                symbols[symbol][field] = stamp
        return {"state": "DEGRADED" if expired else value["state"],
                "data_ready": not expired and value["state"] == "READY" and value.get("data_ready") is True,
                "reason": "status_expired" if expired else value.get("reason"),
                "checked_ms": value["checked_ms"], "valid_until_ms": value["valid_until_ms"],
                "symbols": symbols, "data_type": "minute_ohlcv"}
    except (OSError, ValueError, TypeError):
        return {**default, "state": "DEGRADED", "reason": "invalid_status"}


@router.post("/health")
def health(tenant_id: str, request: Request):
    manager = get_manager(request, tenant_id)
    return {"storage": manager.storage(), "tasks": manager.jobs(), "monitor": manager.monitor.read()}
