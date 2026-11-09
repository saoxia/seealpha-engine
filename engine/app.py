"""Internal HTTP API. The business backend is the only intended client."""

import hmac
import json
import logging
import os
import subprocess
import sys
import threading
from contextlib import aclosing
from pathlib import Path
from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.engine_contracts import ENGINE_CONTRACT_HEADER, ENGINE_CONTRACT_VERSION
from core.trading_contracts import TRADING_SYMBOL, CancelOrderValues, SubmitOrderValues
from engine.config import EngineConfig
from engine.connectors.binance.queries import QueryError
from engine.data_manager.routes import router as control_router
from engine.data_manager.service import DataService as EngineDataService
from engine.data_queries import router as market_data_router
from engine.public_queries import router as venue_router
from engine.research_api import create_research_app
from engine.runtime import EngineConflict, EngineRuntime, valuation_events
from engine.task_queue import TaskCoordinator
from engine.task_routes import router as task_router


class PackageInspectBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    archive: str = Field(min_length=1, max_length=400_000)
    parameters: dict = Field(default_factory=dict)


class Credentials(BaseModel):
    environment: str = Field(pattern=r"^(production|sandbox)$")
    api_key: str = Field(min_length=1)
    api_secret: str = Field(min_length=1)

    def account(self, tenant_id, account_id):
        return {
            "id": int(account_id) if str(account_id).isdigit() else account_id,
            "tenant_id": str(tenant_id),
            "environment": self.environment,
            "credentials": {"apiKey": self.api_key, "secret": self.api_secret},
        }


class CommandBody(Credentials):
    model_config = ConfigDict(extra="forbid")
    action: Literal["submit", "cancel"]
    values: dict

    @field_validator("values")
    @classmethod
    def validate_values(cls, values, info):
        action = info.data.get("action")
        if action not in {"submit", "cancel"}:
            return values  # The action field already carries its own 422 validation error.
        schema = SubmitOrderValues if action == "submit" else CancelOrderValues
        return schema.model_validate(values).model_dump(mode="json")


class TradingStateBody(Credentials):
    model_config = ConfigDict(extra="forbid")
    symbol: str = Field(pattern=TRADING_SYMBOL)


class ReconcileBody(Credentials):
    model_config = ConfigDict(extra="forbid")
    account_revision: str = Field(pattern=r"^[a-f0-9]{64}$")


class WorkerBody(BaseModel):
    command: list[str]
    log_path: str
    access: list | None = None
    hold_lease: bool = False
    log_mode: str = "ab"


class TenantBody(BaseModel):
    purpose: str = Field(pattern=r"^(production|development)$")
    status: str = Field(default="active", pattern=r"^(active|archived)$")


class RunnableBody(BaseModel):
    action: str = Field(pattern=r"^(live|backtest)$")


class ImportBody(BaseModel):
    purpose: str = Field(pattern=r"^(production|development)$")
    payload: dict


class GapBody(BaseModel):
    scope: str
    gap_key: str
    gap: dict


class WorkerNote(BaseModel):
    command: list[str]


class SubscriptionBody(BaseModel):
    product: str
    symbol: str
    role: str
    state: str
    progress: dict = Field(default_factory=dict)


class StrategyLoadBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account_id: str = Field(min_length=1, max_length=40)
    issuance_id: str = Field(min_length=1, max_length=64)
    code_key: str = Field(min_length=1, max_length=80)
    version: str = Field(min_length=1, max_length=40)
    grant_purpose: str = Field(pattern=r"^(production|development)$")
    parameters: dict
    entry_point: str = Field(min_length=3, max_length=120)
    package_hash: str = Field(min_length=64, max_length=64)
    archive: str = Field(min_length=1, max_length=400_000)


class PreviewBar(BaseModel):
    model_config = ConfigDict(extra="forbid")
    open: str
    high: str
    low: str
    close: str


class PreviewBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    bars: list[PreviewBar] = Field(min_length=1, max_length=2000)


class BacktestBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    start: str = Field(min_length=10, max_length=40)
    end: str = Field(min_length=10, max_length=40)
    interval: str = Field(default="1m", pattern=r"^(1m|5m)$")
    starting_balance: str = Field(min_length=1, max_length=40)
    quote_currency: str = Field(pattern=r"^[A-Z]{3,8}$")


def create_app(config: EngineConfig | None = None, runtime: EngineRuntime | None = None) -> FastAPI:
    config = config or EngineConfig.from_environment()
    runtime = runtime or EngineRuntime(config.state_dir, config.project)

    def authorized(authorization: str = Header(default="")):
        expected = f"Bearer {config.token}"
        if len(authorization) != len(expected) or not hmac.compare_digest(authorization, expected):
            raise HTTPException(401, "引擎令牌无效。")

    app = FastAPI(title="SeeAlpha Engine", version="0.1.0", dependencies=[Depends(authorized)])
    app.state.runtime = runtime
    app.state.market_data = EngineDataService(
        config.tenant_id or "1", project=runtime.project, state_root=runtime.root,
    )
    app.include_router(venue_router)
    app.include_router(market_data_router)
    app.include_router(control_router)
    app.state.tasks = TaskCoordinator(runtime, app.state.market_data, runtime.root)
    app.include_router(task_router)
    # Mount a separate ASGI application: research grants must NEVER authenticate
    # against the administrative Engine HTTP API or its trading routes.
    if config.research_signing_key:
        if config.tenant_id is None:
            raise ValueError("Research API requires a pinned Engine tenant")
        app.state.research_api = create_research_app(
            config, runtime, app.state.tasks,
        )
        app.mount("/research", app.state.research_api)

    stop_monitor = threading.Event()
    monitor_thread = None

    @app.on_event("startup")
    def start_market_supervisor():
        nonlocal monitor_thread
        if config.tenant_id is None:
            return

        def supervise():
            manager = app.state.market_data
            while not stop_monitor.wait(5):
                try:
                    manager.subscriptions.restore()
                    manager.monitor.sample()
                except (OSError, ValueError, HTTPException):
                    logging.getLogger(__name__).warning("租户行情管理器重试恢复失败。")

        monitor_thread = threading.Thread(
            target=supervise, daemon=True, name="engine-data-supervisor",
        )
        monitor_thread.start()

    @app.on_event("shutdown")
    def stop_market_supervisor():
        stop_monitor.set()
        if monitor_thread is not None:
            monitor_thread.join(timeout=8)
        app.state.tasks.close()
        if hasattr(app.state, "research_api"):
            app.state.research_api.state.snapshots.close()
        app.state.market_data.close()

    @app.middleware("http")
    async def tenant_boundary(request: Request, call_next):
        requested_version = request.headers.get(ENGINE_CONTRACT_HEADER)
        if requested_version is not None and requested_version != ENGINE_CONTRACT_VERSION:
            return JSONResponse(
                {"detail": "Engine HTTP 契约版本不兼容。"}, status_code=426,
                headers={ENGINE_CONTRACT_HEADER: ENGINE_CONTRACT_VERSION},
            )
        # A tenant Engine must reject another tenant's HTTP operations.
        if config.tenant_id is not None and request.url.path.startswith("/tenants/"):
            target = request.url.path[len("/tenants/"):].split("/", 1)[0]
            if target != config.tenant_id:
                return JSONResponse(
                    {"detail": "Engine 租户不匹配。"}, status_code=403,
                    headers={ENGINE_CONTRACT_HEADER: ENGINE_CONTRACT_VERSION},
                )
        response = await call_next(request)
        response.headers[ENGINE_CONTRACT_HEADER] = ENGINE_CONTRACT_VERSION
        return response

    @app.exception_handler(QueryError)
    async def venue_error(request: Request, error: QueryError):
        return JSONResponse({"detail": str(error)}, status_code=502)

    def conflict(error: EngineConflict):
        raise HTTPException(409, error.message) from None

    @app.get("/health")
    def health(tenant_id: str | None = None, account_id: str | None = None):
        return {**runtime.health(tenant_id, account_id), "contract_version": ENGINE_CONTRACT_VERSION}

    @app.put("/tenants/{tenant_id}")
    def ensure_tenant(tenant_id: str, body: TenantBody):
        if config.tenant_purpose is not None and body.purpose != config.tenant_purpose:
            raise HTTPException(403, "Engine 租户类型不匹配。")
        try:
            return runtime.ensure_tenant(tenant_id, body.purpose, body.status)
        except EngineConflict as error:
            conflict(error)

    @app.post("/tenants/{tenant_id}/runnable")
    def runnable(tenant_id: str, body: RunnableBody):
        try:
            return runtime.assert_runnable(tenant_id, body.action)
        except EngineConflict as error:
            conflict(error)

    @app.get("/tenants/{tenant_id}/export")
    def export_tenant(tenant_id: str):
        return runtime.export_tenant(tenant_id)

    @app.post("/tenants/{tenant_id}/drop")
    def drop_tenant(tenant_id: str, remove: bool = False):
        return runtime.drop_tenant(tenant_id, remove=remove)

    @app.post("/tenants/{tenant_id}/import")
    def import_tenant(tenant_id: str, body: ImportBody):
        return runtime.import_tenant(tenant_id, body.purpose, body.payload)

    @app.post("/tenants/{tenant_id}/accounts/{account_id}/connections")
    def connect(tenant_id: str, account_id: str, body: Credentials):
        try:
            return runtime.register(tenant_id, account_id, body.environment, body.api_key, body.api_secret)
        except EngineConflict as error:
            conflict(error)

    @app.delete("/tenants/{tenant_id}/accounts/{account_id}/connections")
    def disconnect(tenant_id: str, account_id: str, environment: str):
        runtime.forget(tenant_id, account_id, environment)
        return {
            "tenant_id": tenant_id, "account_id": account_id,
            "environment": environment, "status": "closed",
        }

    @app.get("/tenants/{tenant_id}/accounts/{account_id}/health")
    def account_health(tenant_id: str, account_id: str):
        return runtime.health(tenant_id, account_id)

    @app.post("/tenants/{tenant_id}/accounts/{account_id}/snapshots")
    async def snapshot(tenant_id: str, account_id: str, body: Credentials):
        try:
            result = await runtime.account_snapshot(body.account(tenant_id, account_id))
        except EngineConflict as error:
            conflict(error)
        return {
            "balances": result.balances, "info": result.info,
            "positions": result.positions, "observed_at": result.observed_at,
        }

    @app.post("/tenants/{tenant_id}/accounts/{account_id}/trading/state")
    async def trading_state(tenant_id: str, account_id: str, body: TradingStateBody):
        try:
            return await runtime.trading_state(body.account(tenant_id, account_id), body.symbol)
        except EngineConflict as error:
            conflict(error)

    @app.post("/tenants/{tenant_id}/accounts/{account_id}/orders")
    async def order(tenant_id: str, account_id: str, body: CommandBody):
        if body.environment != body.values["environment"]:
            raise HTTPException(422, "账户环境与订单环境不匹配。")
        try:
            return await runtime.command(body.account(tenant_id, account_id), body.action, body.values)
        except EngineConflict as error:
            conflict(error)

    @app.get("/tenants/{tenant_id}/accounts/{account_id}/orders")
    def orders(tenant_id: str, account_id: str, environment: str):
        return {"orders": runtime.store.orders(tenant_id, account_id, environment)}

    @app.get("/tenants/{tenant_id}/accounts/{account_id}/fills")
    def fills(tenant_id: str, account_id: str, environment: str):
        return {"fills": runtime.store.fills(tenant_id, account_id, environment)}

    @app.get("/tenants/{tenant_id}/accounts/{account_id}/positions")
    def positions(tenant_id: str, account_id: str, environment: str):
        return {"positions": runtime.store.positions(tenant_id, account_id, environment)}

    @app.get("/tenants/{tenant_id}/accounts/{account_id}/gaps")
    def gaps(tenant_id: str, account_id: str, scope: str | None = None):
        return {"gaps": runtime.store.gaps(tenant_id, account_id, scope)}

    @app.post("/tenants/{tenant_id}/accounts/{account_id}/gaps")
    def put_gap(tenant_id: str, account_id: str, body: GapBody):
        return runtime.note_gap(tenant_id, account_id, body.scope, body.gap_key, body.gap)

    @app.get("/tenants/{tenant_id}/accounts/{account_id}/requests/{request_id}")
    def request_row(tenant_id: str, account_id: str, request_id: str):
        stored = runtime.store.get_request(tenant_id, account_id, request_id)
        if stored is None:
            raise HTTPException(404, "服务器没有该请求记录，请在交易所核对后处理。")
        return stored

    @app.post("/tenants/{tenant_id}/accounts/{account_id}/requests/{request_id}/reconcile")
    async def reconcile_request(tenant_id: str, account_id: str, request_id: str, body: ReconcileBody):
        try:
            stored = await runtime.reconcile_request(
                body.account(tenant_id, account_id), request_id, body.account_revision,
            )
        except EngineConflict as error:
            conflict(error)
        if stored is None:
            raise HTTPException(404, "服务器没有该请求记录，请在交易所核对后处理。")
        return stored

    @app.post("/tenants/{tenant_id}/accounts/{account_id}/requests/{request_id}/ledger-sync")
    def mark_ledger_synced(tenant_id: str, account_id: str, request_id: str):
        stored = runtime.store.mark_request_ledger_synced(tenant_id, account_id, request_id)
        if stored is None:
            raise HTTPException(404, "服务器没有该请求记录，请在交易所核对后处理。")
        return stored

    @app.get("/tenants/{tenant_id}/accounts/{account_id}/events")
    def events(tenant_id: str, account_id: str, after: int = 0):
        rows = runtime.store.events(tenant_id, account_id, after)
        return {"events": rows, "next": rows[-1]["id"] if rows else after}

    @app.get("/tenants/{tenant_id}/accounts/{account_id}/market/subscriptions")
    def subscriptions(tenant_id: str, account_id: str):
        return {"subscriptions": runtime.store.subscriptions(tenant_id, account_id)}

    @app.post("/tenants/{tenant_id}/accounts/{account_id}/market/subscriptions")
    def put_subscription(tenant_id: str, account_id: str, body: SubscriptionBody):
        return runtime.note_subscription(
            tenant_id, account_id, body.product, body.symbol, body.role, body.state, body.progress
        )

    @app.post("/tenants/{tenant_id}/accounts/{account_id}/market/workers/note")
    def note_worker(tenant_id: str, account_id: str, body: WorkerNote):
        noted = runtime.note_worker(tenant_id, account_id, body.command)
        if noted is None:
            raise HTTPException(422, "无法从命令识别行情订阅。")
        return noted

    @app.post("/tenants/{tenant_id}/accounts/{account_id}/market/workers")
    def start_worker(tenant_id: str, account_id: str, body: WorkerBody):
        try:
            started = runtime.spawn_worker(
                body.command, body.log_path, tenant_id=tenant_id,
                access=tuple(body.access) if body.access else None,
                hold_lease=body.hold_lease, log_mode=body.log_mode,
            )
        except EngineConflict as error:
            conflict(error)
        runtime.note_worker(tenant_id, account_id, body.command)
        return started

    @app.get("/workers/{worker_id}")
    def worker(worker_id: str):
        try:
            return runtime.worker(worker_id)
        except EngineConflict as error:
            raise HTTPException(404, error.message) from None

    @app.post("/workers/{worker_id}/stop")
    def stop_worker(worker_id: str):
        try:
            return runtime.stop_worker(worker_id)
        except EngineConflict as error:
            raise HTTPException(404, error.message) from None

    @app.post("/tenants/{tenant_id}/strategy-packages/inspect")
    def inspect_strategy(tenant_id: str, body: PackageInspectBody):
        """Validate an archive in a short-lived, credential-free worker process."""
        source_root = Path(__file__).resolve().parents[1]
        request_body = body.model_dump_json().encode("utf-8")
        env = {
            "PATH": os.environ.get("PATH", ""),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONIOENCODING": "utf-8",
        }
        try:
            finished = subprocess.run(
                [sys.executable, "-m", "engine.strategies.inspect"],
                input=request_body, capture_output=True, timeout=20,
                cwd=source_root, env=env, check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise HTTPException(503, "策略包校验工作进程不可用。") from exc
        if finished.returncode or len(finished.stdout) > 65536:
            raise HTTPException(422, "策略包校验失败。")
        try:
            result = json.loads(finished.stdout)
        except ValueError:
            raise HTTPException(422, "策略包校验失败。") from None
        if not isinstance(result, dict):
            raise HTTPException(422, "策略包校验失败。")
        if result.get("error"):
            raise HTTPException(422, str(result["error"])[:300])
        if not isinstance(result.get("metadata"), dict) or not isinstance(result.get("parameters"), dict):
            raise HTTPException(422, "策略包校验结果无效。")
        return result

    @app.post("/tenants/{tenant_id}/strategy-loads")
    def receive_strategy(tenant_id: str, body: StrategyLoadBody):
        try:
            return runtime.receive_strategy(tenant_id, body.model_dump())
        except EngineConflict as error:
            conflict(error)

    @app.get("/tenants/{tenant_id}/strategy-loads")
    def strategy_loads(tenant_id: str, account_id: str | None = None):
        return {"loads": runtime.store.strategy_loads(tenant_id, account_id)}

    @app.post("/tenants/{tenant_id}/accounts/{account_id}/strategy-preview")
    def strategy_preview(tenant_id: str, account_id: str, body: PreviewBody):
        try:
            return runtime.preview_strategy(
                tenant_id, account_id, [item.model_dump() for item in body.bars],
            )
        except EngineConflict as error:
            conflict(error)

    @app.post("/tenants/{tenant_id}/accounts/{account_id}/backtests")
    def start_backtest(tenant_id: str, account_id: str, body: BacktestBody):
        try:
            return runtime.run_backtest(
                tenant_id, account_id, body.start, body.end, body.interval,
                body.starting_balance, body.quote_currency,
            )
        except EngineConflict as error:
            conflict(error)

    @app.get("/tenants/{tenant_id}/accounts/{account_id}/backtests/latest")
    def latest_backtest(tenant_id: str, account_id: str):
        try:
            found = runtime.latest_backtest(tenant_id, account_id)
        except EngineConflict as error:
            conflict(error)
        if found is None:
            raise HTTPException(404, "还没有回测结果。")
        return found

    @app.post("/tenants/{tenant_id}/accounts/{account_id}/valuation")
    async def valuation(tenant_id: str, account_id: str, body: Credentials):
        account = body.account(tenant_id, account_id)

        async def lines():
            try:
                async with aclosing(valuation_events(account)) as events:
                    async for event in events:
                        runtime.store.add_event(tenant_id, account_id, "valuation", {
                            "environment": body.environment, "reason": event.get("reason"), "time": event.get("time"),
                        })
                        yield json.dumps(event, ensure_ascii=False) + "\n"
            except QueryError as error:
                yield json.dumps({"error": str(error)}, ensure_ascii=False) + "\n"

        return StreamingResponse(lines(), media_type="application/x-ndjson")

    return app
