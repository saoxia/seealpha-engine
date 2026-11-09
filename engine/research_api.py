"""Research-only HTTP surface. Distinct credentials from the Engine admin API.

Mounted as a separate ASGI application so the Engine's global administrative
Depends(authorized) can NEVER be satisfied by a scoped research grant.
"""
from __future__ import annotations

import hmac
import re
from hashlib import sha256
from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from engine.research_auth import (
    ENGINE_HEADER,
    ENGINE_VERSION,
    RESEARCH_HEADER,
    RESEARCH_VERSION,
    identifier,
    require_op,
    verify_research_grant,
)
from engine.research_snapshots import SnapshotService
from engine.runtime import EngineConflict
from engine.task_routes import BacktestParameters

_ACCOUNT = re.compile(r"^account:([A-Za-z0-9._-]{1,40})$")
_STATUS = {
    "PENDING": "queued", "RUNNING": "running", "SUCCEEDED": "completed",
    "FAILED": "failed", "CANCELLED": "cancelled", "INTERRUPTED": "interrupted",
}


class SnapshotRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dataset_id: str = Field(min_length=1, max_length=128)
    start: str = Field(min_length=10, max_length=40)
    end: str = Field(min_length=10, max_length=40)
    format: Literal["parquet"]


class BacktestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    purpose: Literal["development"]
    execution: Literal["simulation"]
    strategy_ref: str = Field(min_length=1, max_length=128)
    dataset_refs: list[str] = Field(min_length=1, max_length=1)
    start: str = Field(min_length=10, max_length=40)
    end: str = Field(min_length=10, max_length=40)
    starting_balance: str = Field(min_length=1, max_length=40)
    quote_currency: str = Field(pattern=r"^[A-Z]{3,8}$")
    parameters: dict = Field(default_factory=dict)
    idempotency_key: str = Field(
        min_length=1, max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$",
    )


def create_research_app(config, runtime, tasks):
    key = config.research_signing_key
    if not key or len(key) < 32 or hmac.compare_digest(key, config.token):
        raise ValueError("Dedicated research signing key must be strong and differ from Engine token")
    snapshots = SnapshotService(runtime, runtime.root, config.tenant_id)
    app = FastAPI(
        title="SeeAlpha Engine Research", version="1",
        docs_url=None, redoc_url=None, openapi_url=None,
    )
    app.state.snapshots = snapshots

    @app.middleware("http")
    async def version_response(request: Request, call_next):
        response = await call_next(request)
        response.headers[RESEARCH_HEADER] = RESEARCH_VERSION
        response.headers[ENGINE_HEADER] = ENGINE_VERSION
        return response

    def grant(
        authorization: str = Header(default=""),
        engine_version: str = Header(default="", alias=ENGINE_HEADER),
        research_version: str = Header(default="", alias=RESEARCH_HEADER),
    ) -> dict:
        if engine_version != ENGINE_VERSION or research_version != RESEARCH_VERSION:
            raise HTTPException(426, "Research contract version mismatch")
        scheme, _, bearer = authorization.partition(" ")
        if scheme.lower() != "bearer" or not bearer:
            raise HTTPException(401, "Research authorization required")
        claims = verify_research_grant(key, bearer)
        if (
            config.tenant_id is None or config.tenant_purpose != "development"
            or claims["tenant_id"] != config.tenant_id
        ):
            raise HTTPException(403, "Research tenant scope denied")
        tenant = runtime.tenant_row(config.tenant_id)
        if (
            not tenant or tenant["status"] != "active"
            or tenant["purpose"] != "development"
        ):
            raise HTTPException(403, "Research requires an active development tenant")
        return claims

    grant_dependency = Depends(grant)

    @app.get("/v1/datasets")
    def datasets(claims: dict = grant_dependency):
        return {"datasets": snapshots.discover(claims)}

    @app.post("/v1/snapshots", status_code=202)
    def create_snapshot(body: SnapshotRequest, claims: dict = grant_dependency):
        try:
            dataset_id = identifier(body.dataset_id)
        except ValueError:
            raise HTTPException(422, "Invalid research dataset identifier") from None
        return snapshots.create(dataset_id, body.start, body.end, claims)

    @app.get("/v1/snapshots/{snapshot_id}")
    def get_snapshot(snapshot_id: str, claims: dict = grant_dependency):
        return snapshots.get(snapshot_id, claims)

    @app.get("/v1/snapshots/{snapshot_id}/content")
    def download_snapshot(snapshot_id: str, claims: dict = grant_dependency):
        path = snapshots.file(snapshot_id, claims)
        return FileResponse(
            path, media_type="application/octet-stream",
            headers={"Cache-Control": "private, no-store"},
        )

    def _scoped_backtest(record_id: str, claims: dict) -> dict:
        # The TaskCoordinator journal owns backtest lifecycles. Bind its job
        # identity to the research task by the prefix of its idempotency key.
        try:
            identity = identifier(record_id)
        except ValueError:
            raise HTTPException(404, "Research backtest not found") from None
        prefix = "research." + sha256(claims["task_id"].encode()).hexdigest()[:32] + "."
        try:
            job = tasks.get(config.tenant_id, identity)
        except HTTPException as exc:
            if exc.status_code == 404:
                raise HTTPException(404, "Research backtest not found") from None
            raise
        if (
            job.get("kind") != "backtest"
            or not str(tasks.journal.get(config.tenant_id, identity).get(
                "idempotency_key", "",
            )).startswith(prefix)
        ):
            raise HTTPException(404, "Research backtest not found")
        return job

    def _public_backtest(job: dict, *, with_result: bool = False) -> dict:
        data = {
            "backtest_id": job["id"],
            "status": _STATUS.get(job["status"], "unknown"),
        }
        if job["status"] == "SUCCEEDED" and with_result:
            data["result"] = tasks.result(config.tenant_id, job["id"])
        return data

    @app.post("/v1/backtests", status_code=202)
    def submit_backtest(body: BacktestRequest, claims: dict = grant_dependency):
        try:
            strategy_ref = identifier(body.strategy_ref)
            dataset_id = identifier(body.dataset_refs[0])
        except ValueError:
            raise HTTPException(422, "Invalid research reference") from None
        require_op(claims, "backtests:submit", strategy=strategy_ref)
        if body.parameters:
            # Current engine backtester takes only the fixed BacktestParameters.
            raise HTTPException(422, "Free-form strategy parameters not supported by this Engine")
        match = _ACCOUNT.fullmatch(body.strategy_ref)
        if match is None:
            raise HTTPException(422, "strategy_ref must identify an issued development account")
        account_id = match.group(1)
        require_op(claims, "backtests:submit", dataset=dataset_id)
        try:
            market, symbol, interval = dataset_id.split(":")
        except ValueError:
            raise HTTPException(422, "Invalid backtest dataset reference") from None
        if market != "futures_um" or interval not in {"1m", "5m"}:
            raise HTTPException(422, "Existing Engine backtester supports only futures 1m/5m")
        if dataset_id not in {d["id"] for d in snapshots._discover()}:
            raise HTTPException(404, "Research dataset not found")
        loads = runtime.store.strategy_loads(
            config.tenant_id, account_id, include_archive=True,
        )
        if not loads or any(
            item.get("grant_purpose") != "development"
            or item.get("parameters", {}).get("symbol") != symbol
            for item in loads
        ):
            raise HTTPException(409, "No matching issued development strategy")
        try:
            params = BacktestParameters(
                account_id=account_id,
                start=body.start, end=body.end, interval=interval,
                starting_balance=body.starting_balance,
                quote_currency=body.quote_currency,
            ).model_dump(mode="json")
            # Use the existing 366-day/interval guard rather than changing it.
            from engine.strategies.backtest import parse_window
            parse_window(body.start, body.end, interval)
        except (EngineConflict, ValueError):
            raise HTTPException(422, "Invalid development backtest window") from None
        idem = (
            "research." + sha256(claims["task_id"].encode()).hexdigest()[:32] + "."
            + sha256(body.idempotency_key.encode()).hexdigest()[:32]
        )
        job = tasks.submit(
            config.tenant_id, "backtest", params, idempotency_key=idem,
        )
        return _public_backtest(job)

    @app.get("/v1/backtests/{backtest_id}")
    def get_backtest(backtest_id: str, claims: dict = grant_dependency):
        require_op(claims, "backtests:read")
        return _public_backtest(_scoped_backtest(backtest_id, claims), with_result=True)

    @app.post("/v1/backtests/{backtest_id}/cancel")
    def cancel_backtest(backtest_id: str, claims: dict = grant_dependency):
        require_op(claims, "backtests:cancel")
        _scoped_backtest(backtest_id, claims)
        return _public_backtest(tasks.cancel(config.tenant_id, backtest_id))

    return app
