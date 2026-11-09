"""Authenticated, tenant-local task API.

A single endpoint shape covers the resident Engine backtest queue and the
existing Engine-owned market-data subprocess jobs. Strategy validation may be
added as a new kind without changing the lifecycle contract.
"""
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError

router = APIRouter(prefix="/tenants/{tenant_id}/jobs", tags=["engine-jobs"])


class BacktestParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account_id: str = Field(min_length=1, max_length=40)
    start: str = Field(min_length=10, max_length=40)
    end: str = Field(min_length=10, max_length=40)
    interval: Literal["1m", "5m"] = "1m"
    starting_balance: str = Field(min_length=1, max_length=40)
    quote_currency: str = Field(pattern=r"^[A-Z]{3,8}$")


class SubmitJob(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["data", "backtest"]
    parameters: dict
    idempotency_key: str | None = Field(
        default=None, min_length=1, max_length=128,
        pattern=r"^[A-Za-z0-9._:-]{1,128}$",
    )


def coordinator(request: Request):
    return request.app.state.tasks


@router.post("", status_code=202)
def submit(tenant_id: str, body: SubmitJob, request: Request):
    parameters = body.parameters
    if body.kind == "backtest":
        try:
            parameters = BacktestParameters.model_validate(parameters).model_dump(mode="json")
        except ValidationError:
            raise HTTPException(422, "回测任务参数无效。") from None
    return coordinator(request).submit(
        tenant_id, body.kind, parameters, idempotency_key=body.idempotency_key,
    )


@router.get("")
def list_jobs(tenant_id: str, request: Request):
    return {"items": coordinator(request).list(tenant_id)}


@router.get("/{identity}")
def task(tenant_id: str, identity: str, request: Request):
    return coordinator(request).get(tenant_id, identity)


@router.get("/{identity}/result")
def result(tenant_id: str, identity: str, request: Request):
    return coordinator(request).result(tenant_id, identity)


@router.post("/{identity}/cancel")
def cancel(tenant_id: str, identity: str, request: Request):
    return coordinator(request).cancel(tenant_id, identity)


@router.post("/{identity}/retry", status_code=202)
def retry(tenant_id: str, identity: str, request: Request):
    return coordinator(request).retry(tenant_id, identity)
