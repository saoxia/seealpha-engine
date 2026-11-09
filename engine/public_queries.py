"""Typed, tenant-scoped Engine HTTP endpoints for venue read-only queries.

No exchange credentials or connector objects are returned to the business API.
Decimals are serialized as strings to preserve trading/asset precision.
"""
import asyncio
import json
from decimal import Decimal
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from engine.connectors.binance import assets, manual, queries

router = APIRouter(prefix="/tenants/{tenant_id}/venue", tags=["engine-venue"])


def wire(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {str(key): wire(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [wire(item) for item in value]
    return value


class PublicRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    environment: Literal["production", "sandbox"] = "production"
    product: Literal["futures_um", "options"] = "futures_um"


class PriceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    environment: Literal["production", "sandbox"]
    currency: str = Field(pattern=r"^[A-Z0-9]{1,30}$")


class AccountRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    environment: Literal["production", "sandbox"]
    api_key: str = Field(min_length=1)
    api_secret: str = Field(min_length=1)
    account_id: str
    account_name: str = ""
    exchange: str = "binance"
    positions: bool = False

    def account(self, tenant_id):
        return {
            "tenant_id": tenant_id,
            "id": self.account_id,
            "account_name": self.account_name,
            "exchange": self.exchange,
            "environment": self.environment,
            "credentials": {"apiKey": self.api_key, "secret": self.api_secret},
        }


@router.post("/catalog")
async def market_catalog(tenant_id: str, request: PublicRequest):
    if request.product == "options":
        if request.environment != "production":
            raise ValueError("期权数据仅使用正式公开行情")
        from engine.connectors.binance.options import markets

        return {"items": await asyncio.to_thread(markets)}
    instruments = await queries.instruments(request.environment)
    rows = []
    for instrument in instruments:
        quote = str(instrument.quote_currency)
        symbol = str(instrument.id.symbol).removesuffix("-PERP")
        if (quote not in {"USDT", "USDC"} or str(instrument.settlement_currency) != quote
                or not symbol.endswith(quote)):
            continue
        base = symbol[:-len(quote)]
        rows.append({
            "symbol": f"{base}/{quote}:{instrument.settlement_currency}",
            "market_type": "linear", "base": base, "quote": quote, "expiry": None,
        })
    rows.sort(key=lambda item: item["symbol"])
    return {"items": rows}


@router.post("/tickers")
async def tickers(tenant_id: str, request: PublicRequest):
    return await manual.market_tickers(request.environment)


@router.post("/asset-price")
async def price(tenant_id: str, request: PriceRequest):
    return wire(await asyncio.to_thread(
        assets.asset_price, request.environment, request.currency,
    ))


@router.post("/asset-account")
async def account_assets(tenant_id: str, request: AccountRequest, http_request: Request):
    # A research tenant must never become a tunnel for production credentials.
    tenant = http_request.app.state.runtime.tenant_row(tenant_id)
    if tenant is None or tenant["purpose"] != "production" or tenant["status"] != "active":
        raise HTTPException(403, "仅正式活跃租户可读取真实账户资产。")
    return wire(await __import__("asyncio").to_thread(
        assets.asset_account, request.account(tenant_id), positions=request.positions,
    ))

class HistoryRequest(AccountRequest):
    start_ms: int = Field(gt=0)
    end_ms: int = Field(gt=0)


class OperationRequest(AccountRequest):
    lookback_minutes: int = Field(ge=1, le=125280)


class OptionsRequest(AccountRequest):
    symbol: str | None = Field(default=None, max_length=100)
    start_ms: int | None = Field(default=None, gt=0)
    end_ms: int | None = Field(default=None, gt=0)
    endpoint: Literal["userTrades", "bill", "exerciseRecord"] | None = None


def require_production(runtime, tenant_id):
    tenant = runtime.tenant_row(tenant_id)
    if tenant is None or tenant["purpose"] != "production" or tenant["status"] != "active":
        raise HTTPException(403, "仅正式活跃租户可读取真实账户信息。")


def json_lines(source):
    async def chunks():
        try:
            async for page in source:
                yield json.dumps(wire(page), ensure_ascii=False, separators=(",", ":")) + "\n"
        except (ValueError, queries.QueryError):
            # Never send raw connector stack traces or credential-bearing URLs.
            yield json.dumps({"error": "交易所历史数据读取未完成。"}, ensure_ascii=False) + "\n"

    return StreamingResponse(chunks(), media_type="application/x-ndjson")


@router.post("/income")
async def income(tenant_id: str, request: HistoryRequest, http_request: Request):
    from engine.connectors.binance.income import income_history

    require_production(http_request.app.state.runtime, tenant_id)
    return json_lines(income_history(request.account(tenant_id), request.start_ms, request.end_ms))


@router.post("/operation-history")
async def operations(tenant_id: str, request: OperationRequest, http_request: Request):
    from engine.connectors.binance.operations import operation_history

    require_production(http_request.app.state.runtime, tenant_id)
    return {"records": wire(await operation_history(
        request.account(tenant_id), lookback_minutes=request.lookback_minutes,
    ))}


@router.post("/options/time")
async def option_time(tenant_id: str, request: PublicRequest):
    from engine.connectors.binance import options

    return await asyncio.to_thread(options.get, "time")


@router.post("/options/positions")
async def option_positions(tenant_id: str, request: OptionsRequest, http_request: Request):
    from engine.connectors.binance.options_account import BinanceOptionsAccountClient

    require_production(http_request.app.state.runtime, tenant_id)
    client = BinanceOptionsAccountClient(request.api_key, request.api_secret, environment=request.environment)
    return {"rows": await client.positions(request.symbol)}


@router.post("/options/open-orders")
async def option_open_orders(tenant_id: str, request: OptionsRequest, http_request: Request):
    from engine.connectors.binance.options_account import BinanceOptionsAccountClient

    require_production(http_request.app.state.runtime, tenant_id)
    client = BinanceOptionsAccountClient(request.api_key, request.api_secret, environment=request.environment)
    return await client.open_orders(request.symbol)


@router.post("/options/order-history")
async def option_order_history(tenant_id: str, request: OptionsRequest, http_request: Request):
    from engine.connectors.binance.options_account import BinanceOptionsAccountClient

    require_production(http_request.app.state.runtime, tenant_id)
    if request.symbol is None or request.start_ms is None or request.end_ms is None:
        raise HTTPException(422, "期权订单查询需要合约和时间范围。")
    client = BinanceOptionsAccountClient(request.api_key, request.api_secret, environment=request.environment)
    return await client.order_history(request.symbol, request.start_ms, request.end_ms)


@router.post("/options/pages")
async def option_pages(tenant_id: str, request: OptionsRequest, http_request: Request):
    from engine.connectors.binance.options_account import BinanceOptionsAccountClient

    require_production(http_request.app.state.runtime, tenant_id)
    if request.endpoint is None or request.start_ms is None or request.end_ms is None:
        raise HTTPException(422, "期权查询需要类型和时间范围。")
    client = BinanceOptionsAccountClient(request.api_key, request.api_secret, environment=request.environment)
    return json_lines(client.history_pages(
        request.endpoint, request.start_ms, request.end_ms, symbol=request.symbol,
    ))
