"""Explicit manual USD-M commands through public Nautilus risk/execution engines."""

import asyncio
import hashlib
import hmac
import json
import logging
import re
import time
from decimal import Decimal, localcontext
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

from nautilus_trader.common import LogLevel
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model import (
    ClientOrderId,
    InstrumentId,
    OrderSide,
    PositionId,
    Price,
    Quantity,
    StrategyId,
    TimeInForce,
)
from nautilus_trader.trading import Strategy

from .operations import cached_order_observations, enum_text, normalize_event
from .queries import QueryError, _clear_exception_frames, account_node


class ManualValidationError(QueryError):
    """A definitive failure before any execution command was dispatched."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def account_context(account, symbol, *, http_url=None, include_risk=True):
    """Read actual position mode and display-only risk prices/leverage; never change settings.

    The pinned public Python node does not publish these Binance configuration/risk
    fields. Only these fixed official GET endpoints supplement its native snapshot.
    """
    bases = {"production": "https://fapi.binance.com", "sandbox": "https://testnet.binancefuture.com"}
    if account["environment"] not in bases or not re.fullmatch(r"[A-Z0-9]+(?:USDT|USDC)-PERP", symbol):
        raise QueryError("不支持的交易账户环境或合约。")
    base = http_url or bases[account["environment"]]
    credentials = account["credentials"]
    if (not credentials.get("apiKey") or not credentials.get("secret")
            or credentials["apiKey"] == credentials["secret"]
            or any(c in credentials["apiKey"] for c in "\r\n")):
        raise QueryError("交易账户凭据无效。")
    opener = build_opener(_NoRedirect())

    def read(path, params=None):
        headers = {}
        if params is not None:
            query = urlencode({**params, "timestamp": int(time.time() * 1000) + offset, "recvWindow": 5000})
            signature = hmac.new(credentials["secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
            path += "?" + query + "&signature=" + signature
            headers["X-MBX-APIKEY"] = credentials["apiKey"]
        with opener.open(Request(base + path, headers=headers, method="GET"), timeout=8) as response:
            raw = response.read(128 * 1024 + 1)
        if len(raw) > 128 * 1024:
            raise ValueError("Oversize account context")
        return json.loads(raw)

    try:
        server = read("/fapi/v1/time")
        if type(server.get("serverTime")) is not int:
            raise ValueError("Missing server time")
        offset = server["serverTime"] - int(time.time() * 1000)
        mode = read("/fapi/v1/positionSide/dual", {})
        if type(mode.get("dualSidePosition")) is not bool:
            raise ValueError("Missing position mode")
        rows = read("/fapi/v3/positionRisk", {"symbol": symbol.removesuffix("-PERP")}) if include_risk else []
        if not isinstance(rows, list):
            raise TypeError("Invalid risk rows")
        positions = {}
        for row in rows:
            side = row["positionSide"]
            if row["symbol"] != symbol.removesuffix("-PERP") or side not in {"BOTH", "LONG", "SHORT"} or side in positions:
                raise ValueError("Risk identity mismatch")
            values = {key: str(row[key]) for key in ("positionAmt", "entryPrice", "breakEvenPrice", "liquidationPrice")}
            if any(not Decimal(value).is_finite() for value in values.values()):
                raise ValueError("Invalid risk prices")
            positions[side] = values
        leverage = None
        if include_risk:
            try:
                configs = read("/fapi/v1/symbolConfig", {"symbol": symbol.removesuffix("-PERP")})
                if (isinstance(configs, list) and len(configs) == 1
                        and configs[0].get("symbol") == symbol.removesuffix("-PERP")
                        and type(configs[0].get("leverage")) is int and configs[0]["leverage"] > 0):
                    leverage = configs[0]["leverage"]
            except Exception:  # noqa: BLE001 - Optional display data must not affect native trading or leak credentials.
                leverage = None
        return {"position_mode": "hedge" if mode["dualSidePosition"] else "oneway",
                "positions": positions, "leverage": leverage}
    except Exception:  # noqa: BLE001 - Signed URLs, response bodies and credentials stay private.
        raise QueryError("无法核对账户持仓模式与风险价格，请检查合约读取权限、校时和网络。") from None


def resolve_command(command, mode):
    """Preserve explicit legacy legs; new tickets express open/close intent only."""
    result = dict(command)
    if command["action"] != "submit":
        result["position_side"] = "BOTH" if mode == "oneway" else "LONG"
        return result
    expected = command.get("expected_position_mode")
    if expected is not None and expected != mode:
        raise ManualValidationError("账户持仓模式已变化，请刷新账户并重新确认。")
    legacy = command.get("position_side")
    close = command.get("position_effect", "open") == "close" or command.get("reduce_only", False)
    if legacy is not None:
        if (mode == "oneway") != (legacy == "BOTH"):
            raise ManualValidationError("委托方向与账户持仓模式不符。")
        result["position_side"] = legacy
    elif mode == "oneway":
        result["position_side"] = "BOTH"
    else:
        result["position_side"] = "LONG" if (command["side"] == "buy") != close else "SHORT"
    result["reduce_only"] = command.get("reduce_only", False) or close
    return result


def exact(value, increment, minimum=None, maximum=None):
    number = Decimal(str(value))
    with localcontext() as context:
        context.prec = 80
        aligned = number.is_finite() and number % Decimal(str(increment)) == 0
    if (not number.is_finite() or number <= 0 or not aligned
            or minimum is not None and number < Decimal(str(minimum))
            or maximum is not None and number > Decimal(str(maximum))):
        raise ValueError("价格或数量不符合交易所精度及范围限制。")
    return number


def order_view(order):
    return {
        "client_order_id": str(order.client_order_id),
        "order_id": str(order.venue_order_id) if order.venue_order_id else None,
        "symbol": str(order.instrument_id).removesuffix(".BINANCE"),
        "side": enum_text(order.side), "order_type": enum_text(order.order_type),
        "quantity": str(order.quantity), "filled": str(order.filled_qty),
        "price": str(order.price) if getattr(order, "price", None) is not None else None,
        "trigger_price": str(order.trigger_price) if getattr(order, "trigger_price", None) else None,
        "status": enum_text(order.status), "reduce_only": order.is_reduce_only,
        "timestamp": order.ts_last // 1_000_000,
        "open": order.is_open,
    }


class ManualCommand(Strategy):
    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    def __init__(self, account, instrument_id, command):
        super().__init__(StrategyConfig(
            strategy_id=StrategyId("ManualTrade-001"), external_order_instrument_ids=[instrument_id],
            log_events=False, log_commands=False,
        ))
        self.account, self.instrument_id, self.command = account, instrument_id, command
        self.loop = asyncio.get_running_loop()
        self.completed = asyncio.Event()
        self.error, self.result, self.records = None, None, []
        self.outcome = None
        self.sent = False

    def finish(self):
        self.loop.call_soon_threadsafe(self.completed.set)

    def on_start(self):
        try:
            action = self.command["action"]
            if action == "snapshot":
                self.finish()
            elif action == "cancel":
                key = ClientOrderId(self.command["client_order_id"])
                order = self.cache.order(key)
                if order is None or order.instrument_id != self.instrument_id:
                    raise ValueError("未找到该标的的委托，请刷新后再操作。")
                self.result = key
                if order.is_closed:
                    self.finish()
                else:
                    self.cancel_order(key)
            else:
                # Market orders also need an actual fresh native quote for risk checks.
                self.subscribe_quotes(self.instrument_id)
        except (ValueError, RuntimeError) as exc:
            self.error = str(exc) if isinstance(exc, ValueError) else "原生交易初始化失败。"
            self.finish()

    def on_quote(self, tick):
        if self.sent or tick.instrument_id != self.instrument_id:
            return
        if abs(time.time_ns() - tick.ts_event) > 5_000_000_000:
            return
        self.sent = True
        try:
            instrument = self.cache.instrument(self.instrument_id)
            if instrument is None:
                raise ValueError("交易所品种尚未就绪。")
            p = self.command
            quantity = exact(p["quantity"], instrument.size_increment,
                             instrument.min_quantity, instrument.max_quantity)
            if p["reduce_only"]:
                net = sum((Decimal(str(position.quantity)) * (1 if enum_text(position.side) == "long" else -1)
                           for position in self.cache.positions_open(instrument_id=self.instrument_id)
                           if p["position_side"] == "BOTH" or str(position.id).endswith("-" + p["position_side"])), Decimal(0))
                if quantity > abs(net) or (p["side"] == "buy" and net >= 0) or (p["side"] == "sell" and net <= 0):
                    raise ValueError("只减仓数量或方向与当前仓位不符。")
            values = {"instrument_id": self.instrument_id,
                      "order_side": OrderSide.BUY if p["side"] == "buy" else OrderSide.SELL,
                      "quantity": Quantity.from_str(format(quantity, f".{instrument.size_precision}f")),
                      "client_order_id": ClientOrderId(p["client_order_id"]),
                      "reduce_only": p["reduce_only"], "tags": ["MANUAL"]}
            if p["order_type"] == "limit":
                price = exact(p["price"], instrument.price_increment,
                              instrument.min_price, instrument.max_price)
                values.update(price=Price.from_str(format(price, f".{instrument.price_precision}f")),
                              time_in_force=getattr(TimeInForce, p["time_in_force"]),
                              post_only=p["post_only"])
                order = self.order_factory.limit(**values)
            else:
                order = self.order_factory.market(**values)
            # Fail before dispatch for a venue minimum notional; native RiskEngine
            # remains authoritative for balances, reduce-only and order lifecycle.
            reference = Decimal(str(tick.ask_price if p["side"] == "buy" else tick.bid_price))
            notional = quantity * (Decimal(p["price"]) if p["order_type"] == "limit" else reference)
            if (not p["reduce_only"] and instrument.min_notional is not None
                    and notional < instrument.min_notional.as_decimal()):
                raise ValueError("委托金额低于交易所最小名义金额。")
            self.result = order.client_order_id
            position_id = (PositionId(f"{self.instrument_id}-{p['position_side']}")
                           if p["position_side"] != "BOTH" else None)
            self.submit_order(order, position_id=position_id)
        except (ValueError, RuntimeError):
            self.error = "下单校验失败，请检查价格、数量、最小金额和持仓方向。"
            self.finish()

    def on_order_event(self, event):
        self.loop.call_soon_threadsafe(self.capture_event, event)

    def capture_event(self, event):
        order = self.cache.order(event.client_order_id)
        record = normalize_event(event, order, self.account)
        if record is not None:
            self.records.append(record)
        if event.client_order_id != self.result:
            return
        name = type(event).__name__
        if self.command["action"] == "cancel" and (
            name == "OrderAccepted" or name == "OrderFilled" and not order.is_closed
        ):
            return
        if name in {"OrderAccepted", "OrderFilled", "OrderDenied", "OrderRejected", "OrderCanceled",
                    "OrderCancelRejected", "OrderExpired"}:
            self.outcome = name
            self.finish()


async def _run(account, symbol, command, *, http_url=None, websocket_url=None, timeout=40, log_level=LogLevel.OFF):
    context = account_context(account, symbol, http_url=http_url, include_risk=command["action"] == "snapshot")
    command = resolve_command(command, context["position_mode"])
    node, account_id = account_node(
        account, http_url=http_url, websocket_url=websocket_url, timeout=timeout,
        reconciliation_lookback_minutes=1440, manual=True, log_level=log_level,
        manual_position_side=command.get("position_side"),
    )
    strategy = ManualCommand(account, InstrumentId.from_str(f"{symbol}.BINANCE"), command)
    node.add_strategy(strategy)
    cache, handle = node.cache, node.handle()
    task = asyncio.create_task(node.run_async())
    waiter = asyncio.create_task(strategy.completed.wait())
    try:
        done, _ = await asyncio.wait([task, waiter], timeout=timeout,
                                     return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            await task
            raise QueryError("原生交易节点提前停止；若已提交，请核对委托，不要重复下单。")
        if waiter not in done:
            if command["action"] == "snapshot":
                raise QueryError("账户连接或对账超时，请检查交易所网络、凭据和 API 权限后刷新。")
            raise QueryError("交易请求超时，结果待核实，请刷新委托，不要重复下单。")
        if strategy.error:
            raise ManualValidationError(strategy.error)
        native_account = cache.account(account_id)
        if native_account is None:
            raise QueryError("账户尚未就绪。")
        orders = [order_view(o) for o in cache.orders()
                  if o.instrument_id == strategy.instrument_id and (o.account_id == account_id or o.client_order_id == strategy.result)]
        positions = [{"id": str(p.id), "symbol": str(p.instrument_id).removesuffix(".BINANCE"),
                      "side": enum_text(p.side), "quantity": str(p.quantity),
                      "entry_price": str(p.avg_px_open)}
                     for p in cache.positions_open(account_id=account_id)
                     if p.instrument_id == strategy.instrument_id]
        for position in positions:
            leg = "BOTH" if context["position_mode"] == "oneway" else position["side"].upper()
            position.update(position_side=leg, break_even_price=None, liquidation_price=None)
            risk = context["positions"].get(leg)
            if (risk is not None and abs(Decimal(risk["positionAmt"])) == Decimal(position["quantity"])
                    and Decimal(risk["entryPrice"]) == Decimal(position["entry_price"])):
                position["break_even_price"] = risk["breakEvenPrice"] if Decimal(risk["breakEvenPrice"]) > 0 else None
                position["liquidation_price"] = risk["liquidationPrice"] if Decimal(risk["liquidationPrice"]) > 0 else None
        instrument = cache.instrument(strategy.instrument_id)
        if instrument is None:
            raise QueryError("交易所未提供所选永续合约。")
        result = {"checked_at": int(time.time() * 1000), "position_mode": context["position_mode"], "leverage": context["leverage"],
                  "orders": sorted(orders, key=lambda o: o["timestamp"], reverse=True),
                  "positions": positions,
                  "balances": {str(c): {"total": str(v.total.as_decimal()),
                                        "free": str(v.free.as_decimal()),
                                        "locked": str(v.locked.as_decimal())}
                               for c, v in native_account.balances().items()},
                  "instrument": {"symbol": symbol, "price_increment": str(instrument.price_increment),
                                 "size_increment": str(instrument.size_increment),
                                 "min_quantity": str(instrument.min_quantity) if instrument.min_quantity else None,
                                 "min_notional": str(instrument.min_notional.as_decimal()) if instrument.min_notional else None},
                  "result": next((o for o in orders if o["client_order_id"] == str(strategy.result)), None),
                  "records": strategy.records + [r for o in cache.orders(account_id=account_id)
                                                 for r in cached_order_observations(o, account)],
                  "outcome": strategy.outcome}
        return result
    finally:
        waiter.cancel()
        handle.stop()
        try:
            await asyncio.wait_for(asyncio.shield(task), 6)
        except (Exception, asyncio.CancelledError):  # noqa: BLE001 - Release native thread ownership.
            task.cancel()
        await asyncio.gather(task, waiter, return_exceptions=True)
        # A hosted run can fail while shutdown is already in progress. Its error
        # is separate from the request error; release its native traceback here.
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                _clear_exception_frames(error)
        node.dispose()


async def execute(account, symbol, command, **options):
    """Keep every native object and traceback on the thread owning its node."""
    def run():
        try:
            return asyncio.run(_run(account, symbol, command, **options))
        except Exception as exc:  # noqa: BLE001 - Sanitize native exceptions before crossing threads.
            if not isinstance(exc, QueryError):
                location = exc.__traceback__
                while location.tb_next is not None:
                    location = location.tb_next
                logging.getLogger(__name__).warning(
                    "Manual %s failed (%s at %s:%d)", command["action"], type(exc).__name__,
                    location.tb_frame.f_code.co_name, location.tb_lineno,
                )
            validation = isinstance(exc, ManualValidationError)
            message = str(exc) if isinstance(exc, QueryError) else "交易节点失败，请核对委托状态后再操作。"
            _clear_exception_frames(exc)
            return {"validation_error": message} if validation else message
    result = await asyncio.to_thread(run)
    if isinstance(result, str):
        raise QueryError(result)
    return result


async def market_tickers(env):
    """Two bounded public snapshots for the symbol picker, with no execution client."""
    from .transport import PublicREST

    class TickerREST(PublicREST):
        allowed_endpoints = frozenset({"ticker/24hr", "premiumIndex"})

    if env not in {"production", "sandbox"}:
        raise ValueError("不支持的行情环境。")
    source = TickerREST(attempts=1, timeout=8)
    if env == "sandbox":
        source.base_url = "https://demo-fapi.binance.com/fapi/v1/"
    try:
        tickers, premiums = await asyncio.gather(source.get("ticker/24hr"), source.get("premiumIndex"))
        if not isinstance(tickers, list) or not isinstance(premiums, list):
            raise TypeError("Invalid market snapshot")
        funding = {p["symbol"]: p["lastFundingRate"] for p in premiums}
        rows = {}
        for item in tickers:
            symbol = item["symbol"]
            if symbol in rows:
                raise ValueError("Duplicate ticker")
            values = {"last_price": item["lastPrice"], "change_percent": item["priceChangePercent"],
                      "quote_volume": item["quoteVolume"], "funding_rate": funding.get(symbol)}
            if any(v is not None and not Decimal(str(v)).is_finite() for v in values.values()):
                raise ValueError("Invalid ticker number")
            if Decimal(values["last_price"]) < 0 or Decimal(values["quote_volume"]) < 0:
                raise ValueError("Invalid ticker price or volume")
            rows[symbol] = {k: str(v) if v is not None else None for k, v in values.items()}
        return {"checked_at": int(time.time()*1000), "rows": rows}
    except Exception:  # noqa: BLE001 - Public display errors do not expose upstream payloads.
        raise QueryError("合约行情列表读取失败，请稍后重试。") from None
