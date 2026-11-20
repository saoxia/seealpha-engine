"""Options writes and recovery use fictitious credentials and exchange evidence."""

import asyncio
import gc
import hashlib
import hmac
import io
import json
import threading
import time
from decimal import Decimal
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.parse import parse_qs

import pytest
from nautilus_trader.common import Environment, LoggerConfig, LogLevel
from nautilus_trader.core import UUID4
from nautilus_trader.live import LiveExecutionEngineConfig, LiveNode
from nautilus_trader.model import (
    AccountId,
    ClientOrderId,
    InstrumentId,
    LimitOrder,
    OrderSide,
    OrderStatus,
    OrderSubmitted,
    Price,
    Quantity,
    QuoteTick,
    StrategyId,
    TimeInForce,
    TraderId,
)
from nautilus_trader.trading import Strategy

from engine.connectors.binance import options
from engine.connectors.binance.options_client import (
    BinanceOptionsDataClient,
    BinanceOptionsDataClientConfig,
    BinanceOptionsDataClientFactory,
)
from engine.connectors.binance.options_execution import (
    BinanceOptionsExecutionClient,
    BinanceOptionsExecutionClientConfig,
    BinanceOptionsExecutionClientFactory,
)
from engine.connectors.binance.options_orders import BinanceOptionsOrderTransport, OptionsOrderError
from tests.connectors.binance.options_account_test import KEY, SECRET, assets
from tests.connectors.binance.options_queries_test import exchange_info

SYMBOL = "BTC-261225-85000-C"
ID = InstrumentId.from_str(SYMBOL + ".BINANCE")
ACCOUNT = AccountId("BINANCE-OPTIONS-001")
TRADER = TraderId("OPTEXEC-001")
BASE = 1793145600000


def order(*, key="order-1", post_only=False, reduce_only=False, price="1000.000", tif=TimeInForce.GTC):
    return LimitOrder(TRADER, StrategyId("Test-001"), ID, ClientOrderId(key), OrderSide.BUY,
                      Quantity.from_str("0.10"), Price.from_str(price), tif, post_only, reduce_only,
                      False, UUID4(), BASE * 1_000_000)


def client(monkeypatch):
    monkeypatch.setattr(options, "get", lambda *a, **k: exchange_info())
    item = options.instrument(options.markets()[0])
    clock = SimpleNamespace(timestamp_ns=lambda: (BASE + 60000) * 1_000_000)
    quote = QuoteTick(ID, Price.from_str("995.000"), Price.from_str("1005.000"), Quantity.from_str("1.00"),
                      Quantity.from_str("1.00"), clock.timestamp_ns(), clock.timestamp_ns())
    cache = SimpleNamespace(quote=lambda i: quote, orders=lambda **kw: [], client_order_id=lambda venue_id: None)
    config = BinanceOptionsExecutionClientConfig(account_id=ACCOUNT, symbols=[SYMBOL], api_key=KEY, api_secret=SECRET)
    value = BinanceOptionsExecutionClient(name="BINANCE_OPTIONS", config=config, cache=cache, clock=clock, trader_id=TRADER)
    value.instruments[SYMBOL] = item
    value._closing, value._ready, value._last_sync = False, True, clock.timestamp_ns()
    value._poller = SimpleNamespace(done=lambda: False)
    value._last_account = clock.timestamp_ns()
    return value


def row(value, native, *, status="NEW", filled="0.00"):
    return {"symbol": SYMBOL, "orderId": 9007199254740993, "clientOrderId": value._wire(native.client_order_id),
            "side": "BUY", "type": "LIMIT", "timeInForce": "GTX" if native.is_post_only else str(native.time_in_force),
            "quantity": "0.10", "price": str(native.price), "executedQty": filled,
            "avgPrice": "1000.000" if filled != "0.00" else "0", "status": status,
            "reduceOnly": native.is_reduce_only, "postOnly": native.is_post_only,
            "createTime": BASE, "updateTime": BASE + 1000}


@pytest.mark.parametrize("post_only,reduce_only", [(False, False), (True, False), (False, True)])
def test_single_submit_maps_flags_and_queries_authoritative_state(monkeypatch, post_only, reduce_only):
    async def scenario():
        value, native = client(monkeypatch), order(post_only=post_only, reduce_only=reduce_only)
        calls, events = [], []

        async def call(method, endpoint, **params):
            calls.append((method, endpoint, params))
            if method == 'GET' and not any(c[0] == 'POST' for c in calls):
                raise OptionsOrderError(code=-2013, unknown=False)
            return row(value, native)

        monkeypatch.setattr(value, "_call", call)
        monkeypatch.setattr(value, "generate_order_submitted", lambda *args: events.append("submitted"))
        monkeypatch.setattr(value, "_handle_report", lambda report, fills: events.append(report))
        await value._submit_order(SimpleNamespace(order=native, params=None))
        await value._submit_order(SimpleNamespace(order=native, params=None))
        assert len([c for c in calls if c[0] == "POST"]) == 1
        params = next(c[2] for c in calls if c[0] == "POST")
        assert params["reduceOnly"] == str(reduce_only).lower()
        assert params["postOnly"] == str(post_only).lower()
        assert params["timeInForce"] == ("GTX" if post_only else "GTC")
        assert params["quantity"] == "0.10" and len(params["clientOrderId"]) == 35
        assert events[1].client_order_id == native.client_order_id
        assert events[1].post_only == post_only and events[1].reduce_only == reduce_only
        assert value.execution_status["unresolved_orders"] == []
        assert SECRET not in repr(value.config) and KEY not in repr(value.config)
    asyncio.run(scenario())


def test_unknown_submit_never_reposts_and_blocks_new_risk(monkeypatch):
    async def scenario():
        value, native = client(monkeypatch), order()
        calls, rejected = [], []

        async def call(method, endpoint, **params):
            calls.append(method)
            if len(calls) == 1:
                raise OptionsOrderError(code=-2013, unknown=False)
            raise OptionsOrderError(unknown=True)

        monkeypatch.setattr(value, "_call", call)
        monkeypatch.setattr(value, "generate_order_submitted", lambda *a: None)
        monkeypatch.setattr(value, "generate_order_rejected", lambda *a: rejected.append(a))
        monkeypatch.setattr(value, "generate_order_denied", lambda *a: rejected.append(a))
        await value._submit_order(SimpleNamespace(order=native, params=None))
        with pytest.raises(OptionsOrderError):
            await value._submit_order(SimpleNamespace(order=native, params=None))
        await value._submit_order(SimpleNamespace(order=order(key="order-2"), params=None))
        assert calls.count("POST") == 1 and len(rejected) == 1
        assert value.execution_status["unresolved_orders"] == ["order-1"]
        recovered = client(monkeypatch)
        assert recovered._wire(native.client_order_id) == value._wire(native.client_order_id)
    asyncio.run(scenario())


def test_timeout_with_found_order_recovers_without_rejection(monkeypatch):
    async def scenario():
        value, native = client(monkeypatch), order()
        reports, attempts = [], []

        async def call(method, endpoint, **params):
            attempts.append(method)
            if len(attempts) == 1:
                raise OptionsOrderError(code=-2013, unknown=False)
            if method == "POST":
                raise OptionsOrderError(unknown=True)
            return row(value, native)

        monkeypatch.setattr(value, "_call", call)
        monkeypatch.setattr(value, "generate_order_submitted", lambda *a: None)
        monkeypatch.setattr(value, "_handle_report", lambda report, fills: reports.append(report))
        await value._submit_order(SimpleNamespace(order=native, params=None))
        assert reports[0].order_status == OrderStatus.ACCEPTED and not value._uncertain
    asyncio.run(scenario())


@pytest.mark.parametrize("price,tif,post_only", [("1000.001", TimeInForce.GTC, False), ("1001.000", TimeInForce.GTC, False),
                                             ("1000.000", TimeInForce.DAY, False), ("1000.000", TimeInForce.IOC, True)])
def test_invalid_orders_never_reach_transport(monkeypatch, price, tif, post_only):
    async def scenario():
        value = client(monkeypatch)
        denied = []
        monkeypatch.setattr(value, "generate_order_denied", lambda *args: denied.append(args))
        native = order(price=price, tif=tif, post_only=post_only)
        await value._submit_order(SimpleNamespace(order=native, params=None))
        assert len(denied) == 1 and not value._sent
    asyncio.run(scenario())


def test_stale_quotes_block_new_risk_but_not_reduce_only(monkeypatch):
    value = client(monkeypatch)
    value.cache.quote = lambda i: None
    with pytest.raises(ValueError, match="报价"):
        value._validate(order())
    assert value._validate(order(reduce_only=True)).id == ID


def test_stale_account_or_dead_poller_blocks_new_risk(monkeypatch):
    value = client(monkeypatch)
    value._last_account -= 16_000_000_000
    with pytest.raises(ValueError, match="尚未核对"):
        value._validate(order())
    assert value._validate(order(reduce_only=True)).id == ID
    value._last_account = value.clock.timestamp_ns()
    value._poller = SimpleNamespace(done=lambda: True)
    assert not value.execution_status["ready"]


def test_contract_price_bounds_and_order_identity_are_preserved(monkeypatch):
    value, native = client(monkeypatch), order()
    packet = exchange_info()
    packet["optionSymbols"][0]["filters"][0].update(minPrice="5.000", maxPrice="1500.000")
    monkeypatch.setattr(options, "get", lambda *a, **k: packet)
    item = options.instrument(options.markets()[0])
    value.instruments[SYMBOL] = item
    assert str(item.min_price) == "5.000" and str(item.max_price) == "1500.000"
    with pytest.raises(ValueError, match="最小变动单位"):
        value._validate(order(price="1505.000"))
    evidence = row(value, native)
    value._order_report(evidence, expected=native)
    with pytest.raises(ValueError, match="不同交易所订单"):
        value._order_report(evidence | {"orderId": 8}, expected=native)
    with pytest.raises(ValueError, match="参数冲突"):
        value._order_report(evidence | {"reduceOnly": True}, expected=native)


def test_fill_history_nanosecond_boundary_and_rebate_sign(monkeypatch):
    async def scenario():
        value = client(monkeypatch)
        rows = [{"symbol": SYMBOL, "tradeId": i + 1, "orderId": 1, "price": "1000.000", "quantity": "0.10",
                 "fee": "0.01000000", "side": "BUY", "liquidity": "MAKER", "time": BASE + i, "quoteAsset": "USDT"}
                for i in range(2)]

        async def call(*args, **kwargs):
            return rows

        monkeypatch.setattr(value, "_call", call)
        fills = await value._fills(value.instruments[SYMBOL], BASE * 1_000_000 + 1, (BASE + 2) * 1_000_000)
        assert len(fills) == 1 and str(fills[0].trade_id) == "2"
        assert fills[0].commission.as_decimal() == Decimal("-0.01")
    asyncio.run(scenario())


def test_preflight_existing_order_is_recovered_without_post(monkeypatch):
    async def scenario():
        value, native = client(monkeypatch), order()
        calls, reports = [], []

        async def call(method, endpoint, **params):
            calls.append(method)
            return row(value, native)

        monkeypatch.setattr(value, "_call", call)
        monkeypatch.setattr(value, "generate_order_submitted", lambda *a: None)
        monkeypatch.setattr(value, "_handle_report", lambda report, fills: reports.append(report))
        await value._submit_order(SimpleNamespace(order=native, params=None))
        assert "POST" not in calls and reports[0].client_order_id == native.client_order_id
    asyncio.run(scenario())


def test_stop_during_preflight_never_sends_new_order(monkeypatch):
    async def scenario():
        value, native = client(monkeypatch), order()
        calls, denied = [], []

        async def call(method, endpoint, **params):
            calls.append(method)
            value._closing = True
            raise OptionsOrderError(code=-2013, unknown=False)

        monkeypatch.setattr(value, "_call", call)
        monkeypatch.setattr(value, "generate_order_denied", lambda *a: denied.append(a))
        await value._submit_order(SimpleNamespace(order=native, params=None))
        assert calls == ["GET"] and denied and not value._sent
    asyncio.run(scenario())


@pytest.mark.parametrize("status", [None, "NEW", "FILLED"])
def test_cancel_unknown_open_or_fill_race_never_invents_canceled(monkeypatch, status):
    async def scenario():
        value, native = client(monkeypatch), order(reduce_only=True)
        native.apply(OrderSubmitted(TRADER, native.strategy_id, ID, native.client_order_id, ACCOUNT, UUID4(), BASE * 1_000_000, BASE * 1_000_000))
        value.cache.order = lambda key: native
        reports, rejected, calls = [], [], []

        async def call(method, endpoint, **params):
            calls.append(method)
            if method == "DELETE" or status is None:
                raise OptionsOrderError(unknown=True)
            if endpoint == "userTrades":
                return [{"symbol": SYMBOL, "tradeId": 1, "orderId": 9007199254740993, "price": "1000.000", "quantity": "0.10",
                         "fee": "-0.01", "side": "BUY", "liquidity": "TAKER", "time": BASE + 1000, "quoteAsset": "USDT"}]
            return row(value, native, status=status, filled="0.10" if status == "FILLED" else "0.00")

        monkeypatch.setattr(value, "_call", call)
        monkeypatch.setattr(value, "_handle_report", lambda report, fills: reports.append((report, fills)))
        monkeypatch.setattr(value, "generate_order_cancel_rejected", lambda *a: rejected.append(a))
        await value._cancel_order(SimpleNamespace(client_order_id=native.client_order_id, instrument_id=ID,
                                                  strategy_id=native.strategy_id, venue_order_id=None))
        assert calls.count("DELETE") == 1
        assert not reports if status is None else reports[0][0].order_status != OrderStatus.CANCELED
        assert bool(rejected) == (status == "NEW")
        assert bool(value._uncertain) == (status is None)
        if status == "FILLED":
            assert len(reports[0][1]) == 1 and reports[0][1][0].commission.as_decimal() == Decimal("0.01")
    asyncio.run(scenario())


def test_truncated_startup_history_fails_closed(monkeypatch):
    async def scenario():
        value = client(monkeypatch)
        value._sync_lock = asyncio.Lock()

        async def call(method, endpoint, **params):
            return [{}] * 1000 if endpoint == "historyOrders" else []

        monkeypatch.setattr(value, "_call", call)
        with pytest.raises(ValueError, match="截断"):
            await value._generate_mass_status(60)
        assert not value.execution_status["ready"] and not value._ready
    asyncio.run(scenario())


def test_write_cancellation_joins_the_already_running_sender(monkeypatch):
    async def scenario():
        transport = BinanceOptionsOrderTransport(KEY, SECRET)
        started, release = threading.Event(), threading.Event()
        completed = []

        def request(*a):
            started.set()
            assert release.wait(2)
            completed.append(True)
            return {"orderId": 1}

        monkeypatch.setattr(transport, "_request", request)
        task = asyncio.create_task(transport.send("POST", "order"))
        while not started.is_set():
            await asyncio.sleep(.01)
        task.cancel()
        await asyncio.sleep(.01)
        assert not task.done()
        release.set()
        assert await task == {"orderId": 1} and completed == [True]
    asyncio.run(scenario())


@pytest.mark.parametrize("method,status,code", [("POST", 500, -1007), ("POST", 429, -1003), ("DELETE", 503, -1006), ("POST", 400, -1111)])
def test_signed_writes_are_exact_and_never_retried(monkeypatch, method, status, code):
    transport = BinanceOptionsOrderTransport(KEY, SECRET)
    calls = []
    monkeypatch.setattr(time, "sleep", lambda x: None)

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, limit):
            return json.dumps({"serverTime": BASE}).encode()

    def open_request(request, timeout):
        assert timeout == 5
        if request.full_url.endswith("/time"):
            assert not request.headers.get("X-mbx-apikey")
            return Response()
        calls.append(request)
        encoded = request.data.decode() if method == "POST" else request.full_url.split("?", 1)[1]
        query, signature = encoded.rsplit("&signature=", 1)
        assert hmac.compare_digest(signature, hmac.new(SECRET.encode(), query.encode(), hashlib.sha256).hexdigest())
        assert request.get_method() == method and parse_qs(query)["clientOrderId"] == ["sa-test"]
        raise HTTPError(request.full_url, status, SECRET, {}, io.BytesIO(json.dumps({"code": code, "msg": SECRET}).encode()))

    transport._opener = SimpleNamespace(open=open_request)
    params = {"symbol": SYMBOL, "clientOrderId": "sa-test"}
    if method == "POST":
        params |= {"side": "BUY", "type": "LIMIT", "quantity": "0.10", "price": "1000.000", "timeInForce": "GTC",
                   "postOnly": "false", "reduceOnly": "true", "newOrderRespType": "RESULT"}
    with pytest.raises(OptionsOrderError) as failure:
        transport._request(method, "order", params)
    assert len(calls) == 1 and failure.value.unknown == (code != -1111)
    assert SECRET not in str(failure.value) and "signature" not in str(failure.value)


def test_native_node_partial_fill_cancel_commission_and_shutdown(monkeypatch):
    async def scenario():
        packet = exchange_info()
        packet["serverTime"] = int(time.time() * 1000)
        monkeypatch.setattr(options, "get", lambda *a, **k: packet)
        definition = options.markets()[0]
        monkeypatch.setattr(options, "markets", lambda: [definition])
        records, calls, fills = {}, [], []
        completed = asyncio.Event()
        clients = []

        async def send(self, method, endpoint, **params):
            calls.append((method, endpoint, params))
            now = int(time.time() * 1000)
            if endpoint == "marginAccount":
                snapshot = assets()
                snapshot['asset'][0].update(marginBalance='10000', equity='10000', available='9000', initialMargin='1000', maintMargin='100')
                return snapshot
            if endpoint in {"openOrders", "historyOrders", "position"}:
                return []
            if endpoint == "userTrades":
                return [{"symbol": SYMBOL, "tradeId": 7, "orderId": 123, "price": "1000.000", "quantity": "0.05",
                         "fee": "-0.01000000", "side": "BUY", "liquidity": "MAKER", "time": now - 1,
                         "quoteAsset": "USDT"}] if fills else []
            if endpoint == 'order' and method == 'GET' and not records:
                raise OptionsOrderError(code=-2013, unknown=False)
            if method == "POST":
                records.update(symbol=SYMBOL, orderId=123, clientOrderId=params["clientOrderId"], side="BUY", type="LIMIT",
                               timeInForce=params["timeInForce"], quantity="0.10", price="1000.000", executedQty="0.00",
                               avgPrice="0", status="NEW", reduceOnly=False, postOnly=False, createTime=now, updateTime=now)
            elif method == "DELETE":
                records.update(status="CANCELED", updateTime=now)
            elif endpoint == "order" and records["status"] == "NEW" and any(c[1] == "userTrades" for c in calls):
                records.update(status="PARTIALLY_FILLED", executedQty="0.05", avgPrice="1000.000", updateTime=now)
                fills.append(True)
            return dict(records)

        monkeypatch.setattr(BinanceOptionsOrderTransport, "send", send)

        class Data(BinanceOptionsDataClient):
            async def _connect(self):
                await super()._connect()
                now = self.clock.timestamp_ns()
                self._handle_data(QuoteTick(ID, Price.from_str("995.000"), Price.from_str("1005.000"),
                                            Quantity.from_str("1.00"), Quantity.from_str("1.00"), now, now))

        class DataFactory(BinanceOptionsDataClientFactory):
            @staticmethod
            def create(**kwargs):
                return Data(**kwargs)

        class ExecFactory(BinanceOptionsExecutionClientFactory):
            @staticmethod
            def create(**kwargs):
                value = BinanceOptionsExecutionClientFactory.create(**kwargs)
                clients.append(value)
                return value

        events = []

        class Test(Strategy):
            def on_start(self):
                self.test_order = self.order_factory.limit(ID, OrderSide.BUY, Quantity.from_str("0.10"), Price.from_str("1000.000"))
                self.submit_order(self.test_order, client_id=clients[0].client_id)

            def on_order_denied(self, event):
                events.append(event)
                completed.set()

            def on_order_rejected(self, event):
                events.append(event)
                completed.set()

            def on_order_accepted(self, event):
                events.append(event)

            def on_order_filled(self, event):
                events.append(event)
                self.cancel_order(event.client_order_id, client_id=clients[0].client_id)

            def on_order_canceled(self, event):
                events.append(event)
                completed.set()

        builder = LiveNode.builder("options-execution-test", TRADER, Environment.LIVE)
        builder.with_logging(LoggerConfig(stdout_level=LogLevel.OFF))
        builder.with_delay_post_stop_secs(0).with_delay_shutdown_secs(0)
        builder.with_timeout_connection(10).with_timeout_reconciliation(10).with_timeout_disconnection_secs(15)
        builder.with_exec_engine_config(LiveExecutionEngineConfig(reconciliation=True, reconciliation_startup_delay_secs=0,
                                                                 reconciliation_lookback_mins=60, inflight_check_interval_ms=0))
        builder.add_data_client("BINANCE_OPTIONS", DataFactory(), BinanceOptionsDataClientConfig(symbols=[SYMBOL]))
        builder.add_exec_client("BINANCE_OPTIONS", ExecFactory(), BinanceOptionsExecutionClientConfig(
            account_id=ACCOUNT, symbols=[SYMBOL], api_key=KEY, api_secret=SECRET))
        node = builder.build()
        strategy = Test()
        node.add_strategy(strategy)
        cache, handle = node.cache, node.handle()
        task = asyncio.create_task(node.run_async())
        try:
            await asyncio.wait_for(completed.wait(), 15)
            cached = cache.order(strategy.test_order.client_order_id)
            assert cached.status == OrderStatus.CANCELED and str(cached.filled_qty) == "0.05", (str(cached), [str(e) for e in events], clients[0].execution_status)
            assert sum(type(e).__name__ == "OrderFilled" for e in events) == 1
            filled = next(e for e in events if type(e).__name__ == "OrderFilled")
            assert filled.commission.as_decimal() == Decimal("0.01")
            assert len([c for c in calls if c[0] == "POST"]) == 1
            assert len([c for c in calls if c[0] == "DELETE"]) == 1
        finally:
            handle.stop()
            await asyncio.wait_for(task, 20)
            node.dispose()
        assert clients[0]._poller is None
    asyncio.run(scenario())
    gc.collect()
