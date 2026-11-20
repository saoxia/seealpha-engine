"""Real native order dispatch against a local exchange with fictitious keys only."""

import asyncio
import json
import threading
import time
from http.server import ThreadingHTTPServer
from typing import ClassVar
from urllib.parse import parse_qs, urlparse

import pytest
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from engine.connectors.binance.manual import ManualValidationError, exact, execute, resolve_command
from tests.connectors.binance.account_queries_test import AccountHandler


def test_exact_precision_never_rounds_orders():
    assert str(exact("0.010", "0.001", "0.001", "100")) == "0.010"
    for value in ("NaN", "Infinity", "0", "-1", "0.0001", "1000"):
        with pytest.raises(ValueError):
            exact(value, "0.001", "0.001", "100")


@pytest.mark.parametrize("action", ["snapshot", "limit", "market", "invalid", "rejected", "denied", "cancel", "hedge", "hedge_close_long", "hedge_close_short", "stale", "reduce"])
def test_native_manual_request(action, capfd):
    class Handler(AccountHandler):
        sent: ClassVar[list] = []
        position_rows: ClassVar[list] = []
        orders: ClassVar[list] = []

        def do_GET(self):
            if action.startswith('hedge') and urlparse(self.path).path.endswith('/positionSide/dual'):
                self.reply({'dualSidePosition': True})
            elif urlparse(self.path).path.endswith('/symbolConfig'):
                self.reply([{'symbol': 'ETHUSDT', 'leverage': 5}])
            elif action == 'cancel' and urlparse(self.path).path.endswith(('/openOrders', '/allOrders')):
                self.reply(self.orders)
            else:
                super().do_GET()

        def do_DELETE(self):
            if urlparse(self.path).path.endswith('/order'):
                params = parse_qs(urlparse(self.path).query)
                self.sent.append(params)
                self.reply({**self.orders[0], 'status': 'CANCELED'})
                self.notify({'newClientOrderId': params['origClientOrderId'],
                             'side': ['BUY'], 'type': ['LIMIT'], 'quantity': ['0.010'], 'price': ['2000.00']}, 'CANCELED')
            else:
                super().do_DELETE()

        def do_POST(self):
            if urlparse(self.path).path.endswith('/order'):
                params = parse_qs(urlparse(self.path).query)
                if not params:
                    params = parse_qs(self.rfile.read(int(self.headers.get('Content-Length', '0'))).decode())
                self.sent.append(params)
                if action == 'rejected':
                    raw = json.dumps({'code': -2010, 'msg': 'fixture rejected'}).encode()
                    self.send_response(400)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    return
                self.reply({
                    'orderId': 123, 'symbol': 'ETHUSDT', 'status': 'NEW',
                    'clientOrderId': params['newClientOrderId'][0], 'price': params.get('price', ['0'])[0],
                    'avgPrice': '0', 'origQty': params['quantity'][0], 'executedQty': '0',
                    'cumQty': '0', 'cumQuote': '0', 'timeInForce': params.get('timeInForce', ['GTC'])[0],
                    'type': params['type'][0], 'reduceOnly': False, 'closePosition': False,
                    'side': params['side'][0], 'positionSide': 'BOTH', 'stopPrice': '0',
                    'workingType': 'CONTRACT_PRICE', 'priceProtect': False,
                    'origType': params['type'][0], 'priceMatch': 'NONE',
                    'selfTradePreventionMode': 'NONE', 'goodTillDate': 0,
                    'updateTime': int(time.time() * 1000),
                })
                self.notify(params)
            else:
                super().do_POST()

    async def scenario():
        if action.startswith('hedge_close'):
            row = {"symbol": "ETHUSDT", "entryPrice": "2000", "breakEvenPrice": "2001", "markPrice": "2010",
                   "unRealizedProfit": "0", "liquidationPrice": "0", "leverage": "5", "marginType": "cross",
                   "isolatedMargin": "0", "isAutoAddMargin": "false", "notional": "20", "isolatedWallet": "0",
                   "updateTime": int(time.time()*1000), "marginAsset": "USDT", "maxNotionalValue": "100000"}
            Handler.position_rows = [{**row, "positionSide": "LONG", "positionAmt": "0.010"},
                                     {**row, "positionSide": "SHORT", "positionAmt": "-0.020"}]
        clients = []
        loop = asyncio.get_running_loop()
        async def notify(params, status="NEW"):
            now = int(time.time() * 1000)
            event = {'e': 'ORDER_TRADE_UPDATE', 'E': now, 'T': now, 'o': {
                's': 'ETHUSDT', 'c': params['newClientOrderId'][0], 'S': params['side'][0],
                'o': params['type'][0], 'f': 'GTC', 'q': params['quantity'][0],
                'p': params.get('price', ['0'])[0], 'ap': '0', 'sp': '0', 'x': status,
                'X': status, 'i': 123, 'l': '0', 'z': '0', 'L': '0', 'N': 'USDT', 'n': '0',
                'T': now, 't': 0, 'b': '0', 'a': '0', 'm': False, 'R': False,
                'wt': 'CONTRACT_PRICE', 'ot': params['type'][0], 'ps': 'SHORT' if action == 'hedge_close_short' else 'LONG' if action.startswith('hedge') else 'BOTH', 'cp': False,
                'rp': '0', 'pP': False, 'si': 0, 'ss': 0, 'V': 'NONE', 'pm': 'NONE', 'gtd': 0,
            }}
            for ws in clients:
                await ws.send(json.dumps(event))
        Handler.notify = staticmethod(lambda params, status="NEW": asyncio.run_coroutine_threadsafe(notify(params, status), loop))
        if action == 'cancel':
            Handler.orders = [{'orderId': 123, 'symbol': 'ETHUSDT', 'status': 'NEW',
                'clientOrderId': 'SA12345678901234567890123456789012', 'price': '2000.00',
                'avgPrice': '0', 'origQty': '0.010', 'executedQty': '0', 'cumQuote': '0',
                'timeInForce': 'GTC', 'type': 'LIMIT', 'reduceOnly': False,
                'closePosition': False, 'side': 'BUY', 'positionSide': 'BOTH', 'stopPrice': '0',
                'workingType': 'CONTRACT_PRICE', 'priceProtect': False, 'origType': 'LIMIT',
                'time': int(time.time()*1000), 'updateTime': int(time.time()*1000),
                'priceMatch': 'NONE', 'selfTradePreventionMode': 'NONE', 'goodTillDate': 0}]
        http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=http.serve_forever, daemon=True)
        thread.start()

        async def socket(ws):
            clients.append(ws)
            try:
                async for raw in ws:
                    request = json.loads(raw)
                    await ws.send(json.dumps({'id': request.get('id'), 'result': None}))
                    if any('bookTicker' in p for p in request.get('params', [])):
                        await ws.send(json.dumps({'e': 'bookTicker', 'u': 1, 's': 'ETHUSDT',
                                                  'b': '1999.00', 'B': '10.000',
                                                  'a': '2001.00', 'A': '10.000',
                                                  'T': int(time.time()*1000) - (60_000 if action == 'stale' else 0), 'E': int(time.time()*1000)}))
            except ConnectionClosed:
                pass
        try:
            async with serve(socket, '127.0.0.1', 0) as server:
                command = {'action': 'snapshot' if action == 'snapshot' else 'cancel' if action == 'cancel' else 'submit',
                           'client_order_id': 'SA12345678901234567890123456789012',
                           'order_type': 'market' if action == 'market' else 'limit',
                           'side': 'sell' if action == 'hedge_close_long' else 'buy', 'quantity': '0.0101' if action == 'invalid' else '1000' if action == 'denied' else '0.010',
                           'price': '2000.00', 'post_only': False, 'reduce_only': action == 'reduce',
                           'time_in_force': 'GTC', 'position_effect': 'close' if action.startswith('hedge_close') else 'open',
                           'expected_position_mode': 'hedge' if action.startswith('hedge') else 'oneway'}
                account = {'id': 1, 'environment': 'production', 'exchange': 'binance',
                           'account_name': 'fixture',
                           'credentials': {'apiKey': 'fictional-key', 'secret': 'fictional-secret'}}
                options = {'http_url': f'http://127.0.0.1:{http.server_port}',
                           'websocket_url': f'ws://127.0.0.1:{server.sockets[0].getsockname()[1]}',
                           'timeout': 12}
                if action == 'stale':
                    from engine.connectors.binance.queries import QueryError
                    with pytest.raises(QueryError):
                        await execute(account, 'ETHUSDT-PERP', command, **{**options, 'timeout': 2})
                    assert not Handler.sent
                elif action in {'invalid', 'reduce'}:
                    result = await execute(account, 'ETHUSDT-PERP', command, **options)
                    assert 'validation_error' in result
                else:
                    result = await execute(account, 'ETHUSDT-PERP', command, **options)
                    assert result['balances']['USDT']['total'] == '100.00000000'
                    assert result['leverage'] == (5 if action == 'snapshot' else None)
                    if action != 'snapshot':
                        assert result['result']['status'] == ('rejected' if action == 'rejected' else 'denied' if action == 'denied' else 'canceled' if action == 'cancel' else 'accepted'), result
                        if action == 'denied':
                            assert not Handler.sent
                        else:
                            assert len(Handler.sent) == 1
                        if action in {'limit', 'market', 'hedge', 'hedge_close_long', 'hedge_close_short'}:
                            assert Handler.sent[0]['type'] == ['MARKET' if action == 'market' else 'LIMIT']
                            assert Handler.sent[0].get('positionSide', ['BOTH']) == ['SHORT' if action == 'hedge_close_short' else 'LONG' if action.startswith('hedge') else 'BOTH']
                            if action.startswith('hedge_close'):
                                assert Handler.sent[0]['side'] == ['SELL' if action.endswith('long') else 'BUY']
                                assert 'reduceOnly' not in Handler.sent[0]
                        assert result['records']
                if action in {'snapshot', 'invalid'}:
                    assert not Handler.sent
        finally:
            http.shutdown()
            http.server_close()
            thread.join(2)
    asyncio.run(scenario())
    assert 'unsendable' not in capfd.readouterr().err


def test_partial_fill_does_not_finish_an_outstanding_cancellation(monkeypatch):
    from types import SimpleNamespace

    from engine.connectors.binance.manual import ManualCommand

    order = SimpleNamespace(is_closed=False)
    finished = []
    monkeypatch.setattr('engine.connectors.binance.manual.normalize_event', lambda *args: None)
    observer = SimpleNamespace(cache=SimpleNamespace(order=lambda _: order), account={},
                               command={'action': 'cancel'}, result='order', records=[],
                               finish=lambda: finished.append(True), outcome=None)
    event = type('OrderFilled', (), {'client_order_id': 'order'})()
    ManualCommand.capture_event(observer, event)
    assert not finished
    order.is_closed = True
    ManualCommand.capture_event(observer, event)
    assert finished == [True]


@pytest.mark.parametrize(("mode", "side", "close", "leg"), [
    ("oneway", "buy", False, "BOTH"), ("oneway", "sell", True, "BOTH"),
    ("hedge", "buy", False, "LONG"), ("hedge", "sell", False, "SHORT"),
    ("hedge", "buy", True, "SHORT"), ("hedge", "sell", True, "LONG"),
])
def test_actual_account_mode_resolves_open_and_close_legs(mode, side, close, leg):
    original = {"action": "submit", "side": side, "position_effect": "close" if close else "open"}
    result = resolve_command(original, mode)
    assert result["position_side"] == leg
    assert result["reduce_only"] is close
    assert "position_side" not in original


def test_changed_account_mode_and_incompatible_legacy_legs_fail_before_dispatch():
    for values in ({"expected_position_mode": "hedge"}, {"position_side": "LONG"}):
        with pytest.raises(ManualValidationError):
            resolve_command({"action": "submit", "side": "buy", **values}, "oneway")
    assert resolve_command({"action": "submit", "side": "buy", "reduce_only": True}, "hedge")["position_side"] == "SHORT"


@pytest.mark.parametrize("invalid", [False, True])
def test_account_context_uses_signed_readonly_endpoints_and_rejects_invalid_mode(invalid):
    import hashlib
    import hmac

    from engine.connectors.binance.manual import account_context
    from engine.connectors.binance.queries import QueryError

    class ContextHandler(AccountHandler):
        calls: ClassVar[list] = []

        def do_GET(self):
            parsed = urlparse(self.path)
            self.calls.append(parsed.path)
            if parsed.path.endswith("/time"):
                return self.reply({"serverTime": int(time.time()*1000)})
            unsigned, signature = parsed.query.rsplit("&signature=", 1)
            assert hmac.new(b"fixture-secret", unsigned.encode(), hashlib.sha256).hexdigest() == signature
            assert self.headers["X-MBX-APIKEY"] == "fixture-key"
            if parsed.path.endswith("/dual"):
                return self.reply({"dualSidePosition": "false" if invalid else True})
            if parsed.path.endswith("/symbolConfig"):
                assert parse_qs(parsed.query)["symbol"] == ["ETHUSDT"]
                return self.reply([{"symbol": "ETHUSDT", "leverage": 21}])
            self.reply([{"symbol": "ETHUSDT", "positionSide": "LONG", "positionAmt": "0.01",
                         "entryPrice": "2000", "breakEvenPrice": "2001", "liquidationPrice": "1500"}])

    http = ThreadingHTTPServer(("127.0.0.1",0),ContextHandler)
    thread = threading.Thread(target=http.serve_forever,daemon=True); thread.start()
    account = {"environment": "production", "credentials": {"apiKey": "fixture-key", "secret": "fixture-secret"}}
    try:
        if invalid:
            with pytest.raises(QueryError) as error:
                account_context(account,"ETHUSDT-PERP",http_url=f"http://127.0.0.1:{http.server_port}")
            assert "fixture-secret" not in str(error.value)
            assert ContextHandler.calls == ["/fapi/v1/time", "/fapi/v1/positionSide/dual"]
        else:
            result = account_context(account,"ETHUSDT-PERP",http_url=f"http://127.0.0.1:{http.server_port}")
            assert result["position_mode"] == "hedge"
            assert result["leverage"] == 21
            assert result["positions"]["LONG"]["breakEvenPrice"] == "2001"
            assert ContextHandler.calls == ["/fapi/v1/time", "/fapi/v1/positionSide/dual", "/fapi/v3/positionRisk", "/fapi/v1/symbolConfig"]
    finally:
        http.shutdown(); http.server_close(); thread.join(2)


@pytest.mark.parametrize("env", ["production", "sandbox"])
def test_picker_market_snapshots_are_public_bounded_and_keep_decimal_text(monkeypatch, env):
    from engine.connectors.binance.manual import market_tickers
    from engine.connectors.binance.transport import PublicREST

    calls = []

    def request(self, endpoint, params):
        calls.append((self.base_url, endpoint, params, self.attempts))
        if endpoint == "premiumIndex":
            return [{"symbol": "ETHUSDC", "lastFundingRate": "-0.00002520"}]
        return [{"symbol": "ETHUSDC", "lastPrice": "2225.330000", "priceChangePercent": "-6.760", "quoteVolume": "3700000000"},
                {"symbol": "BTCUSDT", "lastPrice": "75000", "priceChangePercent": "1", "quoteVolume": "100"}]

    monkeypatch.setattr(PublicREST, "_request", request)
    result = asyncio.run(market_tickers(env))
    assert result["rows"]["ETHUSDC"]["last_price"] == "2225.330000"
    assert result["rows"]["ETHUSDC"]["funding_rate"] == "-0.00002520"
    assert result["rows"]["BTCUSDT"]["funding_rate"] is None
    assert len(calls) == 2
    assert all(params == {} and attempts == 1 for _, _, params, attempts in calls)
    assert all(("demo-fapi" in url) == (env == "sandbox") for url, _, _, _ in calls)


@pytest.mark.parametrize("invalid", ["NaN", "-1"])
def test_picker_rejects_invalid_public_price_data(monkeypatch, invalid):
    from engine.connectors.binance.manual import market_tickers
    from engine.connectors.binance.queries import QueryError
    from engine.connectors.binance.transport import PublicREST

    def request(self, endpoint, params):
        return [] if endpoint == "premiumIndex" else [{"symbol": "BTCUSDT", "lastPrice": invalid, "priceChangePercent": "1", "quoteVolume": "100"}]

    monkeypatch.setattr(PublicREST, "_request", request)
    with pytest.raises(QueryError, match="行情列表读取失败"):
        asyncio.run(market_tickers("production"))


@pytest.mark.parametrize("config", [[], [{"symbol": "BTCUSDT", "leverage": 20}],
    [{"symbol": "ETHUSDT", "leverage": 0}], [{"symbol": "ETHUSDT", "leverage": True}],
    [{"symbol": "ETHUSDT", "leverage": "20"}],
    [{"symbol": "ETHUSDT", "leverage": 20}] * 2, None])
def test_optional_leverage_never_fabricates_a_value_or_blocks_snapshot(monkeypatch, config):
    from io import BytesIO

    from engine.connectors.binance.manual import account_context

    calls = []

    class Opener:
        def open(self, request, **kwargs):
            path = urlparse(request.full_url).path
            calls.append(path)
            if path.endswith('/time'):
                data = {"serverTime": int(time.time() * 1000)}
            elif path.endswith('/dual'):
                data = {"dualSidePosition": False}
            elif path.endswith('/positionRisk'):
                data = []
            elif config is None:
                raise OSError('private-fixture-secret')
            else:
                data = config
            return BytesIO(json.dumps(data).encode())

    monkeypatch.setattr('engine.connectors.binance.manual.build_opener', lambda *args: Opener())
    account = {"environment": "sandbox", "credentials": {"apiKey": "fixture-key", "secret": "fixture-secret"}}
    result = account_context(account, 'ETHUSDT-PERP')
    assert result == {"position_mode": "oneway", "positions": {}, "leverage": None}
    calls.clear()
    account_context(account, 'ETHUSDT-PERP', include_risk=False)
    assert calls == ['/fapi/v1/time', '/fapi/v1/positionSide/dual']


def test_native_failure_logs_only_type_and_location_without_private_exception_text(monkeypatch, caplog):
    from engine.connectors.binance.queries import QueryError

    async def fail(*args, **kwargs):
        raise RuntimeError("private-fixture-secret signed-url?signature=private-signature")

    monkeypatch.setattr('engine.connectors.binance.manual._run', fail)
    with pytest.raises(QueryError, match="交易节点失败"):
        asyncio.run(execute({}, 'ETHUSDT-PERP', {"action": "snapshot"}))
    assert "RuntimeError at fail:" in caplog.text
    assert "private-fixture-secret" not in caplog.text
    assert "private-signature" not in caplog.text


@pytest.mark.parametrize("boundary", ["manual", "account"])
def test_cause_only_native_traceback_is_released_on_its_worker_thread(monkeypatch, boundary):
    import gc

    from engine.connectors.binance.queries import QueryError

    created, released = [], []

    class ThreadBound:
        def __init__(self):
            created.append(threading.get_ident())

        def __del__(self):
            released.append(threading.get_ident())

    def native_failure():
        owner = ThreadBound()
        error = RuntimeError('private-native-cause')
        assert owner is not None
        raise error

    async def fail(*args, **kwargs):
        try:
            native_failure()
        except RuntimeError as error:
            cause = error
        failure = RuntimeError('private-hosted-run-failure')
        failure.__cause__ = cause
        failure.__context__ = None
        raise failure

    if boundary == "manual":
        monkeypatch.setattr('engine.connectors.binance.manual._run', fail)
    else:
        monkeypatch.setattr('engine.connectors.binance.queries._account_snapshot', fail)
    enabled = gc.isenabled()
    gc.disable()
    try:
        from engine.connectors.binance.queries import account_snapshot

        with pytest.raises(QueryError):
            asyncio.run(execute({}, 'ETHUSDT-PERP', {'action': 'snapshot'}) if boundary == "manual" else account_snapshot({}))
        assert created and created[0] != threading.get_ident()
        assert released == created
    finally:
        gc.collect()
        if enabled:
            gc.enable()
