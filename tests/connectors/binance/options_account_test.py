"""Only GET requests, exact values, bounded pages, and secret-safe failures."""

import asyncio
import hashlib
import hmac
import json
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

import pytest

from engine.connectors.binance import options, options_account
from engine.connectors.binance.options_account import BinanceOptionsAccountClient
from engine.connectors.binance.queries import QueryError

SYMBOL = "BTC-261225-85000-C"
KEY, SECRET = "fictional-options-api-key", "fictional-options-secret"


def order():
    return {"symbol": SYMBOL, "orderId": 9007199254740993, "clientOrderId": "example-1", "price": "1000.000",
            "quantity": "0.25", "executedQty": "0.10", "side": "BUY", "status": "PARTIALLY_FILLED"}


def position():
    return {"symbol": SYMBOL, "quantity": "-0.25", "entryPrice": "1000.000", "markPrice": "1050.000",
            "unrealizedPNL": "-12.5000", "side": "SHORT"}


def assets():
    return {"asset": [{"asset": "USDT", "marginBalance": "100.00001", "equity": "99.00001",
                       "available": "90.00001", "initialMargin": "9", "maintMargin": "1", "unrealizedPNL": "-1"}]}


def test_signing_uses_server_time_get_only_and_preserves_large_order_id(monkeypatch):
    client = BinanceOptionsAccountClient(KEY, SECRET)
    seen = []
    monkeypatch.setattr(options, "get", lambda endpoint: {"serverTime": 1793145600000})
    monkeypatch.setattr(options_account.time, "sleep", lambda delay: None)

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, limit):
            assert limit == 8 * 1024 * 1024 + 1
            return json.dumps(order()).encode()

    def open_request(request, timeout):
        assert request.get_method() == "GET" and timeout == 15
        assert request.headers["X-mbx-apikey"] == KEY
        query, signature = urlsplit(request.full_url).query.rsplit("&signature=", 1)
        assert hmac.compare_digest(signature, hmac.new(SECRET.encode(), query.encode(), hashlib.sha256).hexdigest())
        assert parse_qs(query)["timestamp"] == ["1793145600000"]
        seen.append(request)
        return Response()

    client._opener = SimpleNamespace(open=open_request)
    value = asyncio.run(client.order(SYMBOL, order_id=9007199254740993))
    assert value["orderId"] == 9007199254740993 and value["price"] == "1000.000"
    assert len(seen) == 1 and KEY not in repr(client) and SECRET not in repr(client)
    with pytest.raises(ValueError):
        client._get("order", side="BUY")
    with pytest.raises(ValueError):
        client._get("listenKey")


def test_readonly_account_assets_positions_and_explicit_order_pages(monkeypatch):
    client = BinanceOptionsAccountClient(KEY, SECRET)
    seen = []

    def get(endpoint, **params):
        seen.append((endpoint, params))
        return {"marginAccount": assets(), "position": [position()], "openOrders": [order()],
                "historyOrders": [order()], "order": order()}[endpoint]

    monkeypatch.setattr(client, "_get", get)
    assert asyncio.run(client.assets())["asset"][0]["equity"] == "99.00001"
    assert asyncio.run(client.positions(SYMBOL))[0]["quantity"] == "-0.25"
    page = asyncio.run(client.open_orders(SYMBOL, limit=1))
    assert page["possibly_truncated"] and len(page["orders"]) == 1
    monkeypatch.setattr(options_account.time, "time", lambda: 1793145600)
    page = asyncio.run(client.order_history(SYMBOL, 1793145500000, 1793145600000, limit=1, order_id=100))
    assert page["possibly_truncated"]
    assert seen[-1][1]["orderId"] == 100
    with pytest.raises(ValueError):
        asyncio.run(client.order_history(SYMBOL, 0, 1793145600000))
    assert len(seen) == 4


@pytest.mark.parametrize("packet", [[{**position(), "side": "LONG"}], [{**position(), "quantity": "NaN"}],
                                   [position(), position()], [{**position(), "symbol": "BTCUSDT"}], {}])
def test_invalid_positions_are_not_returned_as_valid_empty_accounts(monkeypatch, packet):
    client = BinanceOptionsAccountClient(KEY, SECRET)
    monkeypatch.setattr(client, "_get", lambda *a, **k: packet)
    with pytest.raises(QueryError):
        asyncio.run(client.positions())


def test_empty_positions_valid_but_missing_asset_fields_rejected(monkeypatch):
    client = BinanceOptionsAccountClient(KEY, SECRET)
    monkeypatch.setattr(client, "_get", lambda *a, **k: [])
    assert asyncio.run(client.positions()) == []
    packet = assets()
    del packet["asset"][0]["equity"]
    monkeypatch.setattr(client, "_get", lambda *a, **k: packet)
    with pytest.raises(QueryError):
        asyncio.run(client.assets())


@pytest.mark.parametrize("status", [302, 401, 403, 429, 500])
def test_signed_failures_are_sanitized_and_retries_bounded(monkeypatch, status):
    client = BinanceOptionsAccountClient(KEY, SECRET)
    monkeypatch.setattr(options, "get", lambda endpoint: {"serverTime": 1793145600000})
    delays, attempts = [], []
    monkeypatch.setattr(options_account.time, "sleep", delays.append)

    def fail(request, timeout):
        attempts.append(True)
        raise HTTPError(request.full_url, status, SECRET, {"Retry-After": "1000"}, None)

    client._opener = SimpleNamespace(open=fail)
    with pytest.raises(QueryError) as failure:
        asyncio.run(client.assets())
    assert SECRET not in str(failure.value) and KEY not in str(failure.value) and "signature=" not in str(failure.value)
    assert len(attempts) == (3 if status in {429, 500} else 1)
    assert max(delays) <= 30


def test_signed_redirect_handler_never_forwards_credentials():
    assert options_account._NoRedirect().redirect_request(None, None, 302, "redirect", {}, "https://elsewhere.example") is None
    with pytest.raises(ValueError):
        BinanceOptionsAccountClient(KEY, SECRET, environment="sandbox")
    with pytest.raises(ValueError):
        BinanceOptionsAccountClient(KEY, KEY)


def trade(stamp, identity=1):
    return {"symbol": SYMBOL, "tradeId": identity, "orderId": 9007199254740993,
            "side": "BUY", "type": "LIMIT", "quantity": "0.25", "price": "1000.000001",
            "fee": "-0.00001", "quoteAsset": "USDT", "time": stamp}


async def history_rows(client, endpoint, first, last, **params):
    return [page async for page in client.history_pages(endpoint, first, last, **params)]


def test_readonly_trade_pages_split_time_without_losing_or_double_counting_records(monkeypatch):
    client = BinanceOptionsAccountClient(KEY, SECRET)
    now = 1793145600000
    monkeypatch.setattr(options_account.time, "time", lambda: now / 1000)
    rows = [trade(now - 10 if identity < 500 else now, identity) for identity in range(1000)]
    calls = []

    def get(endpoint, **params):
        calls.append((endpoint, params))
        assert endpoint == "userTrades" and params["symbol"] == SYMBOL and params["limit"] == 1000
        return [row for row in rows if params["startTime"] <= row["time"] <= params["endTime"]]

    monkeypatch.setattr(client, "_get", get)
    pages = asyncio.run(history_rows(client, "userTrades", now - 10, now, symbol=SYMBOL))
    assert sum(len(page["rows"]) for page in pages) == 1000 and len(calls) == 3
    assert pages[0]["end_ms"] + 1 == pages[1]["start_ms"]
    assert pages[-1]["rows"][-1]["orderId"] == 9007199254740993
    with pytest.raises(ValueError):
        asyncio.run(history_rows(client, "userTrades", now - 1, now))
    with pytest.raises(ValueError):
        asyncio.run(history_rows(client, "bill", now - 1, now, symbol=SYMBOL))


@pytest.mark.parametrize("change", [{"quantity": "NaN"}, {"quantity": "0"}, {"price": "-1"},
                                   {"side": "UNKNOWN"}, {"quoteAsset": ""}, {"orderId": "1.1"},
                                   {"tradeId": True}, {"time": 1}, {"symbol": "ETH-261225-2500-P"}])
def test_readonly_trade_history_rejects_invalid_or_mismatched_evidence(monkeypatch, change):
    client = BinanceOptionsAccountClient(KEY, SECRET)
    now = 1793145600000
    monkeypatch.setattr(options_account.time, "time", lambda: now / 1000)
    monkeypatch.setattr(client, "_get", lambda *a, **k: [{**trade(now), **change}])
    with pytest.raises(QueryError, match="未确认覆盖"):
        asyncio.run(history_rows(client, "userTrades", now - 1, now, symbol=SYMBOL))


def test_readonly_history_does_not_acknowledge_saturated_milliseconds(monkeypatch):
    client = BinanceOptionsAccountClient(KEY, SECRET)
    now = 1793145600000
    monkeypatch.setattr(options_account.time, "time", lambda: now / 1000)
    monkeypatch.setattr(client, "_get", lambda *a, **k: [trade(now, identity) for identity in range(1000)])
    with pytest.raises(QueryError, match="单毫秒满页"):
        asyncio.run(history_rows(client, "userTrades", now, now, symbol=SYMBOL))


def test_readonly_bill_and_exercise_queries_keep_signed_amounts_and_direction(monkeypatch):
    client = BinanceOptionsAccountClient(KEY, SECRET)
    now = 1793145600000
    monkeypatch.setattr(options_account.time, "time", lambda: now / 1000)
    seen = []
    bill = {"id": 9007199254740993, "asset": "USDT", "amount": "-0.00000001", "type": "TRANSFER", "createDate": now}
    exercise = {"id": "9007199254740994", "symbol": SYMBOL, "positionSide": "SHORT", "quantity": "0.25",
                "exercisePrice": "85000", "amount": "-100", "fee": "-1", "quoteAsset": "USDT", "createDate": now}

    def get(endpoint, **params):
        seen.append((endpoint, params))
        return [bill if endpoint == "bill" else exercise]

    monkeypatch.setattr(client, "_get", get)
    assert asyncio.run(history_rows(client, "bill", now, now))[0]["rows"] == [bill]
    assert seen[-1][1]["currency"] == "USDT" and "symbol" not in seen[-1][1]
    assert asyncio.run(history_rows(client, "exerciseRecord", now, now))[0]["rows"] == [exercise]
    assert "symbol" not in seen[-1][1]
    exercise["quantity"] = "-1"
    with pytest.raises(QueryError):
        asyncio.run(history_rows(client, "exerciseRecord", now, now))
