"""No private session for valuation: account REST + native public marks coexist with reads."""

import asyncio
import json
import threading
import time
from contextlib import aclosing
from decimal import Decimal
from http.server import ThreadingHTTPServer
from urllib.parse import urlparse

import pytest
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from engine.connectors.binance import assets
from engine.connectors.binance.queries import QueryError, account_snapshot
from engine.connectors.binance.valuation import valuation_events
from tests.connectors.binance.account_queries_test import AccountHandler


class ValuationHandler(AccountHandler):
    balance = "100"

    def reply(self, data):
        if isinstance(data, dict) and "multiAssetsMargin" in data:
            data = {**data, "positions": self.position_rows}
            data["assets"][0]["walletBalance"] = self.balance
            data["totalWalletBalance"] = self.balance
            data["totalMarginBalance"] = self.balance
        super().reply(data)


def test_rest_account_events_public_marks_and_short_native_reads_coexist(monkeypatch, capfd):
    async def scenario():
        ValuationHandler.calls = []
        ValuationHandler.balance = "100"
        ValuationHandler.position_rows = [{
            "symbol": "ETHUSDT", "positionAmt": "0.010", "entryPrice": "2000", "markPrice": "2010",
            "unRealizedProfit": "0.1", "liquidationPrice": "0", "leverage": "5", "marginType": "cross",
            "isolatedMargin": "0", "isAutoAddMargin": "false", "positionSide": "BOTH", "notional": "20.1",
            "isolatedWallet": "0", "updateTime": int(time.time() * 1000), "marginAsset": "USDT",
            "maxNotionalValue": "100000",
        }]
        http = ThreadingHTTPServer(("127.0.0.1", 0), ValuationHandler)
        thread = threading.Thread(target=http.serve_forever, daemon=True)
        thread.start()
        http_url = f"http://127.0.0.1:{http.server_port}"
        monkeypatch.setitem(assets.BASES, "production", http_url)
        subscribed, changed = asyncio.Event(), asyncio.Event()
        market_socket = None

        async def websocket(ws):
            nonlocal market_socket
            try:
                async for raw in ws:
                    request = json.loads(raw)
                    await ws.send(json.dumps({"id": request.get("id"), "result": None}))
                    if any("@markPrice" in name for name in request.get("params", [])):
                        market_socket = ws
                        stamp = int(time.time() * 1000)
                        await ws.send(json.dumps({"e": "markPriceUpdate", "E": stamp, "s": "ETHUSDT",
                                                  "p": "2010", "i": "2010", "P": "2010", "r": "0.0001", "T": stamp + 60_000}))
                        subscribed.set()
                        await asyncio.wait_for(changed.wait(), 20)
                        stamp = int(time.time() * 1000)
                        await ws.send(json.dumps({"e": "markPriceUpdate", "E": stamp, "s": "ETHUSDT",
                                                  "p": "2020", "i": "2020", "P": "2020", "r": "0.0001", "T": stamp + 60_000}))
            except ConnectionClosed:
                pass

        try:
            async with serve(websocket, "127.0.0.1", 0) as server:
                websocket_url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
                account = {"id": 1, "environment": "production", "credentials": {"apiKey": "fixture-key", "secret": "fixture-secret"}}
                async with aclosing(valuation_events(account, http_url=http_url, websocket_url=websocket_url, timeout=12)) as events:
                    first = await asyncio.wait_for(anext(events), 15)
                    assert first["wallet"]["USDT"] == "100"
                    assert first["positions"][0]["quantity"] == "0.010"
                    await asyncio.wait_for(subscribed.wait(), 10)
                    while True:
                        point = await asyncio.wait_for(anext(events), 5)
                        if point["marks"]:
                            break
                    assert Decimal(point["marks"]["ETHUSDT-PERP.BINANCE"]["price"]) == 2010
                    # The valuation node itself never creates/keeps/deletes a private listenKey.
                    assert all(method == "GET" for method, _ in ValuationHandler.calls)
                    await account_snapshot(account, http_url=http_url, websocket_url=websocket_url, timeout=12)
                    assert any(method == "DELETE" and path.endswith("/listenKey") for method, path in ValuationHandler.calls)
                    changed.set()
                    while True:
                        point = await asyncio.wait_for(anext(events), 5)
                        if Decimal(point["marks"]["ETHUSDT-PERP.BINANCE"]["price"]) == 2020:
                            break
                    # Funding/fees and fills are detected as authoritative REST state changes.
                    ValuationHandler.balance = "99"
                    ValuationHandler.position_rows[0]["positionAmt"] = "0.020"
                    while True:
                        point = await asyncio.wait_for(anext(events), 8)
                        if Decimal(point["wallet"]["USDT"]) == 99 and Decimal(point["positions"][0]["quantity"]) == Decimal("0.02"):
                            break
                    await market_socket.close()
                    with pytest.raises(QueryError, match="中断"):
                        while True:
                            await asyncio.wait_for(anext(events), 8)
            assert all(method == "GET" or urlparse(path).path.endswith("/listenKey") for method, path in ValuationHandler.calls)
        finally:
            changed.set()
            http.shutdown()
            http.server_close()
            thread.join(timeout=2)
    asyncio.run(scenario())
    assert "unsendable" not in capfd.readouterr().err


@pytest.mark.parametrize("change", [None, "bad_side", "bad_quantity", "bad_entry", "bad_symbol", "duplicate"])
def test_valuation_rest_positions_keep_zero_wallet_and_validate_evidence(monkeypatch, change):
    from tests.connectors.binance.assets_test import opener

    position = {"symbol": "ETHUSDT", "positionSide": "SHORT", "positionAmt": "-0.0123456789", "entryPrice": "2000.12345678"}
    positions = [position]
    if change == "bad_side":
        position["positionAmt"] = "0.01"
    elif change == "bad_quantity":
        position["positionAmt"] = "NaN"
    elif change == "bad_entry":
        position["entryPrice"] = "0"
    elif change == "bad_symbol":
        position["symbol"] = "BTCUSD"
    elif change == "duplicate":
        positions.append(dict(position))
    opener(monkeypatch, [{"serverTime": 12345}, {
        "multiAssetsMargin": False, "totalWalletBalance": "0", "totalUnrealizedProfit": "0",
        "totalMarginBalance": "0", "totalMaintMargin": "0",
        "assets": [{"asset": "USDT", "walletBalance": "0", "unrealizedProfit": "0", "maintMargin": "0"}],
        "positions": positions,
    }])
    account = {"environment": "production", "credentials": {"apiKey": "fixture-key", "secret": "fixture-secret"}}
    if change:
        with pytest.raises(QueryError, match="资产明细读取失败"):
            assets.asset_account(account, positions=True)
    else:
        state = assets.asset_account(account, positions=True)
        assert state["assets"]["USDT"]["walletBalance"] == 0
        assert state["positions"][0]["quantity"] == "-0.0123456789"
        assert state["positions"][0]["entry"] == "2000.12345678"
