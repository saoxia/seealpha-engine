"""Exercise the real native execution factory using local signed-API fixtures only."""

import asyncio
import gc
import json
import threading
import time
from http.server import ThreadingHTTPServer
from types import SimpleNamespace
from typing import ClassVar
from urllib.parse import urlparse

import pytest
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from engine.connectors.binance.queries import QueryError, account_snapshot, instruments
from tests.connectors.binance.websocket_transport_test import Handler


def test_identical_saved_credentials_fail_before_starting_native_node(monkeypatch):
    def unexpected_builder(*args):
        pytest.fail("Invalid credentials must fail before creating any native clients")

    monkeypatch.setattr(
        "engine.connectors.binance.queries.LiveNode", SimpleNamespace(builder=unexpected_builder)
    )
    with pytest.raises(QueryError, match="API Key 与 Secret Key 相同") as error:
        asyncio.run(account_snapshot({
            "id": 1, "environment": "production",
            "credentials": {"apiKey": "private-identical-value", "secret": "private-identical-value"},
        }))
    assert "private-identical-value" not in str(error.value)


def test_native_build_failure_is_sanitized_without_cross_thread_traceback(monkeypatch):
    def failing_builder(*args):
        raise RuntimeError("native failure with private-fixture-secret")

    monkeypatch.setattr(
        "engine.connectors.binance.queries.LiveNode", SimpleNamespace(builder=failing_builder)
    )
    with pytest.raises(QueryError, match="Binance 账户查询失败") as error:
        asyncio.run(account_snapshot({
            "id": 1, "environment": "production",
            "credentials": {"apiKey": "fixture-key", "secret": "fixture-secret"},
        }))
    assert "private-fixture-secret" not in str(error.value)
    assert error.value.__context__ is None


class AccountHandler(Handler):
    calls: ClassVar[list] = []
    position_rows: ClassVar[list] = []

    def reply(self, data):
        raw = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        path = urlparse(self.path).path
        self.calls.append(("GET", path))
        if path.endswith("/account"):
            self.reply(
                {
                    "feeTier": 0,
                    "canTrade": True,
                    "canDeposit": True,
                    "canWithdraw": True,
                    "updateTime": int(time.time() * 1000),
                    "multiAssetsMargin": False,
                    "totalInitialMargin": "0",
                    "totalMaintMargin": "0",
                    "totalWalletBalance": "100",
                    "totalUnrealizedProfit": "0",
                    "totalMarginBalance": "100",
                    "totalPositionInitialMargin": "0",
                    "totalOpenOrderInitialMargin": "0",
                    "totalCrossWalletBalance": "100",
                    "totalCrossUnPnl": "0",
                    "availableBalance": "100",
                    "maxWithdrawAmount": "100",
                    "assets": [
                        {
                            "asset": "USDT",
                            "walletBalance": "100",
                            "unrealizedProfit": "0",
                            "marginBalance": "100",
                            "maintMargin": "0",
                            "initialMargin": "0",
                            "positionInitialMargin": "0",
                            "openOrderInitialMargin": "0",
                            "crossWalletBalance": "100",
                            "crossUnPnl": "0",
                            "availableBalance": "100",
                            "maxWithdrawAmount": "100",
                            "marginAvailable": True,
                            "updateTime": 0,
                        }
                    ],
                    "positions": [],
                }
            )
        elif path.endswith("/positionSide/dual"):
            self.reply({"dualSidePosition": len(self.position_rows) > 1})
        elif path.endswith("/multiAssetsMargin"):
            self.reply({"multiAssetsMargin": False})
        elif path.endswith("/listenKey"):
            self.reply({"listenKey": "local-fixture-stream"})
        elif path.endswith("/positionRisk"):
            self.reply(self.position_rows)
        elif path.endswith(("/openOrders", "/openAlgoOrders", "/userTrades", "/allOrders")):
            self.reply([])
        else:
            super().do_GET()

    def do_POST(self):
        path = urlparse(self.path).path
        self.calls.append(("POST", path))
        if path.endswith("/listenKey"):
            self.reply({"listenKey": "local-fixture-stream"})
        else:
            self.send_error(405)

    def do_DELETE(self):
        path = urlparse(self.path).path
        self.calls.append(("DELETE", path))
        if path.endswith("/listenKey"):
            self.reply({})
        else:
            self.send_error(405)

    def do_PUT(self):
        path = urlparse(self.path).path
        self.calls.append(("PUT", path))
        if path.endswith("/listenKey"):
            self.reply({})
        else:
            self.send_error(405)


@pytest.mark.parametrize("with_position", [False, True, "hedge"])
def test_native_catalog_and_account_snapshot_do_not_mutate_exchange(with_position, capfd):
    async def scenario():
        AccountHandler.calls = []
        AccountHandler.position_rows = (
            [
                {
                    "symbol": "ETHUSDT",
                    "positionAmt": "0.010",
                    "entryPrice": "2000",
                    "markPrice": "2010",
                    "unRealizedProfit": "0.1",
                    "liquidationPrice": "0",
                    "leverage": "5",
                    "marginType": "cross",
                    "isolatedMargin": "0",
                    "isAutoAddMargin": "false",
                    "positionSide": "BOTH",
                    "notional": "20.1",
                    "isolatedWallet": "0",
                    "updateTime": int(time.time() * 1000),
                    "marginAsset": "USDT",
                    "maxNotionalValue": "100000",
                }
            ]
            if with_position
            else []
        )
        if with_position == "hedge":
            row = AccountHandler.position_rows[0]
            AccountHandler.position_rows = [
                {**row, "positionSide": "LONG"},
                {**row, "positionSide": "SHORT", "positionAmt": "-0.020"},
            ]
        http = ThreadingHTTPServer(("127.0.0.1", 0), AccountHandler)
        thread = threading.Thread(target=http.serve_forever, daemon=True)
        thread.start()

        async def websocket(ws):
            try:
                async for raw in ws:
                    request = json.loads(raw)
                    await ws.send(json.dumps({"id": request.get("id"), "result": None}))
            except ConnectionClosed:
                pass

        try:
            async with serve(websocket, "127.0.0.1", 0) as server:
                http_url = f"http://127.0.0.1:{http.server_port}"
                websocket_url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
                catalog = await instruments("production", http_url=http_url)
                assert [str(i.id) for i in catalog] == ["ETHUSDT-PERP.BINANCE"]
                async def read(account_id, timeout=12):
                    return await account_snapshot(
                        {
                            "id": account_id,
                            "environment": "production",
                            "credentials": {
                                "apiKey": "local-fixture-key",
                                "secret": "local-fixture-secret",
                            },
                        },
                        http_url=http_url,
                        websocket_url=websocket_url,
                        timeout=timeout,
                    )
                # Mirrors portfolio reads overlapping another account or ledger sync.
                snapshots = await asyncio.gather(read(1), read(2))
                with pytest.raises(QueryError):
                    await read(1, timeout=0.001)
                snapshots.append(await read(1))
                for item in snapshots:
                    assert item.balances == {"USDT": "100.00000000"}
                    assert len(item.positions) == len(AccountHandler.position_rows)
                snapshot = snapshots[-1]
                assert snapshot.info["total_wallet_balance"] == "100"
                assert snapshot.info["total_margin_balance"] == "100"
                if with_position:
                    assert snapshot.positions[0]["symbol"] == "ETH/USDT:USDT"
                    assert "0.010" in {p["contracts"] for p in snapshot.positions}
                if with_position == "hedge":
                    assert {p["side"] for p in snapshot.positions} == {"long", "short"}
                    assert len({p["id"] for p in snapshot.positions}) == 2
                assert any(path.endswith("/account") for _, path in AccountHandler.calls)
                assert all(
                    method == "GET" or path.endswith("/listenKey")
                    for method, path in AccountHandler.calls
                )
        finally:
            http.shutdown()
            http.server_close()
            thread.join(timeout=2)

    asyncio.run(scenario())
    gc.collect()
    assert "unsendable" not in capfd.readouterr().err
