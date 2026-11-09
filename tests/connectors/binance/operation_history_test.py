"""Real native report bus capture, against a local fake Binance (no trading mutations)."""

import asyncio
import json
import threading
import time
from http.server import ThreadingHTTPServer
from urllib.parse import urlparse

from nautilus_trader.model import (
    AccountId,
    ClientOrderId,
    Currency,
    FillReport,
    InstrumentId,
    LiquiditySide,
    Money,
    OrderSide,
    Price,
    Quantity,
    TradeId,
    VenueOrderId,
)
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from engine.connectors.binance.operations import normalize_report, operation_history
from tests.connectors.binance.account_queries_test import AccountHandler


def test_normalize_native_fill_preserves_ids_and_commission():
    report = FillReport(
        account_id=AccountId("BINANCE-1"),
        instrument_id=InstrumentId.from_str("ETHUSDT-PERP.BINANCE"),
        venue_order_id=VenueOrderId("42"),
        trade_id=TradeId("99"),
        order_side=OrderSide.BUY,
        last_qty=Quantity.from_str("0.010"),
        last_px=Price.from_str("2000.00"),
        commission=Money(0.01, Currency.from_str("USDT")),
        liquidity_side=LiquiditySide.TAKER,
        ts_event=1_000_000_000,
        ts_init=1_000_000_000,
        client_order_id=ClientOrderId("client-1"),
    )
    result = normalize_report(
        report,
        {"id": 1, "account_name": "main", "environment": "production", "exchange": "binance"},
    )
    assert result["trade_id"] == "99" and result["order_id"] == "42"
    assert result["cost"] == "20.00000" and result["fee_currency"] == "USDT"
    assert result["evidence"] == "venue" and result["strategy_id"] is None
    assert result == normalize_report(
        report,
        {"id": 1, "account_name": "main", "environment": "production", "exchange": "binance"},
    )


def test_native_history_captures_external_order_and_real_fill_from_public_cache():
    class Handler(AccountHandler):
        def do_GET(self):
            path = urlparse(self.path).path
            if path.endswith(("/allOrders", "/openOrders")):
                self.calls.append(("GET", path))
                now = self.fixture_time
                self.reply(
                    [
                        {
                            "symbol": "ETHUSDT",
                            "orderId": 42,
                            "orderListId": -1,
                            "clientOrderId": "manual-order",
                            "price": "2000.00",
                            "origQty": "0.010",
                            "executedQty": "0.010",
                            "cumQuote": "0",
                            "status": "FILLED",
                            "timeInForce": "GTC",
                            "type": "LIMIT",
                            "side": "BUY",
                            "stopPrice": "0",
                            "icebergQty": "0",
                            "time": now,
                            "updateTime": now,
                            "isWorking": True,
                            "workingType": "CONTRACT_PRICE",
                            "reduceOnly": False,
                            "closePosition": False,
                            "positionSide": "BOTH",
                            "priceProtect": False,
                            "origType": "LIMIT",
                            "avgPrice": "2000.00",
                        }
                    ]
                )
            elif path.endswith("/userTrades"):
                self.calls.append(("GET", path))
                self.reply(
                    [
                        {
                            "symbol": "ETHUSDT",
                            "id": 99,
                            "orderId": 42,
                            "side": "BUY",
                            "price": "2000.00",
                            "qty": "0.010",
                            "realizedPnl": "0",
                            "quoteQty": "20",
                            "commission": "0.01",
                            "commissionAsset": "USDT",
                            "time": self.fixture_time,
                            "buyer": True,
                            "maker": False,
                            "positionSide": "BOTH",
                        }
                    ]
                )
            else:
                super().do_GET()

    async def run():
        Handler.position_rows = [
            {
                "symbol": "ETHUSDT",
                "positionAmt": "0.010",
                "entryPrice": "2000",
                "markPrice": "2000",
                "unRealizedProfit": "0",
                "liquidationPrice": "0",
                "leverage": "5",
                "marginType": "cross",
                "isolatedMargin": "0",
                "isAutoAddMargin": "false",
                "positionSide": "BOTH",
                "notional": "20",
                "isolatedWallet": "0",
                "updateTime": int(time.time() * 1000),
                "marginAsset": "USDT",
                "maxNotionalValue": "100000",
            }
        ]
        Handler.calls = []
        Handler.fixture_time = int(time.time() * 1000) - 2000
        http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
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
            async with serve(websocket, "127.0.0.1", 0) as ws:
                records = await operation_history(
                    {
                        "id": 1,
                        "account_name": "main",
                        "exchange": "binance",
                        "environment": "production",
                        "credentials": {"apiKey": "fixture-key", "secret": "fixture-secret"},
                    },
                    http_url=f"http://127.0.0.1:{http.server_port}",
                    websocket_url=f"ws://127.0.0.1:{ws.sockets[0].getsockname()[1]}",
                    timeout=10,
                )
                assert any(r["order_id"] == "42" and r["evidence"] == "venue" for r in records), (
                    records
                )
                assert any(
                    r["trade_id"] == "99" and r["event_type"] == "fill"
                    for r in records
                    if r.get("trade_id")
                ), (records, Handler.calls)
                assert all(r.get("strategy_id") is None for r in records)
                assert not any(
                    method != "GET" and not path.endswith("/listenKey")
                    for method, path in Handler.calls
                )
        finally:
            http.shutdown()
            http.server_close()
            thread.join(timeout=2)

    asyncio.run(run())
