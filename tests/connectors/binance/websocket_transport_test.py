"""Real native Binance adapter against local HTTP and WebSocket fixtures."""

import asyncio
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from engine.connectors.binance.native import NativeMarketData
from engine.connectors.binance.transport import KlineSocket

BASE = 1785542400000


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        symbol = {
            "symbol": "ETHUSDT",
            "pair": "ETHUSDT",
            "contractType": "PERPETUAL",
            "deliveryDate": 4133404800000,
            "onboardDate": 1569398400000,
            "status": "TRADING",
            "maintMarginPercent": "2.5",
            "requiredMarginPercent": "5",
            "baseAsset": "ETH",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "pricePrecision": 2,
            "quantityPrecision": 3,
            "baseAssetPrecision": 8,
            "quotePrecision": 8,
            "underlyingType": "COIN",
            "underlyingSubType": [],
            "settlePlan": 0,
            "triggerProtect": "0.05",
            "liquidationFee": "0.01",
            "marketTakeBound": "0.05",
            "filters": [
                {
                    "filterType": "PRICE_FILTER",
                    "minPrice": "0.01",
                    "maxPrice": "100000",
                    "tickSize": "0.01",
                },
                {
                    "filterType": "LOT_SIZE",
                    "minQty": "0.001",
                    "maxQty": "10000",
                    "stepSize": "0.001",
                },
                {
                    "filterType": "MARKET_LOT_SIZE",
                    "minQty": "0.001",
                    "maxQty": "10000",
                    "stepSize": "0.001",
                },
                {"filterType": "MIN_NOTIONAL", "notional": "10"},
            ],
            "orderTypes": ["LIMIT", "MARKET", "STOP_MARKET"],
            "timeInForce": ["GTC", "IOC"],
        }
        if "exchangeInfo" in self.path:
            value = {
                "timezone": "UTC",
                "serverTime": BASE,
                "futuresType": "U_MARGINED",
                "rateLimits": [],
                "exchangeFilters": [],
                "assets": [],
                "symbols": [symbol],
            }
        elif "klines" in self.path:
            value = [
                [
                    BASE,
                    "2000",
                    "2001",
                    "1999",
                    "2000",
                    "10",
                    BASE + 59999,
                    "20000",
                    10,
                    "5",
                    "10000",
                    "0",
                ]
            ]
        elif "time" in self.path:
            value = {"serverTime": int(time.time() * 1000)}
        else:
            value = []
        raw = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def test_native_binance_stream_history_reconnect_and_shutdown():
    async def scenario():
        http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=http.serve_forever, daemon=True)
        thread.start()
        delivered = []
        losses = []
        received = asyncio.Event()
        streaming_connections = 0

        async def handler(ws):
            nonlocal streaming_connections
            try:
                async for raw in ws:
                    request = json.loads(raw)
                    await ws.send(json.dumps({"result": None, "id": request["id"]}))
                    if any("@kline_1m" in p for p in request.get("params", [])):
                        streaming_connections += 1
                        message = {
                            "e": "kline",
                            "E": BASE + 60000,
                            "s": "ETHUSDT",
                            "k": {
                                "t": BASE,
                                "T": BASE + 59999,
                                "s": "ETHUSDT",
                                "i": "1m",
                                "f": 1,
                                "L": 10,
                                "o": "2000",
                                "c": "2000",
                                "h": "2001",
                                "l": "1999",
                                "v": "10",
                                "n": 10,
                                "x": True,
                                "q": "20000",
                                "V": "5",
                                "Q": "10000",
                                "B": "0",
                            },
                        }
                        await ws.send(json.dumps(message))
                        if streaming_connections == 1:
                            await ws.close()
            except ConnectionClosed:
                pass

        def on_message(item):
            if item[2] is not None:
                delivered.append(item)
                if len(delivered) >= 2:
                    received.set()

        try:
            async with serve(handler, "127.0.0.1", 0) as server:
                port = server.sockets[0].getsockname()[1]
                source = NativeMarketData(
                    ["ETHUSDT"],
                    http_url=f"http://127.0.0.1:{http.server_port}",
                    websocket_url=f"ws://127.0.0.1:{port}",
                    timeout=10,
                )
                socket = KlineSocket(
                    ["ETHUSDT"], on_message, lambda: losses.append(True), source=source
                )
                try:
                    await socket.connect()
                    values = await source.bars("ETHUSDT", BASE, BASE + 60000)
                    assert len(values) == 1 and values[0].start_ms == BASE
                    assert str(values[0].close) == "2000.00"
                    await asyncio.wait_for(received.wait(), 15)
                    assert streaming_connections >= 2
                    assert losses
                    assert delivered[0][2] == values[0]
                    assert socket.active
                finally:
                    await socket.close()
                assert not socket.active
        finally:
            http.shutdown()
            http.server_close()
            thread.join(timeout=2)

    asyncio.run(scenario())
