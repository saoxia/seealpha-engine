"""Raw futures trades retain venue identity and reconnect without aggTrade mixing."""

import asyncio
import json

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from engine.connectors.binance.pair_data import PairTradeSocket


def test_raw_socket_reconnects_rejects_aggregate_payloads_and_closes(monkeypatch):
    async def scenario():
        connections = []
        closed = asyncio.Event()

        async def handler(socket):
            connections.append(socket)
            if len(connections) == 1:
                await socket.send(json.dumps({"e": "aggTrade", "s": "ETHUSDT", "a": 999}))
            else:
                await socket.send(json.dumps({"e": "trade", "s": "ETHUSDT", "t": 42, "T": 1100,
                                               "p": "10.00", "q": "0.25", "m": False}))
            try:
                await socket.wait_closed()
            finally:
                if len(connections) > 1:
                    closed.set()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            def local(url, **kwargs):
                assert url == "wss://fstream.binance.com/ws/ethusdt@trade"
                return connect(f"ws://127.0.0.1:{port}", **kwargs)
            monkeypatch.setattr("engine.connectors.binance.pair_data.connect", local)
            stream = PairTradeSocket().stream("ETHUSDT")
            assert (await anext(stream))["kind"] == "connected"
            assert (await anext(stream))["kind"] == "disconnected"
            assert (await asyncio.wait_for(anext(stream), 3))["kind"] == "connected"
            event = await asyncio.wait_for(anext(stream), 3)
            assert event == {"kind": "trade", "row": {"id": 42, "time": 1100,
                                                      "price": "10.00", "qty": "0.25", "isBuyerMaker": False}}
            await stream.aclose()
            await asyncio.wait_for(closed.wait(), 3)

    asyncio.run(scenario())
