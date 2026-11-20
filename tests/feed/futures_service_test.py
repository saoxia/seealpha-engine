"""Synthetic combined USD-M WebSocket: multiplexing and scope isolation."""

import asyncio
import json

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from engine.market_data.collection.futures_service import (
    MAX_FUTURES_SYMBOLS_PER_SOCKET,
    FuturesCombinedSocket,
    QueueSocket,
)


def test_trades_and_funding_use_two_category_specific_shared_connections(monkeypatch):
    async def scenario():
        urls, commands = [], []

        async def handler(socket):
            message = json.loads(await socket.recv())
            commands.append(message)
            assert message["method"] == "SUBSCRIBE"
            selected = set(message["params"])
            if selected == {"ethusdt@trade", "btcusdt@trade"}:
                row = {"e": "trade", "s": "ETHUSDT", "t": 4, "T": 200,
                       "p": "100", "q": "2", "m": False}
                payload = {"stream": "ethusdt@trade", "data": row}
            else:
                assert selected == {"ethusdt@markPrice@1s", "btcusdt@markPrice@1s"}
                row = {"e": "markPriceUpdate", "s": "BTCUSDT",
                       "E": 200, "T": 500, "r": "0.0001"}
                payload = {"stream": "btcusdt@markPrice@1s", "data": row}
            await socket.send(json.dumps({"result": None, "id": message["id"]}))
            await socket.send(json.dumps(payload))
            await socket.wait_closed()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]

            def local(url, **kwargs):
                urls.append(url)
                return connect(f"ws://127.0.0.1:{port}", **kwargs)

            monkeypatch.setattr("engine.market_data.collection.futures_service.connect", local)
            scope = {"members": {"ETHUSDT", "BTCUSDT"}}
            trades = FuturesCombinedSocket().stream(scope, channel="trade")
            funding = FuturesCombinedSocket().stream(scope, channel="mark")
            try:
                trade_events = [await asyncio.wait_for(anext(trades), 3) for _ in range(3)]
                funding_events = [await asyncio.wait_for(anext(funding), 3) for _ in range(3)]
            finally:
                await trades.aclose()
                await funding.aclose()

        assert set(urls) == {
            "wss://fstream.binance.com/public/stream",
            "wss://fstream.binance.com/market/stream",
        }
        assert len(urls) == 2
        assert trade_events[2]["kind"] == "trade"
        assert trade_events[2]["row"]["id"] == 4
        assert funding_events[2]["kind"] == "mark"
        assert funding_events[2]["row"]["s"] == "BTCUSDT"
        assert len(commands) == 2
        assert MAX_FUTURES_SYMBOLS_PER_SOCKET < 1024

    asyncio.run(scenario())


def test_queue_socket_retains_order_and_finishes_on_stop():
    async def scenario():
        queue = asyncio.Queue()
        await queue.put({"kind": "connected"})
        await queue.put({"kind": "trade", "row": {"id": 1}})
        await queue.put(None)
        values = [event async for event in QueueSocket(queue).stream("ETHUSDT")]
        assert [event["kind"] for event in values] == ["connected", "trade"]

    asyncio.run(scenario())


def test_rejects_more_than_bounded_group_without_network(monkeypatch):
    async def scenario():
        class NeverConnect:
            def __call__(self, *args, **kwargs):
                raise AssertionError("Should reject group before consuming events")
        # The socket enforces group size before attempting any network call.
        monkeypatch.setattr("engine.market_data.collection.futures_service.connect", NeverConnect())
        scope = {"members": {f"A{i}USDT" for i in range(MAX_FUTURES_SYMBOLS_PER_SOCKET + 1)}}
        stream = FuturesCombinedSocket().stream(scope)
        # One disconnection signal is emitted for each member; reject as a protocol error.
        item = await asyncio.wait_for(anext(stream), 3)
        assert item["kind"] == "disconnected"
        await stream.aclose()

    asyncio.run(scenario())


def test_batch_fans_out_two_pairs_without_opening_per_symbol_sockets(tmp_path, monkeypatch):
    from engine.feed import futures_service as module

    async def scenario():
        root = tmp_path / "data/market_data/binance_um_1m/managed"
        root.mkdir(parents=True)
        manifest = tmp_path / "futures-runtime.json"
        manifest.write_text(json.dumps({"version": 1, "policies": [
            {"symbol": symbol, "product": "futures_um", "base_level": "1m",
             "intervals": ["1m"], "realtime_enabled": True}
            for symbol in ("ETHUSDT", "BTCUSDT")
        ]}))
        received = {}
        connected = asyncio.Event()
        opened = []

        class FakeMarket:
            async def stream(self, scope, *, channel):
                opened.append((scope, channel))
                for symbol in sorted(scope["members"]):
                    yield {"symbol": symbol, "kind": "connected"}
                    if channel == "trade":
                        yield {"symbol": symbol, "kind": "trade", "row": {"id": symbol}}
                await asyncio.Event().wait()

        async def fake_serve(policy, root, catalog, *, calculate, trade_socket, funding_socket):
            async for event in trade_socket.stream(policy["symbol"]):
                if event["kind"] == "trade":
                    received[policy["symbol"]] = event["row"]["id"]
                    if len(received) == 2:
                        connected.set()

        monkeypatch.setattr(module, "serve_pair", fake_serve)
        task = asyncio.create_task(module.serve_futures_batch(
            manifest, root, tmp_path / "data/catalog", project=tmp_path,
            socket_factory=FakeMarket,
        ))
        try:
            await asyncio.wait_for(connected.wait(), 4)
            assert received == {"BTCUSDT": "BTCUSDT", "ETHUSDT": "ETHUSDT"}
            assert len(opened) == 2
            assert {channel for _, channel in opened} == {"trade", "mark"}
            assert all(len(scope["members"]) == 2 for scope, _ in opened)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
