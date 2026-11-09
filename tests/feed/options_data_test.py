"""Managed option collection recovery and expiry without trading readiness."""

import asyncio
import json

from engine.feed import options_service


def test_stream_flush_checkpoint_reconnect_and_expiry(tmp_path, monkeypatch):
    now = 1800000000000
    expiry = now + 10000
    calls, connections = [], []
    monkeypatch.setattr(options_service.time, "time", lambda: now / 1000)
    def evidence(self, symbol, **kwargs):
        calls.append(kwargs)
        return {"expiry_ms": expiry, "records_written": len(kwargs.get("events", []))}
    monkeypatch.setattr(options_service.CatalogHistory, "options_evidence", evidence)
    async def stream(symbol, intervals, *, trades, depth):
        nonlocal now
        assert trades and intervals == ["1m", "5m", "15m"]
        connections.append(symbol)
        yield {"level": "status", "connected": True, "gap_count": 0}
        yield {"level": "tick", "row": {"id": len(connections)}}
        if len(connections) == 1:
            raise ConnectionError("disconnected")
        yield {"level": "status", "connected": True, "gap_count": 0}
        now = expiry
        yield {"level": "status", "connected": True, "gap_count": 0}
    monkeypatch.setattr(options_service, "market_events", stream)
    original_sleep = asyncio.sleep
    async def quick_sleep(_seconds):
        await original_sleep(0)
    monkeypatch.setattr(options_service.asyncio, "sleep", quick_sleep)
    policy = {"symbol": "BTC-270129-85000-C", "base_level": "tick"}
    asyncio.run(options_service.serve_options(policy, tmp_path, tmp_path / "catalog"))
    assert len(connections) == 2
    assert sum(len(call.get("events", [])) for call in calls) == 2
    assert sum("repair_from" in call for call in calls) >= 1
    saved = json.loads((tmp_path / "checkpoint.json").read_text())
    assert saved["gap_count"] == 1
    status = json.loads((tmp_path / "status.json").read_text())
    assert status["state"] == "STOPPED" and status["reason"] == "expired"
    assert not status["allow_new_risk"] and not status["historical_tick_complete"]
    asyncio.run(options_service.serve_options(policy, tmp_path, tmp_path / "catalog"))
    assert len(connections) == 2


def test_batch_collector_applies_single_contract_stop_and_keeps_shared_transport(tmp_path, monkeypatch):
    import fcntl
    import time

    from engine.connectors.binance import options, options_client
    from tests.connectors.binance.options_queries_test import definition

    template = definition(monkeypatch)
    rows = [template, {**template, "symbol": "BTC-261225-86000-C", "strike": "86000"}]
    monkeypatch.setattr(options, "markets", lambda: rows)
    captures = []
    def evidence(self, symbol, **kwargs):
        captures.append(symbol)
        return {"expiry_ms": template["expiry"], "records_written": 0, "repaired_until_ms": int(time.time() * 1000)}
    monkeypatch.setattr(options_service.CatalogHistory, "options_evidence", evidence)
    transports = []
    async def stream(items, *, tick_symbols, scope, depth):
        assert depth and set(tick_symbols) == {str(item.raw_symbol) for item in items}
        transports.append(scope)
        while True:
            for item in scope["selection"][0]:
                yield {"level": "status", "symbol": str(item.raw_symbol), "connected": True, "gap_count": 0}
            await asyncio.sleep(.05)
    monkeypatch.setattr(options_client, "batch_market_events", stream)
    policies = [{"symbol": row["symbol"], "product": "options", "base_level": "1m", "intervals": ["1m", "5m", "15m"],
                 "realtime_enabled": True, "history_enabled": False} for row in rows]
    manifest = tmp_path / "runtime.json"
    manifest.write_text(json.dumps({"version": 1, "policies": policies}))
    managed = tmp_path / "data/managed"
    managed.mkdir(parents=True)
    async def wait_for(predicate):
        async with asyncio.timeout(6):
            while not predicate():
                await asyncio.sleep(.05)
    async def scenario():
        task = asyncio.create_task(options_service.serve_batch(manifest, managed, tmp_path / "catalog", project=tmp_path))
        try:
            await wait_for(lambda: all(symbol in captures for symbol in (row["symbol"] for row in rows)))
            first = managed / rows[0]["symbol"] / "1m/status.json"
            second = managed / rows[1]["symbol"] / "1m/status.json"
            await wait_for(lambda: first.exists() and second.exists() and json.loads(second.read_text())["data_ready"])
            assert len(transports) == 1
            manifest.write_text("{broken")
            await asyncio.sleep(1.2)
            access = tmp_path / ".local/run/api/data/options" / f"{rows[0]['symbol']}.access.lock"
            with access.open("a") as lease:
                try:
                    fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    pass
                else:
                    raise AssertionError("A damaged manifest must not stop the existing collector")
            policies[0]["realtime_enabled"] = False
            manifest.write_text(json.dumps({"version": 1, "policies": policies}))
            await wait_for(lambda: json.loads(first.read_text())["reason"] == "service_stopped")
            with access.open("a") as lease:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            assert json.loads(second.read_text())["state"] in {"READY", "WARMING_UP"}
            assert len(transports) == 1
            assert {str(item.raw_symbol) for item in transports[0]["selection"][0]} == {rows[1]["symbol"]}
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


def test_managed_batch_entry_point_validates_mount_without_single_contract_args(tmp_path, monkeypatch):
    from engine.feed import managed

    data = tmp_path / "data"
    root = data / "managed/.options"
    data.mkdir()
    manifest = tmp_path / "runtime.json"
    manifest.write_text('{"version":1,"policies":[]}')
    called = []
    async def serve_batch(path, managed_root, catalog):
        called.append((path, managed_root, catalog))
    monkeypatch.setattr(options_service, "serve_batch", serve_batch)
    monkeypatch.setattr("sys.argv", ["engine.feed.managed", "--options-config", str(manifest), "--root", str(root),
                                     "--data-root", str(data), "--catalog", str(data / "catalog")])
    managed.main()
    assert called == [(manifest, root.parent, data / "catalog")]


def test_batch_worker_flushes_all_accepted_events_before_releasing_access(tmp_path, monkeypatch):
    import time

    calls = []
    def evidence(self, symbol, **kwargs):
        calls.extend(kwargs.get("events", []))
        return {"expiry_ms": int(time.time() * 1000) + 60000, "records_written": len(kwargs.get("events", []))}
    monkeypatch.setattr(options_service.CatalogHistory, "options_evidence", evidence)
    async def stream(symbol, intervals, *, trades, depth):
        assert trades and depth
        yield {"level": "status", "connected": True, "gap_count": 0}
        for identity in range(5):
            yield {"level": "tick", "row": {"id": identity}}
        yield {"level": "depth", "row": {"u": 42}}
        # End-of-queue stops this worker; it must never subscribe again.
    policy = {"symbol": "BTC-261225-85000-C", "base_level": "1m"}
    asyncio.run(options_service.serve_options(policy, tmp_path, tmp_path / "catalog", stream_factory=stream))
    assert [event["row"]["id"] for event in calls if event["level"] == "tick"] == list(range(5))
    assert calls[-1] == {"level": "depth", "row": {"u": 42}}
    assert json.loads((tmp_path / "status.json").read_text())["reason"] == "service_stopped"
    assert "depth" not in json.loads((tmp_path / "checkpoint.json").read_text())["bar_watermarks"]


def test_expiring_batch_worker_flushes_previously_accepted_queue_tail(tmp_path, monkeypatch):
    import threading
    import time

    from engine.connectors.binance import options, options_client
    from tests.connectors.binance.options_queries_test import definition

    item = definition(monkeypatch)
    monkeypatch.setattr(options, "markets", lambda: [item])
    queued = threading.Event()
    written = []
    def evidence(self, symbol, **kwargs):
        if kwargs.get("recover"):
            assert queued.wait(5)
        written.extend(kwargs.get("events", []))
        # Collection discovers expiry after transport has already queued two events.
        return {"expiry_ms": int(time.time() * 1000) - 1, "records_written": len(kwargs.get("events", []))}
    monkeypatch.setattr(options_service.CatalogHistory, "options_evidence", evidence)
    async def stream(items, *, tick_symbols, scope, depth):
        for identity in (1, 2):
            yield {"level": "tick", "symbol": item["symbol"], "row": {"id": identity}}
        queued.set()
        await asyncio.Event().wait()
    monkeypatch.setattr(options_client, "batch_market_events", stream)
    policy = {"symbol": item["symbol"], "product": "options", "base_level": "tick", "realtime_enabled": True}
    manifest = tmp_path / "runtime.json"
    manifest.write_text(json.dumps({"version": 1, "policies": [policy]}))
    managed = tmp_path / "managed"
    managed.mkdir()
    async def scenario():
        task = asyncio.create_task(options_service.serve_batch(manifest, managed, tmp_path / "catalog", project=tmp_path))
        try:
            async with asyncio.timeout(7):
                while len(written) != 2:
                    await asyncio.sleep(.05)
            assert [event["row"]["id"] for event in written] == [1, 2]
            status = json.loads((managed / item["symbol"] / "tick/status.json").read_text())
            assert status["reason"] == "expired"
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())
