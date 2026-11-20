"""Demand-driven shared collector integration, with only synthetic journal evidence."""

import json
import sqlite3
import time
from decimal import Decimal

import pytest

from engine.data_manager.contracts import DataPolicy
from engine.data_manager.service import DataService
from engine.feed.strategy_live import ManagedJournalBridge
from engine.feed.subscription_hub import MarketDataHubError, MarketStream
from engine.runtime import EngineRuntime

ETH = MarketStream("BINANCE", "futures_um", "ETHUSDT", "1m")
BTC = MarketStream("BINANCE", "spot", "BTCUSDT", "1m")


def _setup(tmp_path, monkeypatch):
    (tmp_path / "data").mkdir()
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    runtime.ensure_tenant("prod", "production")
    runtime.store.upsert_strategy_load(
        "prod", "desk", "issued", "alpha", "1", "production",
        {"symbol": "ETHUSDT"}, "strategy:Alpha", "hash", b"archive",
    )
    runtime.note_subscription(
        "prod", "desk", "futures_um", "ETHUSDT", "intent", "configured", {},
    )
    manager = DataService("prod", project=tmp_path, state_root=tmp_path / "state")
    manager.policy_repository.save(
        [DataPolicy(
            symbol="ETHUSDT", product="futures_um", base_level="1m",
            intervals=["1m"], realtime_enabled=False,
        )], tenant_id="prod",
    )
    bridge = ManagedJournalBridge(runtime, manager)
    manager.subscriptions._read(refresh=True)
    monkeypatch.setattr(manager.subscriptions, "restore", lambda: None)
    return runtime, manager, bridge


def _write_status(path, *, ready=True, calculation=False):
    now = int(time.time() * 1000)
    value = {
        "state": "READY" if ready else "DEGRADED",
        "valid_until_ms": now + 5000 if ready else now - 1,
        "data_ready": ready, "scope": "managed_public_evidence_no_strategy_readiness",
        "backlog": 0, "realtime_pending": 0,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def _journal(root):
    root.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(root / "collection.sqlite")
    db.execute("CREATE TABLE config (symbol TEXT, base_level TEXT)")
    db.execute("INSERT INTO config VALUES ('ETHUSDT','1m')")
    db.execute(
        "CREATE TABLE candles (interval TEXT,start_ms INTEGER,end_ms INTEGER,value TEXT)"
    )
    now = int(time.time() * 1000) // 60000 * 60000
    for offset in (3, 2):
        left = now - offset * 60000
        value = {
            "symbol": "ETHUSDT", "interval": "1m",
            "start_ms": left, "end_ms": left + 60000,
            "is_closed": True, "is_complete": True, "retained": True,
            "open": "100", "high": "101", "low": "99", "close": "100",
            "volume": "1",
        }
        db.execute(
            "INSERT INTO candles VALUES (?,?,?,?)",
            ("1m", left, left + 60000, json.dumps(value)),
        )
    db.commit()
    db.close()
    return now


def test_live_demand_overlays_only_existing_configured_symbols(tmp_path, monkeypatch):
    runtime, manager, bridge = _setup(tmp_path, monkeypatch)
    try:
        assert bridge.subscribe("prod", "desk", "issued", (ETH,), lookback_bars=2) == {ETH: 2}
        assert manager.subscriptions._effective_policies()[0].realtime_enabled is True
        manifest = manager.subscriptions._publish_futures()
        rows = json.loads(manifest.read_text())["policies"]
        assert len(rows) == 1 and rows[0]["realtime_enabled"] is True
        assert manager.policy_repository.list("prod")[0]["realtime_enabled"] is False
        with pytest.raises(MarketDataHubError, match="未获准"):
            bridge.subscribe("prod", "desk", "wrong", (BTC,))
        assert bridge.unsubscribe("prod", "desk", "issued") == {}
        assert not manager.subscriptions._effective_policies()[0].realtime_enabled
    finally:
        runtime.close()
        manager.close()


def test_collector_journal_to_strategy_view_and_disconnect_invalidates(tmp_path, monkeypatch):
    runtime, manager, bridge = _setup(tmp_path, monkeypatch)
    try:
        bridge.subscribe("prod", "desk", "issued", (ETH,), lookback_bars=2)
        policy = manager.subscriptions._read()[0]
        root = manager.subscriptions.root(policy)
        now = _journal(root)
        _write_status(root / "status.json")
        _write_status(root / "calculation" / "status.json", calculation=True)
        bridge.poll()
        assert bridge.status()["requirements"][0]["data_ready"] is True
        snapshot = bridge.view(
            "prod", "desk", "issued",
            now_ns=now * 1_000_000,
            positions={ETH: Decimal("0")},
            max_age_ns=300_000_000_000,
        )
        assert snapshot["markets"]["futures_um:ETHUSDT"]["price"] == "100"
        assert len(snapshot["history"]["futures_um:ETHUSDT"]) == 2
        assert snapshot["allow_new_risk"] is False
        _write_status(root / "status.json", ready=False)
        bridge.poll()
        assert bridge.status()["requirements"][0]["data_ready"] is False
        with pytest.raises(MarketDataHubError, match="尚未"):
            bridge.view(
                "prod", "desk", "issued",
                now_ns=now * 1_000_000,
                positions={ETH: Decimal("0")},
                max_age_ns=300_000_000_000,
            )
    finally:
        runtime.close()
        manager.close()
