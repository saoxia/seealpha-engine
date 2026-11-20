"""Configuration-owned collectors and read-only strategy market integration."""

import json
import sqlite3
import time
from decimal import Decimal

import pytest

from engine.market_data.access.strategy_live import ManagedJournalBridge
from engine.market_data.access.subscription_hub import MarketDataHubError, MarketStream
from engine.market_data.control.contracts import DataPolicy
from engine.market_data.control.service import DataService
from engine.trading.runtime import EngineRuntime

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
            intervals=["1m"], realtime_enabled=True,
        )], tenant_id="prod",
    )
    bridge = ManagedJournalBridge(runtime, manager)
    manager.subscriptions._read(refresh=True)
    monkeypatch.setattr(manager.subscriptions, "restore", lambda: None)
    return runtime, manager, bridge


def _write_status(path, *, ready=True):
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


def test_live_reads_cannot_start_or_stop_configured_collectors(tmp_path, monkeypatch):
    runtime, manager, bridge = _setup(tmp_path, monkeypatch)
    restores = []
    monkeypatch.setattr(manager.subscriptions, "restore", lambda: restores.append(True))
    try:
        policy = manager.subscriptions._read()[0]
        assert policy.realtime_enabled is True
        assert bridge.subscribe("prod", "desk", "issued", (ETH,), lookback_bars=2) == {ETH: 2}
        assert bridge.unsubscribe("prod", "desk", "issued") == {}
        assert restores == []
        manifest = manager.subscriptions._publish_futures()
        assert len(json.loads(manifest.read_text())["policies"]) == 1

        disabled = policy.model_copy(update={"realtime_enabled": False})
        manager.policy_repository.save([disabled], tenant_id="prod")
        manager.subscriptions._read(refresh=True)
        with pytest.raises(MarketDataHubError, match="实时采集未在配置中启用"):
            bridge.subscribe("prod", "desk", "issued", (ETH,))
        manifest = manager.subscriptions._publish_futures()
        assert json.loads(manifest.read_text())["policies"][0]["realtime_enabled"] is False
        assert manager.policy_repository.list("prod")[0]["realtime_enabled"] is False
        assert restores == []
        with pytest.raises(MarketDataHubError, match="未获准"):
            bridge.subscribe("prod", "desk", "issued", (BTC,))
    finally:
        runtime.close()
        manager.close()


def test_policy_disable_immediately_invalidates_existing_live_reader(tmp_path, monkeypatch):
    runtime, manager, bridge = _setup(tmp_path, monkeypatch)
    try:
        bridge.subscribe("prod", "desk", "issued", (ETH,))
        policy = manager.subscriptions._read()[0]
        root = manager.subscriptions.root(policy)
        _journal(root)
        _write_status(root / "status.json")
        _write_status(root / "calculation" / "status.json")
        bridge.poll()
        assert bridge.status()["requirements"][0]["data_ready"] is True
        manager.policy_repository.save(
            [policy.model_copy(update={"realtime_enabled": False})], tenant_id="prod",
        )
        bridge.poll()
        assert bridge.status()["requirements"][0]["data_ready"] is False
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
        _write_status(root / "calculation" / "status.json")
        bridge.poll()
        assert bridge.status()["requirements"][0]["data_ready"] is True
        snapshot = bridge.view(
            "prod", "desk", "issued",
            now_ns=now * 1_000_000,
            positions={ETH: Decimal(0)},
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
                positions={ETH: Decimal(0)},
                max_age_ns=300_000_000_000,
            )
    finally:
        runtime.close()
        manager.close()


def test_new_longer_lookback_rewarms_one_shared_stream(tmp_path, monkeypatch):
    runtime, manager, bridge = _setup(tmp_path, monkeypatch)
    try:
        runtime.store.upsert_strategy_load(
            "prod", "desk", "hedge", "alpha", "1", "production",
            {"symbol": "ETHUSDT"}, "strategy:Alpha", "hash", b"archive",
        )
        bridge.subscribe("prod", "desk", "issued", (ETH,), lookback_bars=1)
        policy = manager.subscriptions._read()[0]
        root = manager.subscriptions.root(policy)
        now = _journal(root)
        _write_status(root / "status.json")
        _write_status(root / "calculation" / "status.json")
        bridge.poll()

        assert bridge.subscribe(
            "prod", "desk", "hedge", (ETH,), lookback_bars=2,
        ) == {ETH: 2}
        bridge.poll()
        snapshot = bridge.view(
            "prod", "desk", "hedge",
            now_ns=now * 1_000_000,
            positions={ETH: Decimal(0)},
            max_age_ns=300_000_000_000,
        )
        assert len(snapshot["history"]["futures_um:ETHUSDT"]) == 2
        assert bridge.unsubscribe("prod", "desk", "hedge") == {ETH: 1}
        assert bridge.status()["requirements"][0]["lookback_bars"] == 1
    finally:
        runtime.close()
        manager.close()


def test_cannot_delete_an_issued_strategy_live_market(tmp_path, monkeypatch):
    from fastapi import HTTPException

    runtime, manager, bridge = _setup(tmp_path, monkeypatch)
    try:
        bridge.subscribe("prod", "desk", "issued", (ETH,))
        with pytest.raises(HTTPException, match="策略仍在订阅"):
            manager.subscriptions.pause_for_deletion("ETHUSDT", "futures_um")
        with pytest.raises(HTTPException, match="策略仍在订阅"):
            manager.subscriptions.remove("ETHUSDT", "futures_um")
        assert manager.policy_repository.list("prod")[0]["symbol"] == "ETHUSDT"
    finally:
        runtime.close()
        manager.close()


def test_older_committed_bar_revision_invalidates_all_live_readers(tmp_path, monkeypatch):
    runtime, manager, bridge = _setup(tmp_path, monkeypatch)
    try:
        bridge.subscribe("prod", "desk", "issued", (ETH,), lookback_bars=2)
        root = manager.subscriptions.root(manager.subscriptions._read()[0])
        _journal(root)
        _write_status(root / "status.json")
        _write_status(root / "calculation" / "status.json")
        bridge.poll()
        assert bridge.status()["requirements"][0]["data_ready"] is True

        db = sqlite3.connect(root / "collection.sqlite")
        row = db.execute("SELECT start_ms,value FROM candles ORDER BY start_ms LIMIT 1").fetchone()
        body = json.loads(row[1])
        body["high"] = "102"
        db.execute(
            "UPDATE candles SET value=? WHERE start_ms=?",
            (json.dumps(body), row[0]),
        )
        db.commit()
        db.close()
        bridge.poll()
        assert bridge.status()["requirements"][0]["data_ready"] is False
        assert "修订" in bridge.status()["requirements"][0]["reason"]
    finally:
        runtime.close()
        manager.close()


def test_pinned_engine_refuses_other_tenant_market_requests(tmp_path, monkeypatch):
    runtime, manager, bridge = _setup(tmp_path, monkeypatch)
    try:
        runtime.ensure_tenant("other", "production")
        runtime.store.upsert_strategy_load(
            "other", "desk", "issued", "alpha", "1", "production",
            {"symbol": "ETHUSDT"}, "strategy:Alpha", "hash", b"archive",
        )
        runtime.note_subscription(
            "other", "desk", "futures_um", "ETHUSDT",
            "intent", "configured", {},
        )
        with pytest.raises(MarketDataHubError, match="租户"):
            bridge.subscribe("other", "desk", "issued", (ETH,))
        with pytest.raises(MarketDataHubError, match="租户"):
            bridge.unsubscribe("other", "desk", "issued")
        assert bridge.status()["requirements"] == []
    finally:
        runtime.close()
        manager.close()
