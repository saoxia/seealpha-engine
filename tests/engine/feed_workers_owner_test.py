"""Only Engine owns live feed/history worker processes and option batches."""

import json

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from engine.app import create_app
from engine.config import EngineConfig
from engine.data_manager.contracts import DataPolicy
from engine.data_manager.service import DataService


class Worker:
    def __init__(self):
        self.returncode = None

    def poll(self):
        return self.returncode


def engine_manager(tmp_path, monkeypatch):
    (tmp_path / "data").mkdir()
    manager = DataService("42", project=tmp_path, state_root=tmp_path / "state")
    commands, stopped = [], []

    def launch(command, log_name, access=None):
        worker = Worker()
        commands.append((command, log_name, access, worker))
        return worker

    def stop(worker):
        worker.returncode = 0
        stopped.append(worker)

    monkeypatch.setattr(manager.subscriptions, "_launch", launch)
    monkeypatch.setattr(manager, "_stop", stop)
    return manager, commands, stopped


def test_engine_only_starts_and_stops_futures_feed_and_history(tmp_path, monkeypatch):
    manager, commands, stopped = engine_manager(tmp_path, monkeypatch)
    policy = DataPolicy(
        symbol="ETHUSDT", base_level="1m",
        realtime_enabled=True, history_enabled=True,
        history_mode="start", history_start="2026-08-01T00:00:00Z",
    )
    manager.subscriptions._write([policy])
    manager.subscriptions.restore()
    assert len(commands) == 3
    assert {row[1] for row in commands} == {
        "ETHUSDT-collection.log", "ETHUSDT-calculation.log", "ETHUSDT-history.log",
    }
    assert all(row[2] == ("ETHUSDT", "futures_um", True) for row in commands)
    collection = manager.subscriptions.processes[("futures_um", "ETHUSDT")]
    manager.subscriptions.restore()
    assert len(commands) == 3  # A policy refresh never duplicates active workers.
    disabled = policy.model_copy(update={"realtime_enabled": False, "history_enabled": False})
    # Engine /sync loads the prior cached intent, saves new ORM-equivalent
    # policy state, and then restores. Do not pre-replace the cache here.
    manager.policy_repository.save([disabled], tenant_id="42")
    manager.subscriptions.restore()
    assert collection in stopped
    assert not manager.subscriptions.processes and not manager.subscriptions.auxiliary
    manager.close()


def test_engine_options_use_two_shared_workers_and_durable_manifest(tmp_path, monkeypatch):
    manager, commands, stopped = engine_manager(tmp_path, monkeypatch)
    scope = DataPolicy(product="options", symbol="ALL", realtime_enabled=True, history_enabled=True)
    count = 1514
    contracts = [
        DataPolicy(
            product="options", symbol=f"BTC-261225-{10000 + i}-C",
            underlying="BTCUSDT", realtime_enabled=True, history_enabled=True,
        )
        for i in range(count)
    ]
    manager.subscriptions._write([
        scope, DataPolicy(product="options", symbol="BTCUSDT"), *contracts,
    ])
    manager.subscriptions.restore()
    assert len(commands) == 2
    assert "engine.feed.managed" in commands[0][0]
    assert "engine.pipeline.pair_data" in commands[1][0]
    assert all("--options-config" in args for args, *_ in commands)
    assert len(json.loads((manager.directory / "options-runtime.json").read_text())["policies"]) == count
    assert set(manager.subscriptions.processes) == {("options", "ALL")}
    assert set(manager.subscriptions.auxiliary) == {("options", "ALL", "history")}
    manager.subscriptions.restore()
    assert len(commands) == 2

    # A new contract only changes the Engine-owned manifest, not worker count.
    extra = DataPolicy(
        product="options", symbol="BTC-270129-20000-P",
        underlying="BTCUSDT", realtime_enabled=True, history_enabled=True,
    )
    manager.subscriptions._write([scope, *contracts, extra])
    manager.subscriptions.restore()
    assert len(commands) == 2
    assert len(json.loads((manager.directory / "options-runtime.json").read_text())["policies"]) == count + 1

    manager.subscriptions.pause_for_deletion(contracts[0].symbol, "options")
    assert len(commands) == 2
    assert stopped == []  # Other option contracts keep the shared workers.
    rows = manager.subscriptions._read()
    selected = next(p for p in rows if p.symbol == contracts[0].symbol)
    assert not selected.realtime_enabled and not selected.history_enabled
    assert next(p for p in rows if p.symbol == extra.symbol).realtime_enabled

    disabled = [p.model_copy(update={"realtime_enabled": False, "history_enabled": False})
                for p in rows]
    manager.subscriptions._write(disabled)
    manager.subscriptions.restore()
    assert len(stopped) == 2
    assert not manager.subscriptions.processes
    assert not manager.subscriptions.auxiliary
    manager.close()


def test_engine_http_sync_starts_and_stops_workers_with_tenant_authorization(tmp_path, monkeypatch):
    (tmp_path / "data").mkdir()
    app = create_app(EngineConfig(
        tmp_path / "state", "secret", project=tmp_path,
        tenant_id="42", tenant_purpose="development",
    ))
    app.state.runtime.ensure_tenant("42", "development")
    manager = app.state.market_data
    started, stopped = [], []

    def launch(command, _log_name, access=None):
        worker = Worker()
        started.append((command, access, worker))
        return worker

    def stop(worker):
        stopped.append(worker)
        worker.returncode = 0

    monkeypatch.setattr(manager.subscriptions, "_launch", launch)
    monkeypatch.setattr(manager, "_stop", stop)
    headers = {"Authorization": "Bearer secret"}
    path = "/tenants/42/data/control/sync"
    enabled = {"policies": [{"symbol": "ETHUSDT", "base_level": "1m",
                             "realtime_enabled": True, "history_enabled": False}]}
    with TestClient(app) as client:
        assert client.post(path, json=enabled).status_code == 401
        assert client.post(
            "/tenants/43/data/control/sync", json=enabled, headers=headers,
        ).status_code == 403
        # The Engine must reject policy changes while the pair is under
        # deletion's exclusive data access lease, without changing the file.
        with manager._lease("ETHUSDT", access=True):
            blocked = client.post(path, json=enabled, headers=headers)
            assert blocked.status_code == 409
            assert manager.policy_repository.list() == []
        first = client.post(path, json=enabled, headers=headers)
        assert first.status_code == 200, first.text
        assert len(started) == 2  # Feed collection plus bar calculation.
        assert all(access == ("ETHUSDT", "futures_um", True) for _, access, _ in started)
        again = client.post(path, json=enabled, headers=headers)
        assert again.status_code == 200
        assert len(started) == 2
        disabled = {"policies": [{**enabled["policies"][0], "realtime_enabled": False}]}
        with manager._lease("ETHUSDT", access=True):
            blocked = client.post(path, json=disabled, headers=headers)
            assert blocked.status_code == 409
            assert manager.policy_repository.list()[0]["realtime_enabled"] is True
        result = client.post(path, json=disabled, headers=headers)
        assert result.status_code == 200, result.text
        assert len(stopped) == 2
        assert not manager.subscriptions.processes
        assert not manager.subscriptions.auxiliary
    app.state.runtime.close()


def test_engine_option_delete_waits_for_access_lease_and_preserves_files(tmp_path, monkeypatch):
    manager, _commands, _stopped = engine_manager(tmp_path, monkeypatch)
    contract = "BTC-261225-85000-C"
    scope = DataPolicy(product="options", symbol="BTCUSDT", realtime_enabled=False)
    member = DataPolicy(
        product="options", symbol=contract, underlying="BTCUSDT",
        realtime_enabled=False, history_enabled=False,
    )
    manager.subscriptions._write([scope, member])
    evidence = tmp_path / "data/raw/options" / contract / "source.json"
    evidence.parent.mkdir(parents=True)
    evidence.write_text("retained")

    reader = manager._lease(contract, "options", access=True, shared=True)
    from time import time as wall_time
    from types import SimpleNamespace

    clock = iter([100.0, 106.0])
    monkeypatch.setattr(
        "engine.data_manager.subscriptions.time",
        SimpleNamespace(
            monotonic=lambda: next(clock, 106.0),
            sleep=lambda _seconds: None,
            time=wall_time,
        ),
    )
    try:
        with pytest.raises(HTTPException, match="重新预览") as failure:
            manager.subscriptions.pause_for_deletion("BTCUSDT", "options")
        assert failure.value.status_code == 409
        assert manager.repository.list_jobs() == []
        assert evidence.read_text() == "retained"
    finally:
        reader.close()

    manager.subscriptions.pause_for_deletion("BTCUSDT", "options")
    assert evidence.read_text() == "retained"
    manager.close()
