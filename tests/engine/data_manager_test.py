"""Tenant Engine owns market-data configuration, recovery and job endpoints."""
import json

import pytest
from fastapi.testclient import TestClient

from engine.app import create_app
from engine.config import EngineConfig
from engine.data_manager.contracts import DataPolicy
from engine.data_manager.repository import PolicyRepository, StorageUnavailable
from engine.data_manager.service import DataService


def test_engine_persists_market_policy_and_rejects_cross_tenant(tmp_path):
    (tmp_path / "data").mkdir()
    app = create_app(EngineConfig(
        tmp_path / "state", "secret", project=tmp_path,
        tenant_id="42", tenant_purpose="development",
    ))
    app.state.runtime.ensure_tenant("42", "development")
    headers = {"Authorization": "Bearer secret"}
    body = {"policies": [{"symbol": "ETHUSDT", "realtime_enabled": False}]}

    with TestClient(app) as client:
        assert client.post("/tenants/42/data/control/sync", json=body).status_code == 401
        assert client.post("/tenants/99/data/control/sync", json=body, headers=headers).status_code == 403
        synced = client.post("/tenants/42/data/control/sync", json=body, headers=headers)
        assert synced.status_code == 200, synced.text
        assert synced.json()["synced"] == 1
        assert client.post("/tenants/42/data/control/sync", json=body, headers=headers).status_code == 200

        state = client.post("/tenants/42/data/control/subscriptions", json={}, headers=headers)
        assert state.status_code == 200, state.text
        assert state.json()["items"][0]["policy"]["symbol"] == "ETHUSDT"
        assert state.json()["items"][0]["runtime"]["state"] == "STOPPED"
        status = client.post("/tenants/42/data/control/storage", json={}, headers=headers)
        assert status.status_code == 200 and status.json()["available"]
        plan = client.post("/tenants/42/data/control/configuration-plan", headers=headers, json={
            "policy": {"symbol": "ETHUSDT", "realtime_enabled": False},
        })
        assert plan.status_code == 200, plan.text
        assert plan.json()["requires_confirmation"] is False
        assert client.post(
            "/tenants/42/data/control/job/start", headers=headers,
            json={"payload": {"action": "unknown"}},
        ).status_code == 422
        assert client.post(
            "/tenants/42/data/control/job", headers=headers, json={"id": "bad"},
        ).status_code == 422

    manager = app.state.market_data
    assert manager.policy_repository.list("42")[0]["symbol"] == "ETHUSDT"
    assert manager.directory.is_relative_to(tmp_path / "state")
    assert not (tmp_path / ".local/state/api/data_jobs").exists()


def test_engine_restores_policy_after_process_recreation(tmp_path):
    (tmp_path / "data").mkdir()
    app = create_app(EngineConfig(
        tmp_path / "state", "secret", project=tmp_path,
        tenant_id="42", tenant_purpose="development",
    ))
    app.state.runtime.ensure_tenant("42", "development")
    with TestClient(app) as client:
        res = client.post("/tenants/42/data/control/sync", json={
            "policies": [{"symbol": "ETHUSDT", "realtime_enabled": False}],
        }, headers={"Authorization": "Bearer secret"})
        assert res.status_code == 200
    app.state.runtime.close()

    restored = DataService("42", project=tmp_path, state_root=tmp_path / "state")
    assert restored.policy_repository.list("42")[0]["symbol"] == "ETHUSDT"
    restored.subscriptions.restore()
    assert restored.subscriptions.items()[0]["policy"]["symbol"] == "ETHUSDT"
    restored.close()


def test_tenant_policy_repository_never_accepts_cross_tenant_or_corrupt_state(tmp_path):
    repo = PolicyRepository(tmp_path / "policies.json", "42")
    repo.save([DataPolicy(symbol="ETHUSDT", realtime_enabled=False)], tenant_id="42")
    assert repo.list("42")[0]["symbol"] == "ETHUSDT"
    with pytest.raises(StorageUnavailable):
        repo.list("43")
    assert json.loads((tmp_path / "policies.json").read_text())[0]["symbol"] == "ETHUSDT"
    (tmp_path / "policies.json").write_text("{truncated")
    with pytest.raises(StorageUnavailable):
        repo.list("42")


def test_engine_owns_service_health_sampling_and_expired_evidence(tmp_path):
    (tmp_path / "data").mkdir()
    app = create_app(EngineConfig(
        tmp_path / "state", "secret", project=tmp_path,
        tenant_id="42", tenant_purpose="development",
    ))
    app.state.runtime.ensure_tenant("42", "development")
    headers = {"Authorization": "Bearer secret"}
    with TestClient(app) as client:
        monitor = app.state.market_data.monitor
        sample = monitor.sample()
        assert sample["state"] == "READY"
        assert monitor.path.is_file()
        assert client.post(
            "/tenants/42/data/control/services", headers=headers,
        ).json()["state"] == "READY"

        saved = json.loads(monitor.path.read_text())
        saved["valid_until_ms"] = 0
        monitor.path.write_text(json.dumps(saved))
        expired = client.post("/tenants/42/data/control/services", headers=headers)
        assert expired.status_code == 200
        assert expired.json()["state"] == "DEGRADED"
        assert expired.json()["reason"] == "monitor_expired"
        assert client.post(
            "/tenants/43/data/control/services", headers=headers,
        ).status_code == 403
    app.state.runtime.close()
