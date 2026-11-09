"""Engine is the sole owner of durable market-data jobs and recovery.

These tests replace the former Platform-local worker tests and operate on
the same EngineDataService used by the authenticated Engine HTTP routes.
"""

import json
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from engine.app import create_app
from engine.config import EngineConfig
from engine.data_manager.contracts import PairHistoryJob
from engine.data_manager.repository import StorageUnavailable
from engine.data_manager.service import DataService


def make_manager(root):
    (root / "data").mkdir(exist_ok=True)
    return DataService("42", project=root, state_root=root / "engine-state")


def job(identity="a" * 32, status="running", action="validate", parameters=None):
    return {
        "id": identity, "action": f"tick_data:{action}", "status": status,
        "created_at": datetime(2026, 8, 1, tzinfo=UTC).isoformat(),
        "finished_at": None, "parameters": parameters or {
            "symbol": "ETHUSDT", "product": "futures_um",
        },
        "result": None, "error": None,
    }


def test_engine_recovers_orphan_only_after_pair_lock_released(tmp_path):
    manager = make_manager(tmp_path)
    entry = job()
    manager.repository.save_job(entry)
    with manager._lease("ETHUSDT"):
        rows = manager.jobs()
        assert rows[0]["status"] == "unknown"
        assert manager.repository.list_jobs()[0]["status"] == "running"
    rows = manager.jobs()
    assert rows[0]["status"] == "interrupted"
    assert manager.repository.list_jobs()[0]["status"] == "interrupted"
    assert "保留" in rows[0]["error"]
    assert manager.task_snapshot()["tasks"][0]["status"] == "interrupted"
    manager.close()


def test_engine_does_not_clobber_job_finished_during_recovery(tmp_path, monkeypatch):
    manager = make_manager(tmp_path)
    active = job()
    snapshots = iter([[active], [{**active, "status": "succeeded"}]])
    monkeypatch.setattr(manager, "_records", lambda: next(snapshots))
    monkeypatch.setattr(
        manager.repository, "save_job",
        lambda *_: pytest.fail("Engine recovery overwrote a committed terminal job"),
    )
    assert manager.recover()[0]["status"] == "succeeded"
    manager.close()


def test_engine_backfill_retry_keeps_resolved_history_bounds(tmp_path, monkeypatch):
    manager = make_manager(tmp_path)
    payload = PairHistoryJob(
        symbol="ETHUSDT", base_level="1m", realtime_enabled=False,
        history_enabled=True, history_mode="duration", history_days=30,
    )
    parameters = payload.model_dump(mode="json", exclude={"action"})
    parameters.update(
        resolved_start="2026-08-01T00:00:00+00:00",
        resolved_end="2026-09-01T00:00:00+00:00",
    )
    manager.repository.save_job(job(status="failed", action="backfill", parameters=parameters))
    captured = []

    def record(next_payload, *, resolved_history=None, **_):
        captured.append((next_payload, resolved_history))
        return {"id": "b" * 32, "status": "running"}

    monkeypatch.setattr(manager, "start", record)
    assert manager.retry("a" * 32)["status"] == "running"
    assert captured[0][0].symbol == "ETHUSDT"
    assert captured[0][1] == {
        "resolved_start": "2026-08-01T00:00:00+00:00",
        "resolved_end": "2026-09-01T00:00:00+00:00",
    }
    manager.close()


def test_engine_never_adopts_legacy_platform_job_directory(tmp_path):
    (tmp_path / "data").mkdir()
    old = tmp_path / ".local/state/api/data_jobs/operations"
    old.mkdir(parents=True)
    previous = job(status="succeeded")
    path = old / f"{previous['id']}.json"
    path.write_text(json.dumps(previous))
    manager = DataService("42", project=tmp_path, state_root=tmp_path / "engine-state")
    assert manager.directory == tmp_path / "engine-state/data_jobs"
    assert manager.jobs() == []
    assert json.loads(path.read_text()) == previous
    manager.close()


def test_engine_corrupt_task_evidence_fails_closed_without_replacement(tmp_path):
    manager = make_manager(tmp_path)
    path = manager.repository.directory / ("a" * 32 + ".json")
    path.parent.mkdir(parents=True)
    path.write_text("{truncated")
    with pytest.raises(StorageUnavailable):
        manager.jobs()
    assert path.read_text() == "{truncated"
    manager.close()


def test_engine_http_job_list_detail_cancel_and_tenant_isolation(tmp_path):
    (tmp_path / "data").mkdir()
    app = create_app(EngineConfig(
        tmp_path / "engine-state", "secret", project=tmp_path,
        tenant_id="42", tenant_purpose="development",
    ))
    app.state.runtime.ensure_tenant("42", "development")
    entry = job(status="succeeded")
    app.state.market_data.repository.save_job(entry)
    headers = {"Authorization": "Bearer secret"}
    root = "/tenants/42/data/control"
    with TestClient(app) as client:
        assert client.post(root + "/jobs").status_code == 401
        assert client.post(
            "/tenants/99/data/control/jobs", headers=headers,
        ).status_code == 403
        listed = client.post(root + "/jobs", headers=headers)
        assert listed.status_code == 200
        assert [v["id"] for v in listed.json()] == [entry["id"]]
        detail = client.post(
            root + "/job", json={"id": entry["id"]}, headers=headers,
        )
        assert detail.status_code == 200
        assert detail.json()["status"] == "succeeded"
        cancelled = client.post(
            root + "/job/cancel", json={"id": entry["id"]}, headers=headers,
        )
        assert cancelled.status_code == 200
        assert cancelled.json()["status"] == "succeeded"
        assert client.post(
            root + "/job/retry", json={"id": entry["id"]}, headers=headers,
        ).status_code == 409
        snapshot = client.post(
            root + "/snapshot", json={}, headers=headers,
        )
        assert snapshot.status_code == 200
        assert snapshot.json()["tasks"][0]["status"] == "succeeded"
        assert client.post(
            root + "/job", json={"id": "../invalid"}, headers=headers,
        ).status_code == 422
    app.state.runtime.close()


def test_engine_export_rejects_symlink_escape_and_missing_file(tmp_path):
    (tmp_path / "data").mkdir()
    app = create_app(EngineConfig(
        tmp_path / "engine-state", "secret", project=tmp_path,
        tenant_id="42", tenant_purpose="development",
    ))
    app.state.runtime.ensure_tenant("42", "development")
    entry = job(status="succeeded", action="export")
    app.state.market_data.repository.save_job(entry)
    export_dir = tmp_path / "data/exports/ticks"
    export_dir.mkdir(parents=True)
    destination = export_dir / f"{entry['id']}.csv"
    destination.write_text("timestamp,price\\n1,2000\\n")
    url = f"/tenants/42/data/control/exports/{entry['id']}"
    headers = {"Authorization": "Bearer secret"}
    with TestClient(app) as client:
        assert client.get(url).status_code == 401
        assert client.get(url.replace("/42/", "/99/"), headers=headers).status_code == 403
        exported = client.get(url, headers=headers)
        assert exported.status_code == 200
        assert exported.text == "timestamp,price\\n1,2000\\n"
        destination.unlink()
        secret = tmp_path / "private-business.db"
        secret.write_text("never-expose")
        destination.symlink_to(secret)
        assert client.get(url, headers=headers).status_code == 404
        assert secret.read_text() == "never-expose"
    app.state.runtime.close()


def test_engine_rejects_unconfirmed_or_stale_pair_deletion_without_data_loss(tmp_path):
    (tmp_path / "data").mkdir()
    archive = tmp_path / "data/raw/futures/um/daily/trades/ETHUSDT/archive.zip"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"retained-evidence")
    app = create_app(EngineConfig(
        tmp_path / "engine-state", "secret", project=tmp_path,
        tenant_id="42", tenant_purpose="development",
    ))
    app.state.runtime.ensure_tenant("42", "development")
    route = "/tenants/42/data/control/job/start"
    headers = {"Authorization": "Bearer secret"}
    payload = {
        "action": "delete_pair", "symbol": "ETHUSDT",
        "product": "futures_um", "deletion_snapshot": "0" * 64,
        "confirmation": "DELETE ETHUSDT",
    }
    with TestClient(app) as client:
        invalid = client.post(route, json={"payload": {
            **payload, "confirmation": "DELETE BTCUSDT",
        }}, headers=headers)
        assert invalid.status_code == 422
        stale = client.post(route, json={"payload": payload}, headers=headers)
        assert stale.status_code == 409, stale.text
        assert archive.read_bytes() == b"retained-evidence"
        assert app.state.market_data.repository.list_jobs() == []
    app.state.runtime.close()
