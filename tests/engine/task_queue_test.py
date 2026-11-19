"""Durable Engine job semantics: idempotency, isolation and fail-closed recovery."""
import json
import sqlite3
import sys
import threading
import time
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from engine.app import create_app
from engine.config import EngineConfig
from engine.task_queue import TaskCoordinator, TaskJournal

TEST_TOKEN = "integration-test-engine-token-" + "a" * 48

FAKE_WORKER = """
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

root, tenant, identity, gate, fixed_balance = sys.argv[1:]
root = Path(root)
with sqlite3.connect(root / "book.sqlite") as conn:
    row = conn.execute(
        "SELECT record_json FROM backtest_tasks WHERE tenant_id=? AND id=?",
        (tenant, identity),
    ).fetchone()
record = json.loads(row[0])
(root / f"started-{identity}").write_text(str(os.getpid()))
with (root / "child-calls.log").open("a") as handle:
    handle.write(identity + "\\n")
if gate:
    while not Path(gate).exists():
        time.sleep(0.02)
result = {
    "tenant_id": tenant, "account_id": record["parameters"]["account_id"],
    "ending_balance": fixed_balance or record["parameters"]["starting_balance"],
    "quote_currency": "USDT", "fills": [],
    "env_clean": "ENGINE_TOKEN" not in os.environ,
}
output = root / "tasks" / tenant / f"{identity}.result.json"
output.parent.mkdir(parents=True, exist_ok=True)
staged = output.with_suffix(".tmp")
staged.write_text(json.dumps(result))
os.replace(staged, output)
"""


def fake_worker_factory(root, *, gate=None, balance=""):
    root = str(root)
    return lambda tenant, identity: [
        sys.executable, "-c", FAKE_WORKER, root, tenant, identity,
        str(gate) if gate is not None else "", balance,
    ]


class FakeRuntime:
    def __init__(self, gate=None):
        self.store = SimpleNamespace(strategy_loads=lambda *_a, **_kw: [{"grant_purpose": "development"}])
        self.gate = gate
        self.calls = 0

    def tenant_row(self, tenant_id):
        if tenant_id == "2":
            return {"purpose": "production", "status": "active"}
        return {"purpose": "development", "status": "active"}

    def run_backtest(self, tenant_id, account_id, start, end, interval, balance, quote):
        self.calls += 1
        if self.gate is not None:
            assert self.gate.wait(5), "test release gate timed out"
        return {
            "tenant_id": tenant_id, "account_id": account_id, "starting_balance": balance,
            "ending_balance": balance, "quote_currency": quote, "fills": [],
        }


class FakeData:
    tenant_id = "1"

    def __init__(self):
        self.entries = {}
        self.started = 0

    def start(self, payload, *, idempotency_key=None, request_hash=None):
        self.started += 1
        row = {
            "id": "a" * 32, "action": payload.action, "status": "running",
            "created_at": "2026-01-01T00:00:00+00:00", "finished_at": None,
            "result": None, "error": None, "cancellable": True,
        }
        self.entries[row["id"]] = row
        return row

    def jobs(self):
        return list(self.entries.values())

    def job(self, identity):
        if identity not in self.entries:
            raise HTTPException(404, "data task not found")
        return self.entries[identity]

    def cancel(self, identity):
        row = self.job(identity)
        row["status"] = "cancelled"
        return row

    def retry(self, identity):
        return self.job(identity)


def parameters(account_id="desk"):
    return {
        "account_id": account_id,
        "start": "2024-01-01", "end": "2024-01-02",
        "interval": "1m", "starting_balance": "1000", "quote_currency": "USDT",
    }


def done(manager, tenant, identity):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        row = manager.get(tenant, identity)
        if row["status"] in {"SUCCEEDED", "FAILED", "CANCELLED", "INTERRUPTED"}:
            return row
        time.sleep(0.01)
    pytest.fail(f"task {identity} never became terminal")


def test_durable_backtest_idempotency_and_result_are_tenant_scoped(tmp_path):
    runtime = FakeRuntime()
    jobs = TaskCoordinator(runtime, FakeData(), tmp_path,
                           command_factory=fake_worker_factory(tmp_path))
    try:
        initial = jobs.submit("1", "backtest", parameters(), idempotency_key="experiment-1")
        # Previously accepted requests must replay even when issuance is gone.
        runtime.store.strategy_loads = lambda *_a, **_kw: []
        same = jobs.submit("1", "backtest", parameters(), idempotency_key="experiment-1")
        assert initial["id"] == same["id"]
        assert "idempotency_key" not in initial and "fingerprint" not in initial
        with pytest.raises(HTTPException) as error:
            jobs.submit("1", "backtest", parameters("other"), idempotency_key="experiment-1")
        assert error.value.status_code == 409
        result = done(jobs, "1", initial["id"])
        assert result["status"] == "SUCCEEDED"
        assert result["result_ref"].endswith(initial["id"] + "/result")
        assert jobs.result("1", initial["id"])["ending_balance"] == "1000"
        assert (tmp_path / "child-calls.log").read_text().splitlines() == [initial["id"]]
        assert jobs.result("1", initial["id"])["env_clean"]
        assert "experiment-1" not in str(jobs.list("1"))
        with pytest.raises(HTTPException) as missing:
            jobs.get("2", initial["id"])
        assert missing.value.status_code == 404
    finally:
        jobs.close()
    restarted = TaskCoordinator(FakeRuntime(), FakeData(), tmp_path,
                                  command_factory=fake_worker_factory(tmp_path))
    try:
        assert restarted.get("1", initial["id"])["status"] == "SUCCEEDED"
        assert restarted.result("1", initial["id"])["account_id"] == "desk"
    finally:
        restarted.close()


def test_queued_cancellation_and_interrupted_recovery(tmp_path):
    gate = tmp_path / "gate"
    jobs = TaskCoordinator(FakeRuntime(), FakeData(), tmp_path,
                           command_factory=fake_worker_factory(tmp_path, gate=gate))
    try:
        first = jobs.submit("1", "backtest", parameters())
        deadline = time.monotonic() + 5
        while jobs.get("1", first["id"])["status"] != "RUNNING":
            assert time.monotonic() < deadline
            time.sleep(0.01)
        second = jobs.submit("1", "backtest", parameters("other"))
        assert jobs.get("1", second["id"])["status"] == "PENDING"
        cancelled = jobs.cancel("1", second["id"])
        assert cancelled["status"] == "CANCELLED"
        stopping = jobs.cancel("1", first["id"])
        assert stopping["status"] == "CANCELLING"
        assert done(jobs, "1", first["id"])["status"] == "CANCELLED"
        gate.touch()
        new = jobs.retry("1", second["id"])
        assert new["id"] != second["id"] and new["retry_of"] == second["id"]
        assert done(jobs, "1", new["id"])["status"] == "SUCCEEDED"
    finally:
        gate.touch()
        jobs.close()

    journal = TaskJournal(tmp_path / "tasks")
    journal.save({
        "id": "c" * 32, "tenant_id": "1", "kind": "backtest", "action": "run",
        "status": "RUNNING", "parameters": parameters(),
        "fingerprint": "dummy", "created_at": "2026-01-01T00:00:00+00:00",
        "started_at": "2026-01-01T00:00:00+00:00", "finished_at": None,
        "progress": "running", "error": None, "result_ref": None,
        "cancellable": False, "retryable": False,
    })
    recovered = TaskCoordinator(FakeRuntime(), FakeData(), tmp_path,
                                command_factory=fake_worker_factory(tmp_path))
    try:
        row = recovered.get("1", "c" * 32)
        assert row["status"] == "INTERRUPTED"
        assert row["result_ref"] is None
        restarted = recovered.retry("1", "c" * 32)
        assert restarted["id"] != "c" * 32
        assert done(recovered, "1", restarted["id"])["status"] == "SUCCEEDED"
    finally:
        recovered.close()


def test_legacy_data_jobs_appear_in_same_lifecycle(tmp_path):
    manager = FakeData()
    jobs = TaskCoordinator(FakeRuntime(), manager, tmp_path,
                           command_factory=fake_worker_factory(tmp_path))
    try:
        value = jobs.submit("1", "data", {
            "action": "validate", "symbol": "ETHUSDT",
            "start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z",
            "intervals": ["1m"],
        })
        assert value["kind"] == "data" and value["status"] == "RUNNING"
        assert jobs.get("1", value["id"])["status"] == "RUNNING"
        assert jobs.list("1")[0]["kind"] == "data"
        with pytest.raises(HTTPException) as no_result:
            jobs.result("1", value["id"])
        assert no_result.value.status_code == 409
        assert jobs.cancel("1", value["id"])["status"] == "CANCELLED"
    finally:
        jobs.close()


def test_real_engine_http_async_task_auth_validation_and_result(tmp_path, monkeypatch):
    app = create_app(EngineConfig(
        tmp_path / "state", TEST_TOKEN, project=tmp_path, auth_mode="token",
        tenant_id="42", tenant_purpose="development",
    ))
    runtime = app.state.runtime
    runtime.ensure_tenant("42", "development")
    monkeypatch.setattr(runtime.store, "strategy_loads", lambda *_a, **_kw: [{"ok": True}])
    app.state.tasks.command_factory = fake_worker_factory(tmp_path / "state", balance="1010")
    body = {"kind": "backtest", "parameters": parameters(), "idempotency_key": "http-1"}
    headers = {"Authorization": f"Bearer {TEST_TOKEN}"}

    with TestClient(app) as client:
        assert client.post("/tenants/42/jobs", json=body).status_code == 401
        assert client.post("/tenants/43/jobs", json=body, headers=headers).status_code == 403
        assert client.post("/tenants/42/jobs", json={
            "kind": "backtest", "parameters": {"start": "2024-01-01"},
        }, headers=headers).status_code == 422
        created = client.post("/tenants/42/jobs", json=body, headers=headers)
        assert created.status_code == 202, created.text
        identity = created.json()["id"]
        assert client.post("/tenants/42/jobs", json=body, headers=headers).json()["id"] == identity
        assert done(app.state.tasks, "42", identity)["status"] == "SUCCEEDED"
        result = client.get(f"/tenants/42/jobs/{identity}/result", headers=headers)
        assert result.status_code == 200 and result.json()["ending_balance"] == "1010"
        assert client.get("/tenants/42/jobs", headers=headers).json()["items"][0]["id"] == identity
        assert client.get(f"/tenants/42/jobs/{'f' * 32}", headers=headers).status_code == 404
        assert client.post(
            f"/tenants/42/jobs/{identity}/retry", headers=headers,
        ).status_code == 409
    assert len((tmp_path / "state" / "child-calls.log").read_text().splitlines()) == 1
    runtime.close()


def test_real_data_manager_rejects_duplicate_key_before_second_process(tmp_path):
    from engine.data_manager.service import DataService

    manager = object.__new__(DataService)
    manager.require_storage = lambda **_kw: None
    manager.request = lambda _payload: (None, None)
    manager.subscriptions = SimpleNamespace(_read=list)
    manager.recover = lambda: None
    manager.lock = threading.RLock()
    manager.processes = {}
    manager.closed = False
    existing = {
        "id": "a" * 32, "action": "tick_data:validate", "status": "succeeded",
        "created_at": "2026-01-01T00:00:00+00:00", "finished_at": None,
        "parameters": {}, "result": {}, "error": None,
        "idempotency_key": "unique-key", "request_hash": "fingerprint-a",
    }
    manager._records = lambda: [existing]
    payload = SimpleNamespace(
        action="validate", symbol="ETHUSDT", product="futures_um",
    )
    same = manager.start(
        payload, idempotency_key="unique-key", request_hash="fingerprint-a",
    )
    assert same["id"] == existing["id"]
    assert "idempotency_key" not in same
    with pytest.raises(HTTPException) as conflict:
        manager.start(payload, idempotency_key="unique-key", request_hash="fingerprint-b")
    assert conflict.value.status_code == 409


def test_actual_nautilus_backtest_runs_inside_background_engine_worker(tmp_path):
    """Exercise a real issued strategy, not only the mocked HTTP executor."""
    from datetime import UTC, datetime

    from tests.engine.backtest_test import (
        _intent,
        _issue,
        _window,
        write_crossover_catalog,
    )

    start = datetime(2024, 1, 1, tzinfo=UTC)
    write_crossover_catalog(tmp_path / "data" / "catalog", start)
    app = create_app(EngineConfig(
        tmp_path / "state", TEST_TOKEN, project=tmp_path, auth_mode="token",
        tenant_id="dev", tenant_purpose="development",
    ))
    runtime = app.state.runtime
    runtime.ensure_tenant("dev", "development")
    _intent(runtime, "dev")
    _issue(runtime, "dev", "development", "real-1")

    with TestClient(app) as client:
        response = client.post(
            "/tenants/dev/jobs",
            headers={"Authorization": f"Bearer {TEST_TOKEN}"},
            json={"kind": "backtest", "parameters": {
                **_window(start), "account_id": "desk",
            }},
        )
        assert response.status_code == 202, response.text
        identity = response.json()["id"]
        assert done(app.state.tasks, "dev", identity)["status"] == "SUCCEEDED"
        result = client.get(
            f"/tenants/dev/jobs/{identity}/result",
            headers={"Authorization": f"Bearer {TEST_TOKEN}"},
        )
        assert result.status_code == 200, result.text
        assert result.json()["bar_count"] > 0
        assert result.json()["fills"]
    runtime.close()


def test_backtest_task_metadata_uses_existing_engine_sqlite(tmp_path):
    jobs = TaskCoordinator(FakeRuntime(), FakeData(), tmp_path,
                           command_factory=fake_worker_factory(tmp_path))
    try:
        task = jobs.submit("1", "backtest", parameters(), idempotency_key="db-key")
        assert done(jobs, "1", task["id"])["status"] == "SUCCEEDED"
        assert not list((tmp_path / "tasks").rglob("*.record.json"))
        assert (tmp_path / "tasks" / "1" / f"{task['id']}.result.json").exists()
        with sqlite3.connect(tmp_path / "book.sqlite") as db:
            saved = db.execute(
                "SELECT status, idempotency_key, record_json FROM backtest_tasks "
                "WHERE tenant_id=? AND id=?", ("1", task["id"]),
            ).fetchone()
        assert saved[:2] == ("SUCCEEDED", "db-key")
        assert json.loads(saved[2])["id"] == task["id"]
    finally:
        jobs.close()


def test_legacy_json_task_record_migrates_once_and_restarts_fail_closed(tmp_path):
    root = tmp_path / "tasks"
    folder = root / "dev"
    folder.mkdir(parents=True)
    identity = "e" * 32
    original = {
        "id": identity, "tenant_id": "dev", "kind": "backtest", "action": "run",
        "status": "RUNNING", "parameters": parameters(), "fingerprint": "original",
        "idempotency_key": "old-key", "created_at": "2026-01-01T00:00:00+00:00",
        "started_at": "2026-01-01T00:00:00+00:00", "finished_at": None,
        "progress": "running", "error": None, "result_ref": None,
        "cancellable": False, "retryable": False,
    }
    legacy = folder / f"{identity}.record.json"
    legacy.write_text(json.dumps(original), encoding="utf-8")
    journal = TaskJournal(root)
    assert journal.get("dev", identity)["status"] == "RUNNING"
    assert not legacy.exists()
    assert legacy.with_name(legacy.name + ".migrated").exists()
    journal.recover_interrupted()
    assert journal.get("dev", identity)["status"] == "INTERRUPTED"
    assert journal.find_idempotent("dev", "old-key")["id"] == identity
    reopened = TaskJournal(root)
    assert reopened.get("dev", identity)["status"] == "INTERRUPTED"
    assert reopened.get("other", identity) is None
    reopened.store.delete_runtime("dev")
    assert reopened.get("dev", identity) is None
    assert TaskJournal(root).get("dev", identity) is None


def test_legacy_task_identity_mismatch_is_not_imported(tmp_path):
    root = tmp_path / "tasks"
    folder = root / "tenant"
    folder.mkdir(parents=True)
    identity = "d" * 32
    (folder / f"{identity}.record.json").write_text(
        json.dumps({"id": identity, "tenant_id": "another", "kind": "backtest"}),
        encoding="utf-8",
    )
    with pytest.raises(HTTPException) as error:
        TaskJournal(root)
    assert error.value.status_code == 503
    assert (folder / f"{identity}.record.json").exists()


def test_bounded_workers_cancel_active_job_and_start_waiting_job(tmp_path):
    gate = tmp_path / "release"
    jobs = TaskCoordinator(FakeRuntime(), FakeData(), tmp_path, max_workers=2,
                           command_factory=fake_worker_factory(tmp_path, gate=gate))
    try:
        first = jobs.submit("1", "backtest", parameters("one"))
        second = jobs.submit("1", "backtest", parameters("two"))
        third = jobs.submit("1", "backtest", parameters("three"))
        deadline = time.monotonic() + 8
        while not all((tmp_path / f"started-{item['id']}").exists() for item in (first, second)):
            assert time.monotonic() < deadline
            time.sleep(0.01)
        assert jobs.get("1", third["id"])["status"] == "PENDING"
        assert not (tmp_path / f"started-{third['id']}").exists()
        assert jobs.cancel("1", first["id"])["status"] == "CANCELLING"
        assert done(jobs, "1", first["id"])["status"] == "CANCELLED"
        with pytest.raises(HTTPException) as missing:
            jobs.result("1", first["id"])
        assert missing.value.status_code == 409
        deadline = time.monotonic() + 8
        while not (tmp_path / f"started-{third['id']}").exists():
            assert time.monotonic() < deadline
            time.sleep(0.01)
        gate.touch()
        assert done(jobs, "1", second["id"])["status"] == "SUCCEEDED"
        assert done(jobs, "1", third["id"])["status"] == "SUCCEEDED"
    finally:
        gate.touch()
        jobs.close()


def test_running_cancel_endpoint_is_authenticated_and_tenant_scoped(tmp_path):
    gate = tmp_path / "release"
    app = create_app(EngineConfig(
        tmp_path / "state", TEST_TOKEN, project=tmp_path, auth_mode="token",
        tenant_id="42", tenant_purpose="development",
    ))
    runtime = app.state.runtime
    runtime.ensure_tenant("42", "development")
    runtime.store.strategy_loads = lambda *_a, **_kw: [{"ok": True}]
    app.state.tasks.command_factory = fake_worker_factory(tmp_path / "state", gate=gate)
    headers = {"Authorization": f"Bearer {TEST_TOKEN}"}
    try:
        with TestClient(app) as client:
            result = client.post(
                "/tenants/42/jobs", headers=headers,
                json={"kind": "backtest", "parameters": parameters()},
            )
            assert result.status_code == 202
            task_id = result.json()["id"]
            deadline = time.monotonic() + 8
            while not (tmp_path / "state" / f"started-{task_id}").exists():
                assert time.monotonic() < deadline
                time.sleep(0.01)
            path = f"/tenants/42/jobs/{task_id}/cancel"
            assert client.post(path).status_code == 401
            assert client.post(path.replace("/42/", "/43/"), headers=headers).status_code == 403
            canceled = client.post(path, headers=headers)
            assert canceled.status_code == 200
            assert canceled.json()["status"] in {"CANCELLING", "CANCELLED"}
            assert done(app.state.tasks, "42", task_id)["status"] == "CANCELLED"
    finally:
        gate.touch()
        app.state.tasks.close()
        runtime.close()


def test_worker_timeout_fails_without_exposing_result(tmp_path):
    gate = tmp_path / "never-released"
    jobs = TaskCoordinator(
        FakeRuntime(), FakeData(), tmp_path, timeout_seconds=0.5,
        command_factory=fake_worker_factory(tmp_path, gate=gate),
    )
    try:
        task = jobs.submit("1", "backtest", parameters())
        result = done(jobs, "1", task["id"])
        assert result["status"] == "FAILED"
        assert "时限" in result["error"]
        assert result["retryable"] is True
        assert not (tmp_path / "tasks" / "1" / f"{task['id']}.result.json").exists()
    finally:
        jobs.close()


@pytest.mark.parametrize("count", [0, 9, -1, True])
def test_worker_count_has_hard_limit(tmp_path, count):
    with pytest.raises(ValueError, match="MAX_WORKERS"):
        TaskCoordinator(FakeRuntime(), FakeData(), tmp_path, max_workers=count)


def test_worker_settings_default_update_and_recovery(tmp_path):
    jobs = TaskCoordinator(FakeRuntime(), FakeData(), tmp_path,
                           command_factory=fake_worker_factory(tmp_path))
    try:
        original = jobs.worker_settings("1")
        assert original["max_workers"] == 1
        assert original["timeout_seconds"] == 3600
        assert original["source"] == "default"
        changed = jobs.update_worker_settings("1", max_workers=3, timeout_seconds=90)
        assert changed["source"] == "saved"
        assert changed["max_workers"] == 3
        assert changed["timeout_seconds"] == 90
        assert len(jobs.workers) == 3
        jobs.update_worker_settings("1", max_workers=1, timeout_seconds=120)
        assert jobs.max_workers == 1
        assert len(jobs.workers) == 3  # Idle supervisors are reused, not killed.
        with pytest.raises(HTTPException) as invalid:
            jobs.update_worker_settings("1", max_workers=9, timeout_seconds=1)
        assert invalid.value.status_code == 422
        assert jobs.worker_settings("1")["max_workers"] == 1
    finally:
        jobs.close()

    restarted = TaskCoordinator(
        FakeRuntime(), FakeData(), tmp_path, max_workers=4, timeout_seconds=700,
        command_factory=fake_worker_factory(tmp_path),
    )
    try:
        assert restarted.worker_settings("1")["source"] == "saved"
        assert restarted.worker_settings("1")["max_workers"] == 1
        assert restarted.worker_settings("1")["timeout_seconds"] == 120
        assert restarted.worker_settings("1")["default_max_workers"] == 4
        assert restarted.worker_settings("1")["default_timeout_seconds"] == 700
    finally:
        restarted.close()


def test_decreasing_worker_limit_does_not_terminate_running_jobs(tmp_path):
    def gated_worker(tenant, identity):
        return fake_worker_factory(tmp_path, gate=tmp_path / f"release-{identity}")(
            tenant, identity,
        )

    jobs = TaskCoordinator(FakeRuntime(), FakeData(), tmp_path, max_workers=2,
                           command_factory=gated_worker)
    first = second = third = None
    try:
        first = jobs.submit("1", "backtest", parameters("first"))
        second = jobs.submit("1", "backtest", parameters("second"))
        deadline = time.monotonic() + 8
        while not all(
            (tmp_path / f"started-{item['id']}").exists() for item in (first, second)
        ):
            assert time.monotonic() < deadline
            time.sleep(0.01)

        updated = jobs.update_worker_settings("1", max_workers=1, timeout_seconds=20)
        assert updated["running"] == 2
        third = jobs.submit("1", "backtest", parameters("third"))
        (tmp_path / f"release-{first['id']}").touch()
        assert done(jobs, "1", first["id"])["status"] == "SUCCEEDED"
        time.sleep(0.2)
        assert not (tmp_path / f"started-{third['id']}").exists()
        assert jobs.get("1", second["id"])["status"] == "RUNNING"
        (tmp_path / f"release-{second['id']}").touch()
        assert done(jobs, "1", second["id"])["status"] == "SUCCEEDED"
        deadline = time.monotonic() + 8
        while not (tmp_path / f"started-{third['id']}").exists():
            assert time.monotonic() < deadline
            time.sleep(0.01)
        (tmp_path / f"release-{third['id']}").touch()
        assert done(jobs, "1", third["id"])["status"] == "SUCCEEDED"
    finally:
        for row in (first, second, third):
            if row is not None:
                (tmp_path / f"release-{row['id']}").touch()
        jobs.close()


def test_worker_settings_http_admin_token_and_validation(tmp_path):
    app = create_app(EngineConfig(
        tmp_path / "state", TEST_TOKEN, project=tmp_path, auth_mode="token",
        tenant_id="dev", tenant_purpose="development",
    ))
    runtime = app.state.runtime
    runtime.ensure_tenant("dev", "development")
    headers = {"Authorization": f"Bearer {TEST_TOKEN}"}
    with TestClient(app) as client:
        path = "/tenants/dev/jobs/worker-settings"
        assert client.get(path).status_code == 401
        assert client.get(path.replace("/dev/", "/other/"), headers=headers).status_code == 403
        initial = client.get(path, headers=headers)
        assert initial.status_code == 200 and initial.json()["max_workers"] == 1
        assert initial.json()["source"] == "default"
        assert client.put(path, headers=headers, json={
            "max_workers": True, "timeout_seconds": 3600,
        }).status_code == 422
        assert client.put(path, headers=headers, json={
            "max_workers": 9, "timeout_seconds": 3600,
        }).status_code == 422
        saved = client.put(path, headers=headers, json={
            "max_workers": 2, "timeout_seconds": 1800,
        })
        assert saved.status_code == 200, saved.text
        assert saved.json()["max_workers"] == 2
        assert saved.json()["source"] == "saved"
        assert client.get(path, headers=headers).json()["timeout_seconds"] == 1800
    runtime.close()
