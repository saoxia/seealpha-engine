"""Durable Engine job semantics: idempotency, isolation and fail-closed recovery."""
import threading
import time
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from engine.app import create_app
from engine.config import EngineConfig
from engine.task_queue import TaskCoordinator, TaskJournal


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
    jobs = TaskCoordinator(runtime, FakeData(), tmp_path)
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
        assert runtime.calls == 1
        assert "experiment-1" not in str(jobs.list("1"))
        with pytest.raises(HTTPException) as missing:
            jobs.get("2", initial["id"])
        assert missing.value.status_code == 404
    finally:
        jobs.close()
    restarted = TaskCoordinator(FakeRuntime(), FakeData(), tmp_path)
    try:
        assert restarted.get("1", initial["id"])["status"] == "SUCCEEDED"
        assert restarted.result("1", initial["id"])["account_id"] == "desk"
    finally:
        restarted.close()


def test_queued_cancellation_and_interrupted_recovery(tmp_path):
    gate = threading.Event()
    jobs = TaskCoordinator(FakeRuntime(gate), FakeData(), tmp_path)
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
        with pytest.raises(HTTPException) as refused:
            jobs.cancel("1", first["id"])
        assert refused.value.status_code == 409
        gate.set()
        assert done(jobs, "1", first["id"])["status"] == "SUCCEEDED"
        new = jobs.retry("1", second["id"])
        assert new["id"] != second["id"] and new["retry_of"] == second["id"]
        assert done(jobs, "1", new["id"])["status"] == "SUCCEEDED"
    finally:
        gate.set()
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
    recovered = TaskCoordinator(FakeRuntime(), FakeData(), tmp_path)
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
    jobs = TaskCoordinator(FakeRuntime(), manager, tmp_path)
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
        tmp_path / "state", "secret", project=tmp_path,
        tenant_id="42", tenant_purpose="development",
    ))
    runtime = app.state.runtime
    runtime.ensure_tenant("42", "development")
    monkeypatch.setattr(runtime.store, "strategy_loads", lambda *_a, **_kw: [{"ok": True}])
    executed = []
    monkeypatch.setattr(runtime, "run_backtest", lambda *args: executed.append(args) or {
        "tenant_id": args[0], "ending_balance": "1010", "fills": [],
    })
    body = {"kind": "backtest", "parameters": parameters(), "idempotency_key": "http-1"}
    headers = {"Authorization": "Bearer secret"}

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
    assert len(executed) == 1
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
        tmp_path / "state", "secret", project=tmp_path,
        tenant_id="dev", tenant_purpose="development",
    ))
    runtime = app.state.runtime
    runtime.ensure_tenant("dev", "development")
    _intent(runtime, "dev")
    _issue(runtime, "dev", "development", "real-1")

    with TestClient(app) as client:
        response = client.post(
            "/tenants/dev/jobs",
            headers={"Authorization": "Bearer secret"},
            json={"kind": "backtest", "parameters": {
                **_window(start), "account_id": "desk",
            }},
        )
        assert response.status_code == 202, response.text
        identity = response.json()["id"]
        assert done(app.state.tasks, "dev", identity)["status"] == "SUCCEEDED"
        result = client.get(
            f"/tenants/dev/jobs/{identity}/result",
            headers={"Authorization": "Bearer secret"},
        )
        assert result.status_code == 200, result.text
        assert result.json()["bar_count"] > 0
        assert result.json()["fills"]
    runtime.close()
