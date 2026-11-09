"""No-model, no-exchange Research API producer/consumer contract tests."""
from __future__ import annotations

from dataclasses import dataclass
import io
import time
from types import SimpleNamespace
from uuid import uuid4

from fastapi.testclient import TestClient
import pyarrow.parquet as pq
import pytest

from engine.config import EngineConfig
from engine.research_api import create_research_app
from engine.research_auth import (
    ENGINE_HEADER, RESEARCH_HEADER, sign_research_grant,
)

SECRET = "research-signing-key-for-tests-00000000000000"
ADMIN = "engine-admin-token-for-tests"
DATASET = "futures_um:ETHUSDT:1m"
STRATEGY = "account:desk"
OPS = [
    "datasets:read", "snapshots:create", "snapshots:read",
    "backtests:submit", "backtests:read", "backtests:cancel",
]


@dataclass
class FakePipelineSettings:
    catalog_dir: object
    intervals: tuple[str, ...] = ()


class FakeStore:
    def strategy_loads(self, tenant_id, account_id, include_archive=False):
        if tenant_id != "dev" or account_id != "desk":
            return []
        return [{
            "grant_purpose": "development",
            "parameters": {"symbol": "ETHUSDT"},
            "tenant_id": "dev",
        }]


class FakeRuntime:
    def __init__(self, root):
        self.project = root
        self.root = root / "state"
        self.root.mkdir()
        (root / "data" / "catalog").mkdir(parents=True)
        self.store = FakeStore()
        self.tenant = {"purpose": "development", "status": "active"}

    def tenant_row(self, tenant_id):
        return self.tenant if tenant_id == "dev" else None


class FakeTasks:
    def __init__(self):
        self.records = {}
        self.journal = SimpleNamespace(get=lambda _tenant, identity: self.records.get(identity))

    def submit(self, tenant_id, kind, params, *, idempotency_key):
        assert tenant_id == "dev" and kind == "backtest"
        assert params["account_id"] == "desk"
        for row in self.records.values():
            if row["idempotency_key"] == idempotency_key:
                return row
        key = uuid4().hex
        result = {
            "id": key, "kind": kind, "status": "PENDING",
            "idempotency_key": idempotency_key, "parameters": params,
        }
        self.records[key] = result
        return result

    def get(self, tenant_id, identity):
        if identity not in self.records:
            from fastapi import HTTPException
            raise HTTPException(404, "not found")
        row = self.records[identity].copy()
        row.pop("idempotency_key")
        return row

    def result(self, tenant_id, identity):
        return {"net_pnl": "12.34", "currency": "USDT"}

    def cancel(self, tenant_id, identity):
        self.records[identity]["status"] = "CANCELLED"
        return self.get(tenant_id, identity)


def claims(
    *, task_id="task-1", tenant_id="dev", ops=None,
    datasets=None, strategies=None, now=None,
):
    now = int(time.time()) if now is None else now
    return {
        "version": 1, "audience": "seealpha-engine-research",
        "tenant_id": tenant_id, "task_id": task_id,
        "ops": OPS if ops is None else ops,
        "datasets": [DATASET] if datasets is None else datasets,
        "strategies": [STRATEGY] if strategies is None else strategies,
        "not_before": now - 5, "expires_at": now + 600,
    }


def headers(**kwargs):
    return {
        ENGINE_HEADER: "2", RESEARCH_HEADER: "1",
        "Authorization": f"Bearer {sign_research_grant(SECRET, claims(**kwargs))}",
    }


@pytest.fixture
def service(tmp_path, monkeypatch):
    runtime, tasks = FakeRuntime(tmp_path), FakeTasks()
    monkeypatch.setattr(
        "engine.research_snapshots.load_settings",
        lambda: FakePipelineSettings(tmp_path / "data" / "catalog"),
    )
    monkeypatch.setattr(
        "engine.research_snapshots.dataset_inventory",
        lambda _settings, all_levels: [{
            "symbol": "ETHUSDT", "product": "futures_um",
            "level": "1m", "quality": "unverified",
            "event_ranges_ns": [["1767225600000000000", "1798761600000000000"]],
        }],
    )

    def mock_rows(settings, symbol, start, end, *, interval, market):
        assert symbol == "ETHUSDT" and interval == "1m" and market == "futures_um"
        for index in range(4):
            yield {
                "ts_event_ns": str(1767225600000000000 + index * 60_000_000_000),
                "open": "2000.00", "high": "2010.00", "low": "1990.00",
                "close": "2005.00", "volume": "100.0",
            }

    monkeypatch.setattr("engine.research_snapshots.rows", mock_rows)
    config = EngineConfig(
        state_dir=runtime.root, token=ADMIN, project=tmp_path,
        tenant_id="dev", tenant_purpose="development",
        research_signing_key=SECRET,
    )
    app = create_research_app(config, runtime, tasks)
    with TestClient(app) as client:
        yield client, app, runtime, tasks
    app.state.snapshots.close()


def test_scoped_dataset_and_snapshot_binary(service):
    client, app, _, _ = service
    assert client.get("/v1/datasets", headers=headers()).json()["datasets"][0][
        "id"
    ] == DATASET
    create = client.post(
        "/v1/snapshots", headers=headers(),
        json={
            "dataset_id": DATASET, "start": "2026-01-01",
            "end": "2026-01-02", "format": "parquet",
        },
    )
    assert create.status_code == 202, create.text
    snapshot_id = create.json()["snapshot_id"]
    app.state.snapshots.pool.shutdown(wait=True)
    result = client.get(f"/v1/snapshots/{snapshot_id}", headers=headers())
    assert result.status_code == 200, result.text
    metadata = result.json()
    assert metadata["status"] == "ready"
    assert metadata["row_count"] == 4
    response = client.get(
        f"/v1/snapshots/{snapshot_id}/content", headers=headers(),
    )
    assert response.status_code == 200
    assert response.headers[ENGINE_HEADER] == "2"
    assert response.headers[RESEARCH_HEADER] == "1"
    table = pq.read_table(io.BytesIO(response.content))
    assert table.num_rows == 4
    assert table["close"][0].as_py() == "2005.00"
    assert client.get(
        f"/v1/snapshots/{snapshot_id}", headers=headers(task_id="not-owner"),
    ).status_code == 404


def test_backtest_reuses_queue_and_is_bound_to_task(service):
    client, _, _, tasks = service
    body = {
        "purpose": "development", "execution": "simulation",
        "strategy_ref": STRATEGY, "dataset_refs": [DATASET],
        "start": "2026-01-01", "end": "2026-01-02",
        "starting_balance": "10000", "quote_currency": "USDT",
        "parameters": {}, "idempotency_key": "research-run-001",
    }
    response = client.post("/v1/backtests", headers=headers(), json=body)
    assert response.status_code == 202, response.text
    job_id = response.json()["backtest_id"]
    assert response.json()["status"] == "queued"
    assert client.post("/v1/backtests", headers=headers(), json=body).json()[
        "backtest_id"
    ] == job_id
    other = headers(task_id="other-task")
    assert client.get(f"/v1/backtests/{job_id}", headers=other).status_code == 404
    assert client.get(f"/v1/backtests/{job_id}", headers=headers()).json()[
        "status"
    ] == "queued"
    tasks.records[job_id]["status"] = "SUCCEEDED"
    assert client.get(f"/v1/backtests/{job_id}", headers=headers()).json()[
        "result"
    ]["net_pnl"] == "12.34"
    assert client.post(f"/v1/backtests/{job_id}/cancel", headers=headers()).json()[
        "status"
    ] == "cancelled"


def test_scopes_version_and_expiration_fail_closed(service):
    client, _, _, _ = service
    assert client.get("/v1/datasets").status_code == 426
    assert client.get("/v1/datasets", headers={
        "Authorization": f"Bearer {ADMIN}", ENGINE_HEADER: "2", RESEARCH_HEADER: "1",
    }).status_code == 401
    invalid_version = headers()
    invalid_version[RESEARCH_HEADER] = "2"
    assert client.get("/v1/datasets", headers=invalid_version).status_code == 426
    assert client.get("/v1/datasets", headers=headers(tenant_id="other")).status_code == 403
    assert client.get("/v1/datasets", headers=headers(ops=["backtests:read"])).status_code == 403
    assert client.get("/v1/datasets", headers=headers(datasets=[])).json() == {
        "datasets": [],
    }
    past = int(time.time()) - 10000
    expired = sign_research_grant(SECRET, claims(now=past), now=past)
    assert client.get("/v1/datasets", headers={
        "Authorization": f"Bearer {expired}", ENGINE_HEADER: "2", RESEARCH_HEADER: "1",
    }).status_code == 401
    assert client.post("/v1/backtests", headers=headers(), json={
        "purpose": "production", "execution": "live",
        "strategy_ref": STRATEGY, "dataset_refs": [DATASET],
        "start": "2026-01-01", "end": "2026-01-02",
        "starting_balance": "10000", "quote_currency": "USDT",
        "idempotency_key": "no-live",
    }).status_code == 422


def test_dataset_scope_and_snapshot_limits(service):
    client, _, _, _ = service
    assert client.post("/v1/snapshots", headers=headers(datasets=[]), json={
        "dataset_id": DATASET, "start": "2026-01-01",
        "end": "2026-01-02", "format": "parquet",
    }).status_code == 403
    assert client.post("/v1/snapshots", headers=headers(), json={
        "dataset_id": DATASET, "start": "2026-01-01",
        "end": "2026-09-01", "format": "parquet",
    }).status_code == 422
    assert client.post("/v1/snapshots", headers=headers(), json={
        "dataset_id": "../secret", "start": "2026-01-01",
        "end": "2026-01-02", "format": "parquet",
    }).status_code in (403, 422)


def test_no_research_in_production_tenant(service):
    client, _, runtime, _ = service
    runtime.tenant["purpose"] = "production"
    assert client.get("/v1/datasets", headers=headers()).status_code == 403


def test_weak_or_shared_secret_refused(tmp_path):
    runtime, tasks = FakeRuntime(tmp_path)
    for key in ("weak", ADMIN):
        config = EngineConfig(
            state_dir=runtime.root, token=ADMIN, project=tmp_path,
            tenant_id="dev", tenant_purpose="development",
            research_signing_key=key,
        )
        with pytest.raises(ValueError):
            create_research_app(config, runtime, tasks)


def test_research_mount_does_not_share_admin_credentials(tmp_path, monkeypatch):
    """The main Engine's global admin dependency must not guard/bypass Research."""
    from engine.app import create_app
    from engine.runtime import EngineRuntime

    (tmp_path / "data" / "catalog").mkdir(parents=True)
    monkeypatch.setattr(
        "engine.research_snapshots.dataset_inventory",
        lambda _settings, all_levels: [],
    )
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    runtime.ensure_tenant("dev", "development")
    config = EngineConfig(
        state_dir=runtime.root, token=ADMIN, project=tmp_path,
        tenant_id="dev", tenant_purpose="development",
        research_signing_key=SECRET,
    )
    app = create_app(config, runtime)
    try:
        with TestClient(app) as client:
            assert client.get("/health", headers=headers()).status_code == 401
            assert client.get(
                "/health", headers={"Authorization": f"Bearer {ADMIN}"},
            ).status_code == 200
            assert client.get(
                "/research/v1/datasets",
                headers={
                    "Authorization": f"Bearer {ADMIN}",
                    ENGINE_HEADER: "2", RESEARCH_HEADER: "1",
                },
            ).status_code == 401
            response = client.get("/research/v1/datasets", headers=headers())
            assert response.status_code == 200, response.text
            assert response.json()["datasets"] == []
            assert client.get(
                "/tenants/dev/strategy-loads", headers=headers(),
            ).status_code == 401
    finally:
        runtime.close()
