"""No-model, no-exchange Research API producer/consumer contract tests."""
from __future__ import annotations

import io
import time
from dataclasses import dataclass
from types import SimpleNamespace
from uuid import uuid4

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from engine.api.research_api import create_research_app
from engine.config import EngineConfig
from engine.research.auth import (
    ENGINE_HEADER,
    RESEARCH_HEADER,
    sign_research_grant,
)

SECRET = "research-signing-key-for-tests-00000000000000"
ADMIN = "engine-admin-token-for-tests-" + "a" * 48
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
        "engine.research.snapshots.load_settings",
        lambda: FakePipelineSettings(tmp_path / "data" / "catalog"),
    )
    monkeypatch.setattr(
        "engine.research.snapshots.dataset_inventory",
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

    monkeypatch.setattr("engine.research.snapshots.rows", mock_rows)
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
    runtime, tasks = FakeRuntime(tmp_path), FakeTasks()
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
    from engine.api.app import create_app
    from engine.trading.runtime import EngineRuntime

    (tmp_path / "data" / "catalog").mkdir(parents=True)
    monkeypatch.setattr(
        "engine.research.snapshots.dataset_inventory",
        lambda _settings, all_levels: [],
    )
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    runtime.ensure_tenant("dev", "development")
    config = EngineConfig(
        state_dir=runtime.root, token=ADMIN, project=tmp_path,
        tenant_id="dev", tenant_purpose="development",
        research_signing_key=SECRET, auth_mode="token",
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


def _candidate_archive(source=None, *, name="research_ema"):
    import hashlib
    import json
    import zipfile

    source = source if source is not None else (
        "CANDIDATE = {'kind':'ema_cross','symbol':'ETHUSDT','fast_period':8,"
        "'slow_period':21,'allocation':'1','risk_limit':'1'}\n"
    )
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("seealpha-package.json", json.dumps({
            "format": 1, "name": name, "title": "EMA AI Research",
            "version": "1", "entry_point": "strategy:CANDIDATE",
        }))
        archive.writestr("strategy.py", source)
    data = stream.getvalue()
    return data, hashlib.sha256(data).hexdigest()


def _candidate_body(raw, digest, *, candidate_id="a" * 32, key="candidate-v1"):
    import base64

    return {
        "purpose": "development", "execution": "simulation",
        "candidate_id": candidate_id, "package_hash": digest,
        "archive": base64.b64encode(raw).decode("ascii"),
        "strategy_ref": STRATEGY, "dataset_refs": [DATASET],
        "start": "2026-01-01", "end": "2026-01-02",
        "starting_balance": "1000", "quote_currency": "USDT",
        "idempotency_key": key,
    }


def test_candidate_backtest_uses_trusted_recipe_and_scoped_task(service):
    client, _, _, tasks = service
    raw, digest = _candidate_archive()
    body = _candidate_body(raw, digest)
    endpoint = "/v1/candidate-backtests"
    denied = client.post(endpoint, json=body, headers=headers(ops=["backtests:submit"]))
    assert denied.status_code == 403
    result = client.post(endpoint, json=body, headers=headers(ops=["candidates:submit"]))
    assert result.status_code == 202, result.text
    backtest_id = result.json()["backtest_id"]
    record = tasks.records[backtest_id]
    candidate = record["parameters"]["_candidate"]
    assert candidate["candidate_id"] == "a" * 32
    assert candidate["package_hash"] == digest
    assert candidate["recipe"]["kind"] == "ema_cross"
    # Query and cancellation re-use the same task-key-bound research endpoints.
    wrong_task = client.get(
        f"/v1/backtests/{backtest_id}", headers=headers(task_id="another"),
    )
    assert wrong_task.status_code == 404
    assert client.get(
        f"/v1/backtests/{backtest_id}", headers=headers(task_id="task-1"),
    ).status_code == 200


def test_candidate_rejects_executable_source_and_invalid_hash(service):
    from engine.research.candidate_package import parse_candidate_archive

    client, _, _, _tasks = service
    raw, digest = _candidate_archive("import os\nCANDIDATE = {}")
    with pytest.raises(ValueError):
        parse_candidate_archive(raw)
    assert client.post(
        "/v1/candidate-backtests",
        json=_candidate_body(raw, digest),
        headers=headers(ops=["candidates:submit"]),
    ).status_code == 422
    safe, actual = _candidate_archive()
    assert client.post(
        "/v1/candidate-backtests",
        json=_candidate_body(safe, "0" * 64),
        headers=headers(ops=["candidates:submit"]),
    ).status_code == 422
    changed = _candidate_body(safe, actual)
    changed["dataset_refs"] = ["futures_um:BTCUSDT:1m"]
    assert client.post(
        "/v1/candidate-backtests", json=changed,
        headers=headers(ops=["candidates:submit"]),
    ).status_code == 403


def test_candidate_ast_validation_rejects_network_and_wrong_shape():
    from engine.research.candidate_package import parse_candidate_archive
    from engine.research.candidate_template import validate_candidate

    for source in (
        "__import__('os').system('id')",
        "CANDIDATE = dict(kind='ema_cross')",
        "CANDIDATE = {'kind':'ema_cross'}\nprint('hi')",
        "class Strategy: pass",
        ("CANDIDATE = {'kind':'ema_cross', 'symbol':'ETHUSDT', "
         "'fast_period':8,'slow_period':21,'allocation':'1','risk_limit':'1'}\n"
         "import socket"),
    ):
        raw, _ = _candidate_archive(source)
        with pytest.raises(ValueError):
            parse_candidate_archive(raw)
    raw, _ = _candidate_archive()
    parsed = parse_candidate_archive(raw)
    assert parsed["recipe"]["fast_period"] == 8
    with pytest.raises(ValueError):
        validate_candidate({**parsed["recipe"], "kind": "arbitrary_python"})
    with pytest.raises(ValueError):
        validate_candidate({**parsed["recipe"], "risk_limit": "inf"})
    with pytest.raises(ValueError):
        validate_candidate({**parsed["recipe"], "risk_limit": "1E+999999999"})


def _python_strategy_archive(*, source=None, symbol="ETHUSDT"):
    """A genuine Strategy class, not a literal four-template recipe."""
    import hashlib
    import json
    import zipfile

    source = source if source is not None else (
        "from decimal import Decimal\n"
        "from nautilus_trader.trading import Strategy\n"
        "class AiSignal(Strategy):\n"
        "    def on_start(self):\n"
        "        self.subscribe_bars(self.bar_type)\n"
        "    def on_bar(self, bar):\n"
        "        self.publish(symbol=self.parameters['symbol'], "
        "quantity=Decimal(self.parameters['allocation']))\n"
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("seealpha-package.json", json.dumps({
            "format": 1, "name": "ai_research", "title": "AI Python Research",
            "version": "1", "entry_point": "strategy:AiSignal",
            "parameters": {
                "symbol": symbol, "allocation": "1", "risk_limit": "1",
            },
        }))
        archive.writestr("strategy.py", source)
    raw = buffer.getvalue()
    return raw, hashlib.sha256(raw).hexdigest()


def test_python_candidate_opt_in_and_hmac_isolation(service):
    import base64
    from dataclasses import replace

    client, _, runtime, tasks = service
    raw, digest = _python_strategy_archive()
    body = {
        "purpose": "development", "execution": "simulation",
        "candidate_id": "a" * 32, "package_hash": digest,
        "archive": base64.b64encode(raw).decode("ascii"),
        "strategy_ref": STRATEGY, "dataset_refs": [DATASET],
        "start": "2026-01-01", "end": "2026-01-02",
        "starting_balance": "1000", "quote_currency": "USDT",
        "idempotency_key": "python-experiment-1",
    }
    assert client.post(
        "/v1/python-candidate-backtests", headers=headers(
            ops=["python_candidates:submit"],
        ), json=body,
    ).status_code == 404
    enabled = create_research_app(
        replace(EngineConfig(
            state_dir=runtime.root, token=ADMIN, project=runtime.project,
            tenant_id="dev", tenant_purpose="development",
            research_signing_key=SECRET,
        ), research_python_enabled=True),
        runtime, tasks,
    )
    with TestClient(enabled) as api:
        assert api.post(
            "/v1/python-candidate-backtests",
            headers=headers(ops=["candidates:submit"]), json=body,
        ).status_code == 403
        response = api.post(
            "/v1/python-candidate-backtests",
            headers=headers(ops=["python_candidates:submit"]), json=body,
        )
        assert response.status_code == 202, response.text
        job_id = response.json()["backtest_id"]
        candidate = tasks.records[job_id]["parameters"]["_candidate"]
        assert candidate["mode"] == "python"
        assert candidate["entry_point"] == "strategy:AiSignal"
        assert candidate["package_hash"] == digest
        assert candidate["parameters"]["symbol"] == "ETHUSDT"
        assert api.post(
            "/v1/python-candidate-backtests",
            headers=headers(ops=["python_candidates:submit"]), json=body,
        ).json()["backtest_id"] == job_id
        assert api.get(
            f"/v1/backtests/{job_id}", headers=headers(
                task_id="some-other-task", ops=["backtests:read"],
            ),
        ).status_code == 404
        assert api.post(
            "/v1/python-candidate-backtests", headers=headers(
                ops=["python_candidates:submit"],
            ), json={**body, "package_hash": "0" * 64},
        ).status_code == 422
    enabled.state.snapshots.close()


def test_python_candidate_package_is_never_imported_by_api():
    import base64

    from engine.research.python_candidate import inspect_python_candidate

    # Static inspection may accept Python imports; it must not execute them.
    raw, digest = _python_strategy_archive(
        source="raise AssertionError('unexpected import')\n"
    )
    info = inspect_python_candidate(
        base64.b64encode(raw).decode(), package_hash=digest, symbol="ETHUSDT",
    )
    assert info["name"] == "ai_research"
    with pytest.raises(ValueError):
        inspect_python_candidate(
            base64.b64encode(raw).decode(), package_hash="0" * 64,
            symbol="ETHUSDT",
        )
    with pytest.raises(ValueError):
        inspect_python_candidate(
            base64.b64encode(raw).decode(), package_hash=digest, symbol="BTCUSDT",
        )


def test_python_candidate_rejects_archive_path_escape_and_secret_parameters():
    import base64
    import hashlib
    import json
    import zipfile

    from engine.research.python_candidate import inspect_python_candidate

    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as z:
        z.writestr("seealpha-package.json", json.dumps({
            "format": 1, "name": "ai_research", "title": "AI",
            "version": "1", "entry_point": "strategy:AiSignal",
            "parameters": {
                "symbol": "ETHUSDT", "allocation": "1", "risk_limit": "1",
                "token": "forbidden",
            },
        }))
        z.writestr("strategy.py", "class AiSignal: pass")
        z.writestr("../escape.py", "malicious")
    raw = stream.getvalue()
    with pytest.raises(ValueError):
        inspect_python_candidate(
            base64.b64encode(raw).decode(),
            package_hash=hashlib.sha256(raw).hexdigest(),
            symbol="ETHUSDT",
        )


def test_engine_job_views_do_not_return_python_archive_to_clients():
    from engine.infrastructure.task_queue import TaskCoordinator

    job = {
        "id": "a" * 32, "fingerprint": "private", "idempotency_key": "private",
        "parameters": {
            "account_id": "desk",
            "_candidate": {
                "mode": "python", "candidate_id": "c" * 32,
                "package_hash": "d" * 64, "archive": "BASE64-SECRET-SOURCE",
            },
        },
    }
    view = TaskCoordinator._public(job)
    assert "archive" not in view["parameters"]["_candidate"]
    assert "BASE64-SECRET-SOURCE" not in str(view)
    assert job["parameters"]["_candidate"]["archive"] == "BASE64-SECRET-SOURCE"


# Optional Engine-native MCP is deliberately a research-only interface.
def _mcp_app(service):
    from dataclasses import replace

    _, _, runtime, tasks = service
    config = replace(EngineConfig(
        state_dir=runtime.root, token=ADMIN, project=runtime.project,
        tenant_id="dev", tenant_purpose="development",
        research_signing_key=SECRET,
    ), research_mcp_enabled=True)
    return create_research_app(config, runtime, tasks)


def _mcp_headers(**kwargs):
    return {
        "Authorization": headers(**kwargs)["Authorization"],
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": "2025-11-25",
    }


def _mcp_call(client, method, arguments=None, *, identity=1, token_headers=None):
    params = {"name": method, "arguments": arguments or {}}
    return client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": identity, "method": "tools/call", "params": params},
        headers=token_headers or _mcp_headers(),
    )


def test_mcp_is_off_without_explicit_opt_in(service):
    client, _, _, _ = service
    assert client.post(
        "/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
        headers=_mcp_headers(),
    ).status_code == 404


def test_mcp_initialize_and_discovery_are_scoped(service):
    app = _mcp_app(service)
    try:
        with TestClient(app) as client:
            init = client.post("/mcp", json={
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-11-25",
                           "capabilities": {}, "clientInfo": {"name": "test", "version": "1"}},
            }, headers=_mcp_headers())
            assert init.status_code == 200
            assert init.json()["result"]["protocolVersion"] == "2025-11-25"
            assert "tools" in init.json()["result"]["capabilities"]
            assert client.post(
                "/mcp", json={"jsonrpc": "2.0", "method": "notifications/initialized"},
                headers=_mcp_headers(),
            ).status_code == 202
            assert client.get("/mcp", headers=_mcp_headers()).status_code == 405
            tools = client.post(
                "/mcp", json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
                headers=_mcp_headers(),
            ).json()["result"]["tools"]
            names = {t["name"] for t in tools}
            assert names == {
                "list_datasets", "create_snapshot", "get_snapshot",
                "submit_backtest", "get_backtest", "cancel_backtest",
            }
            candidate_tools = client.post(
                "/mcp", json={"jsonrpc": "2.0", "id": 5, "method": "tools/list"},
                headers=_mcp_headers(ops=["candidates:submit"]),
            ).json()["result"]["tools"]
            assert [row["name"] for row in candidate_tools] == [
                "submit_candidate_backtest",
            ]
            assert all(t["inputSchema"]["type"] == "object" for t in tools)
            assert "submit_python_candidate_backtest" not in names
            assert not any("order_submit" in name or "trade" in name for name in names)
            limited = client.post(
                "/mcp", json={"jsonrpc": "2.0", "id": 3, "method": "tools/list"},
                headers=_mcp_headers(ops=["backtests:read"]),
            ).json()["result"]["tools"]
            assert [row["name"] for row in limited] == ["get_backtest"]
            denied = _mcp_call(client, "submit_backtest", token_headers=_mcp_headers(
                ops=["backtests:read"],
            ))
            assert denied.json()["error"]["code"] == -32602
    finally:
        app.state.snapshots.close()


def test_mcp_reuses_backtest_queue_permissions_and_idempotency(service):
    _, _, _, tasks = service
    app = _mcp_app(service)
    body = {
        "purpose": "development", "execution": "simulation",
        "strategy_ref": STRATEGY, "dataset_refs": [DATASET],
        "start": "2026-01-01", "end": "2026-01-02",
        "starting_balance": "10000", "quote_currency": "USDT",
        "parameters": {}, "idempotency_key": "mcp-backtest-1",
    }
    try:
        with TestClient(app) as client:
            first = _mcp_call(client, "submit_backtest", body).json()["result"]
            assert first.get("isError") is not True
            job_id = first["structuredContent"]["backtest_id"]
            assert first["structuredContent"]["status"] == "queued"
            second = _mcp_call(client, "submit_backtest", body).json()["result"]
            assert second["structuredContent"]["backtest_id"] == job_id
            assert len(tasks.records) == 1
            unknown = _mcp_call(
                client, "get_backtest", {"backtest_id": job_id},
                token_headers=_mcp_headers(task_id="wrong-task"),
            ).json()["result"]
            assert unknown["isError"] is True
            assert "HTTP 404" in unknown["content"][0]["text"]
            tasks.records[job_id]["status"] = "SUCCEEDED"
            result = _mcp_call(
                client, "get_backtest", {"backtest_id": job_id},
            ).json()["result"]["structuredContent"]
            assert result["result"]["net_pnl"] == "12.34"
            denied = _mcp_call(client, "submit_backtest", {**body, "execution": "live"})
            assert denied.json()["result"]["isError"] is True
            assert len(tasks.records) == 1
            cancelled = _mcp_call(
                client, "cancel_backtest", {"backtest_id": job_id},
            ).json()["result"]["structuredContent"]
            assert cancelled["status"] == "cancelled"
    finally:
        app.state.snapshots.close()


def test_mcp_snapshots_and_candidate_reuse_existing_research(service):
    app = _mcp_app(service)
    try:
        with TestClient(app) as client:
            discovered = _mcp_call(client, "list_datasets").json()["result"]
            assert discovered["structuredContent"]["datasets"][0]["id"] == DATASET
            snap = _mcp_call(client, "create_snapshot", {
                "dataset_id": DATASET, "start": "2026-01-01",
                "end": "2026-01-02", "format": "parquet",
            }).json()["result"]["structuredContent"]
            snapshot_id = snap["snapshot_id"]
            app.state.snapshots.pool.shutdown(wait=True)
            metadata = _mcp_call(client, "get_snapshot", {
                "snapshot_id": snapshot_id,
            }).json()["result"]["structuredContent"]
            assert metadata["status"] == "ready"
            assert len(metadata["sha256"]) == 64
            # MCP only returns metadata. It never injects Parquet bytes into AI.
            assert "content" not in metadata
            raw, digest = _candidate_archive()
            submitted = _mcp_call(
                client, "submit_candidate_backtest", _candidate_body(raw, digest),
                token_headers=_mcp_headers(ops=["candidates:submit"]),
            ).json()["result"]
            assert submitted["structuredContent"]["status"] == "queued"
            restricted = _mcp_call(
                client, "submit_candidate_backtest", _candidate_body(raw, digest),
                token_headers=_mcp_headers(ops=["backtests:submit"]),
            ).json()
            assert restricted["error"]["code"] == -32602
    finally:
        app.state.snapshots.close()


def test_mcp_auth_transport_errors_and_no_mutating_notifications(service):
    app = _mcp_app(service)
    try:
        with TestClient(app) as client:
            ping = {"jsonrpc": "2.0", "id": 1, "method": "ping"}
            assert client.post("/mcp", json=ping).status_code == 401
            admin = _mcp_headers()
            admin["Authorization"] = f"Bearer {ADMIN}"
            assert client.post("/mcp", json=ping, headers=admin).status_code == 401
            assert client.post(
                "/mcp", json=ping, headers=_mcp_headers(tenant_id="another"),
            ).status_code == 403
            assert client.post(
                "/mcp", json=ping, headers={**_mcp_headers(), "Origin": "https://evil.example"},
            ).status_code == 403
            assert client.post(
                "/mcp", json=ping, headers={
                    **_mcp_headers(), "MCP-Protocol-Version": "unsupported",
                },
            ).status_code == 400
            assert client.post(
                "/mcp", content=b"{invalid", headers={
                    **_mcp_headers(), "Content-Type": "application/json",
                },
            ).json()["error"]["code"] == -32700
            assert client.post(
                "/mcp", json=[ping], headers=_mcp_headers(),
            ).json()["error"]["code"] == -32600
            assert client.post(
                "/mcp", json={"jsonrpc": "2.0", "method": "tools/call", "params": {
                    "name": "submit_backtest", "arguments": {},
                }}, headers=_mcp_headers(),
            ).json()["error"]["code"] == -32600
            assert client.post(
                "/mcp", json={"jsonrpc": "2.0", "id": 4, "method": "orders/submit"},
                headers=_mcp_headers(),
            ).json()["error"]["code"] == -32601
            assert client.post(
                "/mcp", content=b" " * (512 * 1024 + 1),
                headers={**_mcp_headers(), "Content-Type": "application/json"},
            ).status_code == 413
    finally:
        app.state.snapshots.close()


def test_mcp_configuration_fails_closed_without_development_research_key(tmp_path):
    from engine.api.app import create_app

    for config in (
        EngineConfig(state_dir=tmp_path / "one", research_mcp_enabled=True),
        EngineConfig(
            state_dir=tmp_path / "two", tenant_id="production",
            tenant_purpose="production", research_signing_key=SECRET,
            research_mcp_enabled=True,
        ),
    ):
        with pytest.raises(ValueError, match="Research MCP requires"):
            create_app(config)


def test_spot_research_submissions_preserve_product_and_grant_scope(service):
    """Spot strategies must use the Engine's native CASH-account backtester."""
    client, app, _, tasks = service
    spot = "spot:ETHUSDT:1m"
    app.state.snapshots._discover = lambda: [
        {"id": spot, "product": "spot", "symbol": "ETHUSDT", "level": "1m"}
    ]
    body = {
        "purpose": "development", "execution": "simulation",
        "strategy_ref": STRATEGY, "dataset_refs": [spot],
        "start": "2026-01-01", "end": "2026-01-02",
        "starting_balance": "1000", "quote_currency": "USDT",
        "parameters": {}, "idempotency_key": "spot-issued",
    }
    authorized = headers(datasets=[spot])
    issued = client.post("/v1/backtests", headers=authorized, json=body)
    assert issued.status_code == 202, issued.text
    assert tasks.records[issued.json()["backtest_id"]]["parameters"]["product"] == "spot"

    # Dataset permissions are not inferred from the matching symbol.
    assert client.post("/v1/backtests", headers=headers(), json=body).status_code == 403

    raw, digest = _candidate_archive()
    candidate = _candidate_body(raw, digest)
    candidate["dataset_refs"] = [spot]
    restricted = client.post(
        "/v1/candidate-backtests",
        headers=headers(ops=["candidates:submit"], datasets=[spot]),
        json=candidate,
    )
    assert restricted.status_code == 202, restricted.text
    assert tasks.records[restricted.json()["backtest_id"]]["parameters"]["product"] == "spot"

    # Option quote/tick replay needs a different native execution contract; do
    # not silently route options into spot or perpetual order models.
    unsupported = {**body, "dataset_refs": ["options:ETH-261225-2200-C:1m"]}
    app.state.snapshots._discover = lambda: [
        {"id": unsupported["dataset_refs"][0]}
    ]
    assert client.post(
        "/v1/backtests",
        headers=headers(datasets=unsupported["dataset_refs"]), json=unsupported,
    ).status_code == 422


def test_spot_python_candidate_is_dispatched_to_spot_worker(service):
    import base64
    from dataclasses import replace

    _, _, runtime, tasks = service
    spot = "spot:ETHUSDT:1m"
    raw, digest = _python_strategy_archive()

    body = {
        "purpose": "development", "execution": "simulation",
        "candidate_id": "b" * 32, "package_hash": digest,
        "archive": base64.b64encode(raw).decode("ascii"),
        "strategy_ref": STRATEGY, "dataset_refs": [spot],
        "start": "2026-01-01", "end": "2026-01-02",
        "starting_balance": "1000", "quote_currency": "USDT",
        "idempotency_key": "spot-python",
    }
    enabled = create_research_app(
        replace(EngineConfig(
            state_dir=runtime.root, token=ADMIN, project=runtime.project,
            tenant_id="dev", tenant_purpose="development",
            research_signing_key=SECRET,
        ), research_python_enabled=True),
        runtime, tasks,
    )
    enabled.state.snapshots._discover = lambda: [
        {"id": spot, "product": "spot", "symbol": "ETHUSDT", "level": "1m"}
    ]
    try:
        with TestClient(enabled) as api:
            result = api.post(
                "/v1/python-candidate-backtests",
                headers=headers(ops=["python_candidates:submit"], datasets=[spot]),
                json=body,
            )
            assert result.status_code == 202, result.text
            params = tasks.records[result.json()["backtest_id"]]["parameters"]
            assert params["product"] == "spot"
            assert params["_candidate"]["mode"] == "python"
    finally:
        enabled.state.snapshots.close()
