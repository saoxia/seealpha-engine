"""Resident engine facts, account scope, and Engine module boundaries."""

import ast
from pathlib import Path

from fastapi.testclient import TestClient

from engine.app import create_app
from engine.config import EngineConfig
from engine.runtime import numeric_id

PROJECT = Path(__file__).resolve().parents[2]


def test_engine_keeps_per_account_orders_gaps_and_no_secrets(tmp_path, monkeypatch):
    calls = []

    async def execute(account, symbol, command):
        calls.append((account["id"], symbol, command["action"]))
        order = {"client_order_id": command["client_order_id"], "status": "accepted", "symbol": symbol}
        return {"result": order, "records": [{"trade_id": "fill-" + command["client_order_id"], "symbol": symbol}]}

    monkeypatch.setattr("engine.connectors.binance.manual.execute", execute)
    app = create_app(EngineConfig(tmp_path / "state", token="engine-test", project=tmp_path))
    headers = {"Authorization": "Bearer engine-test"}
    order = {
        "environment": "production", "api_key": "key-one", "api_secret": "secret-one",
        "action": "submit",
        "values": {
            "request_id": "00000000-0000-0000-0000-000000000001",
            "symbol": "ETHUSDT-PERP", "account_revision": "f" * 64,
            "environment": "production", "side": "buy", "order_type": "limit",
            "quantity": "0.01", "price": "2000",
        },
    }
    with TestClient(app) as client:
        assert client.get("/health").status_code == 401
        assert client.get("/health", headers=headers).json()["status"] == "ok"
        created = client.put("/tenants/7", headers=headers, json={"purpose": "production"})
        assert created.status_code == 200, created.text
        manual = client.post("/tenants/7/accounts/11/manual", headers=headers, json={
            **order, "command": {"action": "submit"}, "symbol": "ETHUSDT-PERP",
        })
        assert manual.status_code == 404
        first = client.post("/tenants/7/accounts/11/orders", headers=headers, json=order)
        assert first.status_code == 200, first.text
        assert first.json()["response"]["status"] == "completed"
        order_id = first.json()["response"]["client_order_id"]
        assert order_id == "SA" + "00000000000000000000000000000001"
        again = client.post("/tenants/7/accounts/11/orders", headers=headers, json=order)
        assert again.json()["response"] == first.json()["response"]
        assert calls == [(11, "ETHUSDT-PERP", "submit")]
        changed = {**order, "values": {**order["values"], "quantity": "0.02"}}
        assert client.post("/tenants/7/accounts/11/orders", headers=headers, json=changed).status_code == 409
        assert len(calls) == 1
        mismatch = {**order, "values": {**order["values"], "environment": "sandbox"}}
        assert client.post("/tenants/7/accounts/11/orders", headers=headers, json=mismatch).status_code == 422
        assert client.post("/tenants/7/accounts/11/orders", headers=headers,
                           json={**order, "fingerprint": "a" * 64}).status_code == 422
        other = {**order, "api_key": "key-two", "api_secret": "secret-two",
                 "values": {**order["values"], "request_id": "00000000-0000-0000-0000-000000000002"}}
        assert client.post("/tenants/7/accounts/22/orders", headers=headers, json=other).status_code == 200
        orders = client.get("/tenants/7/accounts/11/orders", headers=headers, params={"environment": "production"})
        assert [row["client_order_id"] for row in orders.json()["orders"]] == [order_id]
        fills = client.get("/tenants/7/accounts/22/fills", headers=headers, params={"environment": "production"})
        assert fills.json()["fills"][0]["trade_id"] == "fill-SA00000000000000000000000000000002"
        request_path = "/tenants/7/accounts/11/requests/" + order["values"]["request_id"]
        assert client.put(request_path, headers=headers, json={"response": {"status": "forged"}}).status_code == 405
        assert client.post(request_path + "/ledger-sync", headers=headers).status_code == 200
        assert client.get(request_path, headers=headers).json()["ledger_synced"] is True
        gap = client.post("/tenants/7/accounts/11/gaps", headers=headers, json={
            "scope": "ETHUSDT", "gap_key": "2026-11-08", "gap": {"kind": "missing", "symbol": "ETHUSDT"},
        })
        assert gap.status_code == 200, gap.text
        assert client.get("/tenants/7/accounts/22/gaps", headers=headers).json()["gaps"] == []
        assert client.get("/tenants/8/accounts/11/gaps", headers=headers).json()["gaps"] == []
        assert client.get("/tenants/7/accounts/11/gaps", headers=headers).json()["gaps"][0]["symbol"] == "ETHUSDT"
        noted = client.post("/tenants/7/accounts/11/market/workers/note", headers=headers, json={
            "command": [".venv/bin/python", "-m", "engine.feed.managed", "--symbol", "ETHUSDT", "--role", "collection"],
        })
        assert noted.json()["account_id"] == "11"
        assert noted.json()["tenant_id"] == "7"
        empty = client.get("/tenants/8/accounts/11/market/subscriptions", headers=headers)
        assert empty.json()["subscriptions"] == []
        events = client.get("/tenants/7/accounts/11/events", headers=headers).json()["events"]
        assert {item["kind"] for item in events} >= {"execution", "gap", "subscription"}
    blob = b"".join(path.read_bytes() for path in (tmp_path / "state").glob("book.sqlite*"))
    assert b"secret-one" not in blob and b"secret-two" not in blob and b"key-one" not in blob
    assert numeric_id("11") == 11


def test_connectors_stay_below_feed_pipeline_and_api():
    for path in (PROJECT / "engine/connectors").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        modules = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules.add(node.module)
        forbidden = [
            name for name in modules
            if name.split(".", 1)[0] in {"feed", "pipeline", "api", "web"} or name.startswith("engine.feed")
        ]
        assert not forbidden, f"{path} imports {forbidden}"


def test_strategy_packages_remain_at_the_repository_root():
    assert not (PROJECT / "strategy_python").exists()
    assert (PROJECT / "packages" / "ema_crossover" / "seealpha-package.json").is_file()
    assert not (PROJECT / "engine/strategy_python").exists()
    assert (PROJECT / "engine/feed/managed.py").is_file()
    assert (PROJECT / "engine/connectors/binance/queries.py").is_file()
    assert (PROJECT / "engine/pipeline/pair_data.py").is_file()
    assert not (PROJECT / "feed").exists()
    assert not (PROJECT / "connectors").exists()
    assert not (PROJECT / "pipeline").exists()


def test_trading_state_is_query_only_and_unknown_reconciliation_is_durable(tmp_path, monkeypatch):
    from engine.connectors.binance.queries import QueryError

    sent = []

    async def execute(account, symbol, command):
        sent.append(command["action"])
        if command["action"] == "submit":
            raise QueryError("simulated timeout")
        return {
            "orders": [{"client_order_id": "SA00000000000000000000000000000003", "status": "accepted"}],
            "positions": [], "records": [], "result": None,
        }

    monkeypatch.setattr("engine.connectors.binance.manual.execute", execute)
    app = create_app(EngineConfig(tmp_path / "state", token="engine-test", project=tmp_path))
    headers = {"Authorization": "Bearer engine-test"}
    credentials = {"environment": "production", "api_key": "test-key", "api_secret": "test-secret"}
    path = "/tenants/7/accounts/11"
    with TestClient(app) as http:
        http.put("/tenants/7", headers=headers, json={"purpose": "production"})
        state = http.post(path + "/trading/state", headers=headers,
                          json={**credentials, "symbol": "ETHUSDT-PERP"})
        assert state.status_code == 200
        assert sent == ["snapshot"]
        assert http.post(path + "/trading/state", headers=headers, json={
            **credentials, "symbol": "ETHUSDT-PERP", "command": {"action": "submit"},
        }).status_code == 422
        values = {
            "request_id": "00000000-0000-0000-0000-000000000003",
            "symbol": "ETHUSDT-PERP", "account_revision": "f" * 64,
            "environment": "production", "side": "buy", "order_type": "market",
            "quantity": "0.01",
        }
        pending = http.post(path + "/orders", headers=headers,
                            json={**credentials, "action": "submit", "values": values})
        assert pending.status_code == 200
        assert pending.json()["response"]["status"] == "unknown"
        request_path = path + "/requests/" + values["request_id"]
        denied = http.post(request_path + "/reconcile", headers=headers, json={
            **credentials, "account_revision": "e" * 64,
        })
        assert denied.status_code == 409
        resolved = http.post(request_path + "/reconcile", headers=headers, json={
            **credentials, "account_revision": "f" * 64,
        })
        assert resolved.status_code == 200
        assert resolved.json()["response"]["status"] == "completed"
        assert http.get(request_path, headers=headers).json()["response"]["status"] == "completed"
        assert http.post(path + "/orders", headers=headers,
                         json={**credentials, "action": "submit", "values": values}).json()["replay"] is True
        assert sent == ["snapshot", "submit", "snapshot"]
