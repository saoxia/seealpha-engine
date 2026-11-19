"""Isolated Engine tenant HTTP guard belongs to the Engine implementation.

This test uses only a fresh temporary Engine SQLite store. It does not make
network or exchange calls, send orders, or require Platform API code.
"""

from fastapi.testclient import TestClient

from core.engine_contracts import ENGINE_CONTRACT_HEADER, ENGINE_CONTRACT_VERSION
from engine.app import create_app
from engine.config import EngineConfig

TEST_TOKEN = "integration-test-engine-token-" + "a" * 48


def test_engine_container_rejects_other_tenant_and_purpose(tmp_path):
    config = EngineConfig(
        tmp_path / "state", token=TEST_TOKEN, project=tmp_path, auth_mode="token",
        tenant_id="42", tenant_purpose="development",
    )
    app = create_app(config)
    headers = {"Authorization": f"Bearer {TEST_TOKEN}"}
    with TestClient(app) as http:
        denied = http.put(
            "/tenants/43", headers=headers, json={"purpose": "development"},
        )
        assert denied.status_code == 403
        assert http.get("/tenants/43/export", headers=headers).status_code == 403
        assert http.put(
            "/tenants/42", headers=headers, json={"purpose": "production"},
        ).status_code == 403
        assert http.put(
            "/tenants/42", headers=headers, json={"purpose": "development"},
        ).status_code == 200
        assert http.post(
            "/tenants/42/runnable", headers=headers, json={"action": "backtest"},
        ).status_code == 200
        assert http.post(
            "/tenants/42/runnable", headers=headers, json={"action": "live"},
        ).status_code == 409
        assert http.get("/health").status_code == 401
        healthy = http.get("/health", headers=headers)
        assert healthy.status_code == 200
        assert healthy.headers[ENGINE_CONTRACT_HEADER] == ENGINE_CONTRACT_VERSION
        assert healthy.json()["contract_version"] == ENGINE_CONTRACT_VERSION
        assert http.get(
            "/health", headers={**headers, ENGINE_CONTRACT_HEADER: "3"},
        ).status_code == 426
