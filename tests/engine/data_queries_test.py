"""Resident Engine market-data query endpoints read only their tenant's volume."""
from fastapi.testclient import TestClient

from engine.app import create_app
from engine.config import EngineConfig
from engine.pipeline.models import PipelineSettings


def test_tenant_data_reads_reject_unauthorized_and_other_tenant(tmp_path, monkeypatch):
    from engine import data_queries

    root = tmp_path / "data"
    root.mkdir()
    settings = PipelineSettings(
        "futures_um", "trades", ("ETHUSDT",), ("1m",),
        root / "raw/binance", root / "catalog", root / "pipeline_state/binance",
    )
    monkeypatch.setattr(data_queries, "load_settings", lambda: settings)
    app = create_app(EngineConfig(
        tmp_path / "state", "private", project=tmp_path,
        tenant_id="42", tenant_purpose="development",
    ))
    app.state.runtime.ensure_tenant("42", "development")
    headers = {"Authorization": "Bearer private"}

    with TestClient(app) as client:
        assert client.post("/tenants/42/data/datasets", json={}).status_code == 401
        assert client.post("/tenants/43/data/datasets", json={}, headers=headers).status_code == 403
        inventory = client.post("/tenants/42/data/datasets", json={"symbols": ["ETHUSDT"]}, headers=headers)
        assert inventory.status_code == 200, inventory.text
        assert inventory.json() == {"items": []}
        assert client.post(
            "/tenants/42/data/latest", json={"symbols": ["ETHUSDT"]}, headers=headers,
        ).json() == {"ETHUSDT": None}

        request = {
            "symbol": "ETHUSDT", "start": "2026-08-01T00:00:00Z",
            "end": "2026-08-02T00:00:00Z", "intervals": ["1m"],
        }
        result = client.post("/tenants/42/data/prepare-plan", json=request, headers=headers)
        assert result.status_code == 200, result.text
        assert result.json()["missing_days"] == 1
        assert not settings.catalog_dir.exists()
        assert client.post(
            "/tenants/42/data/coverage", json=request, headers=headers,
        ).status_code == 200
        # A day-long preview must be rejected; this route allows at most 15 minutes.
        assert client.post(
            "/tenants/42/data/page", json={**request, "limit": 30}, headers=headers,
        ).status_code == 422
        recent_preview = client.post("/tenants/42/data/page", json={
            **request, "start": "2026-08-01T09:00:00Z",
            "end": "2026-08-01T09:05:00Z",
        }, headers=headers)
        assert recent_preview.status_code == 404
        chart = client.post("/tenants/42/data/chart", json={
            "symbol": "ETHUSDT", "interval": "1m",
        }, headers=headers)
        assert chart.status_code == 200, chart.text
        assert chart.json()["candles"] == [] and chart.json()["live"] is False
        assert client.post(
            "/tenants/42/data/prepare-plan",
            json={**request, "start": "2026-08-02T00:00:00Z"},
            headers=headers,
        ).status_code == 422
        assert client.post(
            "/tenants/42/data/datasets", json={"symbols": ["../secret"]}, headers=headers,
        ).status_code == 422
        app.state.runtime.ensure_tenant("42", "development", "archived")
        assert client.post(
            "/tenants/42/data/datasets", json={}, headers=headers,
        ).status_code == 409


def test_data_endpoints_never_accept_arbitrary_paths():
    from engine.data_queries import Chart, Inventory, Page, Range

    for cls in (Chart, Inventory, Page, Range):
        assert "path" not in cls.model_fields
        assert "project" not in cls.model_fields


def test_api_and_engine_share_a_single_volume_access_lease(tmp_path):
    import pytest
    from fastapi import HTTPException

    from engine.data_manager.service import DataService
    from engine.data_queries import read_lease

    (tmp_path / "data").mkdir()
    service = DataService("42", project=tmp_path, state_root=tmp_path / "state")

    with (service._lease("ETHUSDT", access=True),
          pytest.raises(HTTPException) as captured,
          read_lease(tmp_path, "ETHUSDT", "futures_um")):
        pass
    assert captured.value.status_code == 409

    with (service._lease("ETHUSDT", access=True, shared=True),
          read_lease(tmp_path, "ETHUSDT", "futures_um")):
        pass
    service.close()
