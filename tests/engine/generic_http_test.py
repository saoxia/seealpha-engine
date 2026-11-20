"""End-to-end generic portfolio API uses real Catalog and issued archives."""
import base64
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from engine.app import create_app
from engine.config import EngineConfig
from engine.pipeline.catalog import ParquetDataCatalog
from engine.runtime import EngineRuntime
from engine.strategies.basket_targets import LegId
from tests.engine.generic_simulation_test import _frames, _load, _perp
from tests.engine.spot_backtest_test import spot_instrument


def test_authorized_multi_asset_http_loads_catalog_and_reconciles_native_fills(tmp_path):
    spot = LegId("dev", "desk", "spot", "ETHUSDT")
    perp = LegId("dev", "desk", "futures_um", "BTCUSDT")
    contracts = {spot: spot_instrument(), perp: _perp()}
    bars = _frames(contracts)
    catalog = ParquetDataCatalog(tmp_path / "data" / "catalog")
    catalog.write_data(list(contracts.values()))
    catalog.write_data([bar for frame in bars for bar in frame.values()])
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    runtime.ensure_tenant("dev", "development")
    for leg in contracts:
        runtime.note_subscription(
            "dev", "market-data", leg.product, leg.symbol,
            "intent", "configured", {"history_enabled": True},
        )
    issued = _load()
    payload = {
        "account_id": "desk",
        "issuance_id": issued["issuance_id"],
        "code_key": issued["code_key"],
        "version": issued["version"],
        "entry_point": issued["entry_point"],
        "package_hash": issued["package_hash"],
        "archive": base64.b64encode(issued["archive"]).decode(),
        "grant_purpose": "development",
        "parameters": issued["parameters"],
    }
    runtime.receive_strategy("dev", payload)
    app = create_app(
        EngineConfig(
            tmp_path / "state", token="generic-http-test-token-" + "x" * 48,
            auth_mode="token", project=tmp_path,
        ),
        runtime=runtime,
    )
    start = datetime(2024, 1, 1, tzinfo=UTC)
    body = {
        "issuance_ids": [issued["issuance_id"]],
        "legs": [{"product": leg.product, "symbol": leg.symbol} for leg in contracts],
        "start": start.isoformat(),
        "end": (start + timedelta(minutes=15)).isoformat(),
        "interval": "1m",
        "starting_spot_balance": "1000",
        "starting_perpetual_balance": "1000",
        "max_gross_notional": "1000",
    }
    endpoint = "/tenants/dev/accounts/desk/generic-portfolio-backtests"
    headers = {"Authorization": "Bearer generic-http-test-token-" + "x" * 48}
    try:
        with TestClient(app) as client:
            assert client.post(endpoint, json=body).status_code == 401
            run = client.post(endpoint, headers=headers, json=body)
            assert run.status_code == 200, run.text
            report = run.json()
            assert report["mode"] == "native_generic_development_only"
            assert report["bar_count"] == len(bars)
            assert {row["symbol"] for row in report["orders"]} == {
                "ETHUSDT", "BTCUSDT",
            }
            assert len(report["fills"]) >= 2
            assert len(report["equity_curve"]) == len(bars)
            assert report["equity_curve"][-1]["positions"]
            assert report["ending_balance"] == report["equity_curve"][-1]["total_equity"]
            assert client.get(
                endpoint + "/latest", headers=headers,
            ).json() == report
            assert client.post(
                endpoint.replace("/dev/", "/foreign/"),
                headers=headers, json=body,
            ).status_code in {403, 409}
            invalid = client.post(endpoint, headers=headers, json={
                **body, "issuance_ids": ["unowned-member"],
            })
            assert invalid.status_code == 409
    finally:
        runtime.close()
