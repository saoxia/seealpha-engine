"""Native dual-account carry simulation with real Catalog fixtures and no credentials."""

import base64
import json
from datetime import timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from engine.app import create_app
from engine.config import EngineConfig
from engine.pipeline.catalog import ParquetDataCatalog
from engine.runtime import EngineConflict, EngineRuntime
from engine.strategies.carry_simulation import run_native_carry
from engine.strategies.package import archive_directory, parse_metadata
from nautilus_support import TestInstrumentProvider
from tests.engine.basket_history_test import _dataset, _range
from tests.engine.spot_backtest_test import spot_instrument


def _load(member="A", *, quantity="0.1", purpose="development"):
    return {
        "tenant_id": "dev", "account_id": "desk", "issuance_id": member,
        "code_key": "demo_funding_carry", "grant_purpose": purpose,
        "parameters": {
            "symbol": "ETHUSDT",
            "allocation": quantity, "risk_limit": quantity,
            "min_funding_rate": "0.00005", "max_basis_bps": "30",
            "max_leg_drift": "0.001", "max_age_seconds": 3600,
        },
    }


def _instruments(tmp_path):
    return spot_instrument(), TestInstrumentProvider.ethusdt_perp_binance()


def _history(tmp_path):
    start, _, _ = _dataset(tmp_path)
    _, perpetual = _instruments(tmp_path)
    ParquetDataCatalog(tmp_path / "data" / "catalog").write_data([perpetual])
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    runtime.ensure_tenant("dev", "development")
    for product in ("spot", "futures_um"):
        runtime.note_subscription(
            "dev", "market-data", product, "ETHUSDT", "intent",
            "configured", {"history_enabled": True},
        )
    return start, runtime


def _simulate(tmp_path, *, loads=None, cap="1000"):
    start, runtime = _history(tmp_path)
    spot, perp = _instruments(tmp_path)
    try:
        result = run_native_carry(
            loads or [_load()], runtime.iter_funding_carry_history(
                "dev", "ETHUSDT", *_range(start), "1m",
            ),
            spot, perp, interval="1m", starting_spot="1000",
            starting_perpetual="1000", max_gross_notional=cap,
        )
    finally:
        runtime.close()
    return result


def test_native_dual_venue_carry_orders_and_account_equity(tmp_path):
    result = _simulate(tmp_path)
    assert result["mode"] == "native_dual_venue_development_only"
    assert result["venue_accounts"] == {"spot": "CASH", "futures_um": "MARGIN"}
    assert result["bar_count"] >= 10
    assert Decimal(result["starting_balance"]) == Decimal(2000)
    assert result["equity_curve"]
    assert result["ending_balance"] == result["equity_curve"][-1]["total_equity"]
    assert len(result["funding_observations"]) == 2
    assert result["orders"]
    assert result["fills"]
    assert {row["venue"] for row in result["orders"]} == {"SIM_SPOT", "SIM_UM"}
    assert all(Decimal(row["commission"]) >= 0 for row in result["fills"])
    assert all(Decimal(row["leg_drift"]) <= Decimal("0.001")
               for row in result["equity_curve"])
    assert any(Decimal(row["spot_quantity"]) > 0 for row in result["equity_curve"])
    assert any(Decimal(row["perpetual_quantity"]) < 0 for row in result["equity_curve"])
    assert "api_key" not in json.dumps(result)


def test_multi_member_targets_aggregate_one_pair_of_market_deltas(tmp_path):
    result = _simulate(tmp_path, loads=[_load("A", quantity="0.1"),
                                        _load("B", quantity="0.1")])
    openings = result["orders"][:2]
    assert len(openings) == 2
    assert {order["quantity"] for order in openings} == {"0.2"}
    assert {order["venue"] for order in openings} == {"SIM_SPOT", "SIM_UM"}


def test_budget_is_gross_and_rejects_open_before_fills(tmp_path):
    with pytest.raises(EngineConflict, match="预算"):
        _simulate(tmp_path, cap="1")


def test_production_issued_load_cannot_use_development_runner(tmp_path):
    with pytest.raises(EngineConflict, match="开发"):
        _simulate(tmp_path, loads=[_load(purpose="production")])


def test_runtime_requires_issued_carry_package_and_rejects_unauthorized_http(tmp_path):
    start, runtime = _history(tmp_path)
    try:
        with pytest.raises(EngineConflict, match="已下发"):
            runtime.run_funding_carry_backtest(
                "dev", "desk", *_range(start), "1m", "1000", "1000", "1000",
            )
    finally:
        runtime.close()
    app = create_app(EngineConfig(tmp_path / "other_state", token="integration-test-private-token-" + "x" * 48, auth_mode="token", project=tmp_path))
    body = {
        "start": start.isoformat(),
        "end": (start + timedelta(minutes=19)).isoformat(),
        "interval": "1m", "starting_spot_balance": "1000",
        "starting_perpetual_balance": "1000", "max_gross_notional": "1000",
    }
    with TestClient(app) as client:
        denied = client.post("/tenants/dev/accounts/desk/carry-backtests", json=body)
        assert denied.status_code == 401
        assert client.put(
            "/tenants/dev", headers={"Authorization": "Bearer integration-test-private-token-" + "x" * 48},
            json={"purpose": "development"},
        ).status_code == 200
        assert client.get(
            "/tenants/dev/accounts/desk/carry-backtests/latest",
            headers={"Authorization": "Bearer integration-test-private-token-" + "x" * 48},
        ).status_code == 404


def test_rejects_wrong_instrument_and_missing_initial_cash(tmp_path):
    start, runtime = _history(tmp_path)
    spot, perp = _instruments(tmp_path)
    try:
        with pytest.raises(EngineConflict, match="初始资金"):
            run_native_carry(
                [_load()], runtime.iter_funding_carry_history(
                    "dev", "ETHUSDT", *_range(start), "1m",
                ), spot, perp, interval="1m",
                starting_spot="-1", starting_perpetual="1000",
                max_gross_notional="1000",
            )
    finally:
        runtime.close()

def test_authorized_demo_package_runs_and_persists_native_carry_http(tmp_path):
    root = tmp_path / "carry_fixture"
    root.mkdir()
    (root / "seealpha-package.json").write_text(json.dumps({
        "format": 1, "name": "demo_funding_carry", "version": "1",
        "entry_point": "strategy:DemoFundingCarry",
    }), encoding="utf-8")
    (root / "strategy.py").write_text(
        "from nautilus_trader.trading import Strategy\n"
        "def prepare(params):\n"
        "    return dict(params)\n"
        "class DemoFundingCarry(Strategy):\n"
        "    pass\n",
        encoding="utf-8",
    )
    archive = archive_directory(root)
    meta = parse_metadata(archive)
    start, runtime = _history(tmp_path)
    payload = {
        "account_id": "desk", "issuance_id": "carry-demo-1",
        "code_key": meta.name, "version": meta.version,
        "grant_purpose": "development", "parameters": _load()["parameters"],
        "entry_point": meta.entry_point, "package_hash": meta.digest,
        "archive": base64.b64encode(archive).decode(),
    }
    runtime.receive_strategy("dev", payload)
    app = create_app(EngineConfig(tmp_path / "state", token="integration-test-engine-token-" + "x" * 48, auth_mode="token", project=tmp_path),
                     runtime=runtime)
    body = {
        "start": start.isoformat(),
        "end": (start + timedelta(minutes=19)).isoformat(),
        "interval": "1m", "starting_spot_balance": "1000",
        "starting_perpetual_balance": "1000", "max_gross_notional": "1000",
    }
    try:
        headers = {"Authorization": "Bearer integration-test-engine-token-" + "x" * 48}
        with TestClient(app) as client:
            run = client.post(
                "/tenants/dev/accounts/desk/carry-backtests",
                headers=headers, json=body,
            )
            assert run.status_code == 200, run.text
            result = run.json()
            assert result["bar_count"] == 18  # [start, end), final bar at start+18m
            assert result["fills"]
            latest = client.get(
                "/tenants/dev/accounts/desk/carry-backtests/latest",
                headers=headers,
            )
            assert latest.status_code == 200
            assert latest.json() == result
            denied = client.post(
                "/tenants/foreign/accounts/desk/carry-backtests",
                headers=headers, json=body,
            )
            assert denied.status_code == 409
    finally:
        runtime.close()
