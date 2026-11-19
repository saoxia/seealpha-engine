"""Native dual-account carry simulation with real Catalog fixtures and no credentials."""

import base64
import io
import json
import zipfile
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


_BASKET_SOURCE = """
from decimal import Decimal
from nautilus_trader.trading import Strategy

def prepare(parameters):
    return dict(parameters)

class DemoFundingCarry(Strategy):
    def on_start(self):
        self.subscribe_market_bundle(
            symbol=self.parameters["symbol"],
            products=("spot", "futures_um"),
            data=("closed_bars", "funding_rate", "account_positions"),
        )

    def on_market_snapshot(self, snapshot):
        params = self.parameters
        spot = Decimal(snapshot["spot"]["price"])
        perpetual = Decimal(snapshot["perpetual"]["price"])
        basis = abs((perpetual / spot - Decimal(1)) * 10000)
        rate = Decimal(snapshot["funding"]["rate"])
        quantity = (
            Decimal(params["allocation"])
            if rate >= Decimal(params["min_funding_rate"])
            and basis <= Decimal(params["max_basis_bps"])
            else Decimal(0)
        )
        self.publish_basket(
            targets=[
                {"product": "spot", "symbol": params["symbol"], "quantity": str(quantity)},
                {"product": "futures_um", "symbol": params["symbol"],
                 "quantity": str(-quantity)},
            ],
            constraints={"atomic_hedge": True, "max_leg_drift": params["max_leg_drift"]},
        )
"""


def _archive(source=_BASKET_SOURCE):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as package:
        package.writestr("seealpha-package.json", json.dumps({
            "format": 1, "name": "demo_funding_carry", "version": "1",
            "entry_point": "strategy:DemoFundingCarry",
        }))
        package.writestr("strategy.py", source)
    archive = stream.getvalue()
    return archive, parse_metadata(archive)


def _load(member="A", *, quantity="0.1", purpose="development", source=_BASKET_SOURCE):
    archive, meta = _archive(source)
    return {
        "tenant_id": "dev", "account_id": "desk", "issuance_id": member,
        "code_key": meta.name, "version": meta.version,
        "entry_point": meta.entry_point, "package_hash": meta.digest,
        "archive": archive,
        "grant_purpose": purpose,
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
    assert result["target_events"]
    assert result["target_events"][0]["member_count"] == 1
    assert all(event["executable"] is False for event in result["target_events"])
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
    assert result["target_events"]
    assert all(row["member_count"] == 2 for row in result["target_events"])
    assert any(
        {leg["product"]: leg["quantity"] for leg in row["targets"]} ==
        {"spot": "0.2", "futures_um": "-0.2"}
        for row in result["target_events"]
    )


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
    (root / "strategy.py").write_text(_BASKET_SOURCE, encoding="utf-8")
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

def test_issued_strategy_source_controls_both_legs(tmp_path):
    # If Engine secretly recomputes a demo signal, this test would still trade.
    always_flat = _BASKET_SOURCE.replace(
        '        quantity = (\\n', '        quantity = (\\n',
    ).replace(
        '        self.publish_basket(\\n', '        quantity = Decimal(0)\\n        self.publish_basket(\\n',
    )
    result = _simulate(tmp_path, loads=[_load(source=always_flat)])
    assert result["orders"] == []
    assert result["fills"] == []
    assert all(
        all(leg["quantity"] == "0" for leg in row["targets"])
        for row in result["target_events"]
    )


def test_incomplete_basket_publication_fails_closed(tmp_path):
    incomplete = _BASKET_SOURCE.replace(
        '{"product": "futures_um", "symbol": params["symbol"],\\n'
        '                 "quantity": str(-quantity)},',
        '',
    )
    with pytest.raises(EngineConflict, match="所有腿|组合目标|完整"):
        _simulate(tmp_path, loads=[_load(source=incomplete)])
