"""Native option BBO fills must precede directional risk; no invented execution."""

import json
from decimal import Decimal

import pytest

from engine.market_data.storage.catalog import ParquetDataCatalog
from engine.trading.runtime import EngineConflict
from engine.backtesting.option_history import read_option_protection_evidence
from engine.backtesting.protected_simulation import run_native_protected
from tests.engine.option_history_test import CALL, PUT, _catalog


def _issued():
    return {
        "tenant_id": "dev",
        "account_id": "desk",
        "issuance_id": "option-demo-1",
        "code_key": "demo_option_hedged_bollinger",
        "grant_purpose": "development",
        "parameters": {
            "symbol": "ETHUSDT",
            "put_symbol": PUT,
            "call_symbol": CALL,
            "allocation": "0.100",
            "risk_limit": "0.100",
            "option_contracts": "0.100",
            "max_total_premium": "30",
            "max_age_seconds": 180,
        },
    }


class EntryExitModel:
    """Deterministic research model tests the engine, not signal profitability."""

    def __init__(self):
        self.proposals = 0
        self.commits = []

    def propose(self, snapshot, params):
        self.proposals += 1
        assert snapshot["put"]["ask"] == "2.10"
        assert snapshot["call"]["ask_size"] == "1.000"
        if self.proposals > 2:
            return {"action": "flat", "targets": []}
        entering = self.proposals == 1
        return {
            "action": "enter_long" if entering else "exit",
            "new_position": 1 if entering else 0,
            "targets": [
                {"product": "options", "symbol": PUT, "quantity": "0.100" if entering else "0"},
                {"product": "options", "symbol": CALL, "quantity": "0.100" if entering else "0"},
                {"product": "futures_um", "symbol": "ETHUSDT", "quantity": "0.100" if entering else "0"},
            ],
        }

    def commit(self, plan):
        self.commits.append(plan["action"])


def _simulation(tmp_path, *, cap="1000", model=None, grant="development"):
    root, start = _catalog(tmp_path)
    from datetime import timedelta

    evidence = read_option_protection_evidence(
        root, "ETHUSDT", PUT, CALL, start, start + timedelta(minutes=4),
        "1m", option_contracts=Decimal("0.100"),
        max_quote_age_ns=1_000_000_000,
    )
    catalog = ParquetDataCatalog(root)
    perp, put, call = [
        catalog.instruments(instrument_ids=[name])[0]
        for name in ("ETHUSDT-PERP.BINANCE", f"{PUT}.BINANCE", f"{CALL}.BINANCE")
    ]
    load = _issued()
    load["grant_purpose"] = grant
    strategy = model or EntryExitModel()
    result = run_native_protected(
        load, evidence, (perp, put, call), interval="1m",
        starting_options_balance="1000", starting_perpetual_balance="1000",
        max_gross_notional=cap, model=strategy,
    )
    return result, strategy


def test_entry_fills_both_wings_before_directional_order_and_exit_reverses(tmp_path):
    result, model = _simulation(tmp_path)
    assert result["mode"] == "native_protected_options_development_only"
    assert result["venue_accounts"] == {"options": "CASH", "futures_um": "MARGIN"}
    assert model.commits == ["enter_long", "exit"]
    assert result["bar_count"] == 3
    order_phases = [row["phase"] for row in result["orders"]]
    assert order_phases == [
        "acquire_and_confirm_protection",
        "acquire_and_confirm_protection",
        "increase_directional_risk_after_protection",
        "reduce_directional_risk",
        "release_protection_after_risk_reduction",
        "release_protection_after_risk_reduction",
    ]
    assert len(result["fills"]) == 6
    assert [row["phase"] for row in result["fills"]] == order_phases
    assert Decimal(result["starting_balance"]) == Decimal(2000)
    assert Decimal(result["ending_balance"]) > 0
    assert Decimal(result["equity_curve"][0]["perpetual_quantity"]) == Decimal("0.1")
    assert Decimal(result["equity_curve"][-1]["perpetual_quantity"]) == 0
    assert Decimal(result["equity_curve"][-1]["put_quantity"]) == 0
    assert Decimal(result["equity_curve"][-1]["call_quantity"]) == 0
    assert "api_key" not in json.dumps(result)


def test_gross_budget_rejected_before_any_protection_order(tmp_path):
    with pytest.raises(EngineConflict, match="预算"):
        _simulation(tmp_path, cap="1")


def test_production_load_is_rejected_before_native_engine_executes(tmp_path):
    with pytest.raises(EngineConflict, match="开发"):
        _simulation(tmp_path, grant="production")


def test_native_executor_does_not_accept_missing_hedge_before_risk(tmp_path):
    class NakedModel(EntryExitModel):
        def propose(self, snapshot, params):
            plan = super().propose(snapshot, params)
            if plan["action"] == "enter_long":
                plan["targets"][0]["quantity"] = "0"
            return plan

    with pytest.raises(EngineConflict, match="保护"):
        _simulation(tmp_path, model=NakedModel())
