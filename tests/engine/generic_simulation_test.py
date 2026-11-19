"""Native multi-symbol research uses a single venue-aware matcher, never fake PnL aggregation."""
import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from nautilus_trader.model import Bar, BarType, CryptoPerpetual

from core.market.instruments import bar_type_for
from engine.runtime import EngineConflict
from engine.strategies.basket_targets import LegId
from engine.strategies.generic_simulation import run_native_generic_portfolio
from engine.strategies.package import parse_metadata
from nautilus_support import TestInstrumentProvider
from tests.engine.spot_backtest_test import spot_instrument

_SOURCE = """
from nautilus_trader.trading import Strategy

def prepare(parameters):
    return dict(parameters)

class MultiInstrument(Strategy):
    def on_start(self):
        self.subscribe_market_bundle(
            legs=(
                {"product": "spot", "symbol": "ETHUSDT"},
                {"product": "futures_um", "symbol": "BTCUSDT"},
            ),
            data=("closed_bars", "account_positions"),
        )

    def on_market_snapshot(self, snapshot):
        self.publish_basket(
            targets=[
                {"product": "spot", "symbol": "ETHUSDT", "quantity": "0.1"},
                {"product": "futures_um", "symbol": "BTCUSDT", "quantity": "-0.1"},
            ],
        )
"""


def _load(source=_SOURCE):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("seealpha-package.json", json.dumps({
            "format": 1, "name": "multi_instrument", "version": "1",
            "entry_point": "strategy:MultiInstrument",
        }))
        archive.writestr("strategy.py", source)
    raw = stream.getvalue()
    meta = parse_metadata(raw)
    return {
        "tenant_id": "dev", "account_id": "desk",
        "issuance_id": "multi-1", "code_key": meta.name, "version": meta.version,
        "entry_point": meta.entry_point, "package_hash": meta.digest,
        "archive": raw, "grant_purpose": "development",
        "parameters": {"symbol": "BTCUSDT", "allocation": "0.1", "risk_limit": "0.1"},
    }


def _perp():
    original = TestInstrumentProvider.ethusdt_perp_binance()
    definition = CryptoPerpetual.to_dict(original)
    definition["id"] = "BTCUSDT.BINANCE"
    definition["raw_symbol"] = "BTCUSDT"
    definition["base_currency"] = "BTC"
    return CryptoPerpetual.from_dict(definition)


def _frames(contracts, *, count=14):
    start = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp() * 1_000_000_000)
    bars = {}
    for leg, instrument in contracts.items():
        bar_type = BarType.from_str(bar_type_for(leg.symbol, "1m", leg.product))
        bars[leg] = [
            Bar(
                bar_type,
                instrument.make_price(100 if leg.product == "spot" else 2000),
                instrument.make_price(101 if leg.product == "spot" else 2001),
                instrument.make_price(99 if leg.product == "spot" else 1999),
                instrument.make_price(100 if leg.product == "spot" else 2000),
                instrument.make_qty(10),
                start + (index + 1) * 60_000_000_000,
                start + (index + 1) * 60_000_000_000,
            )
            for index in range(count)
        ]
    return [
        {leg: bars[leg][i] for leg in contracts} for i in range(count)
    ]


def _replay(*, loads=None, frames=None, cap="1000"):
    eth = LegId("dev", "desk", "spot", "ETHUSDT")
    btc = LegId("dev", "desk", "futures_um", "BTCUSDT")
    contracts = {eth: spot_instrument(), btc: _perp()}
    return run_native_generic_portfolio(
        loads or [_load()], frames if frames is not None else _frames(contracts),
        contracts, interval="1m", starting_spot="1000",
        starting_perpetual="1000", max_gross_notional=cap,
    )


def test_one_native_run_trades_two_different_symbols_and_products():
    result = _replay()
    assert result["mode"] == "native_generic_development_only"
    assert result["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert result["bar_count"] == 14
    assert result["venue_accounts"] == {"spot": "CASH", "futures_um": "MARGIN"}
    assert {(order["product"], order["symbol"]) for order in result["orders"]} == {
        ("spot", "ETHUSDT"), ("futures_um", "BTCUSDT"),
    }
    assert result["fills"]
    assert result["target_events"]
    assert result["funding_accounting"].startswith("NOT_INCLUDED")
    assert Decimal(result["ending_balance"]) > 0
    assert {x["product"] for x in result["equity_curve"][-1]["positions"]} == {
        "spot", "futures_um",
    }


def test_generic_runner_fails_closed_on_shared_budget():
    with pytest.raises(EngineConflict, match="预算"):
        _replay(cap="1")


def test_generic_runner_rejects_unapproved_issued_member():
    with pytest.raises(EngineConflict, match="开发"):
        _replay(loads=[{**_load(), "grant_purpose": "production"}])


def test_generic_runner_rejects_missing_leg_without_forward_fill():
    eth = LegId("dev", "desk", "spot", "ETHUSDT")
    btc = LegId("dev", "desk", "futures_um", "BTCUSDT")
    contracts = {eth: spot_instrument(), btc: _perp()}
    frames = _frames(contracts)
    frames[4] = {eth: frames[4][eth]}
    with pytest.raises(EngineConflict, match="缺少市场腿"):
        _replay(frames=frames)


def test_generic_runner_does_not_execute_strategy_bypassing_target_api():
    source = _SOURCE.replace(
        '        self.publish_basket(',
        '        raise RuntimeError("should not send native orders")' + chr(10)
        + '        self.publish_basket(',
    )
    with pytest.raises(RuntimeError, match="should not send native orders"):
        _replay(loads=[_load(source)])
