"""Native multi-symbol research uses a single venue-aware matcher, never fake PnL aggregation."""
import io
import json
import zipfile
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from nautilus_trader.model import Bar, BarType, CryptoPerpetual

from engine.trading.runtime import EngineConflict
from engine.portfolio.basket_targets import LegId
from engine.backtesting.generic_simulation import run_native_generic_portfolio
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
    definition["id"] = "BTCUSDT-PERP.BINANCE"
    definition["raw_symbol"] = "BTCUSDT"
    definition["base_currency"] = "BTC"
    return CryptoPerpetual.from_dict(definition)


def _frames(contracts, *, count=14):
    start = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp() * 1_000_000_000)
    bars = {}
    for leg, instrument in contracts.items():
        bar_type = BarType.from_str(f"{instrument.id}-1-MINUTE-LAST-EXTERNAL")
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


def test_two_independent_issued_strategies_share_one_native_portfolio():
    spot_source = _SOURCE.replace(
        '                {"product": "futures_um", "symbol": "BTCUSDT"},' + chr(10), "",
    ).replace(
        '                {"product": "futures_um", "symbol": "BTCUSDT", "quantity": "-0.1"},'
        + chr(10), "",
    )
    perp_source = _SOURCE.replace(
        '                {"product": "spot", "symbol": "ETHUSDT"},' + chr(10), "",
    ).replace(
        '                {"product": "spot", "symbol": "ETHUSDT", "quantity": "0.1"},'
        + chr(10), "",
    )
    assert spot_source != _SOURCE and perp_source != _SOURCE
    a = {**_load(spot_source), "issuance_id": "eth-member"}
    b = {**_load(perp_source), "issuance_id": "btc-member"}
    report = _replay(loads=[a, b])
    assert len(report["orders"]) == 2
    assert {row["symbol"] for row in report["orders"]} == {"ETHUSDT", "BTCUSDT"}
    assert len(report["fills"]) >= 2
    assert report["target_events"]
    assert {row["product"] for row in report["target_events"][-1]["targets"]} == {
        "spot", "futures_um",
    }



def test_native_declared_hedge_uses_two_confirmed_risk_phases():
    spot = LegId("dev", "desk", "spot", "ETHUSDT")
    perp = LegId("dev", "desk", "futures_um", "ETHUSDT")
    contracts = {
        spot: spot_instrument(),
        perp: TestInstrumentProvider.ethusdt_perp_binance(),
    }
    source = _SOURCE.replace("BTCUSDT", "ETHUSDT").replace(
        '        self.publish_basket(\n',
        '        self.publish_basket(\n'
        '            constraints={"spot_perpetual_hedges": [{"symbol": "ETHUSDT"}]},\n',
    )
    report = run_native_generic_portfolio(
        [_load(source)], _frames(contracts), contracts,
        interval="1m", starting_spot="1000",
        starting_perpetual="1000", max_gross_notional="1000",
    )
    assert [row["phase"] for row in report["orders"]] == [
        "acquire_hedge_spot", "increase_hedged_perpetual",
    ]
    assert report["declared_hedges"] == [{
        "spot": "ETHUSDT", "perpetual": "ETHUSDT", "base_quantity_drift_limit": "0",
    }]
    assert report["hedge_verification"] == "confirmed_phases_only_non_atomic"
    final = {row["product"]: Decimal(row["quantity"])
             for row in report["equity_curve"][-1]["positions"]}
    assert final["spot"] + final["futures_um"] == 0


def test_native_declared_hedge_fails_on_unbalanced_target():
    spot = LegId("dev", "desk", "spot", "ETHUSDT")
    perp = LegId("dev", "desk", "futures_um", "ETHUSDT")
    contracts = {
        spot: spot_instrument(),
        perp: TestInstrumentProvider.ethusdt_perp_binance(),
    }
    source = _SOURCE.replace("BTCUSDT", "ETHUSDT").replace(
        '"quantity": "-0.1"', '"quantity": "-0.2"',
    ).replace(
        '        self.publish_basket(\n',
        '        self.publish_basket(\n'
        '            constraints={"spot_perpetual_hedges": [{"symbol": "ETHUSDT"}]},\n',
    )
    with pytest.raises(EngineConflict, match="等量"):
        run_native_generic_portfolio(
            [_load(source)], _frames(contracts), contracts,
            interval="1m", starting_spot="1000",
            starting_perpetual="1000", max_gross_notional="1000",
        )


def test_one_strategy_uses_two_product_histories_for_joint_price_ratio_signal():
    # Programmatic decisions are made from PRIOR verified closed bars only.
    source = _SOURCE.replace(
        "from nautilus_trader.trading import Strategy",
        "from decimal import Decimal\nfrom nautilus_trader.trading import Strategy",
    ).replace(
        '            data=("closed_bars", "account_positions"),',
        '            data=("closed_bars", "account_positions"),\n'
        '            lookback_bars=4,',
    ).replace(
        '        self.publish_basket(\n',
        '        histories = snapshot["history"]\n'
        '        assert set(histories) == {"spot:ETHUSDT", "futures_um:BTCUSDT"}\n'
        '        assert all(len(rows) <= 4 for rows in histories.values())\n'
        '        assert all(row["ts_ns"] < snapshot["now_ns"]\n'
        '                   for rows in histories.values() for row in rows)\n'
        '        assert all(rows[-1]["close"] == snapshot["markets"][key]["price"]\n'
        '                   for key, rows in histories.items() if rows)\n'
        '        ready = min(map(len, histories.values())) == 4\n'
        '        ratio = (Decimal(histories["futures_um:BTCUSDT"][-1]["close"])\n'
        '                 / Decimal(histories["spot:ETHUSDT"][-1]["close"])) if ready else 0\n'
        '        trade = ready and ratio > 10\n'
        '        self.publish_basket(\n',
    ).replace(
        '"quantity": "0.1"', '"quantity": "0.1" if trade else "0"',
    ).replace(
        '"quantity": "-0.1"', '"quantity": "-0.1" if trade else "0"',
    )
    report = _replay(loads=[_load(source)])
    assert len(report["orders"]) == 2
    assert report["target_events"][0]["targets"][0]["quantity"] == "0"
    assert any(row["quantity"] == "-0.1"
               for event in report["target_events"] for row in event["targets"])
    assert report["fills"]


def test_strategy_market_snapshot_is_scoped_to_its_own_subscribed_legs():
    spot_source = _SOURCE.replace(
        '                {"product": "futures_um", "symbol": "BTCUSDT"},' + chr(10), "",
    ).replace(
        '                {"product": "futures_um", "symbol": "BTCUSDT", "quantity": "-0.1"},'
        + chr(10), "",
    ).replace(
        '        self.publish_basket(\n',
        '        assert set(snapshot["markets"]) == {"spot:ETHUSDT"}\n'
        '        assert set(snapshot["positions"]) == {"spot:ETHUSDT"}\n'
        '        assert set(snapshot["history"]) == {"spot:ETHUSDT"}\n'
        '        self.publish_basket(\n',
    )
    perp_source = _SOURCE.replace(
        '                {"product": "spot", "symbol": "ETHUSDT"},' + chr(10), "",
    ).replace(
        '                {"product": "spot", "symbol": "ETHUSDT", "quantity": "0.1"},'
        + chr(10), "",
    )
    a = {**_load(spot_source), "issuance_id": "spot-signal"}
    b = {**_load(perp_source), "issuance_id": "perp-signal"}
    assert len(_replay(loads=[a, b])["orders"]) == 2



def test_generic_replay_one_market_two_subscribers_with_different_windows():
    # Two members request the same two physical streams but different windows.
    # Each receives its own scoped historical view, not a new source request.
    def custom_source(window):
        return _SOURCE.replace(
            '            data=("closed_bars", "account_positions"),',
            '            data=("closed_bars", "account_positions"),\n'
            f'            lookback_bars={window},',
        ).replace(
            '        self.publish_basket(\n',
            f'        assert all(len(rows) <= {window} for rows in '
            'snapshot["history"].values())\n'
            '        self.publish_basket(\n',
        )

    report = _replay(loads=[
        {**_load(custom_source(4)), "issuance_id": "four-bars"},
        {**_load(custom_source(2)), "issuance_id": "two-bars"},
    ])
    assert len(report["orders"]) == 2
    assert len(report["fills"]) >= 2
