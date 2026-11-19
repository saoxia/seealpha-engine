"""Read strict multi-product native BBO rather than inventing prices from bars."""
from datetime import timedelta
from decimal import Decimal

import pytest
from nautilus_trader.model import QuoteTick

from engine.pipeline.catalog import ParquetDataCatalog
from engine.strategies.basket_generic_backtest import run_generic_catalog_research
from engine.strategies.basket_generic_history import read_generic_quote_history
from engine.strategies.basket_generic_runtime import BasketMemberRegistration
from engine.strategies.basket_targets import BasketPlanError, LegId
from nautilus_support import TestInstrumentProvider
from tests.engine.basket_history_test import _dataset, _range
from tests.engine.option_history_test import CALL, PUT, _catalog


def _ticks(instrument, bars, *, missing_index=None):
    result = []
    for index, bar in enumerate(bars):
        if index == missing_index:
            continue
        stamp = int(bar.ts_event) - 1
        result.append(QuoteTick(
            instrument.id, instrument.make_price(Decimal(2000)),
            instrument.make_price(Decimal(2001)),
            instrument.make_qty(Decimal(10)), instrument.make_qty(Decimal(10)),
            stamp, stamp,
        ))
    return result


def test_actual_catalog_spot_um_bbo_and_native_bar_alignment(tmp_path):
    start, spots, futures = _dataset(tmp_path)
    root = tmp_path / "data" / "catalog"
    from tests.engine.spot_backtest_test import spot_instrument

    spot = spot_instrument()
    perp = TestInstrumentProvider.ethusdt_perp_binance()
    catalog = ParquetDataCatalog(root)
    catalog.write_data([perp])
    catalog.write_data(_ticks(spot, spots))
    catalog.write_data(_ticks(perp, futures))
    spot_leg = LegId("dev", "account", "spot", "ETHUSDT")
    perp_leg = LegId("dev", "account", "futures_um", "ETHUSDT")
    a, b = _range(start)
    contracts, frames = read_generic_quote_history(
        root, legs=(spot_leg, perp_leg), clock_leg=perp_leg,
        start=a, end=b, interval="1m", max_quote_age_ns=1000,
    )
    assert len(contracts) == 2
    assert len(frames) == len(spots) == len(futures)
    assert {quote.leg.product for quote in frames[0].quotes} == {"spot", "futures_um"}
    assert all(quote.ts_ns < frame.ts_ns for frame in frames for quote in frame.quotes)
    assert all(quote.mark == Decimal("2000.5")
               for frame in frames for quote in frame.quotes)
    assert all(contract.taker_fee_rate >= 0 for contract in contracts)


def test_native_quote_gap_is_rejected_without_candle_close_substitution(tmp_path):
    start, spots, futures = _dataset(tmp_path)
    root = tmp_path / "data" / "catalog"
    from tests.engine.spot_backtest_test import spot_instrument

    catalog = ParquetDataCatalog(root)
    perp = TestInstrumentProvider.ethusdt_perp_binance()
    catalog.write_data([perp])
    catalog.write_data(_ticks(spot_instrument(), spots, missing_index=3))
    catalog.write_data(_ticks(perp, futures))
    spot_leg = LegId("dev", "account", "spot", "ETHUSDT")
    perp_leg = LegId("dev", "account", "futures_um", "ETHUSDT")
    a, b = _range(start)
    with pytest.raises(BasketPlanError, match="缺少"):
        read_generic_quote_history(
            root, legs=(spot_leg, perp_leg), clock_leg=perp_leg,
            start=a, end=b, interval="1m", max_quote_age_ns=1000,
        )


def test_native_um_and_option_bbo_share_actual_bar_clock(tmp_path):
    root, start = _catalog(tmp_path)
    catalog = ParquetDataCatalog(root)
    perp = TestInstrumentProvider.ethusdt_perp_binance()
    bars = catalog.bars(
        bar_types=["ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL"],
        start=start.isoformat(),
        end=(start + timedelta(minutes=4)).isoformat(),
    )
    # Catalog's query endpoint is inclusive, whereas basket [start,end)
    # excludes the last timestamp. Only BBO corresponding to the 3 bars
    # inside the requested basket window may be used.
    bars = [
        bar for bar in bars
        if int(bar.ts_event) < int((start + timedelta(minutes=4)).timestamp() * 1e9)
    ]
    assert len(bars) == 3
    catalog.write_data(_ticks(perp, bars))
    um = LegId("dev", "desk", "futures_um", "ETHUSDT")
    put = LegId("dev", "desk", "options", PUT)
    call = LegId("dev", "desk", "options", CALL)
    contracts, frames = read_generic_quote_history(
        root, legs=(um, put, call), clock_leg=um,
        start=start.isoformat(), end=(start + timedelta(minutes=4)).isoformat(),
        interval="1m", max_quote_age_ns=1000,
    )
    assert len(frames) == 3
    assert {q.leg for q in frames[0].quotes} == {um, put, call}
    options = [contract for contract in contracts if contract.leg.product == "options"]
    assert len(options) == 2
    assert options[0].expiry_ns == options[1].expiry_ns
    assert all(contract.multiplier > 0 for contract in options)


def test_native_catalog_one_call_generic_two_leg_backtest_with_financial_audit(tmp_path):
    start, spots, futures = _dataset(tmp_path)
    root = tmp_path / "data" / "catalog"
    from tests.engine.spot_backtest_test import spot_instrument

    catalog = ParquetDataCatalog(root)
    perp = TestInstrumentProvider.ethusdt_perp_binance()
    catalog.write_data([perp])
    catalog.write_data(_ticks(spot_instrument(), spots))
    catalog.write_data(_ticks(perp, futures))
    spot_leg = LegId("dev", "account", "spot", "ETHUSDT")
    perp_leg = LegId("dev", "account", "futures_um", "ETHUSDT")

    class Model:
        def on_market_bundle(self, member, bundle):
            member.publish_basket(targets=[
                {"product": leg.product, "symbol": leg.symbol,
                 "quantity": "0.1" if leg.product == "spot" else "-0.1"}
                for leg in bundle.member_legs
            ])

    a, b = _range(start)
    report = run_generic_catalog_research(
        root, tenant_id="dev", account_id="account",
        members=(BasketMemberRegistration("carry-1", (spot_leg, perp_leg), Model()),),
        clock_leg=perp_leg, start=a, end=b, interval="1m",
        starting_cash={"spot": Decimal(10000), "futures_um": Decimal(10000)},
        max_quote_age_ns=1000,
        max_gross_notional=Decimal(1000),
        expected_funding={},
    )
    assert report["status"] == "completed"
    assert report["native_matching"] is False
    assert report["bar_count"] == len(spots)
    assert report["funding_schedule_verified"] is True
    assert report["source"] == "native_parquet_bars_contracts_and_bbo"
    assert len(report["fills"]) == 2
    assert all(item["kind"] == "simulation_fill" for item in report["fills"])
    assert report["phase_audits"]
    assert Decimal(report["ending_balance"]) < Decimal(20000)
