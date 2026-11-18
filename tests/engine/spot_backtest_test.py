"""Spot backtests consume distinct catalog bars and use cash accounts."""
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from nautilus_trader.model import (
    Bar,
    BarType,
    Currency,
    CurrencyPair,
    InstrumentId,
    Money,
    Price,
    Quantity,
    Symbol,
    Venue,
)

from core.market.instruments import bar_type_for
from engine.pipeline.catalog import ParquetDataCatalog
from engine.runtime import EngineConflict, EngineRuntime
from tests.engine.backtest_test import _issue, _ns, _ohlc, _window


def spot_instrument():
    return CurrencyPair(
        instrument_id=InstrumentId(Symbol("ETHUSDT"), Venue("BINANCE")),
        raw_symbol=Symbol("ETHUSDT"),
        base_currency=Currency.from_str("ETH"),
        quote_currency=Currency.from_str("USDT"),
        price_precision=2,
        size_precision=3,
        price_increment=Price.from_str("0.01"),
        size_increment=Quantity.from_str("0.001"),
        max_quantity=Quantity.from_str("10000.000"),
        min_quantity=Quantity.from_str("0.001"),
        min_notional=Money(10, Currency.from_str("USDT")),
        max_price=Price.from_str("152588.43"),
        min_price=Price.from_str("29.91"),
        margin_init=Decimal("0"),
        margin_maint=Decimal("0"),
        info={"project_fee_rates": {"maker": "0.0002", "taker": "0.0004"}},
        ts_event=1646199312128000000,
        ts_init=1646199342953849862,
    )


def spot_catalog(root, start):
    instrument = spot_instrument()
    bars = []
    kind = BarType.from_str(bar_type_for("ETHUSDT", "1m", "spot"))
    for index, (o, h, l, c) in enumerate(_ohlc()):
        stamp = _ns(start + timedelta(minutes=index + 1))
        bars.append(Bar(
            kind, instrument.make_price(o), instrument.make_price(h),
            instrument.make_price(l), instrument.make_price(c),
            instrument.make_qty(Decimal("10")), stamp, stamp,
        ))
    catalog = ParquetDataCatalog(root)
    catalog.write_data([instrument])
    catalog.write_data(bars)
    return bars


def test_spot_backtest_is_cash_and_requires_spot_intent(tmp_path):
    start = datetime(2024, 1, 1, tzinfo=UTC)
    spot_catalog(tmp_path / "data/catalog", start)
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    try:
        runtime.ensure_tenant("dev", "development")
        _issue(runtime, "dev", "development", "spot-1")
        runtime.note_subscription("dev", "market-data", "futures_um", "ETHUSDT",
                                  "intent", "configured", {"history_enabled": True})
        with pytest.raises(EngineConflict, match="订阅"):
            runtime.run_backtest("dev", "desk", product="spot", **_window(start))
        runtime.note_subscription("dev", "market-data", "spot", "ETHUSDT",
                                  "intent", "configured", {"history_enabled": True})
        result = runtime.run_backtest("dev", "desk", product="spot", **_window(start))
    finally:
        runtime.close()
    assert result["product"] == "spot"
    assert result["bar_count"] > 0
    assert result["symbol"] == "ETHUSDT"
    assert Decimal(result["ending_balance"]) >= 0
