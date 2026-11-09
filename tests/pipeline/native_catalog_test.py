"""Native Arrow boundaries: exact prices, bounded replay and exclusive end."""

from decimal import Decimal

import pandas as pd
from nautilus_trader.model import Bar, BarType, Price, Quantity

from engine.pipeline.catalog import BarDataWrangler, ParquetDataCatalog, TradeTickDataWrangler
from nautilus_support import TestInstrumentProvider


def test_native_bar_scanner_is_bounded_and_end_exclusive(tmp_path):
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    kind = BarType.from_str(f"{instrument.id}-1-MINUTE-LAST-EXTERNAL")
    catalog = ParquetDataCatalog(tmp_path)
    price, quantity = instrument.make_price(2000), instrument.make_qty(1)
    rows = [
        Bar(kind, price, price, price, price, quantity, t, t)
        for t in range(60_000_000_000, 601_000_000_000, 60_000_000_000)
    ]
    catalog.write_data(rows)
    batches = list(catalog.bar_batches(kind, 120_000_000_000, 600_000_000_000, batch_size=3))
    assert all(0 < len(batch) <= 3 for batch in batches)
    assert [bar for batch in batches for bar in batch] == rows[1:-1]


def test_native_wranglers_preserve_decimal_values_and_aggressor():
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    kind = BarType.from_str(f"{instrument.id}-1-MINUTE-LAST-EXTERNAL")
    index = pd.to_datetime([60_000_000_000, 120_000_000_000], unit="ns", utc=True)
    frame = pd.DataFrame(
        {
            "open": [3000.10, 1999.99],
            "high": [3001.10, 2001.00],
            "low": [2999.99, 1999.00],
            "close": [3000.10, 2000.01],
            "volume": [0.025, 0.123],
        },
        index=index,
    )
    bars = BarDataWrangler(kind, instrument).process(frame)
    assert bars[0].close == Price.from_str("3000.10")
    assert bars[1].volume == Quantity.from_str("0.123")
    trades = TradeTickDataWrangler(instrument).process(
        pd.DataFrame(
            {
                "price": [Decimal("3000.10"), Decimal("1999.99")],
                "quantity": [Decimal(".025"), Decimal(".123")],
                "side": ["SELL", "BUY"],
                "trade_id": ["100", "101"],
            },
            index=index,
        )
    )
    assert [t.price.as_decimal() for t in trades] == [Decimal("3000.10"), Decimal("1999.99")]
    assert [t.aggressor_side.name for t in trades] == ["SELL", "BUY"]


def test_arrow_quantization_matches_native_instrument_for_half_ticks():
    from engine.pipeline.catalog import _decimal
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    values = ["103.525","103.515","100.005","100.025"]
    expected = [instrument.make_price(Decimal(value)).as_decimal() for value in values]
    assert _decimal([Decimal(value) for value in values],2).to_pylist() == expected
    assert _decimal([float(value) for value in values],2).to_pylist() == expected
    sizes = ["1.0005","1.0015"]
    assert _decimal([Decimal(value) for value in sizes],3).to_pylist() == [
        instrument.make_qty(Decimal(value)).as_decimal() for value in sizes]
