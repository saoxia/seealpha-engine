"""Historical option protection requires native bid/ask evidence and aligned bars."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from nautilus_trader.model import Bar, BarType, QuoteTick

from core.market.instruments import bar_type_for
from engine.backtesting.option_history import read_option_protection_evidence
from engine.connectors.binance.options import instrument as option_contract
from engine.market_data.storage.catalog import ParquetDataCatalog
from engine.trading.runtime import EngineConflict
from nautilus_support import TestInstrumentProvider
from tests.engine.backtest_test import write_crossover_catalog

PUT = "ETH-240202-1800-P"
CALL = "ETH-240202-2200-C"


def _option(name, start):
    call = name.endswith("-C")
    return option_contract({
        "symbol": name,
        "base": "ETH",
        "quote": "USDT",
        "settlement": "USDT",
        "right": "CALL" if call else "PUT",
        "strike": "2200.00" if call else "1800.00",
        "expiry": int(datetime(2024, 2, 2, tzinfo=UTC).timestamp() * 1000),
        "fetched_ms": int(start.timestamp() * 1000),
        "multiplier": "1.000",
        "price_precision": 2,
        "size_precision": 3,
        "price_increment": "0.01",
        "size_increment": "0.001",
        "min_quantity": "0.001",
        "max_quantity": "100.000",
        "margin_init": "0",
        "margin_maint": "0",
        "min_price": "0.01",
        "max_price": "500.00",
    })


def _catalog(tmp_path, *, missing_call_quote=False, quote_size="1.000"):
    start = datetime(2024, 1, 1, tzinfo=UTC)
    root = tmp_path / "catalog"
    futures_bars = write_crossover_catalog(root, start)
    catalog = ParquetDataCatalog(root)
    catalog.write_data([TestInstrumentProvider.ethusdt_perp_binance()])
    put, call = _option(PUT, start), _option(CALL, start)
    catalog.write_data([put, call])
    for instrument in (put, call):
        bars, quotes = [], []
        kind = BarType.from_str(bar_type_for(str(instrument.raw_symbol), "1m", "options"))
        for index, perp in enumerate(futures_bars[:3]):
            stamp = perp.ts_event - 1
            bars.append(Bar(
                kind, instrument.make_price(2), instrument.make_price(3),
                instrument.make_price(1), instrument.make_price(2),
                instrument.make_qty(Decimal(1)), stamp, stamp,
            ))
            if not (missing_call_quote and instrument.id == call.id and index == 1):
                event = perp.ts_event - 2
                quotes.append(QuoteTick(
                    instrument.id, instrument.make_price(Decimal("1.90")),
                    instrument.make_price(Decimal("2.10")),
                    instrument.make_qty(Decimal(quote_size)),
                    instrument.make_qty(Decimal(quote_size)),
                    event, event,
                ))
        catalog.write_data(bars)
        if quotes:
            catalog.write_data(quotes)
    return root, start


def _read(root, start, **kwargs):
    return read_option_protection_evidence(
        root, "ETHUSDT", PUT, CALL, start, start + timedelta(minutes=4),
        "1m", option_contracts=Decimal("0.100"),
        max_quote_age_ns=1_000_000_000, **kwargs,
    )


def test_native_catalog_quote_tick_roundtrip_and_option_alignment(tmp_path):
    root, start = _catalog(tmp_path)
    catalog = ParquetDataCatalog(root)
    quotes = catalog.quote_ticks(instrument_ids=[f"{PUT}.BINANCE"])
    assert len(quotes) == 3
    assert all(isinstance(row, QuoteTick) for row in quotes)
    evidence = _read(root, start)
    assert len(evidence) == 3
    assert all(row.perpetual.ts_event == row.put.ts_event + 1
               == row.call.ts_event + 1 for row in evidence)
    assert all(row.put_quote.ask_size.as_decimal() >= Decimal("0.100")
               for row in evidence)
    assert all(row.put_quote.ts_event < row.ts_ns for row in evidence)


def test_missing_quote_fails_instead_of_using_option_bar_close(tmp_path):
    root, start = _catalog(tmp_path, missing_call_quote=True)
    with pytest.raises(EngineConflict, match="过期|缺失"):
        _read(root, start)


def test_insufficient_ask_volume_fails_closed(tmp_path):
    root, start = _catalog(tmp_path, quote_size="0.010")
    with pytest.raises(EngineConflict, match="不足额"):
        _read(root, start)


def test_rejects_option_contract_that_expires_in_window(tmp_path):
    root, _start = _catalog(tmp_path)
    expiry = datetime(2024, 2, 2, tzinfo=UTC)
    with pytest.raises(EngineConflict, match="到期"):
        read_option_protection_evidence(
            root, "ETHUSDT", PUT, CALL,
            expiry - timedelta(minutes=5), expiry + timedelta(minutes=5),
            "1m", option_contracts=Decimal("0.100"),
            max_quote_age_ns=1_000_000_000,
        )
