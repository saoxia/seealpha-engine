"""Persist verified option bookTicker with original snapshots; gaps cannot be reconstructed."""

from datetime import UTC, datetime

import pytest

from engine.connectors.binance import options
from engine.pipeline import options_data
from engine.pipeline.catalog import ParquetDataCatalog
from engine.pipeline.models import PipelineSettings
from tests.connectors.binance.options_queries_test import definition


def context(tmp_path, monkeypatch):
    item = definition(monkeypatch)
    monkeypatch.setattr(options, "markets", lambda: [item])
    settings = PipelineSettings(
        "futures_um", "trades", ("BTCUSDT",), ("1s", "1m", "5m", "15m"),
        tmp_path / "raw", tmp_path / "catalog", tmp_path / "state",
    )
    return settings, item["symbol"]


def quote(symbol, stamp, *, ask="3001.000", bid="3000.000", size="0.20"):
    return {
        "level": "quote", "row": {
            "s": symbol, "T": stamp, "b": bid, "a": ask,
            "B": size, "A": size, "received_ns": (stamp + 1) * 1_000_000,
        },
    }


def test_option_quote_persists_native_and_immutable_sources(tmp_path, monkeypatch):
    settings, symbol = context(tmp_path, monkeypatch)
    stamp = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    events = [quote(symbol, stamp + delta) for delta in (1000, 3000)]
    assert options_data.persist_live(symbol, events, settings)["records_written"] == 2
    assert options_data.persist_live(symbol, events, settings)["records_written"] == 0
    raw = list((settings.raw_dir / "options" / symbol / "quote").rglob("*.json"))
    assert len(raw) == 1
    snapshot = options_data._load(raw[0])
    assert snapshot["source"] == "ws/bookTicker"
    assert snapshot["level"] == "quote"
    catalog = ParquetDataCatalog(settings.catalog_dir)
    ticks = catalog.quote_ticks(instrument_ids=[f"{symbol}.BINANCE"])
    assert len(ticks) == 2
    assert str(ticks[0].ask_price) == "3001.000"
    assert str(ticks[0].ask_size) == "0.20"
    assert ticks[0].ts_event == (stamp + 1000) * 1_000_000
    assert options_data.persist_live(
        symbol, [quote(symbol, stamp + 2000)], settings,
    )["records_written"] == 1
    with pytest.raises(ValueError, match="冲突"):
        options_data.persist_live(
            symbol, [quote(symbol, stamp + 2000, ask="3002.000")], settings,
        )


def test_failed_catalog_install_recovered_from_source_not_inferred(tmp_path, monkeypatch):
    settings, symbol = context(tmp_path, monkeypatch)
    stamp = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000) + 1000
    install = options_data._install

    def fail(*args, **kwargs):
        raise OSError("simulated interrupted Catalog install")

    monkeypatch.setattr(options_data, "_install", fail)
    with pytest.raises(OSError, match="interrupted"):
        options_data.persist_live(symbol, [quote(symbol, stamp)], settings)
    monkeypatch.setattr(options_data, "_install", install)
    result = options_data.persist_live(symbol, (), settings, recover=True)
    assert result["records_written"] == 1
    assert options_data.persist_live(symbol, (), settings, recover=True)["records_written"] == 0
    assert len(ParquetDataCatalog(settings.catalog_dir).quote_ticks(
        instrument_ids=[f"{symbol}.BINANCE"],
    )) == 1


def test_rejects_cross_instrument_and_crossed_quotes(tmp_path, monkeypatch):
    settings, symbol = context(tmp_path, monkeypatch)
    stamp = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000) + 1000
    with pytest.raises(ValueError, match="合约身份"):
        options_data.persist_live(
            symbol, [quote("ETH-261225-85000-C", stamp)], settings,
        )
    with pytest.raises(ValueError, match="盘口|买卖盘"):
        options_data.persist_live(
            symbol, [quote(symbol, stamp, bid="3005.000", ask="3001.000")],
            settings,
        )
