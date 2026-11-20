"""Read-only shared historical data access without collector side effects."""

from datetime import UTC, datetime, timedelta

from engine.market_data.access.market_data_service import EngineMarketDataService
from engine.backtesting.backtest import read_history, read_history_batches
from tests.engine.backtest_test import write_crossover_catalog


def test_history_accessor_reuses_existing_catalog_and_half_open_window(tmp_path):
    start = datetime(2024, 1, 1, tzinfo=UTC)
    root = tmp_path / "catalog"
    native = write_crossover_catalog(root, start)
    market = EngineMarketDataService()
    source = market.history(root)
    end = start + timedelta(minutes=3)
    expected = [item for item in native if int(item.ts_event) < int(end.timestamp() * 1e9)]
    bars = source.bars("ETHUSDT", "1m", start, end)
    batches = list(source.bar_batches(
        "ETHUSDT", "1m", start, end, batch_size=2,
    ))
    assert [int(item.ts_event) for item in bars] == [int(item.ts_event) for item in expected]
    assert [int(item.ts_event) for batch in batches for item in batch] == [
        int(item.ts_event) for item in expected
    ]
    assert [int(item.ts_event) for item in read_history(root, "ETHUSDT", "1m", start, end)] == [
        int(item.ts_event) for item in expected
    ]
    assert sum(len(batch) for batch in read_history_batches(
        root, "ETHUSDT", "1m", start, end,
    )) == len(expected)
    assert market.live_requirements == {}


def test_missing_history_never_starts_collector_or_creates_catalog(tmp_path):
    root = tmp_path / "missing"
    reader = EngineMarketDataService().history(root)
    start = datetime(2024, 1, 1, tzinfo=UTC)
    assert reader.bars("ETHUSDT", "1m", start, start + timedelta(minutes=1)) == []
    assert list(reader.bar_batches(
        "ETHUSDT", "1m", start, start + timedelta(minutes=1),
    )) == []
    assert not root.exists()
