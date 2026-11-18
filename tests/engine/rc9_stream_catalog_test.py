"""RC9 native Catalog access and bounded stream regression tests (offline)."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from nautilus_trader.model import FundingRateUpdate, InstrumentId, Money

from engine.pipeline.catalog import ParquetDataCatalog
from engine.strategies.backtest import read_history, read_history_batches
from engine.strategies.loading import run_issued_history, run_issued_history_stream
from nautilus_support import TestInstrumentProvider
from tests.engine.backtest_test import write_crossover_catalog
from tests.engine.strategy_load_test import ema_archive, ema_parameters, issued_payload


def test_rc9_funding_rate_catalog_round_trip(tmp_path):
    catalog = ParquetDataCatalog(tmp_path / "catalog")
    iid = InstrumentId.from_str("ETHUSDT-PERP.BINANCE")
    updates = [
        FundingRateUpdate(iid, Decimal("0.0001"), 1_700_000_000_000_000_000,
                          1_700_000_000_000_000_000, next_funding_ns=1_700_028_800_000_000_000),
        FundingRateUpdate(iid, Decimal("-0.0002"), 1_700_028_800_000_000_000,
                          1_700_028_800_000_000_000, next_funding_ns=1_700_057_600_000_000_000),
    ]
    catalog.write_data(updates)
    rows = catalog.funding_rate_updates(
        [str(iid)], start=updates[0].ts_event, end=updates[-1].ts_event,
    )
    assert [str(x.rate) for x in rows] == ["0.0001", "-0.0002"]


def test_streamed_backtest_matches_one_shot_for_deterministic_bars(tmp_path):
    from engine.strategies.package import archive_directory
    from engine.strategies.loading import _run_symbol, _instrument
    from nautilus_trader.model import BarType

    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    start = datetime(2024, 1, 1, tzinfo=UTC)
    root = tmp_path / "catalog"
    bars = write_crossover_catalog(root, start)
    batches = list(read_history_batches(
        root, "ETHUSDT", "1m", start, start + timedelta(minutes=100), batch_size=3,
    ))
    assert len(batches) > 1
    assert [bar.ts_event for batch in batches for bar in batch] == [bar.ts_event for bar in bars]

    # _run_symbol requires issued strategy archives; tests use the same immutable
    # sample package in each run and compare economically relevant output fields.
    from tests.engine.strategy_load_test import issued_payload, ema_archive, ema_parameters
    import base64
    payload = issued_payload(ema_archive(), "development", "stream", ema_parameters())
    load = {
        "tenant_id": "dev", "account_id": "desk", "issuance_id": "stream",
        "code_key": payload["code_key"], "version": payload["version"],
        "package_hash": payload["package_hash"], "entry_point": payload["entry_point"],
        "parameters": payload["parameters"], "archive": base64.b64decode(payload["archive"]),
    }
    initial = Money.from_str("1000.50 USDT")
    baseline = run_issued_history([load], bars, initial)
    streamed = run_issued_history_stream([load], iter(batches), initial)
    for field in ("orders", "fills", "positions", "ending_balance", "bar_count", "holdings"):
        assert streamed[field] == baseline[field]
