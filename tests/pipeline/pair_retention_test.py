"""UTC start changes trim existing sources without deleting retained rows."""

import fcntl
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from nautilus_trader.model import (
    AggressorSide,
    Bar,
    BarType,
    CustomData,
    DataType,
    Price,
    Quantity,
    TradeId,
    TradeTick,
)

from core.market.instruments import bar_type_for
from engine.feed.pair_store import PairStore
from engine.pipeline.catalog import ParquetDataCatalog, TradeQuote
from engine.pipeline.models import PipelineSettings
from engine.pipeline.pair_deletion import history_trim_plan, trim_pair_history
from nautilus_support import TestInstrumentProvider

START = datetime(2026, 8, 15, tzinfo=UTC)
CUTOFF = int(START.timestamp() * 1000)


@pytest.fixture
def retained(tmp_path):
    settings = PipelineSettings("futures_um", "trades", ("ETHUSDT",), ("1m",),
                                tmp_path / "data/raw", tmp_path / "data/catalog", tmp_path / "data/state")
    catalog = ParquetDataCatalog(settings.catalog_dir)
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    catalog.write_data([instrument])
    ticks = [TradeTick(instrument.id, Price.from_str("10.00"), Quantity.from_str("1.000"),
                       AggressorSide.BUY, TradeId(str(index + 1)), stamp * 1000000, stamp * 1000000)
             for index, stamp in enumerate((CUTOFF - 1000, CUTOFF, CUTOFF + 1000))]
    catalog.write_data(ticks)
    catalog.supplement_catalog(str(instrument.id)).write_custom_data([
        CustomData(DataType("TradeQuote"), TradeQuote(trade_id=str(tick.trade_id), quote_quantity=10.0,
                                                     ts_event=tick.ts_event, ts_init=tick.ts_init)) for tick in ticks])
    for level, stamps in (("1m", (CUTOFF - 1, CUTOFF + 59999)),
                          ("1w", (int(datetime(2026, 8, 17, tzinfo=UTC).timestamp() * 1000) - 1,
                                  int(datetime(2026, 8, 24, tzinfo=UTC).timestamp() * 1000) - 1))):
        catalog.write_data([Bar(BarType.from_str(bar_type_for("ETHUSDT", level)),
                                *(Price.from_str("10.00") for _ in range(4)), Quantity.from_str("1.000"),
                                stamp * 1000000, stamp * 1000000) for stamp in stamps])
    source = settings.raw_dir / "futures/um/daily/trades/ETHUSDT"
    source.mkdir(parents=True)
    for day in ("2026-08-14", "2026-08-15"):
        (source / f"ETHUSDT-trades-{day}.zip").write_bytes(b"archive")
        (source / f"ETHUSDT-trades-{day}.zip.CHECKSUM").write_bytes(b"checksum")
    monthly = settings.raw_dir / "futures/um/monthly/trades/ETHUSDT/ETHUSDT-trades-2026-08.zip"
    monthly.parent.mkdir(parents=True)
    monthly.write_bytes(b"crossing official archive")
    feature = settings.catalog_dir / "features" / bar_type_for("ETHUSDT", "1m") / "data/custom/BarFeatures/cache.metadata.json"
    feature.parent.mkdir(parents=True)
    feature.write_text("{}")
    root = tmp_path / "data/market_data/binance_um_1m/managed/ETHUSDT/tick"
    root.mkdir(parents=True)
    store = PairStore(root / "collection.sqlite", "ETHUSDT", "tick", ["1m"])
    store.ticks([{"id": index + 1, "time": stamp, "price": "10", "qty": "1", "isBuyerMaker": False}
                 for index, stamp in enumerate((CUTOFF - 1000, CUTOFF, CUTOFF + 1000))], CUTOFF + 61000)
    store.close()
    (root / "history").mkdir()
    (root / "history/checkpoint.json").write_text(json.dumps({"next_day": "2026-08-01T00:00:00Z"}))
    other = settings.raw_dir / "futures/um/daily/trades/BTCUSDT/keep.zip"
    other.parent.mkdir(parents=True)
    other.write_bytes(b"unrelated")
    return settings, tmp_path, catalog, source, monthly, feature, root, other


def test_prefix_trim_keeps_boundary_rows_and_rewrites_catalog_file_ranges(retained):
    settings, project, catalog, source, monthly, feature, root, other = retained
    plan = history_trim_plan("ETHUSDT", "futures_um", START, settings, project=project)
    again = history_trim_plan("ETHUSDT", "futures_um", START, settings, project=project)
    assert plan["snapshot"] == again["snapshot"]
    assert {item["operation"] for item in plan["files"]} == {"delete", "sqlite", "parquet"}
    result = trim_pair_history("ETHUSDT", "futures_um", START, settings, expected_snapshot=plan["snapshot"], project=project)
    assert result["changed_files"] == plan["file_count"]
    assert [str(tick.trade_id) for tick in catalog.trade_ticks()] == ["2", "3"]
    from nautilus_trader.model import NautilusDataType
    quotes = catalog.supplement_catalog("ETHUSDT-PERP.BINANCE").query_custom_data(NautilusDataType.Custom("TradeQuote"))
    assert {str(quote.data.trade_id) for quote in quotes} == {"2", "3"}
    assert catalog.get_intervals(TradeTick, "ETHUSDT-PERP.BINANCE")[0][0] == CUTOFF * 1000000
    assert len(catalog.bars(bar_types=[bar_type_for("ETHUSDT", "1m")])) == 1
    weeks = catalog.bars(bar_types=[bar_type_for("ETHUSDT", "1w")])
    assert len(weeks) == 1 and weeks[0].ts_event > int(datetime(2026, 8, 17, tzinfo=UTC).timestamp() * 1e9)
    assert not (source / "ETHUSDT-trades-2026-08-14.zip").exists()
    assert not (source / "ETHUSDT-trades-2026-08-14.zip.CHECKSUM").exists()
    assert (source / "ETHUSDT-trades-2026-08-15.zip").exists()
    assert monthly.is_file() and result["retained_files"]
    assert not feature.exists() and other.read_bytes() == b"unrelated"
    assert not (root / "history/checkpoint.json").exists()
    with sqlite3.connect(root / "collection.sqlite") as db:
        assert db.execute("SELECT id FROM trades ORDER BY id").fetchall() == [(2,), (3,)]
        assert not db.execute("SELECT 1 FROM candles WHERE start_ms<?", (CUTOFF,)).fetchone()
        assert not db.execute("SELECT 1 FROM pending_updates WHERE start_ms<?", (CUTOFF,)).fetchone()


def test_changed_snapshot_refuses_deletion_before_any_row_is_removed(retained):
    settings, project, catalog, source, *_ = retained
    plan = history_trim_plan("ETHUSDT", "futures_um", START, settings, project=project)
    (source / "ETHUSDT-trades-2026-08-14.zip").write_bytes(b"changed")
    with pytest.raises(ValueError, match="数据已变化"):
        trim_pair_history("ETHUSDT", "futures_um", START, settings, expected_snapshot=plan["snapshot"], project=project)
    assert len(catalog.trade_ticks()) == 3


def test_trim_rejects_live_writers_and_symlinked_storage(retained):
    settings, project, catalog, source, _monthly, _feature, root, _other = retained
    with (root / "writer.lock").open("w") as lease:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="行情进程|采集或计算进程"):
            history_trim_plan("ETHUSDT", "futures_um", START, settings, project=project)
    linked = source / "unsafe.zip"
    linked.symlink_to(project / "private")
    with pytest.raises(ValueError, match="符号链接"):
        history_trim_plan("ETHUSDT", "futures_um", START, settings, project=project)
    assert len(catalog.trade_ticks()) == 3


@pytest.mark.parametrize("start", [START.replace(tzinfo=None), START + timedelta(hours=1)])
def test_trim_only_accepts_explicit_utc_day_boundaries(retained, start):
    settings, project, *_ = retained
    with pytest.raises(ValueError):
        history_trim_plan("ETHUSDT", "futures_um", start, settings, project=project)


def test_interrupted_cross_boundary_write_preserves_original_data(retained, monkeypatch):
    settings, project, catalog, *_ = retained
    plan = history_trim_plan("ETHUSDT", "futures_um", START, settings, project=project)
    def interrupted(*args):
        raise OSError("interrupted")
    monkeypatch.setattr("engine.pipeline.pair_deletion.os.replace", interrupted)
    with pytest.raises(OSError, match="interrupted"):
        trim_pair_history("ETHUSDT", "futures_um", START, settings, expected_snapshot=plan["snapshot"], project=project)
    assert len(catalog.trade_ticks()) == 3


def test_legacy_minute_journal_advances_its_stored_start_and_keeps_unknown_evidence(retained):
    from core.market.models import MinuteBar
    from engine.feed.store import Store

    settings, project, _catalog, _source, _monthly, _feature, root, _other = retained
    path = root.parent / "minutes.sqlite"
    store = Store(path, ("ETHUSDT",), CUTOFF - 60000)
    store.put([MinuteBar("ETHUSDT", stamp, *(Decimal(10) for _ in range(4)), Decimal(1))
               for stamp in (CUTOFF - 60000, CUTOFF)], "rest", CUTOFF + 60000)
    store.incident("conflict", {"unrecognized_legacy_evidence": True}, CUTOFF - 1000)
    store.db.close()
    plan = history_trim_plan("ETHUSDT", "futures_um", START, settings, project=project)
    assert any("异常证据" in item["reason"] for item in plan["retained_files"])
    trim_pair_history("ETHUSDT", "futures_um", START, settings, expected_snapshot=plan["snapshot"], project=project)
    restored = Store(path, ("ETHUSDT",), CUTOFF - 60000)
    assert restored.start_ms == CUTOFF
    assert restored.db.execute("SELECT start_ms FROM bars").fetchall()[0][0] == CUTOFF
    assert restored.next_ms("ETHUSDT") == CUTOFF + 60000
    assert restored.conflicted  # Ambiguous historical conflict evidence is never silently cleared.
    restored.db.close()


@pytest.mark.parametrize("folder", ["features", "bars", "trades"])
def test_configured_parent_folder_names_do_not_change_trim_scope(retained, folder):
    from dataclasses import replace

    settings, project, *_ = retained
    moved = project / folder
    moved.mkdir()
    (project / "data").rename(moved / "data")
    settings = replace(settings, raw_dir=moved / "data/raw", catalog_dir=moved / "data/catalog", state_dir=moved / "data/state")
    plan = history_trim_plan("ETHUSDT", "futures_um", START, settings, project=moved)
    trim_pair_history("ETHUSDT", "futures_um", START, settings, expected_snapshot=plan["snapshot"], project=moved)
    assert [str(tick.trade_id) for tick in ParquetDataCatalog(settings.catalog_dir).trade_ticks()] == ["2", "3"]
    journal = moved / "data/market_data/binance_um_1m/managed/ETHUSDT/tick/collection.sqlite"
    assert journal.is_file()
    with sqlite3.connect(journal) as db:
        assert db.execute("SELECT id FROM trades ORDER BY id").fetchall() == [(2,), (3,)]


def test_file_name_cannot_hide_retained_rows_during_deletion(retained):
    from engine.pipeline.catalog import _timestamps_to_filename

    settings, project, catalog, *_ = retained
    source = next((settings.catalog_dir / "data/trades").rglob("*.parquet"))
    misleading = source.with_name(_timestamps_to_filename((CUTOFF - 10000) * 1000000, (CUTOFF - 1) * 1000000))
    source.rename(misleading)
    plan = history_trim_plan("ETHUSDT", "futures_um", START, settings, project=project)
    item = next(item for item in plan["files"] if item["group"] == "ticks")
    assert item["operation"] == "parquet"
    trim_pair_history("ETHUSDT", "futures_um", START, settings, expected_snapshot=plan["snapshot"], project=project)
    assert [str(tick.trade_id) for tick in catalog.trade_ticks()] == ["2", "3"]


def test_rows_without_event_times_are_preserved_instead_of_assumed_old(retained):
    import pyarrow as pa
    import pyarrow.parquet as pq

    settings, project, _catalog, source, *_ = retained
    path = next((settings.catalog_dir / "data/trades").rglob("*.parquet"))
    table = pq.read_table(path)
    field = pa.field("ts_event", table.schema.field("ts_event").type)
    values = pa.array([None, CUTOFF * 1000000, (CUTOFF + 1000) * 1000000], type=pa.int64()).cast(field.type)
    table = table.set_column(table.schema.get_field_index("ts_event"), field, values)
    pq.write_table(table, path)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="空事件时间"):
        history_trim_plan("ETHUSDT", "futures_um", START, settings, project=project)
    assert path.read_bytes() == before and len(list(source.glob("*.zip"))) == 2
