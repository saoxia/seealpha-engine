"""Real native catalog tests for pagination, verification and safe exports."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from engine.pipeline.catalog import ParquetDataCatalog, _timestamps_to_filename
from engine.pipeline.convert_binance_trades import convert_trade_archive
from engine.pipeline.data_management import (
    checked_request,
    coverage,
    deletion_plan,
    execute,
    preview,
    validate,
)
from engine.pipeline.models import PipelineSettings, SyncedArchive, bar_type_for
from nautilus_support import TestInstrumentProvider
from tests.pipeline.convert_binance_trades_test import trade_row, write_trade_archive


@pytest.fixture
def dataset(tmp_path):
    settings = PipelineSettings("futures_um", "trades", ("ETHUSDT",), ("1s", "1m"),
                                tmp_path / "raw", tmp_path / "catalog", tmp_path / "state")
    start = datetime(2026, 8, 1, tzinfo=UTC)
    stamp = int(start.timestamp() * 1000)
    spec, path = write_trade_archive(settings.raw_dir, [
        trade_row(1, stamp), trade_row(2, stamp, buyer_maker="false"),
        trade_row(3, stamp + 1000), trade_row(4, stamp + 60_000),
    ])
    catalog = ParquetDataCatalog(settings.catalog_dir)
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    catalog.write_data([instrument])
    convert_trade_archive(SyncedArchive(spec, path), instrument, catalog,
                          raw_dir=settings.raw_dir, state_dir=settings.state_dir,
                          bar_intervals=("1s", "1m"))
    return settings, start, path


def test_preview_preserves_same_timestamp_ticks_and_exclusive_end(dataset):
    settings, start, _ = dataset
    end = start + timedelta(minutes=1)
    first = preview(settings, "ETHUSDT", start, end, limit=1)
    second = preview(settings, "ETHUSDT", start, end, limit=1, cursor=first["next_cursor"])
    third = preview(settings, "ETHUSDT", start, end, limit=1, cursor=second["next_cursor"])
    assert [p["records"][0]["trade_id"] for p in (first, second, third)] == ["1", "2", "3"]
    assert first["records"][0]["ts_event_ns"] == second["records"][0]["ts_event_ns"]
    assert third["next_cursor"] is None
    assert first["records"][0]["price"] == "3000.10"
    with pytest.raises(ValueError, match="游标"):
        preview(settings, "ETHUSDT", start, end + timedelta(seconds=1), cursor=first["next_cursor"])
    with pytest.raises(ValueError, match="15 分钟"):
        preview(settings, "ETHUSDT", start, start + timedelta(hours=1))


def test_inventory_reports_each_existing_level_without_implying_continuity(dataset):
    from engine.pipeline.data_management import dataset_inventory
    settings, _start, _archive = dataset
    from dataclasses import replace

    items = dataset_inventory(replace(settings, symbols=("BTCUSDT",)), all_levels=True)
    assert {item["level"] for item in items} == {"tick", "1s", "1m"}
    assert all(item["event_ranges_ns"] and item["quality"] == "unverified" for item in items)
    assert next(item for item in items if item["level"] == "tick")["event_ranges_ns"] != next(item for item in items if item["level"] == "1m")["event_ranges_ns"]


def test_quality_compares_source_and_detects_missing_ticks(dataset):
    settings, start, _ = dataset
    request = checked_request("ETHUSDT", start, start + timedelta(days=1), ["1m"], settings)
    assert coverage(request, settings)["all_present"]
    result = validate(request, settings)
    assert result["ready"], result
    assert result["days"][0]["rows"] == 4
    path = next((settings.catalog_dir / "data/trades").rglob("*.parquet"))
    path.unlink()
    assert not validate(request, settings)["ready"]


def test_export_is_exact_and_checksum_failure_is_not_verified(dataset, tmp_path):
    settings, start, path = dataset
    request = checked_request("ETHUSDT", start, start + timedelta(days=1), ["1m"], settings)
    output = tmp_path / "export" / "ticks.csv"
    result = execute("export", request, settings, output)
    assert result["rows"] == 4
    assert "3000.10" in output.read_text()
    assert not output.with_suffix(".part").exists()
    path.write_bytes(b"corrupt archive")
    assert not validate(request, settings)["ready"]


def test_missing_catalog_read_does_not_create_directory(tmp_path):
    from engine.pipeline.data_management import dataset_inventory, open_catalog
    settings = PipelineSettings("futures_um", "trades", ("ETHUSDT",), ("1m",),
                                tmp_path / "raw", tmp_path / "missing", tmp_path / "state")
    assert dataset_inventory(settings) == []
    with pytest.raises(FileNotFoundError):
        open_catalog(settings)
    assert not settings.catalog_dir.exists()


@pytest.mark.parametrize("intervals", [[], ["1s"], ["1m"]])
@pytest.mark.parametrize("python_only", [False, True])
def test_prepare_respects_tick_only_and_selected_bar_level(dataset, tmp_path, monkeypatch, intervals, python_only):
    from engine.pipeline import prepare_data as preparation
    from engine.pipeline.data_management import dataset_inventory
    from engine.pipeline.sync_binance_batch import plan_daily_archives

    settings, start, archive = dataset
    if python_only:
        monkeypatch.setenv("NAUTILUS_RUST_CONVERTER", "off")
    from dataclasses import replace

    settings = replace(settings, symbols=())
    request = checked_request("ETHUSDT", start, start + timedelta(days=1), intervals, settings)
    plan = deletion_plan(request, settings)
    execute("delete", request, settings, tmp_path / "unused", expected_snapshot=plan["snapshot"])
    # Keep level selection independent of floating-point volume aggregation:
    # whole-unit fixtures have exact sums in both existing converter paths.
    stamp = int(start.timestamp() * 1000)
    write_trade_archive(settings.raw_dir, [trade_row(1, stamp, quantity="1.000"),
                                         trade_row(2, stamp + 1000, quantity="2.000")])
    monkeypatch.setattr(preparation, "sync_archives", lambda *args, **kwargs: [SyncedArchive(plan_daily_archives(request)[0], archive)])
    monkeypatch.setattr(preparation, "ensure_instruments", lambda *args: {"ETHUSDT": TestInstrumentProvider.ethusdt_perp_binance()})
    assert execute("prepare", request, settings, tmp_path / "unused")["ready"]
    assert coverage(request, settings)["all_present"]
    assert dataset_inventory(settings)[0]["event_ranges_ns"]
    for period in settings.intervals:
        assert bool(dataset_inventory(settings, interval=period)) == (period in intervals)


def test_materialization_uses_local_archives_and_bar_preview_is_exclusive(dataset, tmp_path):
    settings, start, _ = dataset
    request = checked_request("ETHUSDT", start, start + timedelta(days=1), ["1m"], settings)
    result = execute("materialize", request, settings, tmp_path / "unused.csv")
    assert result["coverage"]["all_present"]
    # Binance Bar events occur at the close boundary minus 1 ms.
    page = preview(settings, "ETHUSDT", start, start + timedelta(minutes=1), interval="1m")
    assert len(page["records"]) == 1
    assert page["records"][0]["ts_event_ns"] == str(int(start.timestamp()) * 10**9 + 59_999_000_000)
    assert page["records"][0]["close"] == "3000.10"


@pytest.mark.parametrize("delete_archives", [False, True])
def test_delete_cleans_dependents_preserves_other_days_and_can_prepare_again(dataset, tmp_path, delete_archives):
    settings, start, archive = dataset
    catalog = ParquetDataCatalog(settings.catalog_dir)
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    tomorrow = start + timedelta(days=1)
    spec, second = write_trade_archive(settings.raw_dir, [trade_row(5, int(tomorrow.timestamp() * 1000))],
                                       day=tomorrow.date().isoformat())
    convert_trade_archive(SyncedArchive(spec, second), instrument, catalog,
                          raw_dir=settings.raw_dir, state_dir=settings.state_dir,
                          bar_intervals=("1s", "1m"))
    # Rolling feature caches after the deleted day must be invalidated as well.
    features = settings.catalog_dir / "features" / bar_type_for("ETHUSDT", "1m") / "data/custom/BarFeatures"
    features.mkdir(parents=True)
    begin = int(tomorrow.timestamp()) * 10**9
    feature = features / _timestamps_to_filename(begin, begin + 86400 * 10**9 - 1)
    feature.write_bytes(b"derived-feature-fixture")
    feature.with_suffix(".metadata.json").write_text('{}')
    unrelated = settings.catalog_dir / "data/trades/BTCUSDT-PERP.BINANCE/keep.parquet"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_bytes(b"other-symbol")
    request = checked_request("ETHUSDT", start, tomorrow, ["1m"], settings)
    plan = deletion_plan(request, settings, delete_archives=delete_archives)
    assert plan["groups"]["bars"]["files"] == 2  # Includes 1s even though only 1m was selected.
    assert plan["groups"]["features"]["files"] == 2
    assert "supplements" in plan["groups"]
    assert archive.exists() and feature.exists()  # Preview never mutates data.
    result = execute("delete", request, settings, tmp_path / "unused",
                     expected_snapshot=plan["snapshot"], delete_archives=delete_archives)
    assert result["deleted_files"] == plan["file_count"]
    assert archive.exists() is not delete_archives
    assert second.exists() and unrelated.exists()
    assert not feature.exists() and not feature.with_suffix(".metadata.json").exists()
    assert not preview(settings, "ETHUSDT", start, start + timedelta(minutes=1))["records"]
    assert preview(settings, "ETHUSDT", tomorrow, tomorrow + timedelta(minutes=1))["records"]
    assert catalog.instruments(instrument_ids=[str(instrument.id)])
    assert not coverage(request, settings)["all_present"]
    if not delete_archives:
        execute("materialize", request, settings, tmp_path / "unused")
        assert validate(request, settings)["ready"]


def test_delete_rejects_stale_plan_and_symlinks_before_any_changes(dataset, tmp_path):
    settings, start, archive = dataset
    request = checked_request("ETHUSDT", start, start + timedelta(days=1), ["1m"], settings)
    plan = deletion_plan(request, settings, delete_archives=True)
    archive.write_bytes(archive.read_bytes() + b"change")
    with pytest.raises(ValueError, match="数据已变化"):
        execute("delete", request, settings, tmp_path / "unused", expected_snapshot=plan["snapshot"], delete_archives=True)
    assert all((settings.catalog_dir / item["path"]).exists() for item in plan["files"] if item["root"] == "catalog")
    archive.unlink()
    target = tmp_path / "unrelated.zip"
    target.write_bytes(b"keep")
    archive.symlink_to(target)
    with pytest.raises(ValueError, match="符号链接"):
        deletion_plan(request, settings, delete_archives=True)
    assert target.read_bytes() == b"keep"


def test_delete_rejects_partial_day_and_files_crossing_range(dataset):
    settings, start, _ = dataset
    request = checked_request("ETHUSDT", start + timedelta(hours=1), start + timedelta(days=1), ["1m"], settings)
    with pytest.raises(ValueError, match="完整 UTC 日"):
        deletion_plan(request, settings)
    folder = settings.catalog_dir / "data/trades/ETHUSDT-PERP.BINANCE"
    begin = int(start.timestamp()) * 10**9
    path = folder / _timestamps_to_filename(begin - 1, begin + 1)
    path.write_bytes(b"cross-boundary")
    request = checked_request("ETHUSDT", start, start + timedelta(days=1), ["1m"], settings)
    with pytest.raises(ValueError, match="跨越删除边界"):
        deletion_plan(request, settings)
    assert path.exists()


def test_interrupted_delete_invalidates_completion_and_can_resume_after_new_preview(dataset, tmp_path, monkeypatch):
    settings, start, archive = dataset
    request = checked_request("ETHUSDT", start, start + timedelta(days=1), ["1m"], settings)
    plan = deletion_plan(request, settings)
    original_unlink = Path.unlink

    def unavailable_file(path, *args, **kwargs):
        if path.suffix == ".parquet":
            raise OSError("test storage interruption")
        return original_unlink(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "unlink", unavailable_file)
        with pytest.raises(OSError, match="storage interruption"):
            execute("delete", request, settings, tmp_path / "unused", expected_snapshot=plan["snapshot"])
    assert archive.exists()
    assert not coverage(request, settings)["all_present"]
    resumed = deletion_plan(request, settings)
    assert resumed["snapshot"] != plan["snapshot"]
    result = execute("delete", request, settings, tmp_path / "unused", expected_snapshot=resumed["snapshot"])
    assert result["deleted_files"] == resumed["file_count"]
    assert deletion_plan(request, settings)["file_count"] == 0


@pytest.mark.parametrize("python_only", [False, True])
def test_usdc_prepare_read_validate_and_delete_complete_chain(tmp_path, monkeypatch, python_only):
    import hashlib
    from zipfile import ZIP_DEFLATED, ZipFile

    from engine.pipeline.data_management import dataset_inventory
    from engine.pipeline.models import instrument_id_for
    from engine.pipeline.sync_binance_batch import archive_path

    if python_only:
        monkeypatch.setenv("NAUTILUS_RUST_CONVERTER", "off")
    settings = PipelineSettings("futures_um", "trades", ("ETHUSDT",), ("1s", "1m"),
                                tmp_path / "raw", tmp_path / "catalog", tmp_path / "state")
    start = datetime(2026, 8, 1, tzinfo=UTC)
    stamp = int(start.timestamp() * 1000)
    source = archive_path("ETHUSDC", "trades", start.date())
    path = settings.raw_dir / source.removeprefix("data/")
    path.parent.mkdir(parents=True)
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("ETHUSDC-trades-2026-08-01.csv",
                         f"1,3000.10,1.000,3000.10,{stamp},true\n"
                         f"2,3000.10,2.000,6000.20,{stamp + 1000},false\n")
    Path(f"{path}.CHECKSUM").write_text(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n")
    original = TestInstrumentProvider.ethusdt_perp_binance()
    definition = original.to_dict()
    definition.update(id=instrument_id_for("ETHUSDC"), raw_symbol="ETHUSDC",
                      quote_currency="USDC", settlement_currency="USDC", min_notional="10.00000000 USDC")
    instrument = type(original).from_dict(definition)
    catalog = ParquetDataCatalog(settings.catalog_dir)
    catalog.write_data([instrument])
    request = checked_request("ETH-USDC", start, start + timedelta(days=1), ["1s", "1m"], settings)
    assert execute("prepare", request, settings, tmp_path / "unused")["ready"]
    items = dataset_inventory(settings, all_levels=True)
    assert {(item["symbol"], item["level"]) for item in items} == {
        ("ETHUSDC", "tick"), ("ETHUSDC", "1s"), ("ETHUSDC", "1m")}
    assert len(preview(settings, "ETHUSDC", start, start + timedelta(minutes=1))["records"]) == 2
    assert validate(request, settings)["ready"]
    plan = deletion_plan(request, settings, delete_archives=True)
    assert plan["file_count"] > 0
    execute("delete", request, settings, tmp_path / "unused", expected_snapshot=plan["snapshot"], delete_archives=True)
    assert dataset_inventory(settings, all_levels=True) == []
    assert not path.exists() and not Path(f"{path}.CHECKSUM").exists()


def test_managed_gap_bridge_reads_canonical_raw_ids_without_aggregate_substitution(dataset):
    from engine.feed.history import CatalogHistory

    settings, start, _path = dataset
    left = int(start.timestamp() * 1000)
    pages = list(CatalogHistory(settings.catalog_dir).raw_trade_pages("ETHUSDT", left, left + 60001))
    records = [row for page in pages for row in page]
    assert [row["id"] for row in records] == [1, 2, 3, 4]
    assert records[0]["isBuyerMaker"] and not records[1]["isBuyerMaker"]


def test_pair_deletion_removes_real_catalog_inventory_and_all_sources(dataset):
    from engine.pipeline.data_management import dataset_inventory
    from engine.pipeline.pair_deletion import delete_pair_data, pair_deletion_plan
    settings, _start, archive = dataset
    assert dataset_inventory(settings, all_levels=True)
    plan = pair_deletion_plan("ETHUSDT", "futures_um", settings, project=settings.raw_dir.parent)
    delete_pair_data("ETHUSDT", "futures_um", settings, expected_snapshot=plan["snapshot"], project=settings.raw_dir.parent)
    assert dataset_inventory(settings, all_levels=True) == []
    assert not archive.exists()
    assert ParquetDataCatalog(settings.catalog_dir).instruments()
