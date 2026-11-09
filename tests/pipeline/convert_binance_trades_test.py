import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd
import pytest
from nautilus_trader.model import AggressorSide

from engine.pipeline.catalog import ParquetDataCatalog
from engine.pipeline.convert_binance_trades import convert_trade_archive, iter_trade_archive
from engine.pipeline.models import ArchiveSpec, SyncedArchive
from engine.pipeline.rust_trade_converter import find_rust_converter
from engine.pipeline.validate_data import TRADE_COLUMNS, DataValidationError, normalize_trade_frame
from nautilus_support import TestInstrumentProvider


def trade_row(
    trade_id: int,
    timestamp: int,
    *,
    buyer_maker: str = "true",
    price: str = "3000.10",
    quantity: str = "0.025",
) -> list[str]:
    return [
        str(trade_id),
        price,
        quantity,
        "75.0025",
        str(timestamp),
        buyer_maker,
    ]


def write_trade_archive(
    raw_dir: Path,
    rows: list[list[str]],
    *,
    with_header: bool = False,
    day: str = "2026-08-01",
) -> tuple[ArchiveSpec, Path]:
    relative = Path(f"futures/um/daily/trades/ETHUSDT/ETHUSDT-trades-{day}.zip")
    path = raw_dir / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    content_rows = rows
    if with_header:
        content_rows = [["id", "price", "qty", "quoteQty", "time", "isBuyerMaker"], *rows]
    content = "\n".join(",".join(row) for row in content_rows).encode()
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(f"ETHUSDT-trades-{day}.csv", content)
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    Path(f"{path}.CHECKSUM").write_text(
        f"{checksum}  {path.name}\n",
        encoding="utf-8",
    )
    spec = ArchiveSpec(
        symbol="ETHUSDT",
        data_type="trades",
        day=datetime.fromisoformat(day).date(),
        source_path=f"data/{relative.as_posix()}",
    )
    return spec, path


def test_trade_conversion_is_chunked_and_idempotent(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    catalog_dir = tmp_path / "catalog"
    state_dir = tmp_path / "pipeline_state"
    catalog_dir.mkdir()
    start_ms = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    rows = [
        trade_row(1, start_ms, price="3000.10", quantity="0.025"),
        trade_row(2, start_ms, buyer_maker="true", price="2998.00", quantity="0.010"),
        trade_row(2, start_ms + 1, buyer_maker="false", price="3001.00", quantity="0.050"),
        trade_row(3, start_ms + 1, price="2999.00", quantity="0.075"),
        trade_row(4, start_ms + 1_000, price="3002.00", quantity="0.100"),
    ]
    spec, path = write_trade_archive(raw_dir, rows, with_header=True)
    synced = SyncedArchive(spec=spec, path=path)
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    catalog = ParquetDataCatalog(str(catalog_dir))

    first = convert_trade_archive(
        synced,
        instrument,
        catalog,
        raw_dir=raw_dir,
        state_dir=state_dir,
        chunk_rows=2,
        bar_intervals=("1s", "1m", "15m"),
    )
    second = convert_trade_archive(
        synced,
        instrument,
        catalog,
        raw_dir=raw_dir,
        state_dir=state_dir,
        chunk_rows=2,
        bar_intervals=("1s", "1m", "15m"),
    )

    ticks = catalog.trade_ticks(instrument_ids=["ETHUSDT-PERP.BINANCE"])
    assert first == second == 4
    assert [str(tick.trade_id) for tick in ticks] == ["1", "2", "3", "4"]
    assert ticks[0].ts_event == start_ms * 1_000_000
    assert ticks[0].aggressor_side == AggressorSide.SELL
    assert ticks[1].aggressor_side == AggressorSide.BUY

    second_bars = catalog.bars(
        bar_types=["ETHUSDT-PERP.BINANCE-1-SECOND-LAST-EXTERNAL"],
    )
    minute_bars = catalog.bars(
        bar_types=["ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL"],
    )
    fifteen_minute_bars = catalog.bars(
        bar_types=["ETHUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL"],
    )
    assert len(second_bars) == 2
    assert [
        str(value)
        for value in (
            second_bars[0].open,
            second_bars[0].high,
            second_bars[0].low,
            second_bars[0].close,
            second_bars[0].volume,
        )
    ] == ["3000.10", "3001.00", "2999.00", "2999.00", "0.150"]
    assert second_bars[0].ts_event == (start_ms + 999) * 1_000_000
    assert len(minute_bars) == 1
    assert str(minute_bars[0].close) == "3002.00"
    assert str(minute_bars[0].volume) == "0.250"
    assert minute_bars[0].ts_event == (start_ms + 60_000 - 1) * 1_000_000
    assert len(fifteen_minute_bars) == 1
    assert fifteen_minute_bars[0].ts_event == (start_ms + 15 * 60_000 - 1) * 1_000_000

    records = list((state_dir / "converted").rglob("*.json"))
    assert len(records) == 1
    record = json.loads(records[0].read_text())
    assert record["status"] == "ready"
    assert record["bar_intervals"] == ["1s", "1m", "15m"]
    assert record["bars"]["1s"]["rows"] == 2
    if find_rust_converter() is not None:
        assert record["backend"] == "rust-arrow"


def test_rust_output_matches_python_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    binary = find_rust_converter()
    if binary is None:
        pytest.skip("Rust converter has not been built")
    raw_dir = tmp_path / "raw"
    start_ms = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    rows = [
        trade_row(1, start_ms, price="3000.10", quantity="0.025"),
        trade_row(2, start_ms, buyer_maker="true", price="2998.00", quantity="0.010"),
        trade_row(2, start_ms + 1, buyer_maker="false", price="3001.00", quantity="0.050"),
        trade_row(3, start_ms + 1, price="2999.00", quantity="0.075"),
        trade_row(4, start_ms + 1_000, price="3002.00", quantity="0.100"),
    ]
    spec, path = write_trade_archive(raw_dir, rows, with_header=True)
    synced = SyncedArchive(spec=spec, path=path)
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    rust_catalog = ParquetDataCatalog(str(tmp_path / "rust_catalog"))
    python_catalog = ParquetDataCatalog(str(tmp_path / "python_catalog"))

    monkeypatch.setenv("NAUTILUS_RUST_CONVERTER", str(binary))
    convert_trade_archive(
        synced,
        instrument,
        rust_catalog,
        raw_dir=raw_dir,
        state_dir=tmp_path / "rust_state",
        chunk_rows=2,
        bar_intervals=("1s", "1m", "15m"),
    )
    monkeypatch.setenv("NAUTILUS_RUST_CONVERTER", "off")
    convert_trade_archive(
        synced,
        instrument,
        python_catalog,
        raw_dir=raw_dir,
        state_dir=tmp_path / "python_state",
        chunk_rows=2,
        bar_intervals=("1s", "1m", "15m"),
    )

    instrument_id = "ETHUSDT-PERP.BINANCE"
    rust_ticks = rust_catalog.trade_ticks(instrument_ids=[instrument_id])
    python_ticks = python_catalog.trade_ticks(instrument_ids=[instrument_id])
    assert [str(value) for value in rust_ticks] == [str(value) for value in python_ticks]
    bar_types = (
        f"{instrument_id}-1-SECOND-LAST-EXTERNAL",
        f"{instrument_id}-1-MINUTE-LAST-EXTERNAL",
        f"{instrument_id}-15-MINUTE-LAST-EXTERNAL",
    )
    for bar_type in bar_types:
        rust_bars = rust_catalog.bars(bar_types=[bar_type])
        python_bars = python_catalog.bars(bar_types=[bar_type])
        assert [str(value) for value in rust_bars] == [str(value) for value in python_bars]


def test_trade_validation_failure_writes_manifest_without_copying_archive(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw"
    catalog_dir = tmp_path / "catalog"
    state_dir = tmp_path / "pipeline_state"
    catalog_dir.mkdir()
    start_ms = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    spec, path = write_trade_archive(
        raw_dir,
        [trade_row(1, start_ms, buyer_maker="unknown")],
    )
    synced = SyncedArchive(spec=spec, path=path)
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    catalog = ParquetDataCatalog(str(catalog_dir))

    with pytest.raises(DataValidationError, match="isBuyerMaker"):
        convert_trade_archive(
            synced,
            instrument,
            catalog,
            raw_dir=raw_dir,
            state_dir=state_dir,
        )

    failures = list((state_dir / "failures").rglob("*.json"))
    assert len(failures) == 1
    failure = json.loads(failures[0].read_text(encoding="utf-8"))
    assert failure["status"] == "failed"
    assert failure["error_type"] == "DataValidationError"
    assert "isBuyerMaker" in failure["error"]
    assert failure["source"] == spec.source_path
    assert list(tmp_path.rglob("*.zip")) == [path]


def test_trade_archive_keeps_last_duplicate_id_across_chunks(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    start_ms = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    _, path = write_trade_archive(
        raw_dir,
        [
            trade_row(1, start_ms, price="3000.00"),
            trade_row(1, start_ms + 1, price="3001.00"),
            trade_row(2, start_ms + 2, price="3002.00"),
        ],
    )

    rows = pd.concat(iter_trade_archive(path, chunk_rows=1), ignore_index=True)

    assert rows["trade_id"].tolist() == [1, 2]
    assert rows["timestamp"].tolist() == [start_ms + 1, start_ms + 2]
    assert rows["price"].tolist() == [3001.0, 3002.0]


def test_trade_archive_rejects_descending_ids(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    start_ms = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    _, path = write_trade_archive(
        raw_dir,
        [trade_row(2, start_ms), trade_row(1, start_ms + 1)],
    )

    with pytest.raises(DataValidationError, match="Trade ID"):
        list(iter_trade_archive(path, chunk_rows=1))


def test_trade_archive_rejects_invalid_maker_flag(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    start_ms = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    _, path = write_trade_archive(
        raw_dir,
        [trade_row(1, start_ms, buyer_maker="unknown")],
    )

    with pytest.raises(DataValidationError, match="isBuyerMaker"):
        list(iter_trade_archive(path))


@pytest.mark.parametrize("chunk_rows", [1, 2, 10])
def test_trade_archive_validates_discarded_duplicates_and_normalizes_flags(tmp_path, chunk_rows):
    start_ms = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    rows = [
        trade_row(1, start_ms, buyer_maker=" TRUE "),
        trade_row(1, start_ms + 1, buyer_maker=" False "),
        trade_row(2, start_ms + 2, buyer_maker="true"),
    ]
    _, path = write_trade_archive(tmp_path, rows, with_header=True)
    frame = pd.concat(iter_trade_archive(path, chunk_rows=chunk_rows), ignore_index=True)
    assert frame.trade_id.tolist() == [1, 2]
    assert frame.buyer_maker.tolist() == [False, True]
    rows[0][-1] = "invalid"
    _, path = write_trade_archive(tmp_path, rows, with_header=True)
    with pytest.raises(DataValidationError, match="isBuyerMaker"):
        list(iter_trade_archive(path, chunk_rows=chunk_rows))


def test_mixed_boolean_and_numeric_maker_is_not_silently_accepted():
    rows = [trade_row(1, 1), trade_row(2, 2)]
    frame = pd.DataFrame(rows, columns=TRADE_COLUMNS)
    frame["buyer_maker"] = pd.Series([True, 1], dtype=object)
    with pytest.raises(DataValidationError, match="isBuyerMaker"):
        normalize_trade_frame(frame)


def test_trade_header_detection_skips_leading_blank_lines(tmp_path):
    _, path = write_trade_archive(tmp_path, [trade_row(1, 1)], with_header=True)
    with ZipFile(path) as archive:
        name = archive.namelist()[0]
        content = archive.read(name)
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(name, b"\n  \n" + content)
    frames = list(iter_trade_archive(path, chunk_rows=1))
    assert pd.concat(frames).trade_id.tolist() == [1]


def parquet_snapshot(root: Path) -> dict:
    return {
        str(path.relative_to(root)): (
            path.stat().st_mtime_ns,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in root.rglob("*.parquet")
    }


@pytest.fixture(params=["python", "rust"])
def incremental_dataset(tmp_path, monkeypatch, request):
    if request.param == "rust":
        binary = find_rust_converter()
        if binary is None:
            pytest.skip("Rust converter has not been built")
        monkeypatch.setenv("NAUTILUS_RUST_CONVERTER", str(binary))
    else:
        monkeypatch.setenv("NAUTILUS_RUST_CONVERTER", "off")
    raw, state = tmp_path / "raw", tmp_path / "state"
    start = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    rows = [trade_row(i + 1, start + i * 60_000, price=str(3000 + i)) for i in range(5)]
    spec, path = write_trade_archive(raw, rows)
    synced = SyncedArchive(spec, path)
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    catalog = ParquetDataCatalog(str(tmp_path / "catalog"))
    kwargs = {"raw_dir": raw, "state_dir": state, "chunk_rows": 2}
    convert_trade_archive(synced, instrument, catalog, bar_intervals=("1m", "15m"), **kwargs)
    return synced, instrument, catalog, kwargs, rows


def test_subset_order_and_version_changes_do_not_rewrite(incremental_dataset):
    synced, instrument, catalog, kwargs, rows = incremental_dataset
    root = Path(catalog.path)
    before = parquet_snapshot(root)
    (state_path,) = kwargs["state_dir"].rglob("*.json")
    record = json.loads(state_path.read_text())
    record["converter_version"] = -1
    state_path.write_text(json.dumps(record))
    state_before = state_path.read_bytes()
    # 即使原始归档内容变化，普通调用也不能改写已完成的数据。
    write_trade_archive(kwargs["raw_dir"], [*rows, trade_row(6, int(rows[-1][4]) + 1)])
    for intervals in (("1m",), ("15m", "1m")):
        convert_trade_archive(synced, instrument, catalog, bar_intervals=intervals, **kwargs)
    assert parquet_snapshot(root) == before
    assert state_path.read_bytes() == state_before


def test_new_period_aggregates_existing_bars_without_rewriting_ticks(
    incremental_dataset,
    monkeypatch,
):
    from engine.pipeline import convert_binance_trades as converter

    synced, instrument, catalog, kwargs, _ = incremental_dataset
    root = Path(catalog.path)
    before = parquet_snapshot(root)

    def unexpected(*args, **kwargs):
        raise AssertionError("已有低周期数据时不应重新解析 trades 或生成 ticks")

    monkeypatch.setattr(converter, "iter_trade_archive", unexpected)
    monkeypatch.setattr(converter, "_convert_fresh_trade_archive", unexpected)
    convert_trade_archive(synced, instrument, catalog, bar_intervals=("5m", "1m"), **kwargs)
    after = parquet_snapshot(root)
    assert {name: after[name] for name in before} == before
    assert len(after) == len(before) + 1
    bars = catalog.bars(bar_types=["ETHUSDT-PERP.BINANCE-5-MINUTE-LAST-EXTERNAL"])
    assert len(bars) == 1
    assert tuple(
        str(value) for value in (bars[0].open, bars[0].high, bars[0].low, bars[0].close)
    ) == (
        "3000.00",
        "3004.00",
        "3000.00",
        "3004.00",
    )
    assert str(bars[0].volume) == "0.125"
    convert_trade_archive(synced, instrument, catalog, bar_intervals=("1m", "5m", "15m"), **kwargs)
    assert parquet_snapshot(root) == after


def test_force_only_replaces_requested_day_and_periods(incremental_dataset):
    synced, instrument, catalog, kwargs, rows = incremental_dataset
    next_rows = [trade_row(1, int(rows[0][4]) + 86_400_000)]
    next_spec, next_path = write_trade_archive(kwargs["raw_dir"], next_rows, day="2026-08-02")
    convert_trade_archive(
        SyncedArchive(next_spec, next_path),
        instrument,
        catalog,
        bar_intervals=("1m", "15m"),
        **kwargs,
    )
    root = Path(catalog.path)
    before = parquet_snapshot(root)
    write_trade_archive(kwargs["raw_dir"], [trade_row(1, int(rows[0][4]), price="4000.00")])
    convert_trade_archive(synced, instrument, catalog, bar_intervals=("1m",), force=True, **kwargs)
    after = parquet_snapshot(root)
    preserved = {
        name: info for name, info in before.items() if "15-MINUTE" in name or "2026-08-02" in name
    }
    assert preserved
    assert {name: after[name] for name in preserved} == preserved
    bars = catalog.bars(bar_types=["ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL"])
    assert str(bars[0].close) == "4000.00"
    assert len(catalog.trade_ticks(instrument_ids=[str(instrument.id)])) == 2


def test_invalid_forced_conversion_preserves_existing_data(incremental_dataset):
    synced, instrument, catalog, kwargs, rows = incremental_dataset
    before = parquet_snapshot(Path(catalog.path))
    (state_path,) = kwargs["state_dir"].rglob("*.json")
    state_before = state_path.read_bytes()
    write_trade_archive(kwargs["raw_dir"], [trade_row(1, int(rows[0][4]), buyer_maker="bad")])
    with pytest.raises(DataValidationError):
        convert_trade_archive(
            synced, instrument, catalog, bar_intervals=("1m",), force=True, **kwargs
        )
    assert parquet_snapshot(Path(catalog.path)) == before
    assert state_path.read_bytes() == state_before


def test_unmanaged_existing_files_are_preserved(incremental_dataset):
    from engine.pipeline.convert_binance_bars import ConversionError

    synced, instrument, catalog, kwargs, _ = incremental_dataset
    before = parquet_snapshot(Path(catalog.path))
    (state_path,) = kwargs["state_dir"].rglob("*.json")
    state_path.unlink()
    with pytest.raises(ConversionError, match="--force"):
        convert_trade_archive(synced, instrument, catalog, bar_intervals=("1m",), **kwargs)
    assert parquet_snapshot(Path(catalog.path)) == before


def test_missing_ticks_do_not_rebuild_existing_bars(incremental_dataset):
    synced, instrument, catalog, kwargs, _ = incremental_dataset
    root = Path(catalog.path)
    for path in (root / "data" / "trades").rglob("*.parquet"):
        path.unlink()
    before = parquet_snapshot(root)
    convert_trade_archive(synced, instrument, catalog, bar_intervals=("1m",), **kwargs)
    after = parquet_snapshot(root)
    assert {name: after[name] for name in before} == before
    assert len(catalog.trade_ticks(instrument_ids=[str(instrument.id)])) == 5


def test_prepare_only_materializes_requested_period(incremental_dataset, monkeypatch):
    from dataclasses import replace

    from engine.pipeline import prepare_data as preparation
    from engine.pipeline.config import load_settings
    from engine.pipeline.models import DataRequest

    synced, instrument, catalog, kwargs, _ = incremental_dataset
    settings = replace(
        load_settings(),
        raw_dir=kwargs["raw_dir"],
        state_dir=kwargs["state_dir"],
        catalog_dir=Path(catalog.path),
    )
    monkeypatch.setattr(preparation, "sync_archives", lambda *a, **kw: [synced])
    monkeypatch.setattr(preparation, "ensure_instruments", lambda *a: {"ETHUSDT": instrument})
    before = parquet_snapshot(Path(catalog.path))
    dataset = preparation.prepare_data(
        DataRequest(
            symbols=("ETHUSDT",),
            start=datetime(2026, 8, 1, tzinfo=UTC),
            end=datetime(2026, 8, 2, tzinfo=UTC),
            bar_intervals=("1m",),
        ),
        settings=settings,
    )
    assert dataset.ready
    after = parquet_snapshot(Path(catalog.path))
    assert {name: after[name] for name in before} == before
    added = set(after) - set(before)
    assert len(added) == 1
    assert all("custom/BarFeatures" in name and "1-MINUTE" in name for name in added)
    assert dataset.feature_bar_types == dataset.bar_types


def test_missing_finer_period_only_adds_bars(incremental_dataset):
    synced, instrument, catalog, kwargs, _ = incremental_dataset
    root = Path(catalog.path)
    before = parquet_snapshot(root)
    convert_trade_archive(synced, instrument, catalog, bar_intervals=("1s",), **kwargs)
    after = parquet_snapshot(root)
    assert {name: after[name] for name in before} == before
    assert len(after) == len(before) + 1
    assert len(catalog.bars(bar_types=["ETHUSDT-PERP.BINANCE-1-SECOND-LAST-EXTERNAL"])) == 5


def test_partial_force_does_not_aggregate_from_old_source(incremental_dataset):
    synced, instrument, catalog, kwargs, rows = incremental_dataset
    # 只强制更新 ticks 和 1s；保留原有 1m/15m，随后生成 5m 必须用新来源的 1s。
    write_trade_archive(kwargs["raw_dir"], [trade_row(1, int(rows[0][4]), price="4000.00")])
    convert_trade_archive(synced, instrument, catalog, bar_intervals=("1s",), force=True, **kwargs)
    convert_trade_archive(synced, instrument, catalog, bar_intervals=("5m",), **kwargs)
    bars = catalog.bars(bar_types=["ETHUSDT-PERP.BINANCE-5-MINUTE-LAST-EXTERNAL"])
    assert str(bars[0].close) == "4000.00"
    old_bars = catalog.bars(bar_types=["ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL"])
    assert str(old_bars[0].close) == "3000.00"


def test_force_can_replace_data_with_malformed_state(incremental_dataset):
    synced, instrument, catalog, kwargs, _ = incremental_dataset
    (state_path,) = kwargs["state_dir"].rglob("*.json")
    state_path.write_text(
        json.dumps(
            {
                "status": "ready",
                "instrument_id": str(instrument.id),
                "intervals": [["invalid", "invalid"]],
                "bars": None,
                "bar_intervals": None,
            }
        )
    )
    convert_trade_archive(synced, instrument, catalog, bar_intervals=("1m",), force=True, **kwargs)
    assert len(catalog.trade_ticks(instrument_ids=[str(instrument.id)])) == 5
    assert json.loads(state_path.read_text())["status"] == "ready"
