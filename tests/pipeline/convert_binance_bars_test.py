import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd
import pytest

from engine.pipeline.catalog import ParquetDataCatalog
from engine.pipeline.convert_binance_bars import convert_archive, load_kline_archive
from engine.pipeline.models import ArchiveSpec, SyncedArchive
from engine.pipeline.validate_data import DataValidationError, validate_catalog
from nautilus_support import TestInstrumentProvider


def kline_row(open_time: int, *, close: str = "3001.0") -> list[str]:
    return [
        str(open_time),
        "3000.0",
        "3010.0",
        "2990.0",
        close,
        "12.5",
        str(open_time + 15 * 60 * 1000 - 1),
        "37500.0",
        "42",
        "7.0",
        "21000.0",
        "0",
    ]


def write_test_archive(raw_dir: Path, rows: list[list[str]]) -> tuple[ArchiveSpec, Path]:
    day = "2026-08-01"
    relative = Path(
        f"futures/um/daily/klines/ETHUSDT/15m/ETHUSDT-15m-{day}.zip",
    )
    archive_path = raw_dir / relative
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    csv_name = f"ETHUSDT-15m-{day}.csv"
    csv_content = "\n".join(",".join(row) for row in rows).encode()
    with ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(csv_name, csv_content)

    checksum = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    Path(f"{archive_path}.CHECKSUM").write_text(
        f"{checksum}  {archive_path.name}\n",
        encoding="utf-8",
    )
    spec = ArchiveSpec(
        symbol="ETHUSDT",
        data_type="klines",
        interval="15m",
        day=datetime(2026, 8, 1, tzinfo=UTC).date(),
        source_path=f"data/{relative.as_posix()}",
    )
    return spec, archive_path


def test_archive_conversion_writes_catalog_and_is_idempotent(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    catalog_dir = tmp_path / "catalog"
    state_dir = tmp_path / "pipeline_state"
    catalog_dir.mkdir()
    start_ms = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    rows = [kline_row(start_ms), kline_row(start_ms + 15 * 60 * 1000, close="3002.0")]
    spec, path = write_test_archive(raw_dir, rows)
    synced = SyncedArchive(spec=spec, path=path)
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    catalog = ParquetDataCatalog(str(catalog_dir))

    first_count = convert_archive(
        synced,
        instrument,
        catalog,
        raw_dir=raw_dir,
        state_dir=state_dir,
    )
    second_count = convert_archive(
        synced,
        instrument,
        catalog,
        raw_dir=raw_dir,
        state_dir=state_dir,
    )

    bars = catalog.bars(
        bar_types=["ETHUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL"],
    )
    assert first_count == second_count == 2
    assert len(bars) == 2
    assert bars[0].ts_event == (start_ms + 15 * 60 * 1000 - 1) * 1_000_000
    assert list((state_dir / "converted").rglob("*.json"))

    validation = validate_catalog(
        catalog,
        "ETHUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL",
        datetime(2026, 8, 1, tzinfo=UTC),
        datetime(2026, 8, 1, 0, 30, tzinfo=UTC),
        "15m",
    )
    assert validation.ready
    assert validation.missing_event_ranges == ()


def test_archive_validation_failure_writes_manifest_and_clears_after_success(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw"
    catalog_dir = tmp_path / "catalog"
    state_dir = tmp_path / "pipeline_state"
    catalog_dir.mkdir()
    start_ms = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    spec, path = write_test_archive(raw_dir, [kline_row(start_ms, close="4000.0")])
    synced = SyncedArchive(spec=spec, path=path)
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    catalog = ParquetDataCatalog(str(catalog_dir))

    with pytest.raises(DataValidationError, match="OHLC"):
        convert_archive(
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
    assert "OHLC" in failure["error"]
    assert failure["source"] == spec.source_path
    assert failure["source_checksum"]
    assert failure["failed_at"]
    assert list(tmp_path.rglob("*.zip")) == [path]

    valid_spec, valid_path = write_test_archive(raw_dir, [kline_row(start_ms)])
    convert_archive(
        SyncedArchive(spec=valid_spec, path=valid_path),
        instrument,
        catalog,
        raw_dir=raw_dir,
        state_dir=state_dir,
    )

    assert not failures[0].exists()


def test_archive_validation_rejects_internal_gap(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    start_ms = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    rows = [kline_row(start_ms), kline_row(start_ms + 30 * 60 * 1000)]
    _, path = write_test_archive(raw_dir, rows)

    with pytest.raises(DataValidationError, match="时间缺口"):
        load_kline_archive(path, "15m")


def test_archive_validation_rejects_invalid_ohlc(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    start_ms = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    rows = [kline_row(start_ms, close="4000.0")]
    _, path = write_test_archive(raw_dir, rows)

    with pytest.raises(DataValidationError, match="OHLC"):
        load_kline_archive(path, "15m")


def test_csv_header_is_tolerated(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    start_ms = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    rows = [
        list(
            pd.Index(
                [
                    "open_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_volume",
                    "trade_count",
                    "taker_buy_base_volume",
                    "taker_buy_quote_volume",
                    "ignore",
                ]
            )
        ),
        kline_row(start_ms),
    ]
    _, path = write_test_archive(raw_dir, rows)

    frame = load_kline_archive(path, "15m")

    assert len(frame) == 1


def test_legacy_converter_requires_force_to_replace_existing_data(tmp_path):
    raw, state = tmp_path / "raw", tmp_path / "state"
    timestamp = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    spec, path = write_test_archive(raw, [kline_row(timestamp)])
    synced = SyncedArchive(spec, path)
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    catalog = ParquetDataCatalog(str(tmp_path / "catalog"))
    kwargs = {"raw_dir": raw, "state_dir": state}
    convert_archive(synced, instrument, catalog, **kwargs)
    (parquet,) = Path(catalog.path).rglob("*.parquet")
    before = (parquet.stat().st_mtime_ns, parquet.read_bytes())
    write_test_archive(raw, [kline_row(timestamp, close="3005.0")])
    convert_archive(synced, instrument, catalog, **kwargs)
    assert (parquet.stat().st_mtime_ns, parquet.read_bytes()) == before
    convert_archive(synced, instrument, catalog, force=True, **kwargs)
    assert (
        str(catalog.bars(bar_types=["ETHUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL"])[0].close)
        == "3005.00"
    )
