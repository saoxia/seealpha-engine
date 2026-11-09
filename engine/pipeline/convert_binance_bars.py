"""将 Binance USD-M K 线 ZIP 转为 Nautilus Parquet Catalog。"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zipfile import BadZipFile, ZipFile

import pandas as pd
from nautilus_trader.model import Bar, BarType

from engine.pipeline.catalog import BarDataWrangler, ParquetDataCatalog
from engine.pipeline.models import SyncedArchive, bar_type_for
from engine.pipeline.sync_binance_file import SyncError, parse_checksum, sha256_file
from engine.pipeline.validate_data import KLINE_COLUMNS, DataValidationError, validate_kline_frame
from nautilus_support import Instrument

CONVERTER_VERSION = 1


class ConversionError(RuntimeError):
    """表示归档无法安全转换或写入 Catalog。"""


def read_archive_checksum(path: Path) -> str:
    """读取官方 CHECKSUM，并再次确认归档内容。"""
    checksum_path = Path(f"{path}.CHECKSUM")
    try:
        expected = parse_checksum(checksum_path.read_bytes(), path.name)
    except (OSError, SyncError) as exc:
        raise ConversionError(f"无法读取校验文件：{checksum_path}") from exc
    actual = sha256_file(path)
    if actual != expected:
        raise ConversionError(f"归档 SHA-256 不匹配：{path}")
    return expected


def load_kline_archive(path: Path, interval: str) -> pd.DataFrame:
    """直接从 ZIP 读取唯一 CSV，不向磁盘解压。"""
    try:
        with ZipFile(path) as archive:
            csv_members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if len(csv_members) != 1:
                raise DataValidationError(
                    f"归档应包含且只包含一个 CSV，实际为 {len(csv_members)} 个",
                )
            with archive.open(csv_members[0]) as file:
                frame = pd.read_csv(file, header=None, names=KLINE_COLUMNS, dtype=str)
    except (BadZipFile, OSError) as exc:
        raise DataValidationError(f"无法读取 ZIP 归档：{path}") from exc

    if not frame.empty and str(frame.iloc[0]["open_time"]).strip().lower() == "open_time":
        frame = frame.iloc[1:].reset_index(drop=True)
    return validate_kline_frame(frame, interval)


def frame_to_bars(
    frame: pd.DataFrame,
    instrument: Instrument,
    interval: str,
    bar_type: str,
) -> list[Bar]:
    """把已校验 Binance K 线转换为 Nautilus Bar。"""
    parsed_bar_type = BarType.from_str(bar_type)
    wrangler_frame = frame[["open", "high", "low", "close", "volume"]].astype("float64")
    wrangler_frame.index = pd.to_datetime(
        frame["close_time"].astype("int64"),
        unit="ms",
        utc=True,
    )
    wrangler_frame.index.name = "timestamp"
    return BarDataWrangler(bar_type=parsed_bar_type, instrument=instrument).process(wrangler_frame)


def record_path_for(archive: Path, raw_dir: Path, state_dir: Path) -> Path:
    """返回镜像原始目录结构的转换记录路径。"""
    try:
        relative = archive.resolve().relative_to(raw_dir.resolve())
    except ValueError as exc:
        raise ConversionError(f"归档不在配置的 raw 目录中：{archive}") from exc
    return state_dir / "converted" / relative.parent / f"{relative.name}.json"


def failure_path_for(archive: Path, raw_dir: Path, state_dir: Path) -> Path:
    """返回镜像原始目录结构的失败记录路径。"""
    try:
        relative = archive.resolve().relative_to(raw_dir.resolve())
    except ValueError as exc:
        raise ConversionError(f"归档不在配置的 raw 目录中：{archive}") from exc
    return state_dir / "failures" / relative.parent / f"{relative.name}.json"


def read_record(path: Path) -> dict | None:
    """读取转换记录；损坏记录视为不存在。"""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    return value if isinstance(value, dict) else None


def write_record(path: Path, record: dict) -> None:
    """原子保存转换记录。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8") as file:
        json.dump(record, file, ensure_ascii=False, indent=2, sort_keys=True)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())
    os.replace(temporary, path)


def interval_is_cataloged(
    catalog: ParquetDataCatalog,
    bar_type: str,
    first_ts: int,
    last_ts: int,
) -> bool:
    """判断一个转换记录的完整区间是否仍存在于 Catalog。"""
    return any(
        start <= first_ts and end >= last_ts
        for start, end in catalog.get_intervals(Bar, identifier=bar_type)
    )


def record_validation_failure(
    synced: SyncedArchive,
    error: DataValidationError,
    *,
    raw_dir: Path,
    state_dir: Path,
    source_checksum: str,
    converter_version: int,
) -> Path:
    """保存轻量失败清单，不复制原始归档。"""
    path = failure_path_for(synced.path, raw_dir, state_dir)
    write_record(
        path,
        {
            "converter_version": converter_version,
            "error": str(error),
            "error_type": type(error).__name__,
            "failed_at": datetime.now(UTC).isoformat(),
            "source": synced.spec.source_path,
            "source_checksum": source_checksum,
            "status": "failed",
        },
    )
    return path


def clear_failure_record(archive: Path, raw_dir: Path, state_dir: Path) -> None:
    """转换成功后删除同一归档留下的过期失败记录。"""
    failure_path_for(archive, raw_dir, state_dir).unlink(missing_ok=True)


def convert_archive(
    synced: SyncedArchive,
    instrument: Instrument,
    catalog: ParquetDataCatalog,
    *,
    raw_dir: Path,
    state_dir: Path,
    force: bool = False,
) -> int:
    """幂等转换一个日归档，返回写入或确认存在的 Bar 数量。"""
    if synced.spec.interval is None:
        raise ConversionError("K 线归档缺少 interval")
    record_path = record_path_for(synced.path, raw_dir, state_dir)
    record = read_record(record_path)
    expected_bar_type = bar_type_for(synced.spec.symbol, synced.spec.interval)

    if (
        not force
        and record is not None
        and record.get("bar_type") == expected_bar_type
        and isinstance(record.get("first_ts"), int)
        and isinstance(record.get("last_ts"), int)
        and interval_is_cataloged(
            catalog,
            expected_bar_type,
            record["first_ts"],
            record["last_ts"],
        )
    ):
        clear_failure_record(synced.path, raw_dir, state_dir)
        print(f"转换结果已存在，跳过：{synced.path}")
        return int(record.get("rows", 0))

    source_checksum = read_archive_checksum(synced.path)
    try:
        frame = load_kline_archive(synced.path, synced.spec.interval)
        bars = frame_to_bars(frame, instrument, synced.spec.interval, expected_bar_type)
    except DataValidationError as exc:
        failure_path = record_validation_failure(
            synced,
            exc,
            raw_dir=raw_dir,
            state_dir=state_dir,
            source_checksum=source_checksum,
            converter_version=CONVERTER_VERSION,
        )
        print(f"归档校验失败，已写入失败记录：{failure_path}")
        raise

    first_ts = bars[0].ts_init
    last_ts = bars[-1].ts_init
    day_start = datetime.combine(synced.spec.day, datetime.min.time(), tzinfo=UTC)
    day_start_ns = int(day_start.timestamp() * 1_000_000_000)
    day_end_ns = int((day_start + timedelta(days=1)).timestamp() * 1_000_000_000) - 1
    if force:
        catalog.delete_data_range(
            Bar,
            identifier=expected_bar_type,
            start=day_start_ns,
            end=day_end_ns,
        )
    elif any(
        start <= day_end_ns and end >= day_start_ns
        for start, end in catalog.get_intervals(Bar, identifier=expected_bar_type)
    ):
        raise ConversionError(
            f"Catalog 已有未被本管道状态记录管理的重叠区间：{expected_bar_type}；"
            "已有数据已保留，如需重建请显式使用 --force",
        )

    catalog.write_data(bars)
    write_record(
        record_path,
        {
            "bar_type": expected_bar_type,
            "converter_version": CONVERTER_VERSION,
            "first_ts": first_ts,
            "last_ts": last_ts,
            "rows": len(bars),
            "source": synced.spec.source_path,
            "source_checksum": source_checksum,
        },
    )
    clear_failure_record(synced.path, raw_dir, state_dir)
    print(f"已写入 {len(bars)} 根 Bar：{expected_bar_type}")
    return len(bars)
