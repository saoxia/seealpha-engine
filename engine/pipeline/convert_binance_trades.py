"""将 Binance USD-M trades 日归档转换为 Nautilus TradeTick 和多周期 Bar。"""

from __future__ import annotations

import csv
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import BadZipFile, ZipFile

import pandas as pd
from nautilus_trader.model import Bar, BarType, CustomData, DataType, TradeTick

from engine.pipeline.catalog import (
    BarDataWrangler,
    ParquetDataCatalog,
    TradeQuote,
    TradeTickDataWrangler,
    _timestamps_to_filename,
    urisafe_identifier,
)
from engine.pipeline.convert_binance_bars import (
    ConversionError,
    clear_failure_record,
    read_archive_checksum,
    read_record,
    record_path_for,
    record_validation_failure,
    write_record,
)
from engine.pipeline.models import SyncedArchive, bar_type_for, interval_delta
from engine.pipeline.rust_trade_converter import (
    RustConversionError,
    find_rust_converter,
    run_rust_trade_converter,
)
from engine.pipeline.validate_data import (
    TRADE_COLUMNS,
    DataValidationError,
    normalize_trade_frame,
    validate_normalized_trade_frame,
)
from nautilus_support import Instrument

TRADE_CONVERTER_VERSION = 4
CSV_CHUNK_ROWS = 250_000
DEFAULT_BAR_INTERVALS = ("1s", "1m")


def iter_trade_archive(path: Path, chunk_rows: int = CSV_CHUNK_ROWS) -> Iterator[pd.DataFrame]:
    """逐块读取 trades CSV；重复 Trade ID 保留源文件中靠后的记录。"""
    try:
        with ZipFile(path) as archive:
            csv_members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if len(csv_members) != 1:
                raise DataValidationError(
                    f"归档应包含且只包含一个 CSV，实际为 {len(csv_members)} 个",
                )
            with archive.open(csv_members[0]) as file:
                # 先识别表头，避免首块的几十万行因混入表头而退回字符串解析。
                header_reader = csv.reader(line.decode("utf-8-sig") for line in file)
                first_row = next(header_reader, [])
                while not first_row or (len(first_row) == 1 and not first_row[0].strip()):
                    first_row = next(header_reader, None)
                    if first_row is None:
                        break
                has_header = bool(first_row) and first_row[0].strip().lower() in {
                    "id",
                    "trade id",
                    "trade_id",
                }
                skiprows = header_reader.line_num if has_header else 0
                file.seek(0)
                chunks = pd.read_csv(
                    file,
                    header=None,
                    names=TRADE_COLUMNS,
                    # 让 CSV 解析器直接生成数值列，避免先创建逐行字符串再解析。
                    # 混合内容的块仍由下方规范化函数严格校验。
                    # pandas 3.1 起各 float_precision 模式共用同一转换器，再传该参数只会告警。
                    low_memory=False,
                    skiprows=skiprows,
                    chunksize=chunk_rows,
                )
                pending = None
                for chunk in chunks:
                    if chunk.empty:
                        continue
                    combined = normalize_trade_frame(chunk)
                    if pending is not None:
                        combined = pd.concat([pending, combined], ignore_index=True)
                    combined = validate_normalized_trade_frame(combined)
                    ready = combined.iloc[:-1]
                    pending = combined.iloc[-1:].copy()
                    if not ready.empty:
                        yield ready
                if pending is not None:
                    yield pending
    except (BadZipFile, OSError) as exc:
        raise DataValidationError(f"无法读取 Trade ZIP 归档：{path}") from exc


def catalog_contains_record(
    catalog: ParquetDataCatalog,
    instrument_id: str,
    record: dict,
    bar_intervals: tuple[str, ...],
) -> bool:
    """确认状态记录声明的 TradeTick 和聚合 Bar 文件仍存在。"""
    intervals = catalog.get_intervals(TradeTick, identifier=instrument_id)
    recorded = record.get("intervals")
    if not isinstance(recorded, list) or not recorded:
        return False
    ticks_exist = all(
        isinstance(item, list)
        and len(item) == 2
        and all(isinstance(value, int) for value in item)
        and any(start <= item[0] and end >= item[1] for start, end in intervals)
        for item in recorded
    )
    if not ticks_exist:
        return False

    return catalog_contains_bars(catalog, record, bar_intervals)


def catalog_contains_bars(
    catalog: ParquetDataCatalog,
    record: dict,
    bar_intervals: tuple[str, ...],
) -> bool:
    """独立检查 Bar，缺失 TradeTick 不影响已有 Bar 的复用。"""
    bars = record.get("bars")
    if not isinstance(bars, dict):
        return False
    for interval in bar_intervals:
        item = bars.get(interval)
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("first_ts"), int)
            or not isinstance(item.get("last_ts"), int)
            or not any(
                start <= item["first_ts"] and end >= item["last_ts"]
                for start, end in catalog.get_intervals(
                    Bar,
                    identifier=item.get("bar_type", ""),
                )
            )
        ):
            return False
    return True


def aggregate_trade_chunk(frame: pd.DataFrame, interval: str) -> pd.DataFrame:
    """将一个已校验 Trade 块压缩为中间 OHLCV；跨块桶稍后再次合并。"""
    interval_ms = int(interval_delta(interval).total_seconds() * 1_000)
    values = frame[["timestamp", "price", "quantity"]].copy()
    values["bucket"] = (values["timestamp"] // interval_ms) * interval_ms
    return values.groupby("bucket", sort=False).agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("quantity", "sum"),
    )


def aggregate_parts_to_bars(
    parts: list[pd.DataFrame],
    instrument: Instrument,
    interval: str,
    bar_type: str,
) -> list[Bar]:
    """合并跨 CSV 块的时间桶并生成 Nautilus 外部 Bar。"""
    combined = pd.concat(parts)
    frame = combined.groupby(level=0, sort=True).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    )
    interval_ms = int(interval_delta(interval).total_seconds() * 1_000)
    close_times = frame.index.to_numpy(dtype="int64") + interval_ms - 1
    wrangler_frame = pd.DataFrame(index=pd.to_datetime(close_times, unit="ms", utc=True))
    for column in ("open", "high", "low", "close", "volume"):
        # 独立的可写数组兼容 BarDataWrangler 的 Cython memoryview。
        wrangler_frame[column] = frame[column].to_numpy(dtype="float64", copy=True)
    wrangler_frame.index.name = "timestamp"
    parsed_bar_type = BarType.from_str(bar_type)
    return BarDataWrangler(bar_type=parsed_bar_type, instrument=instrument).process(
        wrangler_frame,
    )


def emit_trade_chunk(
    frame: pd.DataFrame,
    instrument: Instrument,
    catalog: ParquetDataCatalog,
) -> tuple[int, int, int]:
    """转换并写入一个不会与下一块共享毫秒时间戳的数据块。"""
    timestamp_index = pd.to_datetime(frame["timestamp"].to_numpy(), unit="ms", utc=True)
    wrangler_frame = pd.DataFrame(
        {
            "price": frame["price"].to_numpy(dtype="float64"),
            "quantity": frame["quantity"].to_numpy(dtype="float64"),
            "side": frame["buyer_maker"].map({True: "SELL", False: "BUY"}).to_numpy(),
            "trade_id": frame["trade_id"].astype(str).to_numpy(),
        },
        index=timestamp_index,
    )
    ticks = TradeTickDataWrangler(instrument).process(wrangler_frame)
    catalog.write_data(ticks)
    quotes = [CustomData(DataType("TradeQuote"), TradeQuote(
        trade_id=str(row.trade_id), quote_quantity=float(row.quote_quantity),
        ts_event=int(row.timestamp)*1_000_000, ts_init=int(row.timestamp)*1_000_000,
    )) for row in frame.itertuples()]
    catalog.supplement_catalog(str(instrument.id)).write_custom_data(quotes)
    return len(ticks), ticks[0].ts_init, ticks[-1].ts_init


def day_bounds_ns(synced: SyncedArchive) -> tuple[int, int]:
    """返回归档 UTC 日的纳秒左闭右开边界。"""
    start = datetime.combine(synced.spec.day, datetime.min.time(), tzinfo=UTC)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1_000_000_000), int(end.timestamp() * 1_000_000_000)


def convert_trade_archive_with_rust(
    synced: SyncedArchive,
    instrument: Instrument,
    catalog: ParquetDataCatalog,
    *,
    binary: Path,
    state_path: Path,
    source_checksum: str,
    raw_dir: Path,
    state_dir: Path,
    day_start_ns: int,
    day_end_ns: int,
    chunk_rows: int,
    bar_intervals: tuple[str, ...],
) -> int:
    """在 staging 中运行 Rust 核心，再将完整文件原子安装进本地 Catalog。"""
    if catalog.fs_protocol != "file":
        raise RustConversionError("Rust 转换器目前只支持本地文件 Catalog")
    instrument_id = str(instrument.id)
    requested_bars = {
        interval: bar_type_for(synced.spec.symbol, interval) for interval in bar_intervals
    }
    for bar_type in requested_bars.values():
        if any(
            start < day_end_ns and end >= day_start_ns
            for start, end in catalog.get_intervals(Bar, identifier=bar_type)
        ):
            raise ConversionError(f"Catalog 已有未被该 trades 状态记录管理的 Bar：{bar_type}")

    catalog_root = Path(catalog.path).resolve()
    catalog_root.mkdir(parents=True, exist_ok=True)
    try:
        with TemporaryDirectory(prefix=".rust-trades-", dir=catalog_root) as temporary:
            manifest = run_rust_trade_converter(
                binary,
                synced.path,
                Path(temporary),
                instrument_id=instrument_id,
                price_precision=int(instrument.price_precision),
                size_precision=int(instrument.size_precision),
                day_start_ns=day_start_ns,
                day_end_ns=day_end_ns,
                bar_types=requested_bars,
                batch_rows=chunk_rows,
            )
            intervals = [[manifest.trade.first_ts, manifest.trade.last_ts]]
            bar_records = {
                bar.interval: {
                    "bar_type": bar.bar_type,
                    "first_ts": bar.first_ts,
                    "last_ts": bar.last_ts,
                    "rows": bar.rows,
                }
                for bar in manifest.bars
            }
            record = {
                "backend": "rust-arrow",
                "bar_intervals": list(bar_intervals),
                "bars": bar_records,
                "converter_version": TRADE_CONVERTER_VERSION,
                "first_ts": manifest.trade.first_ts,
                "instrument_id": instrument_id,
                "intervals": intervals,
                "last_ts": manifest.trade.last_ts,
                "rows": manifest.rows,
                "source": synced.spec.source_path,
                "source_checksum": source_checksum,
                "status": "in_progress",
            }
            write_record(state_path, record)

            trade_directory = (
                catalog_root / "data" / "trades" / urisafe_identifier(instrument_id)
            )
            trade_directory.mkdir(parents=True, exist_ok=True)
            trade_destination = trade_directory / _timestamps_to_filename(
                manifest.trade.first_ts,
                manifest.trade.last_ts,
            )
            if trade_destination.exists():
                raise ConversionError(f"Rust TradeTick 目标文件已存在：{trade_destination}")
            os.replace(manifest.trade.path, trade_destination)
            if manifest.quote is None:
                raise RustConversionError("Rust converter lacks quote supplements; rebuild it")
            quote_root = (catalog_root / "supplements" / urisafe_identifier(instrument_id))
            quote_dir = quote_root / "data/custom/TradeQuote"
            quote_dir.mkdir(parents=True, exist_ok=True)
            os.replace(manifest.quote.path, quote_dir / _timestamps_to_filename(
                manifest.quote.first_ts, manifest.quote.last_ts))

            for bar in manifest.bars:
                bar_directory = catalog_root / "data" / "bars" / urisafe_identifier(bar.bar_type)
                bar_directory.mkdir(parents=True, exist_ok=True)
                destination = bar_directory / _timestamps_to_filename(bar.first_ts, bar.last_ts)
                if destination.exists():
                    raise ConversionError(f"Rust Bar 目标文件已存在：{destination}")
                os.replace(bar.path, destination)
    except DataValidationError as exc:
        failure_path = record_validation_failure(
            synced,
            exc,
            raw_dir=raw_dir,
            state_dir=state_dir,
            source_checksum=source_checksum,
            converter_version=TRADE_CONVERTER_VERSION,
        )
        print(f"Trade 归档校验失败，已写入失败记录：{failure_path}")
        raise

    record["status"] = "ready"
    write_record(state_path, record)
    clear_failure_record(synced.path, raw_dir, state_dir)
    print(f"Rust 已写入 {manifest.rows} 条 TradeTick：{instrument_id}")
    for interval, item in bar_records.items():
        print(f"Rust 已写入 {item['rows']} 根 {interval} Bar：{item['bar_type']}")
    return manifest.rows


def _convert_fresh_trade_archive(
    synced: SyncedArchive,
    instrument: Instrument,
    catalog: ParquetDataCatalog,
    *,
    raw_dir: Path,
    state_dir: Path,
    chunk_rows: int = CSV_CHUNK_ROWS,
    bar_intervals: tuple[str, ...] = DEFAULT_BAR_INTERVALS,
) -> int:
    """分块且幂等地将一个 trades 日归档转换为 TradeTick 和多周期 Bar。"""
    if synced.spec.data_type != "trades":
        raise ConversionError(f"不是 trades 归档：{synced.spec.source_path}")
    checksum = read_archive_checksum(synced.path)
    state_path = record_path_for(synced.path, raw_dir, state_dir)
    instrument_id = str(instrument.id)
    day_start_ns, day_end_ns = day_bounds_ns(synced)

    rust_binary = find_rust_converter()
    if rust_binary is not None and catalog.fs_protocol == "file":
        return convert_trade_archive_with_rust(
            synced,
            instrument,
            catalog,
            binary=rust_binary,
            state_path=state_path,
            source_checksum=checksum,
            raw_dir=raw_dir,
            state_dir=state_dir,
            day_start_ns=day_start_ns,
            day_end_ns=day_end_ns,
            chunk_rows=chunk_rows,
            bar_intervals=bar_intervals,
        )

    pending = pd.DataFrame(columns=TRADE_COLUMNS)
    intervals: list[list[int]] = []
    rows = 0
    previous_time: int | None = None
    previous_trade_id: int | None = None
    aggregate_parts: dict[str, list[pd.DataFrame]] = {interval: [] for interval in bar_intervals}

    try:
        for chunk in iter_trade_archive(synced.path, chunk_rows):
            for interval in bar_intervals:
                aggregate_parts[interval].append(aggregate_trade_chunk(chunk, interval))
            first_time = int(chunk.iloc[0]["timestamp"])
            last_time_in_chunk = int(chunk.iloc[-1]["timestamp"])
            first_trade_id = int(chunk.iloc[0]["trade_id"])
            if (
                first_time * 1_000_000 < day_start_ns
                or last_time_in_chunk * 1_000_000 >= day_end_ns
            ):
                raise DataValidationError("Trade timestamp 超出归档标注的 UTC 日期")
            if previous_time is not None and first_time < previous_time:
                raise DataValidationError("Trade timestamp 跨数据块倒序")
            if previous_trade_id is not None and first_trade_id <= previous_trade_id:
                raise DataValidationError("Trade ID 跨数据块重复或倒序")
            previous_time = last_time_in_chunk
            previous_trade_id = int(chunk.iloc[-1]["trade_id"])

            combined = pd.concat([pending, chunk], ignore_index=True)
            last_time = int(combined.iloc[-1]["timestamp"])
            ready = combined[combined["timestamp"] < last_time]
            pending = combined[combined["timestamp"] == last_time].copy()
            if ready.empty:
                continue
            count, first_ts, last_ts = emit_trade_chunk(ready, instrument, catalog)
            rows += count
            intervals.append([first_ts, last_ts])
            write_record(
                state_path,
                {
                    "converter_version": TRADE_CONVERTER_VERSION,
                    "bar_intervals": list(bar_intervals),
                    "bars": {},
                    "first_ts": intervals[0][0],
                    "instrument_id": instrument_id,
                    "intervals": intervals,
                    "last_ts": intervals[-1][1],
                    "rows": rows,
                    "source": synced.spec.source_path,
                    "source_checksum": checksum,
                    "status": "in_progress",
                },
            )

        if pending.empty:
            raise DataValidationError("Trade 归档没有数据")
        count, first_ts, last_ts = emit_trade_chunk(pending, instrument, catalog)
        rows += count
        intervals.append([first_ts, last_ts])
        write_record(
            state_path,
            {
                "bar_intervals": list(bar_intervals),
                "bars": {},
                "converter_version": TRADE_CONVERTER_VERSION,
                "first_ts": intervals[0][0],
                "instrument_id": instrument_id,
                "intervals": intervals,
                "last_ts": intervals[-1][1],
                "rows": rows,
                "source": synced.spec.source_path,
                "source_checksum": checksum,
                "status": "in_progress",
            },
        )

        bar_records: dict[str, dict[str, int | str]] = {}
        for interval in bar_intervals:
            expected_bar_type = bar_type_for(synced.spec.symbol, interval)
            bars = aggregate_parts_to_bars(
                aggregate_parts[interval],
                instrument,
                interval,
                expected_bar_type,
            )
            if any(
                start < day_end_ns and end >= day_start_ns
                for start, end in catalog.get_intervals(Bar, identifier=expected_bar_type)
            ):
                raise ConversionError(
                    f"Catalog 已有未被该 trades 状态记录管理的 Bar：{expected_bar_type}",
                )
            catalog.write_data(bars)
            bar_records[interval] = {
                "bar_type": expected_bar_type,
                "first_ts": bars[0].ts_init,
                "last_ts": bars[-1].ts_init,
                "rows": len(bars),
            }
            write_record(
                state_path,
                {
                    "bar_intervals": list(bar_intervals),
                    "bars": bar_records,
                    "converter_version": TRADE_CONVERTER_VERSION,
                    "first_ts": intervals[0][0],
                    "instrument_id": instrument_id,
                    "intervals": intervals,
                    "last_ts": intervals[-1][1],
                    "rows": rows,
                    "source": synced.spec.source_path,
                    "source_checksum": checksum,
                    "status": "in_progress",
                },
            )
    except DataValidationError as exc:
        failure_path = record_validation_failure(
            synced,
            exc,
            raw_dir=raw_dir,
            state_dir=state_dir,
            source_checksum=checksum,
            converter_version=TRADE_CONVERTER_VERSION,
        )
        print(f"Trade 归档校验失败，已写入失败记录：{failure_path}")
        raise

    write_record(
        state_path,
        {
            "bar_intervals": list(bar_intervals),
            "bars": bar_records,
            "converter_version": TRADE_CONVERTER_VERSION,
            "first_ts": intervals[0][0],
            "instrument_id": instrument_id,
            "intervals": intervals,
            "last_ts": intervals[-1][1],
            "rows": rows,
            "source": synced.spec.source_path,
            "source_checksum": checksum,
            "status": "ready",
        },
    )
    clear_failure_record(synced.path, raw_dir, state_dir)
    print(f"已写入 {rows} 条 TradeTick：{instrument_id}")
    for interval, item in bar_records.items():
        print(f"已写入 {item['rows']} 根 {interval} Bar：{item['bar_type']}")
    return rows


def stage_missing_bars(
    synced: SyncedArchive,
    instrument: Instrument,
    catalog: ParquetDataCatalog,
    staged_catalog: ParquetDataCatalog,
    record: dict,
    intervals: tuple[str, ...],
    chunk_rows: int,
) -> dict:
    """优先从已有低周期 Bar 聚合；没有可用低周期时只解析 trades，不重写 ticks。"""
    bars_record = {}
    raw_intervals = []
    for interval in intervals:
        target_ms = int(interval_delta(interval).total_seconds() * 1000)
        candidates = [
            source
            for source in record.get("bars", {})
            if interval_delta(source) < interval_delta(interval)
            and target_ms % int(interval_delta(source).total_seconds() * 1000) == 0
            and catalog_contains_record(catalog, str(instrument.id), record, (source,))
            and record["bars"][source].get("source_checksum", record.get("source_checksum"))
            == record.get("source_checksum")
        ]
        if not candidates:
            raw_intervals.append(interval)
            continue
        source = max(candidates, key=interval_delta)
        item = record["bars"][source]
        values = catalog.bars(
            bar_types=[item["bar_type"]],
            start=item["first_ts"],
            end=item["last_ts"],
        )
        source_ms = int(interval_delta(source).total_seconds() * 1000)
        frame = pd.DataFrame(
            [
                {
                    "bucket": ((bar.ts_event // 1_000_000 + 1 - source_ms) // target_ms)
                    * target_ms,
                    "open": bar.open.as_double(),
                    "high": bar.high.as_double(),
                    "low": bar.low.as_double(),
                    "close": bar.close.as_double(),
                    "volume": bar.volume.as_double(),
                }
                for bar in values
            ]
        ).set_index("bucket")
        bars = aggregate_parts_to_bars(
            [frame],
            instrument,
            interval,
            bar_type_for(synced.spec.symbol, interval),
        )
        staged_catalog.write_data(bars)
        bars_record[interval] = {
            "bar_type": str(bars[0].bar_type),
            "first_ts": bars[0].ts_init,
            "last_ts": bars[-1].ts_init,
            "rows": len(bars),
        }
    if raw_intervals:
        parts = {interval: [] for interval in raw_intervals}
        day_start, day_end = day_bounds_ns(synced)
        previous_time = previous_id = None
        for chunk in iter_trade_archive(synced.path, chunk_rows):
            first_time, last_time = (
                int(chunk.iloc[0]["timestamp"]),
                int(chunk.iloc[-1]["timestamp"]),
            )
            first_id = int(chunk.iloc[0]["trade_id"])
            if first_time * 1_000_000 < day_start or last_time * 1_000_000 >= day_end:
                raise DataValidationError("Trade timestamp 超出归档标注的 UTC 日期")
            if previous_time is not None and first_time < previous_time:
                raise DataValidationError("Trade timestamp 跨数据块倒序")
            if previous_id is not None and first_id <= previous_id:
                raise DataValidationError("Trade ID 跨数据块重复或倒序")
            previous_time, previous_id = last_time, int(chunk.iloc[-1]["trade_id"])
            for interval in raw_intervals:
                parts[interval].append(aggregate_trade_chunk(chunk, interval))
        for interval in raw_intervals:
            if not parts[interval]:
                raise DataValidationError("Trade 归档没有数据")
            bars = aggregate_parts_to_bars(
                parts[interval],
                instrument,
                interval,
                bar_type_for(synced.spec.symbol, interval),
            )
            staged_catalog.write_data(bars)
            bars_record[interval] = {
                "bar_type": str(bars[0].bar_type),
                "first_ts": bars[0].ts_init,
                "last_ts": bars[-1].ts_init,
                "rows": len(bars),
            }
    return {**record, "bars": bars_record, "bar_intervals": list(intervals)}


def quote_supplements_equal(left, right, start, end):
    """Compare Arrow slices without materializing a full trade day in Python."""
    import pyarrow as pa
    from nautilus_trader.model import NautilusDataType
    def reader(catalog):
        capsule = catalog.query_data_arrow_stream(NautilusDataType.Custom("TradeQuote"),
                                                  start=start, end=end-1, display=False)
        return iter(pa.RecordBatchReader._import_from_c_capsule(capsule))
    streams = [reader(left), reader(right)]
    batches = [None, None]
    offsets = [0, 0]
    while True:
        for side in (0, 1):
            if batches[side] is None or offsets[side] == batches[side].num_rows:
                batches[side] = next(streams[side], None)
                offsets[side] = 0
        if any(batch is None for batch in batches):
            return all(batch is None for batch in batches)
        count = min(batch.num_rows-offset for batch,offset in zip(batches,offsets,strict=True))
        a,b = [batch.slice(offset,count).select(["trade_id","quote_quantity","ts_event"])
               for batch,offset in zip(batches,offsets,strict=True)]
        if not a.equals(b,check_metadata=False):
            return False
        offsets = [offset+count for offset in offsets]


def convert_trade_archive(
    synced: SyncedArchive,
    instrument: Instrument,
    catalog: ParquetDataCatalog,
    *,
    raw_dir: Path,
    state_dir: Path,
    chunk_rows: int = CSV_CHUNK_ROWS,
    bar_intervals: tuple[str, ...] = DEFAULT_BAR_INTERVALS,
    force: bool = False,
) -> int:
    """默认保留已有数据，仅补缺失；force 仅重建该日 TradeTick 和请求周期。"""
    if synced.spec.data_type != "trades":
        raise ConversionError(f"不是 trades 归档：{synced.spec.source_path}")
    if catalog.fs_protocol != "file":
        raise ConversionError("增量转换目前只支持本地文件 Catalog")
    bar_intervals = tuple(dict.fromkeys(bar_intervals))
    for interval in bar_intervals:
        interval_delta(interval)
    state_path = record_path_for(synced.path, raw_dir, state_dir)
    record = read_record(state_path)
    instrument_id = str(instrument.id)
    day_start, day_end = day_bounds_ns(synced)
    usable = (
        record is not None
        and record.get("status") == "ready"
        and record.get("instrument_id") == instrument_id
    )
    ticks_ready = (
        not force and usable and catalog_contains_record(catalog, instrument_id, record, ())
    )
    missing_bars = tuple(
        interval
        for interval in bar_intervals
        if force or not (usable and catalog_contains_bars(catalog, record, (interval,)))
    )
    if not force and ticks_ready and not missing_bars:
        print(f"Trade 转换结果已存在，跳过：{synced.path}")
        return int(record.get("rows", 0))

    # 任何不完整或未被记录管理的现有文件都保留，要求显式强制才能替换。
    targets = [(TradeTick, instrument_id)] if force or not ticks_ready else []
    targets.extend((Bar, bar_type_for(synced.spec.symbol, interval)) for interval in missing_bars)
    for data_cls, identifier in targets:
        if not force and any(
            start < day_end and end >= day_start
            for start, end in catalog.get_intervals(data_cls, identifier=identifier)
        ):
            raise ConversionError(
                f"已有数据不完整或缺少有效状态记录，已保留：{identifier} "
                f"{synced.spec.day}；如需重建请显式使用 --force",
            )
    checksum = read_archive_checksum(synced.path)
    if not force and usable and record.get("source_checksum") != checksum:
        raise ConversionError("源归档已变化，已有数据已保留；如需重建请显式使用 --force")

    # 先完成转换和校验，再安装选中的数据；转换失败不会删除旧 Parquet。
    catalog_root = Path(catalog.path).resolve()
    catalog_root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".prepare-trades-", dir=catalog_root) as temporary:
        staging = Path(temporary)
        staged_catalog = ParquetDataCatalog(str(staging / "catalog"))
        staged_state = staging / "state"
        try:
            if ticks_ready and not force:
                generated = stage_missing_bars(
                    synced,
                    instrument,
                    catalog,
                    staged_catalog,
                    record,
                    missing_bars,
                    chunk_rows,
                )
            else:
                _convert_fresh_trade_archive(
                    synced,
                    instrument,
                    staged_catalog,
                    raw_dir=raw_dir,
                    state_dir=staged_state,
                    chunk_rows=chunk_rows,
                    bar_intervals=missing_bars,
                )
                generated = read_record(record_path_for(synced.path, raw_dir, staged_state))
        except DataValidationError as exc:
            record_validation_failure(
                synced,
                exc,
                raw_dir=raw_dir,
                state_dir=state_dir,
                source_checksum=checksum,
                converter_version=TRADE_CONVERTER_VERSION,
            )
            raise
        if (
            generated is None
            or (
                (force or not ticks_ready)
                and not catalog_contains_record(
                    staged_catalog, instrument_id, generated, missing_bars
                )
            )
            or any(
                not any(
                    start <= item["first_ts"] and end >= item["last_ts"]
                    for start, end in staged_catalog.get_intervals(Bar, identifier=item["bar_type"])
                )
                for item in generated["bars"].values()
            )
        ):
            raise ConversionError("临时转换结果校验失败，已有数据已保留")
        merged = dict(generated if force or not ticks_ready else record)
        # 保留未请求周期的来源，强制刷新部分周期后不能把旧 Bar 当作新来源继续聚合。
        previous_bars = (record or {}).get("bars", {})
        if not isinstance(previous_bars, dict):
            previous_bars = {}
        old_bars = {
            interval: {
                **item,
                "source_checksum": item.get("source_checksum", record.get("source_checksum")),
                "converter_version": item.get("converter_version", record.get("converter_version")),
            }
            for interval, item in previous_bars.items()
            if isinstance(item, dict)
        }
        merged["bars"] = {
            **old_bars,
            **{
                interval: {
                    **item,
                    "source_checksum": checksum,
                    "converter_version": TRADE_CONVERTER_VERSION,
                }
                for interval, item in generated["bars"].items()
            },
        }
        previous_intervals = (record or {}).get("bar_intervals", [])
        if not isinstance(previous_intervals, list):
            previous_intervals = []
        merged["bar_intervals"] = list(
            dict.fromkeys(
                [
                    *(
                        value
                        for value in previous_intervals
                        if isinstance(value, str) and value in old_bars
                    ),
                    *old_bars,
                    *bar_intervals,
                ],
            )
        )
        merged["status"] = "in_progress"
        write_record(state_path, merged)
        for data_cls, identifier in targets:
            if force:
                catalog.delete_data_range(
                    data_cls,
                    identifier=identifier,
                    start=day_start,
                    end=day_end - 1,
                )
            kind = "trades" if data_cls is TradeTick else "bars"
            relative = Path("data") / kind / urisafe_identifier(identifier)
            destination_dir = catalog_root / relative
            destination_dir.mkdir(parents=True, exist_ok=True)
            for source in (Path(staged_catalog.path) / relative).glob("*.parquet"):
                destination = destination_dir / source.name
                # link 不会覆盖并发产生的同名文件；staging 与 Catalog 位于同一文件系统。
                os.link(source, destination)
        if force or not ticks_ready:
            from nautilus_trader.model import NautilusDataType
            quote_catalog = catalog.supplement_catalog(instrument_id)
            staged_quotes = staged_catalog.supplement_catalog(instrument_id)
            existing = any(left < day_end and right >= day_start
                           for left,right in quote_catalog.get_intervals(NautilusDataType.Custom("TradeQuote")))
            keep_existing = False
            if existing and not force:
                keep_existing = usable and quote_supplements_equal(quote_catalog, staged_quotes, day_start, day_end)
                if not keep_existing:
                    raise ConversionError("已有 quote 补充数据不一致，请 --force 重建")
            if not keep_existing:
                if force:
                    quote_catalog.delete_data_range(NautilusDataType.Custom("TradeQuote"), None, day_start, day_end-1)
                relative = Path("supplements") / urisafe_identifier(instrument_id) / "data/custom/TradeQuote"
                destination_dir = catalog_root / relative
                destination_dir.mkdir(parents=True, exist_ok=True)
                for source in (Path(staged_catalog.path) / relative).glob("*.parquet"):
                    os.link(source, destination_dir / source.name)
        merged["status"] = "ready"
        write_record(state_path, merged)
    clear_failure_record(synced.path, raw_dir, state_dir)
    return int(merged["rows"])


def convert_trade_archives(
    archives: list[SyncedArchive],
    instruments: dict[str, Instrument],
    catalog: ParquetDataCatalog,
    *,
    raw_dir: Path,
    state_dir: Path,
    bar_intervals: tuple[str, ...] = DEFAULT_BAR_INTERVALS,
    force: bool = False,
) -> int:
    """转换一批 trade 日归档。"""
    total = 0
    for archive in archives:
        try:
            instrument = instruments[archive.spec.symbol]
        except KeyError as exc:
            raise ConversionError(f"缺少合约定义：{archive.spec.symbol}") from exc
        total += convert_trade_archive(
            archive,
            instrument,
            catalog,
            raw_dir=raw_dir,
            state_dir=state_dir,
            bar_intervals=bar_intervals,
            force=force,
        )
    return total


def validate_converted_trade_archives(
    archives: list[SyncedArchive],
    instruments: dict[str, Instrument],
    catalog: ParquetDataCatalog,
    *,
    raw_dir: Path,
    state_dir: Path,
    bar_intervals: tuple[str, ...] = DEFAULT_BAR_INTERVALS,
) -> list[str]:
    """返回没有有效 ready 状态或 Catalog 文件的归档路径。"""
    failures: list[str] = []
    for archive in archives:
        record = read_record(record_path_for(archive.path, raw_dir, state_dir))
        instrument_id = str(instruments[archive.spec.symbol].id)
        if (
            record is None
            or record.get("status") != "ready"
            or record.get("instrument_id") != instrument_id
            or not catalog_contains_record(catalog, instrument_id, record, bar_intervals)
        ):
            failures.append(archive.spec.source_path)
    return failures
