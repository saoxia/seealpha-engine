"""按 UTC 日物化行情特征；标准 Bar 与特征分别存储，支持跨日续算和按列读取。"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, date, datetime, timedelta
from math import gcd
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
from nautilus_trader.model import (
    Bar,
    BarType,
    CustomData,
    DataType,
    InstrumentId,
    register_custom_data_class,
)
from nautilus_trader.model.custom import customdataclass
from nautilus_trader.persistence import ParquetDataCatalog as NativeCatalog

from engine.pipeline.catalog import (
    ParquetDataCatalog,
    _parse_filename_timestamps,
    _timestamps_to_filename,
)
from engine.pipeline.convert_binance_bars import (
    ConversionError,
    read_archive_checksum,
    read_record,
    record_path_for,
)
from engine.pipeline.convert_binance_trades import CSV_CHUNK_ROWS, iter_trade_archive
from engine.pipeline.models import (
    SyncedArchive,
    as_utc,
    bar_type_for,
    interval_delta,
    normalize_symbol,
)
from engine.pipeline.validate_data import DataValidationError

FEATURE_VERSION = 2
DAY_NS = 86_400_000_000_000
BASE_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "trade_count",
)
FEATURE_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "taker_buy_volume",
    "taker_sell_volume",
    "taker_buy_quote_volume",
    "taker_sell_quote_volume",
    "delta_volume",
    "delta_quote_volume",
    "trade_count",
    "atr14",
    "natr14",
    "boll_mid",
    "boll_upper",
    "boll_lower",
    "boll_width",
    "boll_pos",
    "ret_5",
    "ma99",
    "std20",
    "buy_ratio",
    "delta_ratio",
)
FEATURE_SCHEMA = pa.schema(
    [("instrument_id", pa.string()), ("bar_type", pa.string())]
    + [(name, pa.int64() if name == "trade_count" else pa.float64()) for name in FEATURE_COLUMNS]
    + [("type", pa.string()), ("ts_event", pa.timestamp("ns", tz="UTC")), ("ts_init", pa.timestamp("ns", tz="UTC"))],
)


@customdataclass(kw_only=True)
class BarFeatures:
    """一个已闭合周期的行情与指标；滚动指标预热不足时为 None。"""

    instrument_id: InstrumentId
    bar_type: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    taker_buy_volume: float
    taker_sell_volume: float
    taker_buy_quote_volume: float
    taker_sell_quote_volume: float
    delta_volume: float
    delta_quote_volume: float
    trade_count: int
    atr14: float | None
    natr14: float | None
    boll_mid: float | None
    boll_upper: float | None
    boll_lower: float | None
    boll_width: float | None
    boll_pos: float | None
    ret_5: float | None
    ma99: float | None
    std20: float | None
    buy_ratio: float | None
    delta_ratio: float | None

    _schema = FEATURE_SCHEMA


    @classmethod
    def arrow_schema_py(cls):
        return FEATURE_SCHEMA

    def encode_record_batch_py(self, items):
        return pa.RecordBatch.from_pylist([item.to_dict() for item in items], schema=FEATURE_SCHEMA)


register_custom_data_class(BarFeatures)


def _bounds(day: date) -> tuple[int, int]:
    start = int(datetime.combine(day, datetime.min.time(), UTC).timestamp()) * 1_000_000_000
    return start, start + DAY_NS


def feature_path(catalog: ParquetDataCatalog, bar_type: str, day: date) -> Path:
    start, end = _bounds(day)
    return Path(catalog._make_path(BarFeatures, bar_type)) / _timestamps_to_filename(start, end - 1)


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def _aggregate(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.groupby(level=0, sort=True).agg(
        **{
            name: (name, operation)
            for name, operation in (
                ("open", "first"),
                ("high", "max"),
                ("low", "min"),
                ("close", "last"),
                ("volume", "sum"),
                ("quote_volume", "sum"),
                ("taker_buy_volume", "sum"),
                ("taker_buy_quote_volume", "sum"),
                ("trade_count", "sum"),
            )
        },
    )


def native_feature_trades(catalog, archive, chunk_rows):
    """Join native Arrow scans by trade identity; never reopen the source ZIP."""
    from nautilus_trader.model import NautilusDataType
    instrument = str(BarType.from_str(bar_type_for(archive.spec.symbol, "1m")).instrument_id)
    start, end = _bounds(archive.spec.day)
    ticks = pa.RecordBatchReader._import_from_c_capsule(catalog.native.query_data_arrow_stream(
        NautilusDataType.TradeTick, [instrument], start, end-1, display=False))
    supplements = catalog.supplement_catalog(instrument)
    quote_reader = pa.RecordBatchReader._import_from_c_capsule(supplements.query_data_arrow_stream(
        NautilusDataType.Custom("TradeQuote"), start=start, end=end-1, display=False))
    # Merge bounded batches in their native replay order; retained IDs validate alignment.
    quote_batches = iter(quote_reader)
    quote_batch = None
    quote_offset = 0
    for tick_batch in ticks:
        for offset in range(0, tick_batch.num_rows, chunk_rows):
            batch = tick_batch.slice(offset, chunk_rows)
            pieces = []
            remaining = batch.num_rows
            while remaining:
                if quote_batch is None or quote_offset == quote_batch.num_rows:
                    quote_batch = next(quote_batches, None)
                    quote_offset = 0
                if quote_batch is None:
                    raise ConversionError("缺少逐笔 quote 补充数据，请 --force 重建归档")
                take = min(remaining, quote_batch.num_rows-quote_offset)
                pieces.append(quote_batch.slice(quote_offset, take))
                quote_offset += take
                remaining -= take
            quotes = pa.Table.from_batches(pieces).to_pandas()
            frame = pa.Table.from_batches([batch]).to_pandas()
            if frame.trade_id.astype(str).tolist() != quotes.trade_id.astype(str).tolist():
                raise ConversionError("Quote supplement trade IDs do not match native ticks")
            yield pd.DataFrame({
                "price":frame.price.astype(float),"quantity":frame["size"].astype(float),
                "timestamp":frame.ts_event.astype("int64")//1_000_000,
                "trade_id":frame.trade_id.astype("int64"),
                "buyer_maker":frame.aggressor_side.astype(str).eq("SELL"),
                "quote_quantity":quotes.quote_quantity.astype(float),
            })


def aggregate_feature_archive(
    archive: SyncedArchive,
    intervals: tuple[str, ...],
    *,
    chunk_rows: int = CSV_CHUNK_ROWS,
    catalog: ParquetDataCatalog | None = None,
) -> dict[str, pd.DataFrame]:
    """一次分块扫描归档，保留真实 quote_quantity 和主动成交方向。"""
    periods = {
        interval: int(interval_delta(interval).total_seconds() * 1000) for interval in intervals
    }
    if not periods:
        return {}
    # 先压缩成所有请求周期的公约数时间桶，高周期只扫描聚合结果。
    base_period = gcd(*periods.values())
    parts: list[pd.DataFrame] = []
    start, end = _bounds(archive.spec.day)
    previous_time = previous_id = None
    chunks = iter_trade_archive(archive.path, chunk_rows) if catalog is None else native_feature_trades(catalog, archive, chunk_rows)
    for chunk in chunks:
        first_time, last_time = int(chunk.timestamp.iloc[0]), int(chunk.timestamp.iloc[-1])
        first_id, last_id = int(chunk.trade_id.iloc[0]), int(chunk.trade_id.iloc[-1])
        if first_time * 1_000_000 < start or last_time * 1_000_000 >= end:
            raise DataValidationError("Trade timestamp 超出归档标注的 UTC 日期")
        if previous_time is not None and first_time < previous_time:
            raise DataValidationError("Trade timestamp 跨数据块倒序")
        if previous_id is not None and first_id <= previous_id:
            raise DataValidationError("Trade ID 跨数据块重复或倒序")
        previous_time, previous_id = last_time, last_id
        values = pd.DataFrame(
            {
                "open": chunk.price,
                "high": chunk.price,
                "low": chunk.price,
                "close": chunk.price,
                "volume": chunk.quantity,
                "quote_volume": chunk.quote_quantity,
                "taker_buy_volume": chunk.quantity.where(~chunk.buyer_maker, 0.0),
                "taker_buy_quote_volume": chunk.quote_quantity.where(~chunk.buyer_maker, 0.0),
                "trade_count": 1,
            }
        )
        values.index = (chunk.timestamp.to_numpy() // base_period) * base_period
        parts.append(_aggregate(values))
    result = {}
    if not parts:
        raise DataValidationError("Trade 归档没有数据")
    base = _aggregate(pd.concat(parts))
    for interval, period in periods.items():
        frame = base.copy(deep=False)
        if period != base_period:
            frame.index = (frame.index // period) * period
            frame = _aggregate(frame)
        frame.index = (frame.index + period - 1) * 1_000_000
        frame.index.name = "ts_event"
        result[interval] = frame
    return result


def _calculate_segment(frame: pd.DataFrame, state: dict) -> tuple[dict[str, np.ndarray], dict]:
    """Native rolling kernels; the checkpoint stores the minimal replay/seed state."""
    from nautilus_trader.indicators import (
        AverageTrueRange,
        BollingerBands,
        MovingAverageType,
        RateOfChange,
        RateOfChangeMode,
        SimpleMovingAverage,
        WilderMovingAverage,
    )
    bands = BollingerBands(20, 2.0, MovingAverageType.Simple)
    mean = SimpleMovingAverage(99)
    returns = RateOfChange(5, mode=RateOfChangeMode.Fraction)
    tr = AverageTrueRange(1, MovingAverageType.Simple, use_previous=True)
    atr = WilderMovingAverage(14)
    history = state.get("closes", [])
    for close in history:
        bands.update_raw(close)
        mean.update_raw(close)
        returns.update_raw(close)
    if history:
        tr.update_raw(history[-1], history[-1], history[-1])
    seed = list(state.get("tr_seed", []))
    if state.get("atr") is not None:
        for _ in range(14):
            atr.update_raw(state["atr"])
        seed = []
    else:
        for value in seed:
            atr.update_raw(value)
    output = {name:np.empty(len(frame)) for name in ("boll_mid", "std20", "ma99", "ret_5", "atr14")}
    closes = list(history)
    for index, row in enumerate(frame.itertuples()):
        close = float(row.close)
        bands.update_raw(close)
        mean.update_raw(close)
        returns.update_raw(close)
        tr.update_raw(float(row.high), float(row.low), close)
        atr.update_raw(tr.value)
        if not atr.initialized:
            seed.append(tr.value)
        else:
            seed = []
        output["boll_mid"][index] = bands.middle if bands.initialized else np.nan
        output["std20"][index] = bands.stddev if bands.initialized else np.nan
        output["ma99"][index] = mean.value if mean.initialized else np.nan
        output["ret_5"][index] = returns.value if returns.initialized else np.nan
        output["atr14"][index] = atr.value if atr.initialized else np.nan
        closes.append(close)
    return output, {"closes":closes[-99:], "atr":atr.value if atr.initialized else None,
                    "tr_seed":seed,"last_ts":int(frame.index[-1])}


def calculate_features(
    frame: pd.DataFrame,
    interval: str,
    state: dict | None = None,
) -> tuple[pd.DataFrame, dict]:
    """只使用当前和过去的闭合 K 线；任何时间缺口都重新预热。"""
    if frame.empty or not frame.index.is_monotonic_increasing or frame.index.has_duplicates:
        raise DataValidationError("特征输入必须是非空、严格递增的 K 线")
    period = int(interval_delta(interval).total_seconds() * 1_000_000_000)
    state = state or {}
    if state.get("last_ts") != int(frame.index[0]) - period:
        state = {}
    boundaries = np.flatnonzero(np.diff(frame.index.to_numpy()) != period) + 1
    rolling = {
        name: np.empty(len(frame)) for name in ("boll_mid", "std20", "ma99", "ret_5", "atr14")
    }
    start = 0
    for end in (*boundaries, len(frame)):
        segment, state = _calculate_segment(frame.iloc[start:end], state if start == 0 else {})
        for name, values in segment.items():
            rolling[name][start:end] = values
        start = end
    # 缺口只重置滚动状态；逐行派生列对整天一次计算，避免为每个短段建表。
    output = frame.assign(**rolling)
    output["natr14"] = 100 * output.atr14 / output.close
    output["boll_upper"] = output.boll_mid + 2 * output.std20
    output["boll_lower"] = output.boll_mid - 2 * output.std20
    band_range = output.boll_upper - output.boll_lower
    output["boll_width"] = band_range / output.boll_mid.replace(0, np.nan)
    output["boll_pos"] = (output.close - output.boll_lower) / band_range.replace(0, np.nan)
    output["taker_sell_volume"] = output.volume - output.taker_buy_volume
    output["taker_sell_quote_volume"] = output.quote_volume - output.taker_buy_quote_volume
    output["delta_volume"] = output.taker_buy_volume - output.taker_sell_volume
    output["delta_quote_volume"] = output.taker_buy_quote_volume - output.taker_sell_quote_volume
    output["buy_ratio"] = output.taker_buy_volume / output.volume.replace(0, np.nan)
    output["delta_ratio"] = output.delta_volume / output.volume.replace(0, np.nan)
    return output.loc[:, FEATURE_COLUMNS], state


def write_native_features(directory, data):
    return NativeCatalog(directory).write_custom_data(data)


class FeatureStore:
    """检查整个前序依赖链，避免历史补写或重建后静默读取失效指标。"""

    def __init__(self, catalog: ParquetDataCatalog) -> None:
        self.catalog = catalog
        self.validated: dict[tuple[str, date], dict | None] = {}
        self.bar_files: dict[str, list[tuple[int, int, str, int, int]]] = {}

    def bar_fingerprint(self, bar_type: str, day: date) -> str:
        if bar_type not in self.bar_files:
            files = []
            for path in sorted(Path(self.catalog._make_path(Bar, bar_type)).glob("*.parquet")):
                bounds = _parse_filename_timestamps(path.name)
                if bounds is not None:
                    stat = path.stat()
                    files.append((*bounds, path.name, stat.st_size, stat.st_mtime_ns))
            self.bar_files[bar_type] = files
        start, end = _bounds(day)
        files = [item for item in self.bar_files[bar_type] if item[0] < end and item[1] >= start]
        if not files:
            raise ConversionError(f"特征缺少对应的标准 Bar：{bar_type} {day}")
        return _digest(files)

    def metadata(self, bar_type: str, day: date) -> dict | None:
        path = feature_path(self.catalog, bar_type, day)
        if not path.exists():
            return None
        try:
            value = json.loads(path.with_suffix(".metadata.json").read_text())
            if not isinstance(value, dict) or not isinstance(value.get("state"), dict):
                raise TypeError("无效的特征元数据")
            return value
        except (KeyError, TypeError, ValueError, OSError, pa.ArrowException) as exc:
            raise ConversionError(f"特征文件无效，请重建：{path}") from exc

    def valid_metadata(self, bar_type: str, day: date) -> dict | None:
        pending = []
        cursor = day
        while (bar_type, cursor) not in self.validated:
            meta = self.metadata(bar_type, cursor)
            if meta is None:
                self.validated[bar_type, cursor] = None
                break
            pending.append((cursor, meta))
            cursor -= timedelta(days=1)
        parent = self.validated[bar_type, cursor]
        for current_day, meta in reversed(pending):
            expected_parent = parent["digest"] if parent else None
            if (
                meta.get("version") != FEATURE_VERSION
                or meta.get("parent") != expected_parent
                or meta.get("bar_fingerprint") != self.bar_fingerprint(bar_type, current_day)
                or meta.get("digest") != _digest({k: v for k, v in meta.items() if k != "digest"})
            ):
                raise ConversionError(
                    f"特征缓存已失效：{bar_type} {current_day}；"
                    "请从该日开始重新运行 prepare_data，直到需要读取的结束日期",
                )
            self.validated[bar_type, current_day] = parent = meta
        return self.validated[bar_type, day]

    def write(
        self,
        bar_type: str,
        day: date,
        frame: pd.DataFrame,
        metadata: dict,
    ) -> None:
        frame = frame.reset_index()
        frame["bar_type"] = bar_type
        frame["instrument_id"] = str(BarType.from_str(bar_type).instrument_id)
        frame["type"] = "BarFeatures"
        frame["ts_init"] = frame.ts_event
        metadata["digest"] = _digest(metadata)
        path = feature_path(self.catalog, bar_type, day)
        path.parent.mkdir(parents=True, exist_ok=True)
        from tempfile import TemporaryDirectory
        with TemporaryDirectory(dir=path.parent, prefix=".features-") as directory:
            # A UTC day is the bounded publication/rollback unit.
            values = frame.astype(object).where(pd.notna(frame), None).to_dict("records")
            wrapped = [CustomData(DataType("BarFeatures"), BarFeatures.from_dict(row)) for row in values]
            relative = write_native_features(directory, wrapped)
            metadata_path = path.with_suffix(".metadata.json")
            pending_metadata = Path(directory) / "pending.metadata.json"
            pending_metadata.write_text(json.dumps(metadata))
            backups = []
            for current, backup_name in ((path,"previous.parquet"), (metadata_path,"previous.json")):
                backup = Path(directory) / backup_name
                if current.exists():
                    os.link(current, backup)
                backups.append((current, backup))
            try:
                os.replace(Path(directory) / relative, path)
                os.replace(pending_metadata, metadata_path)
            except OSError:
                for current, backup in backups:
                    if backup.exists():
                        os.replace(backup, current)
                    else:
                        current.unlink(missing_ok=True)
                raise
        self.validated[bar_type, day] = metadata


def prepare_bar_features(
    archives: list[SyncedArchive],
    catalog: ParquetDataCatalog,
    *,
    raw_dir: Path,
    state_dir: Path,
    bar_intervals: tuple[str, ...],
    force: bool = False,
    chunk_rows: int = CSV_CHUNK_ROWS,
) -> None:
    """在行情转换成功后补建请求周期；已有有效缓存不读取 trades。"""
    store = FeatureStore(catalog)
    for archive in sorted(archives, key=lambda value: (value.spec.day, value.spec.symbol)):
        record = read_record(record_path_for(archive.path, raw_dir, state_dir))
        if not record or record.get("status") != "ready":
            raise ConversionError(f"行情尚未就绪：{archive.spec.source_path}")
        pending = {}
        for interval in bar_intervals:
            bar_type = bar_type_for(archive.spec.symbol, interval)
            parent = store.valid_metadata(bar_type, archive.spec.day - timedelta(days=1))
            item = record.get("bars", {}).get(interval, {})
            checksum = item.get("source_checksum", record.get("source_checksum"))
            expected = {
                "version": FEATURE_VERSION,
                "source_checksum": checksum,
                "parent": parent["digest"] if parent else None,
                "bar_fingerprint": store.bar_fingerprint(bar_type, archive.spec.day),
            }
            existing = None if force else store.metadata(bar_type, archive.spec.day)
            if not force and existing and all(existing.get(k) == v for k, v in expected.items()):
                store.valid_metadata(bar_type, archive.spec.day)
                continue
            if checksum != record.get("source_checksum"):
                raise ConversionError(f"特征与旧 Bar 来源不一致，请 --force 重建：{bar_type}")
            pending[interval] = (expected, parent)
        if not pending:
            continue
        # 检查当前 ZIP 与已物化行情使用同一个源；不允许混合旧 OHLCV 和新成交量。
        if read_archive_checksum(archive.path) != record.get("source_checksum"):
            raise ConversionError("源归档已变化，生成特征前请 --force 重建对应行情")
        frames = aggregate_feature_archive(archive, tuple(pending), chunk_rows=chunk_rows, catalog=catalog)
        for interval, (metadata, parent) in pending.items():
            frame, state = calculate_features(
                frames[interval],
                interval,
                parent["state"] if parent else None,
            )
            metadata["state"] = state
            bar_type = bar_type_for(archive.spec.symbol, interval)
            store.write(bar_type, archive.spec.day, frame, metadata)
            print(f"已写入 {len(frame)} 根指标：{bar_type} {archive.spec.day}")


def read_bar_features(
    catalog: ParquetDataCatalog,
    symbol: str,
    interval: str,
    start: datetime,
    end: datetime,
    *,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """校验依赖链并按列读取；范围左闭右开，缺失或失效的数据明确报错。"""
    start, end = as_utc(start), as_utc(end)
    if start >= end:
        raise ValueError("开始时间必须早于结束时间")
    selected = list(dict.fromkeys(["ts_event", *(FEATURE_COLUMNS if columns is None else columns)]))
    if any(name not in FEATURE_SCHEMA.names for name in selected):
        raise ValueError("未知的指标列")
    bar_type = bar_type_for(normalize_symbol(symbol), interval)
    store = FeatureStore(catalog)
    day, last_day = start.date(), (end - timedelta(microseconds=1)).date()
    while day <= last_day:
        if store.valid_metadata(bar_type, day) is None:
            raise ConversionError(f"缺少指标数据，请运行 prepare_data：{bar_type} {day}")
        day += timedelta(days=1)
    from nautilus_trader.model import NautilusDataType
    native = catalog.feature_catalog(bar_type)
    reader = native.query_data_arrow_stream(
        NautilusDataType.Custom("BarFeatures"), start=int(start.timestamp()*1e9),
        end=int(end.timestamp()*1e9)-1, display=False,
    )
    reader = pa.RecordBatchReader._import_from_c_capsule(reader)
    schema = pa.schema([reader.schema.field(name) for name in selected])
    table = pa.Table.from_batches((batch.select(selected) for batch in reader), schema=schema)
    index = table.schema.get_field_index("ts_event")
    table = table.set_column(index, "ts_event", table.column(index).cast(pa.int64()))
    frame = table.to_pandas()
    start_ns, end_ns = int(start.timestamp() * 1e9), int(end.timestamp() * 1e9)

    return frame.loc[(frame.ts_event >= start_ns) & (frame.ts_event < end_ns)].reset_index(
        drop=True
    )
