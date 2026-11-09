"""校验 Binance 原始 K 线和 Nautilus Catalog 的时间连续性。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from nautilus_trader.model import Bar

from engine.pipeline.catalog import ParquetDataCatalog
from engine.pipeline.models import as_utc, interval_delta

KLINE_COLUMNS = (
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
)
NUMERIC_COLUMNS = KLINE_COLUMNS
TRADE_COLUMNS = (
    "trade_id",
    "price",
    "quantity",
    "quote_quantity",
    "timestamp",
    "buyer_maker",
)


class DataValidationError(ValueError):
    """表示原始数据或 Catalog 未满足严格质量规则。"""


@dataclass(frozen=True)
class CatalogValidation:
    """一个 BarType 在请求范围内的 Catalog 校验结果。"""

    bar_type: str
    ready: bool
    missing_event_ranges: tuple[tuple[int, int], ...]


def numeric_kline_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """将 Binance CSV 字段转换为数值并拒绝无效值。"""
    missing = [column for column in KLINE_COLUMNS if column not in frame.columns]
    if missing:
        raise DataValidationError(f"K 线缺少字段：{', '.join(missing)}")

    converted = frame.loc[:, KLINE_COLUMNS].copy()
    for column in NUMERIC_COLUMNS:
        try:
            converted[column] = pd.to_numeric(converted[column], errors="raise")
        except (TypeError, ValueError) as exc:
            raise DataValidationError(f"K 线字段 {column!r} 包含非数值内容") from exc
    if converted.empty:
        raise DataValidationError("K 线文件为空")
    if converted.isna().any().any():
        raise DataValidationError("K 线文件包含空值")
    return converted


def validate_kline_frame(frame: pd.DataFrame, interval: str) -> pd.DataFrame:
    """执行严格的 Binance USD-M K 线结构、时间和 OHLCV 校验。"""
    converted = numeric_kline_frame(frame)
    interval_ms = int(interval_delta(interval).total_seconds() * 1_000)
    open_times = converted["open_time"].astype("int64")
    close_times = converted["close_time"].astype("int64")

    if open_times.max() >= 100_000_000_000_000:
        raise DataValidationError("USD-M K 线时间戳应为毫秒，但检测到了微秒级数值")
    if not open_times.is_monotonic_increasing or open_times.duplicated().any():
        raise DataValidationError("K 线 open_time 必须严格递增且不能重复")
    if len(open_times) > 1 and not (open_times.diff().iloc[1:] == interval_ms).all():
        raise DataValidationError(f"K 线内部存在不符合 {interval} 的时间缺口")
    if not (close_times == open_times + interval_ms - 1).all():
        raise DataValidationError("K 线 close_time 与周期边界不一致")

    open_price = converted["open"]
    high_price = converted["high"]
    low_price = converted["low"]
    close_price = converted["close"]
    if not (
        (low_price <= open_price)
        & (low_price <= close_price)
        & (high_price >= open_price)
        & (high_price >= close_price)
        & (low_price <= high_price)
    ).all():
        raise DataValidationError("K 线 OHLC 价格关系无效")
    if not (converted["volume"] >= 0).all():
        raise DataValidationError("K 线成交量不能为负数")
    return converted


def parse_buyer_maker(value: object) -> bool:
    """严格解析 Binance CSV 中的买方挂单标志。"""
    normalized = str(value).strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise DataValidationError(f"无效的 isBuyerMaker：{value!r}")


def normalize_trade_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """规范化并校验不依赖行顺序的 Trade 字段。"""
    missing = [column for column in TRADE_COLUMNS if column not in frame.columns]
    if missing:
        raise DataValidationError(f"Trade 缺少字段：{', '.join(missing)}")
    if frame.empty:
        raise DataValidationError("Trade 数据块为空")

    converted = frame.loc[:, TRADE_COLUMNS].copy()
    for column in ("trade_id", "price", "quantity", "quote_quantity", "timestamp"):
        try:
            converted[column] = pd.to_numeric(converted[column], errors="raise")
        except (TypeError, ValueError) as exc:
            raise DataValidationError(f"Trade 字段 {column!r} 包含非数值内容") from exc
    if converted.isna().any().any():
        raise DataValidationError("Trade 数据包含空值")

    converted["trade_id"] = converted["trade_id"].astype("int64")
    converted["timestamp"] = converted["timestamp"].astype("int64")
    if converted["timestamp"].max() >= 100_000_000_000_000:
        raise DataValidationError("USD-M Trade 时间戳应为毫秒，但检测到了微秒级数值")
    if not (converted["price"] > 0).all():
        raise DataValidationError("Trade price 必须大于零")
    if not (converted["quantity"] > 0).all():
        raise DataValidationError("Trade quantity 必须大于零")
    if not (converted["quote_quantity"] >= 0).all():
        raise DataValidationError("Trade quote_quantity 不能为负数")
    maker = converted["buyer_maker"]
    if maker.dtype != bool:
        # CSV 中通常只有两个取值，避免对数百万行重复调用 Python 解析器。
        if pd.api.types.is_string_dtype(maker):
            mapping = {value: parse_buyer_maker(value) for value in maker.unique()}
            converted["buyer_maker"] = maker.map(mapping)
        else:
            # 混合对象中 True 与 1 的哈希相同，不能先去重后验证。
            converted["buyer_maker"] = maker.map(parse_buyer_maker)
    return converted


def validate_trade_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """校验并规范化 Trade；重复 ID 保留源文件中靠后的记录。"""
    return validate_normalized_trade_frame(normalize_trade_frame(frame))


def validate_normalized_trade_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """对已规范化的块去重并检查顺序，允许拼接已校验的跨块尾行。"""
    converted = frame.drop_duplicates(subset="trade_id", keep="last").reset_index(drop=True)
    if not converted["timestamp"].is_monotonic_increasing:
        raise DataValidationError("Trade timestamp 必须单调递增")
    if not converted["trade_id"].is_monotonic_increasing:
        raise DataValidationError("Trade ID 必须严格递增")
    return converted


def required_event_bounds(start: datetime, end: datetime, interval: str) -> tuple[int, int, int]:
    """返回请求对应的首尾 Binance close_time 纳秒值和步长。"""
    step_ns = int(interval_delta(interval).total_seconds() * 1_000_000_000)
    close_offset_ns = 1_000_000  # Binance 毫秒 close_time 位于下个周期前 1ms。
    start_ns = int(as_utc(start).timestamp() * 1_000_000_000)
    end_ns = int(as_utc(end).timestamp() * 1_000_000_000)
    return start_ns + step_ns - close_offset_ns, end_ns - close_offset_ns, step_ns


def missing_event_ranges(
    intervals: list[tuple[int, int]],
    required_start: int,
    required_end: int,
    step_ns: int,
) -> tuple[tuple[int, int], ...]:
    """根据 Catalog 文件区间计算缺失的事件时间范围。"""
    cursor = required_start
    missing: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if end < cursor or start > required_end:
            continue
        if start > cursor:
            missing.append((cursor, min(start - step_ns, required_end)))
        cursor = max(cursor, end + step_ns)
        if cursor > required_end:
            break
    if cursor <= required_end:
        missing.append((cursor, required_end))
    return tuple(gap for gap in missing if gap[0] <= gap[1])


def validate_catalog(
    catalog: ParquetDataCatalog,
    bar_type: str,
    start: datetime,
    end: datetime,
    interval: str,
) -> CatalogValidation:
    """校验 Catalog 的文件区间能否连续覆盖请求范围。"""
    required_start, required_end, step_ns = required_event_bounds(start, end, interval)
    intervals = catalog.get_intervals(Bar, identifier=bar_type)
    missing = missing_event_ranges(intervals, required_start, required_end, step_ns)
    return CatalogValidation(
        bar_type=bar_type,
        ready=not missing,
        missing_event_ranges=missing,
    )
