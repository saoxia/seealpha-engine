"""Historical bars for a development tenant's already issued packages.

The catalog is the existing pipeline catalog. A tenant must declare its own
subscription intent before those bars are fed to the issued loads. This module
does not download data, open exchange sessions, or create job records.
"""

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from engine.runtime import EngineConflict

_INTERVALS = {"1m", "5m"}
_MAX_RANGE = timedelta(days=366)
_DATE = re.compile(
    r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?(?:Z|[+-]\d{2}:\d{2})?)?"
)


def parse_window(start, end, interval):
    if interval not in _INTERVALS:
        raise EngineConflict("K 线周期只支持 1m 或 5m。")
    start_at = _parse_time(start, "开始时间")
    end_at = _parse_time(end, "结束时间")
    if not start_at < end_at:
        raise EngineConflict("开始时间必须早于结束时间。结束时间不包含在区间内。")
    if end_at - start_at > _MAX_RANGE:
        raise EngineConflict("单次回测区间不能超过 366 天。")
    return start_at, end_at, interval


def catalog_directory(project):
    return Path(project) / "data" / "catalog"


def read_history(catalog_dir, symbol, interval, start, end):
    """Return catalog bars in ``[start, end)``. Missing data is an empty list."""
    root = Path(catalog_dir)
    if not root.is_dir():
        return []
    from core.market.instruments import MarketConfigError, bar_type_for
    from engine.pipeline.catalog import ParquetDataCatalog

    try:
        bar_type = bar_type_for(symbol, interval)
        catalog = ParquetDataCatalog(root)
        rows = catalog.bars(bar_types=[bar_type], start=start, end=end) or []
    except (OSError, ValueError, MarketConfigError) as exc:
        raise EngineConflict("本地行情目录无法读取。") from exc
    start_ns = _nanos(start)
    end_ns = _nanos(end)
    bars = [bar for bar in rows if start_ns <= int(bar.ts_event) < end_ns]
    bars.sort(key=lambda bar: int(bar.ts_event))
    return bars


def _parse_time(value, name):
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        raise EngineConflict(f"{name}必须为 ISO 日期或时间。")
    try:
        parsed = datetime.fromisoformat(value)
    except (ValueError, OverflowError) as exc:
        raise EngineConflict(f"{name}不是有效日期或时间。") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _nanos(value):
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = value - epoch
    return (
        delta.days * 86_400 * 1_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )
