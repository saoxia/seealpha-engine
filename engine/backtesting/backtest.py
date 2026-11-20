"""Historical bars for a development tenant's already issued packages.

The catalog is the existing pipeline catalog. A tenant must declare its own
subscription intent before those bars are fed to the issued loads. This module
does not download data, open exchange sessions, or create job records.
"""

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from engine.market_data.access.historical_data import _nanos as _nanos  # noqa: PLC0414
from engine.trading.runtime import EngineConflict

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


def read_history_batches(catalog_dir, symbol, interval, start, end, *, batch_size=50_000,
                         product="futures_um"):
    """Compatibility entry: shared read-only Engine historical-data adapter."""
    from engine.market_data.access.historical_data import HistoricalMarketData

    yield from HistoricalMarketData(catalog_dir).bar_batches(
        symbol, interval, start, end, batch_size=batch_size, product=product,
    )


def read_history(catalog_dir, symbol, interval, start, end, *, product="futures_um"):
    """Compatibility entry: all historical bars now use the same reader."""
    from engine.market_data.access.historical_data import HistoricalMarketData

    return HistoricalMarketData(catalog_dir).bars(
        symbol, interval, start, end, product=product,
    )


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


