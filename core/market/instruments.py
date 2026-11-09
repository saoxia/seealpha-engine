"""Shared Binance USD-M naming and interval rules."""

import re
from datetime import UTC, date, datetime, timedelta

SUPPORTED_MARKET = "futures_um"
INTERVALS: dict[str, tuple[int, str, timedelta | None]] = {
    "1s": (1, "SECOND", timedelta(seconds=1)),
    "1m": (1, "MINUTE", timedelta(minutes=1)),
    "3m": (3, "MINUTE", timedelta(minutes=3)),
    "5m": (5, "MINUTE", timedelta(minutes=5)),
    "15m": (15, "MINUTE", timedelta(minutes=15)),
    "30m": (30, "MINUTE", timedelta(minutes=30)),
    "1h": (1, "HOUR", timedelta(hours=1)),
    "2h": (2, "HOUR", timedelta(hours=2)),
    "4h": (4, "HOUR", timedelta(hours=4)),
    "6h": (6, "HOUR", timedelta(hours=6)),
    "8h": (8, "HOUR", timedelta(hours=8)),
    "12h": (12, "HOUR", timedelta(hours=12)),
    "1d": (1, "DAY", timedelta(days=1)),
    "3d": (3, "DAY", timedelta(days=3)),
    "1w": (1, "WEEK", timedelta(weeks=1)),
    "1M": (1, "MONTH", None),
}


# /fapi/v1/klines rejects 1s; locally derived seconds are not venue K-line intervals.
BINANCE_KLINE_INTERVALS = tuple(interval for interval in INTERVALS if interval != "1s")


class MarketConfigError(ValueError):
    """表示市场命名或周期不受支持。"""


def as_utc(value: datetime) -> datetime:
    """将 datetime 规范化为 UTC。"""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def normalize_symbol(symbol: str, market: str = SUPPORTED_MARKET) -> str:
    """Normalize USD-M symbols or retain full Binance option identifiers."""
    if market == "options":
        normalized = symbol.strip().upper()
        match = re.fullmatch(r"[A-Z0-9]+-(\d{2})(\d{2})(\d{2})-([0-9]+(?:\.[0-9]+)?)-[CP]", normalized)
        if match is None or not any(char != "0" and char != "." for char in match[4]):
            raise MarketConfigError("无效的 Binance 期权合约代码")
        date(2000 + int(match[1]), int(match[2]), int(match[3]))
        return normalized
    if market != SUPPORTED_MARKET:
        raise MarketConfigError("不支持的行情产品")
    normalized = symbol.strip().upper().replace("-", "").replace("/", "").replace("_", "")
    if not normalized or not normalized.endswith(("USDT", "USDC")) or not normalized[:-4].isalnum():
        raise MarketConfigError(f"无效的 USDT / USDC 交易对：{symbol!r}")
    return normalized


def interval_delta(interval: str) -> timedelta:
    """返回受支持 Binance 周期对应的时间长度。"""
    try:
        duration = INTERVALS[interval][2]
        if duration is None:
            raise MarketConfigError("月线须使用日历边界，不能换算为固定时长")
        return duration
    except KeyError as exc:
        supported = ", ".join(INTERVALS)
        raise MarketConfigError(f"暂不支持周期 {interval!r}；可用值：{supported}") from exc


def instrument_id_for(symbol: str, market: str = SUPPORTED_MARKET) -> str:
    """返回 Binance 市场对应的 Nautilus InstrumentId。"""
    if market == "options":
        return f"{normalize_symbol(symbol, market)}.BINANCE"
    if market != SUPPORTED_MARKET:
        raise MarketConfigError("首版管道仅支持 Binance USD-M 永续合约")
    return f"{symbol.upper()}-PERP.BINANCE"


def bar_type_for(symbol: str, interval: str, market: str = SUPPORTED_MARKET) -> str:
    """返回与 Binance 外部 K 线一致的 Nautilus BarType。"""
    instrument_id = instrument_id_for(symbol, market)
    try:
        step, aggregation, _ = INTERVALS[interval]
    except KeyError as exc:
        interval_delta(interval)
        raise AssertionError("unreachable") from exc
    return f"{instrument_id}-{step}-{aggregation}-LAST-EXTERNAL"


def bar_window(interval: str, timestamp_ms: int) -> tuple[int, int]:
    """Binance UTC windows: venue three-day anchor, Monday weeks and calendar months."""
    stamp = datetime.fromtimestamp(timestamp_ms / 1000, UTC)
    if interval == "1M":
        left = stamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        right = left.replace(year=left.year + 1, month=1) if left.month == 12 else left.replace(month=left.month + 1)
        return int(left.timestamp() * 1000), int(right.timestamp() * 1000)
    width = int(interval_delta(interval).total_seconds() * 1000)
    offset = 4 * 86400000 if interval == "1w" else 86400000 if interval == "3d" else 0
    left = (timestamp_ms - offset) // width * width + offset
    return left, left + width
