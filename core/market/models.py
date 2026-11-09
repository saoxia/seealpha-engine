"""Closed-minute bar contract shared by connectors and consumers."""

from dataclasses import dataclass
from decimal import Decimal

from core.market.instruments import bar_type_for, normalize_symbol

MINUTE = 60_000
DAY = 86_400_000


class DataError(RuntimeError):
    """Data cannot safely be used for a new decision."""


@dataclass(frozen=True)
class MinuteBar:
    symbol: str
    start_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    def __post_init__(self):
        if self.symbol != normalize_symbol(self.symbol):
            raise DataError("Symbol must be normalized")
        if type(self.start_ms) is not int or self.start_ms < 0 or self.start_ms % MINUTE:
            raise DataError("Minute must start at a UTC minute boundary")
        prices = (self.open, self.high, self.low, self.close)
        if any(not p.is_finite() or p <= 0 for p in prices):
            raise DataError("Invalid price")
        if not self.volume.is_finite() or self.volume < 0:
            raise DataError("Invalid volume")
        if self.low > min(prices) or self.high < max(prices):
            raise DataError("Invalid OHLC range")

    @property
    def end_ms(self):
        return self.start_ms + MINUTE

    def values(self):
        return tuple(str(x) for x in (self.open, self.high, self.low, self.close, self.volume))

    def matches(self, other):
        # Catalog volume sums use floating point before instrument rounding. Permit only
        # tiny numeric noise, never a price difference or a material volume revision.
        return (
            self.symbol == other.symbol
            and self.start_ms == other.start_ms
            and (self.open, self.high, self.low, self.close)
            == (other.open, other.high, other.low, other.close)
            and abs(self.volume - other.volume)
            <= max(Decimal("0.00000001"), abs(self.volume) * Decimal("0.000000000001"))
        )

    def to_nautilus(self, instrument):
        """Same close-minus-1ms convention as pipeline; no trade ticks are fabricated."""
        from nautilus_trader.model import Bar, BarType

        if str(instrument.id) != bar_type_for(self.symbol, "1m").split("-1-MINUTE")[0]:
            raise DataError("Instrument does not match bar")
        timestamp = (self.end_ms - 1) * 1_000_000
        return Bar(
            BarType.from_str(bar_type_for(self.symbol, "1m")),
            instrument.make_price(self.open),
            instrument.make_price(self.high),
            instrument.make_price(self.low),
            instrument.make_price(self.close),
            instrument.make_qty(self.volume),
            timestamp,
            timestamp,
        )
