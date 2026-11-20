"""Market service states and delivery semantics."""

from dataclasses import dataclass
from enum import StrEnum

from core.market.models import DAY, MINUTE, DataError, MinuteBar

__all__ = ["DAY", "MINUTE", "DataError", "Delivery", "MinuteBar", "State", "parse_message"]


class State(StrEnum):
    WARMING_UP = "WARMING_UP"
    READY = "READY"
    DEGRADED = "DEGRADED"
    STOPPED = "STOPPED"


@dataclass(frozen=True)
class Delivery:
    bar: MinuteBar
    allow_new_risk: bool
    reason: str


def parse_message(item, symbols):
    """Validate a normalized native delivery, after the venue adapter decoded it."""
    if not isinstance(item, tuple) or len(item) != 3:
        raise DataError("Expected normalized native market data")
    symbol, event, bar = item
    if symbol not in symbols or type(event) is not int:
        raise DataError("Unexpected native market data identity")
    if bar is not None and (
        not isinstance(bar, MinuteBar) or bar.symbol != symbol or event < bar.end_ms - 1
    ):
        raise DataError("Invalid native closed minute")
    return symbol, event, bar
