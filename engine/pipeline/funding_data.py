"""Verified Binance USD-M funding-rate history -> NautilusTrader RC9 data.

No live execution paths and no inferred historical rate intervals. A caller
must provide a bounded, rate-limited public fundingRate page reader and retain
its source evidence before invoking persist_history.
"""

from decimal import Decimal, InvalidOperation

from nautilus_trader.model import FundingRateUpdate, InstrumentId

from core.market.instruments import instrument_id_for


async def fetch_history(symbol, start_ms, end_ms, get_page, *, page_size=1000):
    """Fetch exchange settlement observations in [start_ms, end_ms).

    The Binance endpoint uses inclusive time bounds; continue from the last
    confirmed fundingTime + 1 ms and reject malformed or non-advancing pages.
    An empty page is not evidence of all historical intervals being complete.
    """
    if not (isinstance(start_ms, int) and isinstance(end_ms, int)
            and 0 <= start_ms < end_ms and 1 <= page_size <= 1000):
        raise ValueError("Invalid bounded funding history request")
    instrument = InstrumentId.from_str(instrument_id_for(symbol))
    wire_symbol = str(instrument.symbol).removesuffix("-PERP")
    result = []
    cursor = start_ms
    while cursor < end_ms:
        page = await get_page(
            "fundingRate", symbol=wire_symbol, startTime=cursor,
            endTime=end_ms - 1, limit=page_size,
        )
        if not isinstance(page, list):
            raise ValueError("Funding history response must be a list")
        if not page:
            break
        previous = cursor - 1
        for item in page:
            if item.get("symbol") != wire_symbol or type(item.get("fundingTime")) is not int:
                raise ValueError("Funding response instrument/timestamp mismatch")
            stamp = item["fundingTime"]
            if not cursor <= stamp < end_ms or stamp <= previous:
                raise ValueError("Funding history timestamp repeated or regressed")
            try:
                rate = Decimal(item["fundingRate"])
            except (InvalidOperation, KeyError, TypeError) as exc:
                raise ValueError("Invalid funding rate") from exc
            if not rate.is_finite():
                raise ValueError("Non-finite funding rate")
            ts_ns = stamp * 1_000_000
            result.append(FundingRateUpdate(
                instrument_id=instrument, rate=rate, ts_event=ts_ns, ts_init=ts_ns,
                next_funding_ns=ts_ns,
            ))
            previous = stamp
        if previous < cursor:
            raise ValueError("Funding cursor did not advance")
        cursor = previous + 1
        if len(page) < page_size:
            break
    return result


def persist_history(catalog, observations):
    """Write verified native observations without overwriting existing intervals."""
    if not observations:
        return 0
    if any(type(event) is not FundingRateUpdate for event in observations):
        raise TypeError("Only native funding observations may be persisted")
    catalog.write_data(observations)
    return len(observations)
