"""Offline research bridges to NautilusTrader RC9 native models.

No execution endpoint imports this module. Funding must be explicitly supplied
with verified exchange provenance and settlement boundaries; no rate or Greeks
is synthesized from OHLC prices. All helpers fail closed on identity mismatches.
"""

from decimal import Decimal
from itertools import chain
from math import isfinite

from nautilus_trader.backtest import BacktestEngine
from engine.strategies.backtest_engine import backtest_engine_config
from nautilus_trader.model import (
    AccountType,
    Bar,
    BookType,
    OmsType,
    OptionGreeks,
    OrderBookDepth,
)


def join_funding_updates(bar_batches, funding_updates, instrument_id):
    """Merge two already ordered sources without sorting the full funding history.

    A funding stream may be one-pass. Reject duplicate/out-of-order observations,
    and never buffer more than 10,000 mixed events before the native replay.
    """
    max_events = 10_000
    updates = iter(funding_updates)
    previous_rate_ns = None

    def advance():
        nonlocal previous_rate_ns
        item = next(updates, None)
        if item is None:
            return None
        if item.instrument_id != instrument_id:
            raise ValueError("Funding instrument differs from backtest instrument")
        if not item.rate.is_finite():
            raise ValueError("Non-finite funding rate")
        if previous_rate_ns is not None and item.ts_event <= previous_rate_ns:
            raise ValueError("Funding event time repeated or out of order")
        previous_rate_ns = item.ts_event
        return item

    next_rate = advance()
    events = []
    for batch in bar_batches:
        for bar in batch:
            if not isinstance(bar, Bar):
                raise TypeError("Expected historical Bars")
            while next_rate is not None and next_rate.ts_event <= bar.ts_event:
                events.append(next_rate)
                if len(events) == max_events:
                    yield events
                    events = []
                next_rate = advance()
            events.append(bar)
            if len(events) == max_events:
                yield events
                events = []
        if events:
            yield events
            events = []
    while next_rate is not None:
        events.append(next_rate)
        if len(events) == max_events:
            yield events
            events = []
        next_rate = advance()
    if events:
        yield events


def greeks_exposures(positions, greeks, *, now_ns, max_age_ns):
    """Aggregate contract-adjusted Greeks, never mixing distinct underlyings.

    positions: iterable of (instrument_id, underlying, signed_qty, contract_multiplier).
    Uses native OptionGreeks observations, whose convention must be managed by the
    caller and uniform within an underlying group.
    """
    latest = {str(item.instrument_id): item for item in greeks}
    result = {}
    for instrument_id, underlying, signed_qty, multiplier in positions:
        iid = str(instrument_id)
        item = latest.get(iid)
        if not isinstance(item, OptionGreeks):
            raise TypeError("Missing native option Greeks")
        if not 0 <= now_ns - item.ts_event <= max_age_ns:
            raise ValueError("Stale or future option Greeks")
        factor = Decimal(str(signed_qty)) * Decimal(str(multiplier))
        if not factor.is_finite() or not underlying:
            raise ValueError("Invalid option holding or underlying")
        bucket = result.setdefault(underlying, {
            k: Decimal(0) for k in ("delta", "gamma", "vega", "theta", "rho")
        })
        for name in bucket:
            value = getattr(item, name)
            if not isfinite(value):
                raise ValueError("Non-finite native option Greek")
            bucket[name] += Decimal(str(value)) * factor
    return result


def iter_validated_depth_batches(depths, instrument_id, *, batch_size=10_000):
    """Yield bounded, strictly ordered L2 batches without materializing the input."""
    if type(batch_size) is not int or not 1 <= batch_size <= 50_000:
        raise ValueError("L2 batch size must be between 1 and 50,000")
    previous = None
    batch = []
    for depth in depths:
        if not isinstance(depth, OrderBookDepth):
            raise TypeError("Invalid depth snapshot model")
        if depth.instrument_id != instrument_id:
            raise ValueError("Invalid depth snapshot instrument")
        if previous is not None and depth.ts_event <= previous:
            raise ValueError("Depth sequence has repeated or regressing event times")
        previous = depth.ts_event
        batch.append(depth)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
    if previous is None:
        raise ValueError("No L2 snapshots for replay")


def validated_depths(depths, instrument_id):
    """Compatibility list validator for bounded offline export; do not use for replay."""
    return [depth for batch in iter_validated_depth_batches(depths, instrument_id)
            for depth in batch]


def run_depth_backtest(instrument, depths, starting, strategies=(), *,
                       fee_model=None, batch_size=10_000):
    """Run offline native L2 matching with bounded input and a single final end().

    Source snapshots must already have provenance and gap coverage validation.
    This streaming input does not model queue position or reconstruct missing data.
    """
    batches = iter_validated_depth_batches(depths, instrument.id, batch_size=batch_size)
    first = next(batches)  # Reject empty/invalid sources before opening the engine.
    engine = BacktestEngine(backtest_engine_config())
    try:
        kwargs = {"book_type": BookType.L2_MBP}
        if fee_model is not None:
            kwargs["fee_model"] = fee_model
        engine.add_venue(
            instrument.id.venue, OmsType.NETTING, AccountType.MARGIN, [starting],
            **kwargs,
        )
        engine.add_instrument(instrument)
        if strategies:
            engine.add_strategies(list(strategies))
        for batch in chain([first], batches):
            engine.add_data(batch)
            engine.run(streaming=True)
            engine.clear_data()
        engine.end()
        return engine.get_result()
    finally:
        engine.dispose()


def protective_bracket(strategy, *, instrument_id, order_side, quantity,
                       tp_price, sl_trigger_price):
    """RC9 native bracket with both exits verified reduce-only (offline/opt-in).

    Exchange support and post-entry protection must be checked by the caller.
    No adapter submission occurs in this helper.
    """
    bracket = strategy.order_factory.bracket(
        instrument_id=instrument_id, order_side=order_side, quantity=quantity,
        tp_price=tp_price, sl_trigger_price=sl_trigger_price,
    )
    orders = bracket.orders if hasattr(bracket, "orders") else bracket
    if len(orders) != 3 or not all(order.is_reduce_only for order in orders[1:]):
        raise ValueError("Native protective bracket lacks reduce-only exit protection")
    return bracket
