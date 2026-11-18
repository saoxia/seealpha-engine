"""Offline research bridges to NautilusTrader RC9 native models.

No execution endpoint imports this module. Funding must be explicitly supplied
with verified exchange provenance and settlement boundaries; no rate or Greeks
is synthesized from OHLC prices. All helpers fail closed on identity mismatches.
"""

from decimal import Decimal
from itertools import chain
from math import isfinite

from nautilus_trader.backtest import BacktestEngine
from nautilus_trader.config import BacktestEngineConfig, LoggerConfig
from nautilus_trader.model import (
    AccountType,
    Bar,
    BookType,
    OmsType,
    OptionGreeks,
    OrderBookDepth,
)


def join_funding_updates(bar_batches, funding_updates, instrument_id):
    """Merge ordered rate events with bounded Bar batches without buffering history.

    Funding updates sharing a Bar timestamp are inserted ahead of that Bar.
    The model distinguishes observed rate timestamps from next funding boundaries;
    only upstream's settlement logic may create actual account cash flows.
    """
    updates = iter(sorted(funding_updates, key=lambda event: event.ts_event))
    next_rate = next(updates, None)
    previous = None
    for batch in bar_batches:
        events = []
        for bar in batch:
            if not isinstance(bar, Bar):
                raise TypeError("Expected historical Bars")
            while next_rate is not None and next_rate.ts_event <= bar.ts_event:
                if next_rate.instrument_id != instrument_id:
                    raise ValueError("Funding instrument differs from backtest instrument")
                if not next_rate.rate.is_finite():
                    raise ValueError("Non-finite funding rate")
                if previous is not None and next_rate.ts_event <= previous:
                    raise ValueError("Funding event time repeated or out of order")
                previous = next_rate.ts_event
                events.append(next_rate)
                next_rate = next(updates, None)
            events.append(bar)
        if events:
            yield events
    if next_rate is not None:
        tail = list(chain([next_rate], updates))
        for event in tail:
            if event.instrument_id != instrument_id or not event.rate.is_finite():
                raise ValueError("Invalid trailing funding event")
        yield tail


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


def validated_depths(depths, instrument_id):
    """Validate an offline L2 snapshot series; gaps must be assessed upstream."""
    previous = None
    result = []
    for depth in depths:
        if not isinstance(depth, OrderBookDepth):
            raise TypeError("Invalid depth snapshot model")
        if depth.instrument_id != instrument_id:
            raise ValueError("Invalid depth snapshot instrument")
        if previous is not None and depth.ts_event <= previous:
            raise ValueError("Depth sequence has repeated or regressing event times")
        previous = depth.ts_event
        result.append(depth)
    if not result:
        raise ValueError("No L2 snapshots for replay")
    return result


def run_depth_backtest(instrument, depths, starting, strategies=(), *, fee_model=None):
    """Explicitly offline L2 execution research using RC9 native matching.

    Never routes live orders or claims a historical snapshot stream is gap-free.
    Returns native reports only after a completed deterministic simulation.
    """
    values = validated_depths(depths, instrument.id)
    engine = BacktestEngine(BacktestEngineConfig(logging=LoggerConfig(bypass_logging=True)))
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
        engine.add_data(values)
        engine.run()
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
