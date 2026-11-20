"""USD-M historical maintenance; archive conversion is shared with Spot."""

from engine.market_data.sources.binance.common.history import (
    backfill as _backfill,
    serve_history as _serve_history,
)


def _validate(policy):
    if policy.get("product", "futures_um") != "futures_um":
        raise ValueError("Perpetual repair requires futures_um policy")


async def backfill(policy, settings, *, rest=None, now=None):
    _validate(policy)
    return await _backfill(policy, settings, rest=rest, now=now)


async def serve_history(policy, settings, root, *, rest=None):
    _validate(policy)
    return await _serve_history(policy, settings, root, rest=rest)
