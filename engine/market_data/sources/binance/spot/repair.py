"""Spot historical maintenance; archive conversion is shared with USD-M."""

from engine.market_data.sources.binance.common.history import backfill as _backfill
from engine.market_data.sources.binance.common.history import serve_history as _serve_history


def _validate(policy):
    if policy.get("product") != "spot":
        raise ValueError("Spot repair requires spot policy")


async def backfill(policy, settings, *, rest=None, now=None):
    _validate(policy)
    return await _backfill(policy, settings, rest=rest, now=now)


async def serve_history(policy, settings, root, *, rest=None):
    _validate(policy)
    return await _serve_history(policy, settings, root, rest=rest)
