"""Binance Spot history verification and repair entrypoints."""


def _validate(policy):
    if policy.get("product") != "spot":
        raise ValueError("Expected Binance Spot product")


async def serve_history(policy, settings, root, *, rest=None):
    _validate(policy)
    from engine.market_data.sources.binance.shared.history import serve_history as serve

    return await serve(policy, settings, root, rest=rest)


async def backfill(policy, settings, *, rest=None, now=None):
    _validate(policy)
    from engine.market_data.sources.binance.shared.history import backfill as fill

    return await fill(policy, settings, rest=rest, now=now)
