"""Spot derived data calculation entrypoint."""

from engine.market_data.sources.binance.common.calculation import serve_calculation as _calculate


async def serve_calculation(policy, root, catalog):
    if policy.get("product") != "spot":
        raise ValueError("Spot calculation requires spot policy")
    return await _calculate(policy, root, catalog)
