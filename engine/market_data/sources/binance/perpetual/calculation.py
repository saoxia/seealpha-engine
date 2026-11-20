"""USD-M derived data calculation entrypoint."""

from engine.market_data.sources.binance.common.calculation import serve_calculation as _calculate


async def serve_calculation(policy, root, catalog):
    if policy.get("product", "futures_um") != "futures_um":
        raise ValueError("Perpetual calculation requires futures_um policy")
    return await _calculate(policy, root, catalog)
