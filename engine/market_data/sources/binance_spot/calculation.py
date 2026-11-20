"""Spot derived data calculation. Source-owned entrypoint, shared pure calculation core."""


async def serve_calculation(policy, root, catalog):
    if policy.get("product") != "spot":
        raise ValueError("Expected Binance Spot product")
    from engine.market_data.sources.binance_shared.calculation import serve_calculation as calculate

    return await calculate(policy, root, catalog)
