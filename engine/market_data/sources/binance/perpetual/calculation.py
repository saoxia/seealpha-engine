"""USD-M derived data calculation. Source-owned entrypoint, shared pure calculation core."""


async def serve_calculation(policy, root, catalog):
    if policy.get("product", "futures_um") != "futures_um":
        raise ValueError("Expected Binance USD-M product")
    from engine.market_data.sources.binance.shared.calculation import serve_calculation as calculate

    return await calculate(policy, root, catalog)
