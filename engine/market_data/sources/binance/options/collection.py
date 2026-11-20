"""Binance options source-owned collection entrypoint."""


async def serve_batch(manifest, managed_root, catalog):
    from engine.market_data.sources.binance.options.implementation import serve_batch as legacy_serve_batch

    return await legacy_serve_batch(manifest, managed_root, catalog)
