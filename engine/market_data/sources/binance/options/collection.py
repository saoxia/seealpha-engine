"""Options collector boundary; implementation remains in the legacy service."""


async def serve_batch(manifest, managed_root, catalog):
    from engine.market_data.collection.options_service import serve_batch as legacy_serve_batch

    return await legacy_serve_batch(manifest, managed_root, catalog)
