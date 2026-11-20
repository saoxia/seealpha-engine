"""Binance Spot collector; product-specific lifecycle over shared transport."""


async def serve_batch(manifest, managed_root, catalog, *, project, socket_factory=None):
    """Collect Binance Spot only; preserve existing locks, paths and checkpoints."""
    from engine.market_data.sources.binance_shared.batch import serve_futures_batch

    return await serve_futures_batch(
        manifest, managed_root, catalog, project=project,
        socket_factory=socket_factory, product="spot",
    )
