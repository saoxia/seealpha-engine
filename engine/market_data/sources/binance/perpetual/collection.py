"""Binance USD-M collector; product-specific lifecycle over shared transport."""


async def serve_batch(manifest, managed_root, catalog, *, project, socket_factory=None):
    """Collect Binance USD-M only; preserve existing locks, paths and checkpoints."""
    from engine.market_data.sources.binance.shared.batch import serve_futures_batch

    return await serve_futures_batch(
        manifest, managed_root, catalog, project=project,
        socket_factory=socket_factory, product="futures_um",
    )
