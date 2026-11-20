"""Spot collector entrypoint; reuse the current shared transport safely."""


async def serve_batch(manifest, managed_root, catalog, *, project, socket_factory=None):
    """Collect Binance Spot only; preserve existing locks, paths and checkpoints."""
    from engine.market_data.sources.binance.common.batch import serve_futures_batch

    return await serve_futures_batch(
        manifest, managed_root, catalog, project=project,
        socket_factory=socket_factory, product="spot",
    )
