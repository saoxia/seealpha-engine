"""Read-only historical market-data access shared by Engine replay consumers.

The Pipeline owns persistence and repair. This reader only queries an existing
Catalog; it never creates subscriptions, starts workers, or authorizes trading.
The trusted caller must hold product-scoped data read leases when appropriate.
"""

from datetime import UTC, datetime
from pathlib import Path


def _nanos(value):
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = value - epoch
    return (
        delta.days * 86_400 * 1_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


class HistoricalMarketData:
    """One read-side Catalog adapter for bars, instruments, funding and quotes."""

    def __init__(self, catalog_dir):
        self.root = Path(catalog_dir)

    def _catalog(self):
        from engine.market_data.storage.catalog import ParquetDataCatalog

        return ParquetDataCatalog(self.root)

    def bar_batches(self, symbol, interval, start, end, *, batch_size=50_000,
                    product="futures_um"):
        """Yield bounded native Arrow batches in [start, end), rejecting disorder."""
        if not self.root.is_dir():
            return
        from core.market.instruments import MarketConfigError, bar_type_for
        from engine.trading.runtime import EngineConflict

        try:
            bar_type = bar_type_for(symbol, interval, product)
            start_ns, end_ns = _nanos(start), _nanos(end)
            previous_ns = None
            for batch in self._catalog().bar_batches(
                bar_type, start_ns, end_ns, batch_size=batch_size,
            ):
                selected = []
                for bar in batch:
                    stamp = int(bar.ts_event)
                    if not start_ns <= stamp < end_ns:
                        continue
                    if previous_ns is not None and stamp <= previous_ns:
                        raise EngineConflict("历史 K 线时间重复或倒退。")
                    if str(bar.bar_type) != str(bar_type):
                        raise EngineConflict("历史 K 线品种或周期不一致。")
                    previous_ns = stamp
                    selected.append(bar)
                if selected:
                    yield selected
        except (OSError, ValueError, MarketConfigError) as exc:
            raise EngineConflict("本地行情目录无法读取。") from exc

    def bars(self, symbol, interval, start, end, *, product="futures_um"):
        """Return native bars for [start, end), preserving legacy sorted results."""
        if not self.root.is_dir():
            return []
        from core.market.instruments import MarketConfigError, bar_type_for
        from engine.trading.runtime import EngineConflict

        try:
            bar_type = bar_type_for(symbol, interval, product)
            rows = self._catalog().bars(
                bar_types=[bar_type], start=start, end=end,
            ) or []
        except (OSError, ValueError, MarketConfigError) as exc:
            raise EngineConflict("本地行情目录无法读取。") from exc
        start_ns, end_ns = _nanos(start), _nanos(end)
        selected = [bar for bar in rows if start_ns <= int(bar.ts_event) < end_ns]
        selected.sort(key=lambda bar: int(bar.ts_event))
        return selected

    def instruments(self, *, instrument_ids):
        """Native contract definitions; callers retain product-specific checks."""
        return self._catalog().instruments(instrument_ids=instrument_ids)

    def funding_rate_updates(self, instrument_ids, *, start, end):
        """Raw native funding observations; a settlement is not inferred."""
        return self._catalog().funding_rate_updates(
            instrument_ids, start=start, end=end,
        )

    def quote_ticks(self, *, instrument_ids, start, end):
        """Native bid/ask evidence; never replace it with candle close prices."""
        return self._catalog().quote_ticks(
            instrument_ids=instrument_ids, start=start, end=end,
        )
