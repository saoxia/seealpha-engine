"""Read existing historical Catalog by UTC day; never rewrite research data."""

from decimal import Decimal
from pathlib import Path

from core.market.instruments import bar_type_for

from .models import DAY, MINUTE, DataError, MinuteBar


class CatalogHistory:
    def __init__(self, path):
        self.path = Path(path)

    def consume_change(self, path):
        """Keep the existing Feed→history→Pipeline bridge as the sole dependency."""
        import fcntl
        import json

        from engine.pipeline.pair_data import calculate_history_change

        with (path.parent.parent / "changes.lock").open("a") as lease:
            fcntl.flock(lease, fcntl.LOCK_EX)
            if not path.exists():
                return False
            event = json.loads(path.read_text())
            if event["state"] != "ready":
                return False
            calculate_history_change(event, self.path, completed_until=event["end"])
            path.unlink()
            return True

    def options_definition(self, symbol):
        from dataclasses import replace

        from engine.pipeline.config import load_settings
        from engine.pipeline.options_data import _definition

        return _definition(symbol, replace(load_settings(), catalog_dir=self.path), local_only=True).to_dict()

    def options_evidence(self, symbol, events=(), *, recover=False, repair_from=None, recent=False, definition=None):
        """Options share this existing bridge; Feed never installs Catalog files."""
        from dataclasses import replace
        from datetime import UTC, datetime, timedelta

        from engine.pipeline.config import load_settings
        from engine.pipeline.models import DataRequest
        from engine.pipeline.options_data import persist_live, prepare, repair_live

        settings = replace(load_settings(), catalog_dir=self.path)
        result = persist_live(symbol, events, settings, recover=recover, definition=definition)
        if repair_from is not None:
            result["repaired_until_ms"] = repair_live(symbol, repair_from, settings, definition=definition)
        if recent:
            now = datetime.now(UTC)
            request = DataRequest((symbol,), max(now.replace(hour=0, minute=0, second=0, microsecond=0), now - timedelta(seconds=1)),
                                  now, bar_intervals=(), market="options")
            prepare(request, settings, managed=True, definition=definition)
        return result

    def read_day(self, symbol, start_ms, end_ms):
        if not self.path.exists():
            return []
        from engine.pipeline.catalog import ParquetDataCatalog

        catalog = ParquetDataCatalog(str(self.path))
        rows = catalog.bars(
            bar_types=[bar_type_for(symbol, "1m")],
            start=start_ms * 1_000_000,
            end=end_ms * 1_000_000 - 1,
        )
        result = []
        for bar in rows:
            close_ms, remainder = divmod(bar.ts_event, 1_000_000)
            if remainder or (close_ms + 1) % MINUTE:
                raise DataError("Catalog timestamp convention differs from close-minus-1ms")
            start = close_ms + 1 - MINUTE
            if start_ms <= start < end_ms:
                result.append(
                    MinuteBar(
                        symbol,
                        start,
                        *(
                            Decimal(str(v))
                            for v in (bar.open, bar.high, bar.low, bar.close, bar.volume)
                        ),
                    )
                )
        return result

    def chunks(self, start_ms, end_ms):
        cursor = start_ms
        while cursor < end_ms:
            end = min((cursor // DAY + 1) * DAY, end_ms)
            yield cursor, end
            cursor = end

    def raw_trade_pages(self, symbol, start_ms, end_ms):
        """Compatibility read-only raw-trade bridge; not used for live ID-gap repair."""
        if not self.path.is_dir():
            return
        import pyarrow as pa
        from nautilus_trader.model import NautilusDataType

        from core.market.instruments import instrument_id_for
        from engine.pipeline.catalog import ParquetDataCatalog

        catalog = ParquetDataCatalog(str(self.path))
        stream = catalog.native.query_data_arrow_stream(
            NautilusDataType.TradeTick, [instrument_id_for(symbol)],
            start_ms * 1000000, end_ms * 1000000 - 1, display=False,
        )
        reader = pa.RecordBatchReader._import_from_c_capsule(stream)
        try:
            for batch in reader:
                for offset in range(0, batch.num_rows, 1000):
                    piece = batch.slice(offset, 1000)
                    stamps = piece.column("ts_event").cast(pa.int64()).to_pylist()
                    yield [{"id": int(row["trade_id"]), "time": stamps[index] // 1000000,
                            "price": str(row["price"]), "qty": str(row["size"]),
                            "isBuyerMaker": row["aggressor_side"] == "SELL"}
                           for index, row in enumerate(piece.to_pylist())]
        finally:
            reader.close()
