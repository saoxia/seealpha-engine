"""Bounded native Arrow custom-data reads never materialize a full query."""

from types import SimpleNamespace

import pyarrow as pa
import pytest

from engine.pipeline.catalog import ParquetDataCatalog, TradeQuote


def test_custom_data_batches_are_lazy_bounded_and_tagged(tmp_path, monkeypatch):
    catalog = ParquetDataCatalog(tmp_path)
    calls = []
    source = pa.record_batch(
        {"ts_event": [1, 2, 3, 4, 5], "trade_id": ["1", "2", "3", "4", "5"]},
    )

    def stream(kind, *, start, end, display):
        calls.append((str(kind), start, end, display))
        reader = pa.RecordBatchReader.from_batches(source.schema, [source])
        return reader.__arrow_c_stream__()

    monkeypatch.setattr(
        catalog, "feature_catalog",
        lambda identifier: SimpleNamespace(query_data_arrow_stream=stream),
    )
    batches = catalog.custom_data_batches(
        TradeQuote, instrument_ids=["first"], start=1, end=6, batch_size=2,
    )
    assert calls == []
    result = list(batches)
    assert [identifier for identifier, _batch in result] == ["first"] * 3
    assert [batch.num_rows for _identifier, batch in result] == [2, 2, 1]
    assert [value for _identifier, batch in result for value in batch["ts_event"].to_pylist()] == [1, 2, 3, 4, 5]
    assert calls[0][1:] == (1, 5, False)


def test_custom_data_batches_reject_invalid_bounds(tmp_path):
    catalog = ParquetDataCatalog(tmp_path)
    with pytest.raises(ValueError, match="batch size"):
        list(catalog.custom_data_batches(TradeQuote, batch_size=0))
    with pytest.raises(ValueError, match="increasing"):
        list(catalog.custom_data_batches(TradeQuote, start=4, end=4))
