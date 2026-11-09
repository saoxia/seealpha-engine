from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from engine.pipeline.bar_features import (
    BASE_COLUMNS,
    FEATURE_COLUMNS,
    BarFeatures,
    aggregate_feature_archive,
    calculate_features,
    feature_path,
    prepare_bar_features,
    read_bar_features,
)
from engine.pipeline.catalog import ParquetDataCatalog
from engine.pipeline.convert_binance_bars import ConversionError
from engine.pipeline.convert_binance_trades import convert_trade_archive
from engine.pipeline.models import SyncedArchive, bar_type_for
from nautilus_support import TestInstrumentProvider
from tests.pipeline.convert_binance_trades_test import (
    parquet_snapshot,
    trade_row,
    write_trade_archive,
)


def prices_frame(count=150):
    close = 100 + np.arange(count) * 0.2 + np.sin(np.arange(count))
    index = pd.date_range("2026-08-01 23:00:59.999", periods=count, freq="min").as_unit("ns")
    return pd.DataFrame(
        {
            "open": close - 0.1,
            "high": close + 0.5,
            "low": close - 0.4,
            "close": close,
            "volume": 10.0,
            "quote_volume": close * 10,
            "taker_buy_volume": 6.0,
            "taker_buy_quote_volume": close * 6,
            "trade_count": 4,
        },
        index=pd.Index(index.asi8, name="ts_event"),
    )


def test_indicators_match_independent_formulas_and_cross_day_state():
    raw = prices_frame()
    actual, _ = calculate_features(raw, "1m")
    first, state = calculate_features(raw.iloc[:60], "1m")
    second, _ = calculate_features(raw.iloc[60:], "1m", state)
    pd.testing.assert_frame_equal(actual, pd.concat([first, second]), rtol=1e-11, atol=1e-11)
    last = actual.iloc[-1]
    closes = raw.close.to_numpy()
    mean = np.mean(closes[-20:])
    std = np.std(closes[-20:], ddof=0)
    assert last.ma99 == pytest.approx(np.mean(closes[-99:]))
    assert last.std20 == pytest.approx(std)
    assert last.boll_mid == pytest.approx(mean)
    assert last.boll_upper == pytest.approx(mean + 2 * std)
    assert last.boll_lower == pytest.approx(mean - 2 * std)
    assert last.boll_width == pytest.approx(4 * std / mean)
    assert last.boll_pos == pytest.approx((closes[-1] - mean + 2 * std) / (4 * std))
    assert last.ret_5 == pytest.approx(closes[-1] / closes[-6] - 1)
    tr = [raw.high.iloc[0] - raw.low.iloc[0]]
    for index in range(1, len(raw)):
        tr.append(
            max(
                raw.high.iloc[index] - raw.low.iloc[index],
                abs(raw.high.iloc[index] - closes[index - 1]),
                abs(raw.low.iloc[index] - closes[index - 1]),
            )
        )
    atr = sum(tr[:14]) / 14
    for value in tr[14:]:
        atr = (13 * atr + value) / 14
    assert last.atr14 == pytest.approx(atr)
    assert last.natr14 == pytest.approx(100 * atr / closes[-1])
    assert actual.atr14.iloc[:13].isna().all()
    assert actual.boll_mid.iloc[:19].isna().all()
    assert actual.ma99.iloc[:98].isna().all()
    assert actual.ret_5.iloc[:5].isna().all()
    assert last.buy_ratio == pytest.approx(0.6)
    assert last.delta_ratio == pytest.approx(0.2)


def test_warmup_continues_over_small_batches_and_no_future_values():
    raw = prices_frame()
    expected, _ = calculate_features(raw, "1m")
    parts, state = [], None
    for start in range(0, len(raw), 3):
        output, state = calculate_features(raw.iloc[start : start + 3], "1m", state)
        parts.append(output)
    pd.testing.assert_frame_equal(expected, pd.concat(parts), rtol=1e-10, atol=1e-10)
    changed = raw.copy()
    changed.loc[changed.index[110] :, ["open", "high", "low", "close"]] *= 2
    output, _ = calculate_features(changed, "1m")
    pd.testing.assert_frame_equal(expected.iloc[:110], output.iloc[:110])


def test_gaps_restart_warmup_and_zero_denominators_are_null():
    raw = prices_frame()
    raw.loc[:, ["open", "high", "low", "close"]] = 100.0
    raw.loc[:, ["volume", "taker_buy_volume"]] = 0.0
    output, _ = calculate_features(raw.drop(raw.index[110]), "1m")
    assert output.boll_pos.isna().all()
    assert output.buy_ratio.isna().all()
    assert output.delta_ratio.isna().all()
    assert output.boll_width.iloc[20] == 0
    assert output.ma99.iloc[110:].isna().all()
    assert output.atr14.iloc[110:123].isna().all()


def test_trade_aggregation_counts_deduplicated_trades_and_uses_quote_quantity(tmp_path):
    raw = tmp_path / "raw"
    start = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    rows = [
        trade_row(1, start, quantity="1", price="100", buyer_maker="true"),
        trade_row(2, start + 1000, quantity="8", price="999", buyer_maker="true"),
        trade_row(2, start + 1000, quantity="2", price="110", buyer_maker="false"),
        trade_row(3, start + 60_000, quantity="3", price="105", buyer_maker="false"),
    ]
    for row, quote in zip(rows, (100, 7992, 220, 315)):
        row[3] = str(quote)
    spec, path = write_trade_archive(raw, rows)
    frames = aggregate_feature_archive(SyncedArchive(spec, path), ("1m", "5m"), chunk_rows=1)
    minute = frames["1m"].iloc[0]
    assert minute.volume == 3
    assert minute.quote_volume == 320
    assert minute.taker_buy_volume == 2
    assert minute.taker_buy_quote_volume == 220
    assert minute.trade_count == 2
    five = frames["5m"].iloc[0]
    assert five.trade_count == 3
    assert five.volume == 6
    assert five.quote_volume == 635
    assert five.taker_buy_volume == 5
    assert five.open == 100 and five.close == 105 and five.high == 110


@pytest.mark.parametrize("intervals", [("1s", "1m", "5m", "15m"), ("3m", "5m"), ("5m",)])
@pytest.mark.parametrize("chunk_rows", [1, 7, 1000])
def test_multi_period_aggregation_matches_direct_trade_sums(tmp_path, intervals, chunk_rows):
    from engine.pipeline.models import interval_delta

    start = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    rows = []
    for i in range(41):
        # Sparse buckets, exact boundaries and multiple trades per bucket.
        ts = start + (i // 3) * 60_000 + (0, 999, 59_999)[i % 3]
        row = trade_row(
            i + 1,
            ts,
            price=str(100.25 + i % 9),
            quantity=str((i + 1) / 100),
            buyer_maker="false" if i % 2 else "true",
        )
        row[3] = str((i + 1) * 1.2345)
        rows.append(row)
    spec, path = write_trade_archive(tmp_path, rows, with_header=True)
    actual = aggregate_feature_archive(SyncedArchive(spec, path), intervals, chunk_rows=chunk_rows)
    for interval in intervals:
        period = int(interval_delta(interval).total_seconds() * 1000)
        expected = {}
        for _, price, quantity, quote, ts, maker in rows:
            key = (int(ts) // period * period + period - 1) * 1_000_000
            price, quantity, quote = float(price), float(quantity), float(quote)
            item = expected.setdefault(key, [price, price, price, price, 0, 0, 0, 0, 0])
            item[1], item[2], item[3] = max(item[1], price), min(item[2], price), price
            item[4] += quantity
            item[5] += quote
            item[6] += quantity if maker == "false" else 0
            item[7] += quote if maker == "false" else 0
            item[8] += 1
        frame = pd.DataFrame.from_dict(expected, orient="index", columns=BASE_COLUMNS)
        frame.index.name = "ts_event"
        pd.testing.assert_frame_equal(actual[interval], frame, rtol=1e-12, atol=1e-12)


@pytest.fixture
def feature_dataset(tmp_path, monkeypatch):
    monkeypatch.setenv("NAUTILUS_RUST_CONVERTER", "off")
    catalog = ParquetDataCatalog(str(tmp_path / "catalog"))
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    kwargs = {"raw_dir": tmp_path / "raw", "state_dir": tmp_path / "state"}
    start = datetime(2026, 8, 1, 23, tzinfo=UTC)
    archives = []
    for offset, count in ((0, 60), (60, 90)):
        rows = []
        for i in range(offset, offset + count):
            timestamp = int((start + timedelta(minutes=i)).timestamp() * 1000)
            row = trade_row(
                i + 1,
                timestamp,
                price=str(3000 + i),
                quantity="1",
                buyer_maker="false" if i % 2 else "true",
            )
            row[3] = str(3000 + i)
            rows.append(row)
        spec, path = write_trade_archive(
            kwargs["raw_dir"],
            rows,
            day=(start + timedelta(minutes=offset)).date().isoformat(),
        )
        archive = SyncedArchive(spec, path)
        convert_trade_archive(archive, instrument, catalog, bar_intervals=("1m",), **kwargs)
        archives.append(archive)
    return catalog, instrument, archives, kwargs


def test_parquet_roundtrip_projection_idempotence_and_catalog_custom_data(
    feature_dataset, monkeypatch
):
    from engine.pipeline import bar_features

    catalog, _, archives, kwargs = feature_dataset
    prepare_bar_features(archives, catalog, bar_intervals=("1m",), **kwargs)
    before = parquet_snapshot(Path(catalog.path))
    monkeypatch.setattr(
        bar_features, "iter_trade_archive", lambda *a, **kw: pytest.fail("重复解析")
    )
    prepare_bar_features(archives, catalog, bar_intervals=("1m",), **kwargs)
    assert parquet_snapshot(Path(catalog.path)) == before
    start, end = datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 8, 3, tzinfo=UTC)
    frame = read_bar_features(catalog, "ETH-USDT", "1m", start, end, columns=["ma99", "std20"])
    assert list(frame.columns) == ["ts_event", "ma99", "std20"]
    assert len(frame) == 150
    assert frame.ma99.iloc[98] == 3049
    data = catalog.custom_data(
        BarFeatures,
        instrument_ids=[bar_type_for("ETHUSDT", "1m")],
    )
    assert len(data) == 150
    assert data[0].data.ma99 is None
    assert data[-1].data.ma99 == 3100
    assert str(data[-1].data.instrument_id) == "ETHUSDT-PERP.BINANCE"
    assert "ma120" not in FEATURE_COLUMNS
    assert set(BASE_COLUMNS) <= set(FEATURE_COLUMNS)
    path = feature_path(catalog, bar_type_for("ETHUSDT", "1m"), archives[0].spec.day)
    assert pq.read_table(path, columns=["ma99"]).column("ma99").null_count == 60


def test_append_and_earlier_backfill_invalidate_later_features(feature_dataset):
    catalog, _, archives, kwargs = feature_dataset
    # 先做第二天，随后补第一天；第二天原来从零预热的结果必须失效。
    prepare_bar_features(archives[1:], catalog, bar_intervals=("1m",), **kwargs)
    prepare_bar_features(archives[:1], catalog, bar_intervals=("1m",), **kwargs)
    start, end = datetime(2026, 8, 2, tzinfo=UTC), datetime(2026, 8, 3, tzinfo=UTC)
    with pytest.raises(ConversionError, match="已失效"):
        read_bar_features(catalog, "ETHUSDT", "1m", start, end)
    prepare_bar_features(archives[1:], catalog, bar_intervals=("1m",), **kwargs)
    frame = read_bar_features(catalog, "ETHUSDT", "1m", start, end)
    assert frame.ma99.iloc[-1] == 3100


def test_rebuilt_market_data_is_detected_and_feature_repair_preserves_bars(feature_dataset):
    catalog, instrument, archives, kwargs = feature_dataset
    prepare_bar_features(archives, catalog, bar_intervals=("1m",), **kwargs)
    convert_trade_archive(
        archives[0], instrument, catalog, bar_intervals=("1m",), force=True, **kwargs
    )
    start, end = datetime(2026, 8, 2, tzinfo=UTC), datetime(2026, 8, 3, tzinfo=UTC)
    with pytest.raises(ConversionError, match="已失效"):
        read_bar_features(catalog, "ETHUSDT", "1m", start, end)
    before = parquet_snapshot(Path(catalog.path) / "data" / "bars")
    prepare_bar_features(archives, catalog, bar_intervals=("1m",), **kwargs)
    read_bar_features(catalog, "ETHUSDT", "1m", start, end)
    assert parquet_snapshot(Path(catalog.path) / "data" / "bars") == before


def test_failed_feature_write_preserves_previous_parquet(feature_dataset, monkeypatch):
    catalog, _, archives, kwargs = feature_dataset
    prepare_bar_features(archives, catalog, bar_intervals=("1m",), **kwargs)
    before = parquet_snapshot(Path(catalog.path))

    def fail_write(*args, **kwargs):
        raise OSError("模拟磁盘写入失败")

    monkeypatch.setattr("engine.pipeline.bar_features.write_native_features", fail_write)
    with pytest.raises(OSError, match="磁盘"):
        prepare_bar_features(archives, catalog, bar_intervals=("1m",), force=True, **kwargs)
    assert parquet_snapshot(Path(catalog.path)) == before
    assert not list(Path(catalog.path).rglob(".features-*"))


def test_forced_rebuild_repairs_corrupt_feature_metadata(feature_dataset):
    catalog, _, archives, kwargs = feature_dataset
    prepare_bar_features(archives, catalog, bar_intervals=("1m",), **kwargs)
    path = feature_path(catalog, bar_type_for("ETHUSDT", "1m"), archives[0].spec.day)
    path.with_suffix(".metadata.json").write_text("not-json")
    with pytest.raises(ConversionError, match="特征文件无效"):
        prepare_bar_features(archives, catalog, bar_intervals=("1m",), **kwargs)
    prepare_bar_features(archives, catalog, bar_intervals=("1m",), force=True, **kwargs)
    frame = read_bar_features(
        catalog,
        "ETHUSDT",
        "1m",
        datetime(2026, 8, 1, tzinfo=UTC),
        datetime(2026, 8, 3, tzinfo=UTC),
    )
    assert frame.ma99.iloc[-1] == 3100


def test_feature_preparation_does_not_reparse_source_csv(feature_dataset, monkeypatch):
    catalog, _, archives, kwargs = feature_dataset
    def forbidden(*args, **kwargs):
        raise AssertionError("Source CSV was parsed a second time")
    monkeypatch.setattr("engine.pipeline.bar_features.iter_trade_archive", forbidden)
    prepare_bar_features(archives, catalog, bar_intervals=("1m",), **kwargs)
    data = read_bar_features(catalog, "ETHUSDT", "1m",
        datetime(2026,8,1,tzinfo=UTC),datetime(2026,8,3,tzinfo=UTC))
    assert len(data) == 150


def test_feature_metadata_publication_failure_rolls_back_native_parquet(feature_dataset, monkeypatch):
    import os
    catalog, _, archives, kwargs = feature_dataset
    prepare_bar_features(archives, catalog, bar_intervals=("1m",), **kwargs)
    before = parquet_snapshot(Path(catalog.path))
    original = os.replace
    def fail_metadata(source, target):
        if Path(source).name == "pending.metadata.json":
            raise OSError("模拟元数据发布失败")
        return original(source, target)
    monkeypatch.setattr("engine.pipeline.bar_features.os.replace", fail_metadata)
    with pytest.raises(OSError, match="元数据"):
        prepare_bar_features(archives, catalog, bar_intervals=("1m",), force=True, **kwargs)
    assert parquet_snapshot(Path(catalog.path)) == before
    assert not list(Path(catalog.path).rglob(".features-*"))
