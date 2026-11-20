"""Offline real-Catalog spot + USD-M alignment; never opens a venue connection."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from nautilus_trader.model import FundingRateUpdate, InstrumentId

from engine.market_data.storage.catalog import ParquetDataCatalog
from engine.portfolio.basket_history import align_carry_bars
from engine.trading.runtime import EngineConflict, EngineRuntime
from tests.engine.backtest_test import CLOSES, write_crossover_catalog
from tests.engine.spot_backtest_test import spot_catalog, spot_instrument


def _dataset(tmp_path, *, omit_spot=None, include_funding=True):
    start = datetime(2024, 1, 1, tzinfo=UTC)
    root = tmp_path / "data" / "catalog"
    futures = write_crossover_catalog(root, start)
    if omit_spot is None:
        spot = spot_catalog(root, start)
    else:
        original = spot_catalog(tmp_path / "other" / "data" / "catalog", start)
        spot = [bar for index, bar in enumerate(original) if index != omit_spot]
        catalog = ParquetDataCatalog(root)
        catalog.write_data([spot_instrument()])
        catalog.write_data(spot)
    if include_funding:
        catalog = ParquetDataCatalog(root)
        iid = InstrumentId.from_str("ETHUSDT-PERP.BINANCE")
        events = [FundingRateUpdate(
            iid, Decimal("0.0001"), futures[2].ts_event,
            futures[2].ts_event, next_funding_ns=futures[2].ts_event,
        ), FundingRateUpdate(
            iid, Decimal("-0.0002"), futures[9].ts_event,
            futures[9].ts_event, next_funding_ns=futures[9].ts_event,
        )]
        catalog.write_data(events)
    return start, spot, futures


def _range(start):
    return (
        start.isoformat(),
        (start + timedelta(minutes=len(CLOSES) + 1)).isoformat(),
    )


def _intent(runtime, tenant_id, *, product):
    runtime.note_subscription(
        tenant_id, "market-data", product, "ETHUSDT", "intent",
        "configured", {"history_enabled": True},
    )


def test_catalog_history_replays_both_products_and_actual_funding_timestamps(tmp_path):
    start, spot, futures = _dataset(tmp_path)
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    try:
        runtime.ensure_tenant("dev", "development")
        for product in ("spot", "futures_um"):
            _intent(runtime, "dev", product=product)
        events = list(runtime.iter_funding_carry_history(
            "dev", "ETHUSDT", *_range(start), "1m",
        ))
    finally:
        runtime.close()

    bars = [item for item in events if item.kind == "bars"]
    rates = [item for item in events if item.kind == "funding"]
    assert len(bars) == len(CLOSES)
    assert [row.ts_ns for row in bars] == [bar.ts_event for bar in futures]
    assert [row.spot.ts_event for row in bars] == [bar.ts_event for bar in spot]
    assert [row.perpetual.ts_event for row in bars] == [bar.ts_event for bar in futures]
    assert [(row.ts_ns, row.funding.rate) for row in rates] == [
        (futures[2].ts_event, Decimal("0.0001")),
        (futures[9].ts_event, Decimal("-0.0002")),
    ]
    assert [item.ts_ns for item in events] == sorted(item.ts_ns for item in events)
    # Bar-close evidence appears before funding at the same event timestamp.
    at_funding = [row.kind for row in events if row.ts_ns == futures[2].ts_event]
    assert at_funding == ["bars", "funding"]


def test_rejects_time_gaps_even_when_two_products_have_matching_gaps(tmp_path):
    _start, spot, futures = _dataset(tmp_path)
    kept = [index for index in range(len(spot)) if index != 3]
    with pytest.raises(EngineConflict, match="缺失"):
        list(align_carry_bars(
            [[spot[i] for i in kept]], [[futures[i] for i in kept]],
            symbol="ETHUSDT", interval="1m",
        ))



def test_rejects_incomplete_history_window_edges(tmp_path):
    _start, spot, futures = _dataset(tmp_path)
    first_ns = futures[0].ts_event - 60_000_000_000
    end_ns = futures[-1].ts_event + 60_000_000_000
    with pytest.raises(EngineConflict, match="开头"):
        list(align_carry_bars(
            [spot[1:]], [futures[1:]], symbol="ETHUSDT", interval="1m",
            start_ns=first_ns, end_ns=end_ns,
        ))
    with pytest.raises(EngineConflict, match="末尾"):
        list(align_carry_bars(
            [spot[:-1]], [futures[:-1]], symbol="ETHUSDT", interval="1m",
            start_ns=first_ns, end_ns=end_ns,
        ))

def test_rejects_one_product_missing_bar_and_does_not_forward_fill(tmp_path):
    start, _spot, _futures = _dataset(tmp_path, omit_spot=5)
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    try:
        runtime.ensure_tenant("dev", "development")
        for product in ("spot", "futures_um"):
            _intent(runtime, "dev", product=product)
        with pytest.raises(EngineConflict, match="时间戳不一致"):
            list(runtime.iter_funding_carry_history(
                "dev", "ETHUSDT", *_range(start), "1m",
            ))
    finally:
        runtime.close()


def test_rejects_absent_funding_without_synthesizing_zero(tmp_path):
    start, _, _ = _dataset(tmp_path, include_funding=False)
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    try:
        runtime.ensure_tenant("dev", "development")
        for product in ("spot", "futures_um"):
            _intent(runtime, "dev", product=product)
        with pytest.raises(EngineConflict, match="缺少真实历史资金费"):
            list(runtime.iter_funding_carry_history(
                "dev", "ETHUSDT", *_range(start), "1m",
            ))
    finally:
        runtime.close()


def test_development_subscription_gate_and_production_denial(tmp_path):
    start, _, _ = _dataset(tmp_path)
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    try:
        runtime.ensure_tenant("dev", "development")
        runtime.ensure_tenant("prod", "production")
        _intent(runtime, "dev", product="futures_um")
        with pytest.raises(EngineConflict, match="同时声明"):
            list(runtime.iter_funding_carry_history(
                "dev", "ETHUSDT", *_range(start), "1m",
            ))
        for product in ("futures_um", "spot"):
            _intent(runtime, "prod", product=product)
        with pytest.raises(EngineConflict, match="开发租户"):
            list(runtime.iter_funding_carry_history(
                "prod", "ETHUSDT", *_range(start), "1m",
            ))
    finally:
        runtime.close()
