"""Binance products own separate entrypoints while retaining compatible legacy modules."""

import asyncio
import importlib

import pytest

from engine.market_data.sources.binance_options import collection as options
from engine.market_data.sources.binance_perpetual import collection as perpetual
from engine.market_data.sources.binance_spot import collection as spot


@pytest.mark.parametrize(
    ("source", "expected_product"),
    [(perpetual, "futures_um"), (spot, "spot")],
)
def test_batch_source_dispatches_with_fixed_product(monkeypatch, source, expected_product):
    from engine.market_data.sources.binance_shared import batch

    calls = []

    async def collect(manifest, managed_root, catalog, *, project, socket_factory, product):
        calls.append((manifest, managed_root, catalog, project, socket_factory, product))
        return "ok"

    monkeypatch.setattr(batch, "serve_futures_batch", collect)
    result = asyncio.run(source.serve_batch("manifest", "root", "catalog", project="project", socket_factory="socket"))
    assert result == "ok"
    assert calls == [("manifest", "root", "catalog", "project", "socket", expected_product)]


def test_options_batch_uses_source_owned_implementation(monkeypatch):
    from engine.market_data.sources.binance_options import implementation

    calls = []

    async def collect(manifest, managed_root, catalog):
        calls.append((manifest, managed_root, catalog))
        return "ok"

    monkeypatch.setattr(implementation, "serve_batch", collect)
    result = asyncio.run(options.serve_batch("manifest", "root", "catalog"))
    assert result == "ok"
    assert calls == [("manifest", "root", "catalog")]


@pytest.mark.parametrize(
    ("legacy", "canonical"),
    [
        ("engine.market_data.collection.futures_service", "engine.market_data.sources.binance_shared.batch"),
        ("engine.market_data.collection.pair_service", "engine.market_data.sources.binance_shared.trades"),
        ("engine.market_data.collection.options_service", "engine.market_data.sources.binance_options.implementation"),
        ("engine.market_data.collection.funding_service", "engine.market_data.sources.binance_perpetual.funding"),
        ("engine.market_data.calculation.pair_calculation", "engine.market_data.sources.binance_shared.calculation"),
        ("engine.market_data.repair.pair_data", "engine.market_data.sources.binance_shared.history"),
        ("engine.market_data.repair.options_data", "engine.market_data.sources.binance_options.repair"),
        ("engine.market_data.repair.funding_data", "engine.market_data.sources.binance_perpetual.funding_history"),
    ],
)
def test_legacy_import_resolves_same_implementation(legacy, canonical):
    assert importlib.import_module(legacy) is importlib.import_module(canonical)


@pytest.mark.parametrize("product", ["spot", "futures_um"])
def test_product_calculation_does_not_accept_another_market(product):
    from engine.market_data.sources.binance_perpetual import calculation as perp_calculation
    from engine.market_data.sources.binance_spot import calculation as spot_calculation

    module = spot_calculation if product == "futures_um" else perp_calculation
    with pytest.raises(ValueError, match="Expected Binance"):
        asyncio.run(module.serve_calculation({"product": product}, None, None))


@pytest.mark.parametrize("product", ["spot", "futures_um"])
def test_product_history_does_not_accept_another_market(product):
    from engine.market_data.sources.binance_perpetual import repair as perp_repair
    from engine.market_data.sources.binance_spot import repair as spot_repair

    module = spot_repair if product == "futures_um" else perp_repair
    with pytest.raises(ValueError, match="Expected Binance"):
        asyncio.run(module.serve_history({"product": product}, None, None))
