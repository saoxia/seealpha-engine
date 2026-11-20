"""Source ownership, legacy imports and market routing remain compatible."""

import asyncio
import importlib

import pytest

from engine.market_data.sources.binance.options import collection as options
from engine.market_data.sources.binance.perpetual import collection as perpetual
from engine.market_data.sources.binance.spot import collection as spot


@pytest.mark.parametrize(
    ("source", "expected_product"),
    [(perpetual, "futures_um"), (spot, "spot")],
)
def test_batch_source_dispatches_with_fixed_product(monkeypatch, source, expected_product):
    from engine.market_data.sources.binance.common import batch

    calls = []

    async def fake(manifest, managed_root, catalog, *, project, socket_factory, product):
        calls.append((manifest, managed_root, catalog, project, socket_factory, product))
        return "ok"

    monkeypatch.setattr(batch, "serve_futures_batch", fake)
    result = asyncio.run(source.serve_batch("manifest", "root", "catalog", project="project", socket_factory="socket"))
    assert result == "ok"
    assert calls == [("manifest", "root", "catalog", "project", "socket", expected_product)]


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("engine.market_data.collection.futures_service", "engine.market_data.sources.binance.common.batch"),
        ("engine.market_data.collection.pair_service", "engine.market_data.sources.binance.common.pair_collection"),
        ("engine.market_data.collection.options_service", "engine.market_data.sources.binance.options.collection"),
        ("engine.market_data.collection.funding_service", "engine.market_data.sources.binance.perpetual.funding"),
        ("engine.market_data.calculation.pair_calculation", "engine.market_data.sources.binance.common.calculation"),
        ("engine.market_data.repair.pair_data", "engine.market_data.sources.binance.common.history"),
        ("engine.market_data.repair.options_data", "engine.market_data.sources.binance.options.repair"),
        ("engine.market_data.repair.pair_repair", "engine.market_data.sources.binance.common.reconcile"),
        ("engine.market_data.repair.funding_data", "engine.market_data.sources.binance.perpetual.funding_repair"),
        ("engine.market_data.repair.convert_binance_bars", "engine.market_data.sources.binance.common.convert_bars"),
        ("engine.market_data.repair.convert_binance_trades", "engine.market_data.sources.binance.common.convert_trades"),
        ("engine.market_data.repair.sync_binance_batch", "engine.market_data.sources.binance.common.sync_batch"),
        ("engine.market_data.repair.sync_binance_file", "engine.market_data.sources.binance.common.sync_file"),
    ],
)
def test_legacy_module_alias_is_canonical(old, new):
    assert importlib.import_module(old) is importlib.import_module(new)


@pytest.mark.parametrize("source,invalid_product", [
    ("engine.market_data.sources.binance.perpetual.calculation", "spot"),
    ("engine.market_data.sources.binance.spot.calculation", "futures_um"),
])
def test_calculator_rejects_other_markets(source, invalid_product):
    calculator = importlib.import_module(source)
    with pytest.raises(ValueError):
        asyncio.run(calculator.serve_calculation({"product": invalid_product}, None, None))


@pytest.mark.parametrize("source,invalid_product", [
    ("engine.market_data.sources.binance.perpetual.repair", "spot"),
    ("engine.market_data.sources.binance.spot.repair", "futures_um"),
])
def test_repair_rejects_other_markets(source, invalid_product):
    repair = importlib.import_module(source)
    with pytest.raises(ValueError):
        asyncio.run(repair.serve_history({"product": invalid_product}, None, None))
