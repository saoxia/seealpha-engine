"""Product-specific data source entrypoints preserve the legacy call contract."""

import asyncio
import sys
from types import ModuleType

import pytest

from engine.market_data.sources.binance.options import collection as options
from engine.market_data.sources.binance.perpetual import collection as perpetual
from engine.market_data.sources.binance.spot import collection as spot


@pytest.mark.parametrize(
    ("source", "expected_product"),
    [(perpetual, "futures_um"), (spot, "spot")],
)
def test_batch_source_dispatches_with_fixed_product(monkeypatch, source, expected_product):
    calls = []

    async def legacy(manifest, managed_root, catalog, *, project, socket_factory, product):
        calls.append((manifest, managed_root, catalog, project, socket_factory, product))
        return "ok"

    fake = ModuleType("engine.market_data.collection.futures_service")
    fake.serve_futures_batch = legacy
    monkeypatch.setitem(sys.modules, fake.__name__, fake)
    result = asyncio.run(source.serve_batch("manifest", "root", "catalog", project="project", socket_factory="socket"))
    assert result == "ok"
    assert calls == [("manifest", "root", "catalog", "project", "socket", expected_product)]


def test_options_batch_source_preserves_arguments(monkeypatch):
    calls = []

    async def legacy(manifest, managed_root, catalog):
        calls.append((manifest, managed_root, catalog))
        return "ok"

    fake = ModuleType("engine.market_data.collection.options_service")
    fake.serve_batch = legacy
    monkeypatch.setitem(sys.modules, fake.__name__, fake)
    result = asyncio.run(options.serve_batch("manifest", "root", "catalog"))
    assert result == "ok"
    assert calls == [("manifest", "root", "catalog")]
