"""Synthetic USD-M public funding history, no exchange network access."""

import asyncio
from decimal import Decimal

import pytest

from engine.pipeline.funding_data import fetch_history


def test_history_paginates_exclusive_window_and_preserves_signed_rate():
    calls = []
    async def get_page(endpoint, **params):
        calls.append((endpoint, params))
        if len(calls) == 1:
            return [
                {"symbol": "ETHUSDT", "fundingTime": 10, "fundingRate": "0.0001"},
                {"symbol": "ETHUSDT", "fundingTime": 20, "fundingRate": "-0.0002"},
            ]
        return [{"symbol": "ETHUSDT", "fundingTime": 30, "fundingRate": "0.0"}]
    out = asyncio.run(fetch_history("ETHUSDT", 0, 40, get_page, page_size=2))
    assert [x.rate for x in out] == [Decimal("0.0001"), Decimal("-0.0002"), Decimal(0)]
    assert [row[1]["startTime"] for row in calls] == [0, 21]
    assert all(x.next_funding_ns == x.ts_event for x in out)


@pytest.mark.parametrize("page", [
    [{"symbol": "BTCUSDT", "fundingTime": 10, "fundingRate": "0.1"}],
    [{"symbol": "ETHUSDT", "fundingTime": 10, "fundingRate": "NaN"}],
    [{"symbol": "ETHUSDT", "fundingTime": 30, "fundingRate": "0.1"}],
])
def test_history_rejects_invalid_identity_numeric_and_bounds(page):
    async def get_page(*_args, **_kwargs):
        return page
    with pytest.raises(ValueError):
        asyncio.run(fetch_history("ETHUSDT", 0, 30, get_page))
