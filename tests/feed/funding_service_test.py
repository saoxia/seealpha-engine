"""Offline funding fixtures: only confirmed REST settlements become history."""

import asyncio
from decimal import Decimal

import pytest
from nautilus_trader.model import FundingRateUpdate, InstrumentId

from core.market.instruments import instrument_id_for
from core.market.models import DataError
from engine.market_data.collection.funding_service import (
    FundingStore,
    collect_funding,
    read_funding,
)


def mark(event_ms, next_ms, rate):
    return {"e": "markPriceUpdate", "s": "ETHUSDT", "E": event_ms,
            "T": next_ms, "r": rate}


def settlement(stamp, rate):
    ns = stamp * 1_000_000
    return FundingRateUpdate(
        instrument_id=InstrumentId.from_str(instrument_id_for("ETHUSDT")),
        rate=Decimal(rate), ts_event=ns, ts_init=ns, next_funding_ns=ns,
    )


def test_funding_store_retains_one_pending_quote_and_immutable_actuals(tmp_path):
    path = tmp_path / "funding.sqlite"
    store = FundingStore(path, "ETHUSDT")
    assert store.update_estimate(mark(100, 200, "0.0001"))
    assert store.update_estimate(mark(110, 200, "-0.0002"))
    assert not store.update_estimate(mark(100, 200, "0.0001"))
    with pytest.raises(DataError, match="Conflicting funding estimate"):
        store.update_estimate(mark(110, 200, "0.0004"))
    assert store.latest()["rate"] == "-0.0002"
    assert store.last_settlement_ms() is None
    store.persist_settlements([settlement(90, "0.0001")])
    store.persist_settlements([settlement(90, "0.000100")])
    with pytest.raises(DataError, match="Conflicting official"):
        store.persist_settlements([settlement(90, "0.0005")])
    assert store.update_estimate(mark(210, 300, "0.0003"))
    store.persist_settlements([settlement(200, "-0.00025")])
    store.close()
    view = read_funding(path, "ETHUSDT", now_ms=220)
    assert view["current"]["next_funding_ms"] == 300
    assert view["current"]["is_pending"] is True
    assert [(row["funding_time_ms"], row["rate"]) for row in view["settlements"]] == [
        (200, "-0.00025"), (90, "0.0001"),
    ]
    assert FundingStore(path, "ETHUSDT").last_settlement_ms() == 200


def test_funding_rejects_invalid_payloads_without_writing_history(tmp_path):
    store = FundingStore(tmp_path / "funding.sqlite", "ETHUSDT")
    for bad in (
        mark(100, 200, "NaN"),
        mark(100, 100, "0.1"),
        {**mark(100, 200, "0.1"), "s": "BTCUSDT"},
        {**mark(100, 200, "0.1"), "E": True},
    ):
        with pytest.raises(DataError):
            store.update_estimate(bad)
    assert store.latest() is None
    store.close()


def test_collector_rest_reconciles_startup_then_only_new_actual_settlement(tmp_path):
    async def scenario():
        requests = []
        seen = asyncio.Event()

        class REST:
            async def get(self, endpoint, **kwargs):
                assert endpoint == "fundingRate"
                requests.append(kwargs)
                if len(requests) == 1:
                    return [{"symbol": "ETHUSDT", "fundingTime": 90,
                             "fundingRate": "0.0001"}]
                seen.set()
                return [{"symbol": "ETHUSDT", "fundingTime": 200,
                         "fundingRate": "-0.00025"}]

        class Socket:
            async def stream(self, _symbol):
                yield {"kind": "connected"}
                yield {"kind": "mark", "row": mark(100, 200, "0.005")}
                yield {"kind": "mark", "row": mark(210, 300, "0.006")}
                await asyncio.Event().wait()

        path = tmp_path / "funding.sqlite"
        task = asyncio.create_task(collect_funding(
            "ETHUSDT", path, rest=REST(), socket=Socket(),
        ))
        try:
            await asyncio.wait_for(seen.wait(), 3)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        view = read_funding(path, "ETHUSDT", now_ms=220)
        assert [row["funding_time_ms"] for row in view["settlements"]] == [200, 90]
        assert view["settlements"][0]["rate"] == "-0.00025"
        assert view["current"]["rate"] == "0.006"
        assert requests[0]["startTime"] == 0
        assert requests[1]["startTime"] == 91

    asyncio.run(scenario())


def test_reader_does_not_create_missing_storage(tmp_path):
    assert read_funding(tmp_path / "absent.sqlite", "ETHUSDT")["settlements"] == []
    assert not (tmp_path / "absent.sqlite").exists()
