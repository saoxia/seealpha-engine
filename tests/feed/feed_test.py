import asyncio
import json
from decimal import Decimal

import pytest

from engine.feed.clock import ServerClock
from engine.feed.feed import Feed
from engine.feed.models import DAY, MINUTE, DataError, MinuteBar, State, parse_message
from engine.feed.service import Coordinator
from engine.feed.store import Store

BASE = 1_800_000_000_000 // DAY * DAY
SYMBOL = "BTCUSDT"


def bar(index, *, symbol=SYMBOL, price="100", volume="2"):
    p = Decimal(price)
    return MinuteBar(symbol, BASE + index * MINUTE, p, p + 1, p - 1, p, Decimal(volume))


class Clock:
    def __init__(self, minute=3):
        self.value = BASE + minute * MINUTE + 3000

    def now(self):
        return self.value

    confirmed = now


class REST:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def bars(self, symbol, start, end):
        self.calls.append((symbol, start, end))
        await asyncio.sleep(0)
        return [b for b in self.rows if b.symbol == symbol and start <= b.start_ms < end][:1000]


@pytest.fixture
def store(tmp_path):
    value = Store(tmp_path / "minutes.sqlite", (SYMBOL,), BASE)
    yield value
    value.close()


def ready_feed(store, clock, events):
    feed = Feed(store, warmup_minutes=3, on_delivery=events.append)
    feed.connected = feed.clock_ok = True
    feed.observe(SYMBOL, clock.now(), clock.now())
    rest = REST([bar(i) for i in range(3)])
    assert asyncio.run(Coordinator(feed, rest, clock).repair())
    assert feed.state == State.READY
    return feed


def test_cold_start_overlap_and_live_delivery_are_distinct(store):
    clock, events = Clock(), []
    # Realtime arrived before history, and is persisted rather than skipped.
    store.put([bar(2)], "websocket", clock.now())
    assert store.next_ms(SYMBOL) == BASE
    feed = ready_feed(store, clock, events)
    assert [e.bar for e in events] == [bar(i) for i in range(3)]
    assert all(not e.allow_new_risk for e in events)
    clock.value += MINUTE
    feed.observe(SYMBOL, clock.now(), clock.now(), bar(3))
    feed.flush(clock.now())
    assert events[-1].allow_new_risk
    feed.observe(SYMBOL, clock.now(), clock.now(), bar(3))
    feed.flush(clock.now())
    assert len(events) == 4  # Overlap never reaches a consumer twice in the session.


def test_disconnect_catchup_does_not_emit_delayed_signals(store):
    clock, events = Clock(), []
    feed = ready_feed(store, clock, events)
    feed.transport_lost()
    clock.value += 3 * MINUTE
    feed.connected = True
    feed.observe(SYMBOL, clock.now(), clock.now(), bar(5))
    assert not feed.check(clock.now())
    assert store.next_ms(SYMBOL) == BASE + 3 * MINUTE
    rest = REST([bar(i) for i in range(6)])
    assert asyncio.run(Coordinator(feed, rest, clock).repair())
    assert [e.bar.start_ms for e in events] == [BASE + i * MINUTE for i in range(6)]
    assert not any(e.allow_new_risk for e in events)
    clock.value += MINUTE
    feed.observe(SYMBOL, clock.now(), clock.now(), bar(6))
    feed.flush(clock.now())
    assert events[-1].allow_new_risk


def test_out_of_order_and_interior_gap_do_not_advance_checkpoint(store):
    now = Clock(6).now()
    store.put([bar(4), bar(2), bar(0)], "websocket", now)
    assert store.next_ms(SYMBOL) == BASE + MINUTE
    assert list(store.missing(SYMBOL, BASE, BASE + 6 * MINUTE)) == [
        (BASE + MINUTE, BASE + 2 * MINUTE),
        (BASE + 3 * MINUTE, BASE + 4 * MINUTE),
        (BASE + 5 * MINUTE, BASE + 6 * MINUTE),
    ]
    store.put([bar(1), bar(3)], "rest", now)
    assert store.next_ms(SYMBOL) == BASE + 5 * MINUTE


def test_conflicting_source_rolls_back_page_and_latches_across_restart(tmp_path):
    path = tmp_path / "minutes.sqlite"
    store = Store(path, (SYMBOL,), BASE)
    store.put([bar(0)], "catalog", Clock().now())
    with pytest.raises(DataError, match="Conflicting"):
        store.put([bar(1), bar(0, price="105")], "rest", Clock().now())
    assert store.next_ms(SYMBOL) == BASE + MINUTE
    assert list(store.read(SYMBOL, BASE, BASE + 3 * MINUTE)) == [bar(0)]
    store.close()
    store = Store(path, (SYMBOL,), BASE + 100 * MINUTE)
    assert store.start_ms == BASE
    assert store.conflicted
    store.put([bar(1), bar(2)], "rest", Clock().now())
    feed = Feed(store, warmup_minutes=3)
    feed.connected = feed.clock_ok = True
    feed.observe(SYMBOL, Clock().now(), Clock().now())
    assert not feed.recovered(Clock().now(), feed.epoch)
    assert not feed.check(Clock().now())
    store.close()


def test_restart_replays_tail_and_never_trusts_old_ready(tmp_path):
    path = tmp_path / "minutes.sqlite"
    store = Store(path, (SYMBOL,), BASE)
    store.put([bar(i) for i in range(6)], "rest", Clock(6).now())
    # Even a corrupted checkpoint cannot hide missing minutes after restart.
    with store.db:
        store.db.execute("UPDATE cursors SET next_ms=?", (BASE + 1000 * MINUTE,))
    store.close()
    store = Store(path, (SYMBOL,), BASE)
    assert store.next_ms(SYMBOL) == BASE + 6 * MINUTE
    clock, events = Clock(6), []
    feed = Feed(store, warmup_minutes=3, on_delivery=events.append)
    assert not feed.check(clock.now())
    feed.connected = feed.clock_ok = True
    feed.observe(SYMBOL, clock.now(), clock.now())
    assert asyncio.run(Coordinator(feed, REST([bar(i) for i in range(6)]), clock).repair())
    assert [e.bar for e in events] == [bar(i) for i in range(3, 6)]
    assert not any(e.allow_new_risk for e in events)
    store.close()


@pytest.mark.parametrize("condition", ["stale", "clock", "disconnected", "missing"])
def test_gate_closes_and_requires_repair_after_health_recovers(store, condition):
    clock = Clock()
    feed = ready_feed(store, clock, [])
    if condition == "stale":
        clock.value += 16_000
    elif condition == "clock":
        feed.clock_ok = False
    elif condition == "disconnected":
        feed.connected = False
    else:
        clock.value += MINUTE
        feed.observe(SYMBOL, clock.now(), clock.now())
    assert not feed.check(clock.now())
    assert feed.state == State.DEGRADED
    feed.clock_ok = feed.connected = True
    feed.observe(SYMBOL, clock.now(), clock.now())
    store.put([bar(i) for i in range((clock.now() - BASE) // MINUTE)], "rest", clock.now())
    assert not feed.check(clock.now())  # Fresh heartbeat alone cannot reopen the gate.


def test_failure_during_repair_cannot_reopen_gate(store):
    clock = Clock()
    feed = Feed(store, warmup_minutes=3)
    feed.connected = feed.clock_ok = True
    feed.observe(SYMBOL, clock.now(), clock.now())

    class DisconnectingREST(REST):
        async def bars(self, *args):
            feed.transport_lost()
            feed.connected = True
            feed.observe(SYMBOL, clock.now(), clock.now())
            return await super().bars(*args)

    assert not asyncio.run(
        Coordinator(feed, DisconnectingREST([bar(i) for i in range(3)]), clock).repair()
    )
    assert not feed.check(clock.now())


@pytest.mark.parametrize("indices", [[], [1, 2], [0, 2]])
def test_missing_rest_page_does_not_fabricate_bars(store, indices):
    feed = Feed(store, warmup_minutes=3)
    with pytest.raises(DataError):
        asyncio.run(Coordinator(feed, REST([bar(i) for i in indices]), Clock()).repair())
    assert store.next_ms(SYMBOL) == BASE
    assert not feed.check(Clock().now())


def test_pagination_is_bounded_and_rechecks_overlap(store):
    clock = Clock(2100)
    feed = Feed(store, warmup_minutes=2100)
    rest = REST([bar(i) for i in range(2100)])
    coordinator = Coordinator(feed, rest, clock)
    asyncio.run(coordinator.fetch(SYMBOL, BASE, BASE + 2100 * MINUTE))
    assert len(rest.calls) == 3
    assert store.next_ms(SYMBOL) == BASE + 2100 * MINUTE


def test_all_symbols_must_be_contiguous_before_ready(tmp_path):
    symbols = (SYMBOL, "ETHUSDT")
    store = Store(tmp_path / "minutes.sqlite", symbols, BASE)
    clock = Clock()
    feed = Feed(store, warmup_minutes=3)
    feed.connected = feed.clock_ok = True
    for symbol in symbols:
        feed.observe(symbol, clock.now(), clock.now())
    store.put([bar(i) for i in range(3)], "rest", clock.now())
    assert not feed.recovered(clock.now(), feed.epoch)
    store.put([bar(i, symbol="ETHUSDT") for i in range(3)], "rest", clock.now())
    assert feed.recovered(clock.now(), feed.epoch)
    store.close()


def test_consumer_failure_closes_gate_until_restart(store):
    clock = Clock()
    feed = ready_feed(store, clock, [])

    def fail(_):
        raise RuntimeError("consumer unavailable")

    feed.on_delivery = fail
    clock.value += MINUTE
    feed.observe(SYMBOL, clock.now(), clock.now(), bar(3))
    with pytest.raises(RuntimeError, match="consumer"):
        feed.flush(clock.now())
    assert not feed.recovered(clock.now(), feed.epoch)
    assert not feed.check(clock.now())


def test_native_trade_heartbeat_and_closed_bar_delivery():
    symbol, event, value = parse_message((SYMBOL, BASE + 100, None), (SYMBOL,))
    assert symbol == SYMBOL and event > BASE and value is None
    assert parse_message((SYMBOL, BASE + MINUTE, bar(0)), (SYMBOL,))[2] == bar(0)


@pytest.mark.parametrize(
    "item",
    [
        None,
        "raw-json",
        ("ETHUSDT", BASE, None),
        (SYMBOL, "bad", None),
        (SYMBOL, BASE, bar(0)),
        (SYMBOL, BASE + MINUTE, object()),
    ],
)
def test_invalid_native_delivery_is_rejected(item):
    with pytest.raises(DataError):
        parse_message(item, (SYMBOL,))


def test_native_bar_preserves_closed_minute_boundaries_and_decimals():
    from nautilus_trader.model import Bar, BarType

    from engine.connectors.binance.native import minute_bar
    from nautilus_support import TestInstrumentProvider

    inst = TestInstrumentProvider.btcusdt_perp_binance()
    kind = BarType.from_str(f"{inst.id}-1-MINUTE-LAST-EXTERNAL")
    ts = (BASE + MINUTE - 1) * 1_000_000
    native = Bar(kind, *[inst.make_price(x) for x in (100, 101, 99, 100)], inst.make_qty(2), ts, ts)
    assert minute_bar(native) == bar(0)


def test_clock_sync_failure_invalidates_gate_without_losing_monotonic_clock():
    class TimeREST:
        async def get(self, _):
            return {"serverTime": BASE}

    class FailedREST:
        async def get(self, _):
            raise OSError("offline")

    clock = ServerClock()
    asyncio.run(clock.sync(TimeREST()))
    assert clock.healthy
    with pytest.raises(OSError):
        asyncio.run(clock.sync(FailedREST()))
    assert not clock.healthy
    assert clock.now() >= BASE


def test_future_bar_rejected_atomically_and_volume_noise_is_tolerated(store):
    store.put([bar(0)], "catalog", Clock().now())
    store.put([bar(0, volume="2.000000000001")], "rest", Clock().now())
    with pytest.raises(DataError):
        store.put([bar(1), bar(4)], "rest", Clock().now())
    assert store.next_ms(SYMBOL) == BASE + MINUTE


def test_completed_day_audit_conflict_blocks_feed(store):
    clock = Clock(1440)
    store.put([bar(i) for i in range(1440)], "websocket", clock.now())
    feed = Feed(store, warmup_minutes=3)
    rest = REST([bar(i, price="105" if i == 100 else "100") for i in range(1440)])
    with pytest.raises(DataError):
        asyncio.run(Coordinator(feed, rest, clock).audit_day(BASE))
    assert store.conflicted
    assert list(store.read(SYMBOL, BASE + 100 * MINUTE, BASE + 101 * MINUTE)) == [bar(100)]


def test_nautilus_conversion_matches_historical_timestamp():
    from nautilus_support import TestInstrumentProvider

    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    value = bar(0, symbol="ETHUSDT").to_nautilus(instrument)
    assert value.ts_event == (BASE + MINUTE - 1) * 1_000_000
    assert str(value.bar_type) == "ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL"


def test_existing_catalog_history_and_rest_bridge(tmp_path):
    from engine.feed.history import CatalogHistory
    from engine.pipeline.catalog import ParquetDataCatalog
    from nautilus_support import TestInstrumentProvider

    symbol = "ETHUSDT"
    catalog = ParquetDataCatalog(str(tmp_path / "catalog"))
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    historical = [bar(i, symbol=symbol) for i in range(2)]
    catalog.write_data([b.to_nautilus(instrument) for b in historical])
    store = Store(tmp_path / "minutes.sqlite", (symbol,), BASE)
    clock = Clock()
    events = []
    feed = Feed(store, warmup_minutes=3, on_delivery=events.append)
    feed.connected = feed.clock_ok = True
    feed.observe(symbol, clock.now(), clock.now(), bar(2, symbol=symbol))
    coordinator = Coordinator(
        feed,
        REST(historical + [bar(2, symbol=symbol)]),
        clock,
        CatalogHistory(tmp_path / "catalog"),
    )

    async def run():
        await coordinator.load_history()
        assert await coordinator.repair()

    asyncio.run(run())
    assert [e.bar for e in events] == historical + [bar(2, symbol=symbol)]
    assert all(not e.allow_new_risk for e in events)
    assert (
        store.db.execute("SELECT count(*) FROM provenance WHERE source='rest'").fetchone()[0] == 3
    )
    store.close()


def test_retry_after_is_respected_without_duplicate_data(monkeypatch):
    from urllib.error import HTTPError

    from engine.connectors.binance.transport import PublicREST

    rest = PublicREST(minimum_interval=0)
    calls, delays = [], []

    def request(endpoint, params):
        calls.append((endpoint, params))
        if len(calls) == 1:
            raise HTTPError(
                "https://fapi.binance.com", 429, "Rate limited", {"Retry-After": "7"}, None
            )
        return {"serverTime": BASE}

    async def sleep(delay):
        delays.append(delay)

    monkeypatch.setattr(rest, "_request", request)
    monkeypatch.setattr("engine.connectors.binance.transport.asyncio.sleep", sleep)
    assert asyncio.run(rest.get("time")) == {"serverTime": BASE}
    assert 7 in delays
    assert len(calls) == 2


def test_status_command_rejects_expired_ready(tmp_path, monkeypatch, capsys):
    from engine.feed.service import main, write_status

    write_status(tmp_path, {"state": "READY", "data_ready": True, "valid_until_ms": 1})
    monkeypatch.setattr("sys.argv", ["engine.feed.service", "status", "--root", str(tmp_path)])
    assert main() == 1
    assert json.loads(capsys.readouterr().out)["reason"] == "status_expired"
