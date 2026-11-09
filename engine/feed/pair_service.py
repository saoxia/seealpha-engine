"""Event-driven management collection; never grants strategy trading readiness."""

import asyncio
import time

from core.market.models import DataError
from engine.connectors.binance.pair_data import PairREST, PairTradeSocket

from .clock import ServerClock
from .pair_store import PairStore, journal_access
from .service import write_status


async def serve_pair(policy, root, catalog, *, calculate=True):
    rest = PairREST(symbols=(policy["symbol"],))
    clock = ServerClock()
    with journal_access(root):
        store = PairStore(root / "collection.sqlite", policy["symbol"], policy["base_level"], policy["intervals"])
    queue = asyncio.Queue(maxsize=10000)
    connected, last_sync, last_trade = False, 0, None
    error = None
    snapshot_error = None

    def publish(*, stopped=False):
        now = clock.now() if clock.base_ms else int(time.time() * 1000)
        stale = last_trade is None or not 0 <= now - last_trade <= 15000
        failure = error or snapshot_error
        healthy = not stopped and connected and not failure and not store.conflicted and clock.healthy and not stale
        write_status(root, {"state": "STOPPED" if stopped else "READY" if healthy else "DEGRADED",
                     "data_ready": healthy, "reason": "service_stopped" if stopped else failure or
                     ("raw_trade_stream_disconnected" if not connected else "raw_trade_conflict" if store.conflicted else
                      "clock_unhealthy" if not clock.healthy else
                      "raw_trades_stale" if stale else "collecting"),
                     "checked_ms": now, "valid_until_ms": now if stopped else now + 5000,
                     "symbols": {} if stopped else {policy["symbol"]: {"last_websocket_ms": last_trade,
                         "last_update_ms": last_trade}},
                     "bar_updates": [] if stopped else list(store.latest.values()),
                     "last_error": failure, "gap_count": 0, "allow_new_risk": False,
                     "scope": "managed_public_evidence_no_strategy_readiness",
                     "confirmed_ms": clock.confirmed() if clock.healthy else None,
                     "backlog": queue.qsize() + len(pending_events), "last_processed_ms": last_trade})

    async def receive():
        # Backpressure never silently drops a trade when the writer is slower.
        stream = PairTradeSocket().stream(policy["symbol"])
        try:
            async for event in stream:
                await queue.put(event)
            raise ConnectionError("Raw trade stream stopped")
        finally:
            await stream.aclose()

    receiver = asyncio.create_task(receive())
    pending_events = []
    snapshot_pending, retry_delay = False, 1
    try:
        while True:
            if receiver.done():
                await receiver
            if not pending_events:
                try:
                    pending_events = [await asyncio.wait_for(queue.get(), timeout=1)]
                    while not queue.empty() and len(pending_events) < 1000:
                        pending_events.append(queue.get_nowait())
                except TimeoutError:
                    pass
            try:
                if time.monotonic() - last_sync > 45:
                    await clock.sync(rest)
                    last_sync = time.monotonic()
            except (DataError, OSError, TimeoutError) as exc:
                # Keep the buffered batch until the clock is trustworthy again.
                error = str(exc)
                publish()
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)
                continue
            retry_delay = 1
            rows = []
            for event in pending_events:
                if event["kind"] == "connected":
                    connected, snapshot_pending, snapshot_error = True, True, None
                elif event["kind"] == "disconnected":
                    connected = False
                    error = event["error"]
                else:
                    rows.append(event["row"])
            if rows:
                # Raw identity checks, journal writes and dirty ranges precede derived
                # acknowledgements. Integrity failures stop rather than dropping data.
                with journal_access(root):
                    accepted = store.ticks(rows, clock.confirmed(), quarantine_invalid=True, calculate=calculate)
                if accepted:
                    last_trade = max(last_trade or 0, *(row[1] for row in accepted))
            elif calculate and store.db.execute("SELECT 1 FROM pending_updates LIMIT 1").fetchone():
                with journal_access(root):
                    store.process_changes(clock.confirmed())
            pending_events = []
            error = None if connected else error
            # One bounded REST read per connection. Failure stays visible, but
            # only a new connection may request another recent-trade snapshot.
            if snapshot_pending and connected:
                snapshot_pending = False
                try:
                    snapshot = await rest.get("trades", symbol=policy["symbol"], limit=1000)
                except (DataError, OSError, TimeoutError) as exc:
                    snapshot_error = f"raw_trade_handover_failed: {exc}"
                else:
                    with journal_access(root):
                        store.ticks(snapshot, clock.confirmed(), quarantine_invalid=True, calculate=calculate)
                    snapshot_error = None
            publish()
    finally:
        receiver.cancel()
        await asyncio.gather(receiver, return_exceptions=True)
        try:
            publish(stopped=True)
        finally:
            store.close()
