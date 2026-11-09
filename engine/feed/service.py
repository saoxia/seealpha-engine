"""Run an independent, public-data-only continuous minute service."""

import argparse
import asyncio
import contextlib
import fcntl
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

from core.market.instruments import normalize_symbol
from core.paths import PROJECT_ROOT
from engine.connectors.binance.transport import KlineSocket, PublicREST

from .clock import ServerClock
from .feed import Feed
from .history import CatalogHistory
from .models import DAY, MINUTE, DataError, State, parse_message
from .store import Store

DEFAULT_ROOT = PROJECT_ROOT / "data/market_data/binance_um_1m"


def write_status(root, value):
    temporary = root / "status.json.tmp"
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
    os.replace(temporary, root / "status.json")


class Coordinator:
    def __init__(self, feed, rest, clock, history=None):
        self.feed, self.rest, self.clock, self.history = feed, rest, clock, history

    async def fetch(self, symbol, start, end):
        """Bounded pagination; missing/empty pages never advance the coverage checkpoint."""
        cursor = start
        while cursor < end:
            page_end = min(end, cursor + 1000 * MINUTE)
            rows = await self.rest.bars(symbol, cursor, page_end)
            if not rows or rows[0].start_ms != cursor:
                raise DataError(f"Missing minute: {symbol} {cursor}")
            expected = cursor
            for bar in rows:
                if bar.symbol != symbol or bar.start_ms != expected or bar.end_ms > page_end:
                    raise DataError(f"Non-contiguous REST page: {symbol} {expected}")
                expected = bar.end_ms
            self.feed.store.put(rows, "rest", self.clock.confirmed())
            cursor = expected

    async def load_history(self):
        if self.history is None:
            return
        store = self.feed.store
        end = self.clock.confirmed() // MINUTE * MINUTE
        for symbol in store.symbols:
            # Only import beyond the durable contiguous prefix. Read one overlap minute
            # so reopening an existing journal also validates its Catalog boundary.
            start = max(store.start_ms, store.next_ms(symbol) - MINUTE)
            for left, right in self.history.chunks(start, end):
                rows = await asyncio.to_thread(self.history.read_day, symbol, left, right)
                store.put(rows, "catalog", self.clock.confirmed())
                await asyncio.sleep(0)

    async def repair(self):
        feed, store = self.feed, self.feed.store
        epoch = feed.epoch
        end = self.clock.confirmed() // MINUTE * MINUTE
        for symbol in store.symbols:
            # Only the prefix after next_ms can contain holes. Buffered WS rows may lie
            # beyond a hole; they remain journaled and cannot bypass the cursor.
            for left, right in list(store.missing(symbol, store.next_ms(symbol), end)):
                await self.fetch(
                    symbol, max(store.start_ms, left - 2 * MINUTE), min(end, right + 2 * MINUTE)
                )
            await self.fetch(symbol, max(store.start_ms, end - 5 * MINUTE), end)
        return feed.recovered(self.clock.confirmed(), epoch)

    async def audit_day(self, day_start):
        """Reconcile a closed UTC day without rewriting previously consumed values."""
        store = self.feed.store
        end = day_start + DAY
        if end > self.clock.confirmed() // DAY * DAY:
            raise DataError("Audit requires a completed UTC day")
        checked = []
        for symbol in store.symbols:
            start = max(day_start, store.start_ms)
            if start >= end:
                continue
            if self.history:
                rows = await asyncio.to_thread(self.history.read_day, symbol, start, end)
                store.put(rows, "catalog", self.clock.confirmed())
            await self.fetch(symbol, start, end)
            checked.append(symbol)
        if checked:
            store.incident(
                "day_reconciled", {"day_start_ms": day_start, "symbols": checked}, self.clock.now()
            )
        return bool(checked)


async def serve(args, root, *, on_delivery=None, on_bar_update=None):
    rest, clock = PublicREST(symbols=args.symbols), ServerClock()
    await clock.sync(rest)
    start = clock.confirmed() // MINUTE * MINUTE - args.warmup_minutes * MINUTE
    store = Store(root / "minutes.sqlite", tuple(args.symbols), start)
    rollups = None
    intervals = getattr(args, "intervals", None)
    if intervals:
        from .aggregation import Rollups
        rollups = Rollups(store, root / "rollups.sqlite", intervals, on_bar_update)
        rollups.recover(clock.confirmed())

    def deliver(value):
        if rollups:
            rollups.update(value.bar, clock.confirmed(), is_closed=True,
                           allow_new_risk=value.allow_new_risk)
        if on_delivery:
            on_delivery(value)

    feed = Feed(store, warmup_minutes=args.warmup_minutes, on_delivery=deliver)
    history = CatalogHistory(args.catalog)
    coordinator = Coordinator(feed, rest, clock, history)
    queue = asyncio.Queue(maxsize=4096)
    loop = asyncio.get_running_loop()
    transport_error = None

    def enqueue(raw):
        nonlocal transport_error
        try:
            queue.put_nowait(raw)
        except asyncio.QueueFull:
            transport_error = "websocket_buffer_overflow"
            feed.degrade(transport_error)

    def reconnected():
        feed.transport_lost("websocket_reconnected_repair_required")

    socket = KlineSocket(
        args.symbols,
        lambda raw: loop.call_soon_threadsafe(enqueue, raw),
        lambda: loop.call_soon_threadsafe(reconnected),
        source=rest.source,
    )

    async def receive():
        nonlocal transport_error
        while True:
            raw = await queue.get()
            try:
                if raw[0] == "partial":
                    _, partial_bar, event = raw
                    now = clock.confirmed()
                    if now - feed.stale_ms <= event <= now + 2000:
                        rollups.update(partial_bar, now)
                    continue
                symbol, event, bar = parse_message(raw, args.symbols)
                # A server clock estimate carries uncertainty. Hold a final candle
                # until its entire minute is certainly in the past.
                if bar and bar.end_ms > clock.now() + 2000:
                    raise DataError("Future closed WebSocket candle")
                if bar and bar.end_ms > clock.confirmed():
                    await asyncio.sleep(max(0, (bar.end_ms - clock.confirmed()) / 1000))
                feed.observe(symbol, event, clock.confirmed(), bar)
            except Exception as exc:  # noqa: BLE001 -- persist failures and close the data gate
                transport_error = str(exc)
                feed.degrade("websocket_validation_error")
                store.incident("websocket_error", {"error": str(exc)}, clock.now())
            finally:
                queue.task_done()

    if rollups:
        def partial(bar, stamp):
            # Keep validation/consumer failures inside receive(), which closes the gate.
            enqueue(("partial", bar, stamp))
        rest.source.on_partial = partial

    worker = None
    receive_task = None
    started = time.monotonic()
    last_clock_sync = started
    last_repair = 0.0
    history_loaded = False
    audit_day = None
    pending_audit = None
    last_reconciled_day = None
    try:
        feed.clock_ok = True
        write_status(root, feed.status(clock.now()))
        await socket.connect()  # Subscribe first; receiver persists buffers during historical I/O.
        receive_task = asyncio.create_task(receive())
        worker = asyncio.create_task(coordinator.load_history())
        job = "history"
        while args.duration_seconds is None or time.monotonic() - started < args.duration_seconds:
            now_mono = time.monotonic()
            if now_mono - last_clock_sync >= 45:
                try:
                    await clock.sync(rest)
                except Exception as exc:  # noqa: BLE001 -- persist failures and close the data gate
                    feed.degrade("clock_sync_failed")
                    transport_error = str(exc)
                last_clock_sync = now_mono
            feed.clock_ok = clock.healthy
            if not socket.active:
                if feed.connected:
                    feed.transport_lost()
            else:
                feed.connected = True
            if receive_task.done():
                receive_task.result()
                raise DataError("WebSocket receiver stopped")
            feed.check(clock.now())
            if worker and worker.done():
                try:
                    result = worker.result()
                    if job == "history":
                        history_loaded = True
                    if job == "audit":
                        audit_day = pending_audit
                        if result:
                            last_reconciled_day = pending_audit
                except Exception as exc:  # noqa: BLE001 -- persist failures and close the data gate
                    feed.degrade(f"{job}_failed")
                    transport_error = str(exc)
                    store.incident("repair_error", {"job": job, "error": str(exc)}, clock.now())
                worker = None
                last_repair = now_mono
            if worker is None and not store.conflicted and now_mono - last_repair >= 5:
                if not history_loaded:
                    worker, job = asyncio.create_task(coordinator.load_history()), "history"
                elif feed.recovery_required:
                    worker, job = asyncio.create_task(coordinator.repair()), "repair"
                else:
                    yesterday = clock.confirmed() // DAY * DAY - DAY
                    if audit_day != yesterday:
                        pending_audit = yesterday
                        worker, job = asyncio.create_task(coordinator.audit_day(yesterday)), "audit"
            feed.flush(clock.confirmed())
            status = feed.status(clock.now())
            status["last_error"] = transport_error
            status["job"] = job if worker else None
            status["last_reconciled_day_ms"] = last_reconciled_day
            status["scope"] = "closed_1m_OHLCV_public_data_no_orders"
            if rollups:
                status["bar_updates"] = rollups.snapshot(clock.confirmed())
            write_status(root, status)
            await asyncio.sleep(0.25)
    finally:
        for task in (worker, receive_task):
            if task:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        try:
            await socket.close()
        finally:
            feed.transport_lost("service_stopped")
            feed.state = State.STOPPED
            status = feed.status(clock.now())
            status["state"], status["reason"] = State.STOPPED, "service_stopped"
            write_status(root, status)
            if rollups:
                rollups.close()
            store.close()


def positive_int(value):
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("Must be positive")
    return result


def main():
    parser = argparse.ArgumentParser(description="统一已闭合1分钟行情；仅公开行情，无交易权限")
    parser.add_argument("command", choices=("serve", "status"))
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"])
    parser.add_argument("--catalog", type=Path, default=PROJECT_ROOT / "data/catalog")
    parser.add_argument("--warmup-minutes", type=positive_int, default=135 * 1440)
    parser.add_argument("--duration-seconds", type=positive_int, help="有界验证运行时长")
    args = parser.parse_args()
    args.symbols = list(dict.fromkeys(normalize_symbol(s) for s in args.symbols))
    root = args.root.resolve()
    if args.command == "status":
        path = root / "status.json"
        if not path.exists():
            print(json.dumps({"state": "STOPPED", "data_ready": False, "reason": "not_started"}))
            return 1
        status = json.loads(path.read_text())
        # This command is for operators. Trading consumers must use the in-process gate,
        # not a saved READY value; host wall time is only an additional expiry check.
        if int(time.time() * 1000) > status["valid_until_ms"]:
            status.update(state="DEGRADED", data_ready=False, reason="status_expired")
        print(json.dumps(status, indent=2, ensure_ascii=False))
        return int(not status["data_ready"])
    root.mkdir(parents=True, exist_ok=True)
    with (root / "writer.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("This dataset already has an active writer")
        try:
            asyncio.run(serve(args, root))
        except KeyboardInterrupt:
            pass
        except Exception as exc:  # noqa: BLE001 -- persist failures and close the data gate
            now = int(datetime.now(UTC).timestamp() * 1000)
            write_status(
                root,
                {
                    "state": "DEGRADED",
                    "data_ready": False,
                    "reason": str(exc),
                    "checked_ms": now,
                    "valid_until_ms": now,
                },
            )
            print(f"行情服务未就绪：{exc}")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
