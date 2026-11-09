"""Independent calculation consumer; durable source changes own its recovery."""

import asyncio
import fcntl
import json
import time

from core.market.models import DataError
from core.paths import PROJECT_ROOT

from .history import CatalogHistory
from .pair_store import PairStore, journal_access
from .service import write_status


async def serve_calculation(policy, root, catalog):
    state_root = root / "calculation"
    state_root.mkdir(parents=True, exist_ok=True)
    with journal_access(root):
        store = PairStore(root / "collection.sqlite", policy["symbol"], policy["base_level"], policy["intervals"])
    last_processed = None
    state = {"state": "WARMING_UP", "backlog": None, "last_error": None}

    async def heartbeat():
        while True:
            now = int(time.time() * 1000)
            write_status(state_root, {**state, "checked_ms": now, "valid_until_ms": now + 15000})
            await asyncio.sleep(5)

    history_error, history_busy, history_last_processed = None, False, None

    def consume_history(path):
        # The thread owns its lease until the write actually ends, even if its
        # awaiting coroutine is cancelled during service shutdown.
        lock_path = PROJECT_ROOT / ".local/run/api/data" / policy["product"] / f"{policy['symbol']}.job.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a") as lease:
            fcntl.flock(lease, fcntl.LOCK_SH | fcntl.LOCK_NB)
            return CatalogHistory(catalog).consume_change(path)

    async def history_changes():
        nonlocal history_error, history_busy, history_last_processed
        while True:
            try:
                changes = sorted((root / "history/changes").glob("*.json"))
                if changes:
                    history_busy = True
                    if await asyncio.to_thread(consume_history, changes[0]):
                        history_last_processed = int(time.time() * 1000)
                history_error = None
            except (OSError, ValueError, RuntimeError, DataError) as exc:
                history_error = str(exc)
            finally:
                history_busy = False
            await asyncio.sleep(1 if not history_error else 5)

    historical = asyncio.create_task(history_changes())
    beat = asyncio.create_task(heartbeat())
    try:
        while True:
            error = None
            now = int(time.time() * 1000)
            try:
                # Collection supplies an exchange-confirmed watermark. Wall time
                # alone must never close live candles during clock/connection loss.
                status_path = root / "status.json"
                source = json.loads(status_path.read_text()) if status_path.exists() else {}
                confirmed = source.get("confirmed_ms")
                if type(confirmed) is int and now <= source.get("valid_until_ms", 0):
                    with journal_access(root):
                        had_pending = bool(store.db.execute("SELECT 1 FROM pending_updates LIMIT 1").fetchone())
                        store.process_changes(confirmed)
                    if had_pending:
                        last_processed = confirmed
                if historical.done():
                    await historical
            except (OSError, ValueError, RuntimeError, DataError) as exc:
                error = str(exc)
            pending = store.db.execute("SELECT count(*) FROM pending_updates").fetchone()[0]
            backlog = pending + len(list((root / "history/changes").glob("*.json")))
            error = error or history_error
            state.update(state="DEGRADED" if error else "WARMING_UP" if history_busy else "READY",
                         last_error=error, backlog=backlog, realtime_pending=pending,
                         last_processed_ms=last_processed, history_last_processed_ms=history_last_processed,
                         bar_updates=list(store.latest.values()))
            await asyncio.sleep(1 if backlog else 2)
    finally:
        beat.cancel()
        historical.cancel()
        await asyncio.gather(beat, historical, return_exceptions=True)
        store.close()
