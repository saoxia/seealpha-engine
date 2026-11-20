"""Public USD-M funding collector: one mutable estimate, settled-only history.

Funding time comes from Binance, not a fixed eight-hour schedule. A streamed
mark-price estimate never becomes a settlement without REST fundingRate proof.
"""

import asyncio
import sqlite3
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

from nautilus_trader.model import FundingRateUpdate
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from core.market.instruments import instrument_id_for, normalize_symbol
from core.market.models import DataError
from engine.connectors.binance.transport import PublicREST
from engine.market_data.sources.binance.perpetual.funding_history import fetch_history

DAY_MS = 86_400_000
RETRY_SECONDS = 30


class FundingREST(PublicREST):
    allowed_endpoints = frozenset({"fundingRate"})


class FundingSocket:
    """Read only the official mark-price stream's estimated next funding rate."""

    async def stream(self, symbol):
        symbol = normalize_symbol(symbol, "futures_um")
        delay = 1
        while True:
            try:
                async with connect(
                    f"wss://fstream.binance.com/ws/{symbol.lower()}@markPrice@1s",
                    open_timeout=12, close_timeout=5, ping_interval=20,
                    ping_timeout=30, max_queue=128, max_size=65536,
                ) as socket:
                    yield {"kind": "connected"}
                    async for message in socket:
                        import json

                        row = json.loads(message)
                        if row.get("e") != "markPriceUpdate" or row.get("s") != symbol:
                            raise ValueError("Unexpected funding stream payload")
                        yield {"kind": "mark", "row": row}
                        delay = 1
                    raise ConnectionError("Funding stream closed")
            except (WebSocketException, OSError, TimeoutError, ConnectionError,
                    ValueError, KeyError, TypeError) as exc:
                yield {"kind": "disconnected", "error": f"{type(exc).__name__}: {exc}"}
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)


class FundingStore:
    """A symbol-owned SQLite journal, independent of its selected candle level."""

    def __init__(self, path, symbol):
        self.symbol = normalize_symbol(symbol, "futures_um")
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS funding_config (
                symbol TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS funding_settlements (
                funding_time_ms INTEGER PRIMARY KEY, rate TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'binance_fundingRate');
            CREATE TABLE IF NOT EXISTS funding_current (
                id INTEGER PRIMARY KEY CHECK (id=1),
                next_funding_ms INTEGER NOT NULL, rate TEXT NOT NULL,
                event_ms INTEGER NOT NULL);
        """)
        existing = self.db.execute("SELECT symbol FROM funding_config").fetchone()
        if existing and existing["symbol"] != self.symbol:
            raise DataError("Funding journal belongs to a different contract")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO funding_config VALUES (?)", (self.symbol,))

    def latest(self):
        row = self.db.execute(
            "SELECT next_funding_ms, rate, event_ms FROM funding_current WHERE id=1"
        ).fetchone()
        return dict(row) if row else None

    def last_settlement_ms(self):
        return self.db.execute(
            "SELECT max(funding_time_ms) FROM funding_settlements"
        ).fetchone()[0]

    def update_estimate(self, row):
        try:
            event, target = row["E"], row["T"]
            rate = Decimal(str(row["r"]))
        except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
            raise DataError("Invalid funding estimate") from exc
        if (row.get("s") != self.symbol or type(event) is not int
                or type(target) is not int or event < 0 or target <= event
                or not rate.is_finite()):
            raise DataError("Invalid funding estimate identity, time or rate")
        old = self.latest()
        if old is not None and event <= old["event_ms"]:
            if event == old["event_ms"] and (
                target != old["next_funding_ms"] or rate != Decimal(old["rate"])
            ):
                raise DataError("Conflicting funding estimate at same exchange time")
            return False
        with self.db:
            self.db.execute(
                """INSERT INTO funding_current(id,next_funding_ms,rate,event_ms)
                   VALUES (1,?,?,?)
                   ON CONFLICT(id) DO UPDATE SET
                     next_funding_ms=excluded.next_funding_ms,
                     rate=excluded.rate, event_ms=excluded.event_ms""",
                (target, str(rate), event),
            )
        return True

    def persist_settlements(self, observations):
        """Idempotent actual fundingRate records; conflicting history is immutable."""
        with self.db:
            for item in observations:
                if (type(item) is not FundingRateUpdate
                        or str(item.instrument_id) != instrument_id_for(self.symbol)
                        or item.ts_event != item.ts_init
                        or item.next_funding_ns != item.ts_event
                        or item.ts_event < 0 or item.ts_event % 1_000_000):
                    raise DataError("Invalid official funding settlement")
                rate = Decimal(str(item.rate))
                if not rate.is_finite():
                    raise DataError("Invalid official funding rate")
                stamp = item.ts_event // 1_000_000
                old = self.db.execute(
                    "SELECT rate FROM funding_settlements WHERE funding_time_ms=?", (stamp,)
                ).fetchone()
                if old and Decimal(old["rate"]) != rate:
                    raise DataError("Conflicting official funding settlement; old value preserved")
                self.db.execute(
                    "INSERT OR IGNORE INTO funding_settlements(funding_time_ms,rate) VALUES (?,?)",
                    (stamp, str(rate)),
                )
            self.db.execute(
                """DELETE FROM funding_current WHERE next_funding_ms IN
                   (SELECT funding_time_ms FROM funding_settlements)"""
            )

    def close(self):
        self.db.close()


def funding_start_ms(policy):
    """Optional explicit historical window; otherwise recover a recent settled tail."""
    if not policy.get("history_enabled"):
        return None
    mode = policy.get("history_mode", "all")
    if mode == "all":
        return 0
    if mode == "duration":
        return max(0, int(time.time() * 1000) - int(policy["history_days"]) * DAY_MS)
    if mode == "start" and policy.get("history_start"):
        from datetime import datetime

        return max(0, int(datetime.fromisoformat(policy["history_start"]).timestamp() * 1000))
    raise ValueError("Invalid funding history policy")


async def collect_funding(symbol, path, *, history_start_ms=None, rest=None, socket=None):
    """Reconcile at startup/reconnect and after observed settlement boundaries.

    REST retry is delayed if Binance has not yet published the final rate. The
    stream's changing 'r' is never used as the final settlement value.
    """
    store = FundingStore(path, symbol)
    rest = rest or FundingREST(symbols=(symbol,))
    socket = socket or FundingSocket()
    refresh = True
    pending_due = None
    retry_after = 0.0
    try:
        async for event in socket.stream(symbol):
            if event["kind"] == "connected":
                refresh = True
                retry_after = 0.0
                continue
            if event["kind"] == "disconnected":
                refresh = True
                continue
            if event["kind"] != "mark":
                raise DataError("Unknown funding event")
            row = event["row"]
            old = store.latest()
            if not store.update_estimate(row):
                continue
            stamp, target = row["E"], row["T"]
            if (old and old["next_funding_ms"] <= stamp
                    and (pending_due is None or old["next_funding_ms"] < pending_due)):
                pending_due = old["next_funding_ms"]
                retry_after = 0.0
            if target <= stamp:
                pending_due = target if pending_due is None else min(target, pending_due)
            if not refresh and pending_due is None:
                continue
            if time.monotonic() < retry_after:
                continue
            retry_after = time.monotonic() + RETRY_SECONDS
            start = store.last_settlement_ms()
            start = start + 1 if start is not None else max(
                0, history_start_ms if history_start_ms is not None else stamp - 7 * DAY_MS,
            )
            try:
                observations = (
                    await fetch_history(symbol, start, stamp + 1, rest.get)
                    if start < stamp + 1 else []
                )
            except (OSError, TimeoutError, DataError):
                refresh = True  # Retry network/transport errors without inventing a final rate.
                continue
            # Journal conflicts are integrity failures, never transient REST errors.
            store.persist_settlements(observations)
            refresh = False
            last = store.last_settlement_ms()
            if pending_due is not None and last is not None and last >= pending_due:
                pending_due = None
            # A settlement is not acknowledged until it appears in REST history.
    finally:
        store.close()


def read_funding(path, symbol, *, limit=100, now_ms=None):
    """Read-only current+settled view; never create a journal during a query."""
    symbol = normalize_symbol(symbol, "futures_um")
    path = Path(path)
    if not path.is_file() or path.is_symlink():
        return {"symbol": symbol, "current": None, "settlements": []}
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        config = db.execute("SELECT symbol FROM funding_config").fetchone()
        if config is None or config["symbol"] != symbol:
            raise DataError("Unexpected funding journal identity")
        current = db.execute(
            "SELECT next_funding_ms,rate,event_ms FROM funding_current WHERE id=1"
        ).fetchone()
        rows = db.execute(
            """SELECT funding_time_ms,rate FROM funding_settlements
               ORDER BY funding_time_ms DESC LIMIT ?""", (limit,),
        ).fetchall()
    current = dict(current) if current is not None else None
    if current is not None:
        current["is_pending"] = current["next_funding_ms"] > (
            int(time.time() * 1000) if now_ms is None else now_ms
        )
    return {"symbol": symbol, "current": current, "settlements": [dict(row) for row in rows]}
