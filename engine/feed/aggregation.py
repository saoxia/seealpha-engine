"""Versioned minute rollups; forming bars stay in memory, closed bars are journaled."""

import sqlite3
from dataclasses import asdict, dataclass
from decimal import Decimal
from itertools import pairwise

from .models import MINUTE, DataError, MinuteBar

PERIODS = {"1m": MINUTE, "5m": 5 * MINUTE, "15m": 15 * MINUTE}


@dataclass(frozen=True)
class BarUpdate:
    symbol: str
    interval: str
    start_ms: int
    end_ms: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    is_closed: bool
    is_complete: bool
    revision: int
    updated_ms: int
    allow_new_risk: bool = False
    source_levels: tuple[str, ...] = ()

    def public(self):
        return asdict(self)


class Rollups:
    def __init__(self, store, path, intervals, on_update=None):
        if not intervals or any(value not in PERIODS for value in intervals):
            raise ValueError("Unsupported minute rollup")
        self.store, self.intervals, self.on_update = store, tuple(intervals), on_update
        self.forming = {}
        self.latest = {}
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS rollups (
            symbol TEXT, interval TEXT, start_ms INTEGER, end_ms INTEGER,
            open TEXT, high TEXT, low TEXT, close TEXT, volume TEXT,
            is_complete INTEGER, revision INTEGER, updated_ms INTEGER,
            PRIMARY KEY(symbol,interval,start_ms))""")
        self.db.execute("CREATE TABLE IF NOT EXISTS checkpoints (symbol TEXT, interval TEXT, next_ms INTEGER, PRIMARY KEY(symbol,interval))")
        self.db.execute("CREATE TABLE IF NOT EXISTS revisions (id INTEGER PRIMARY KEY, value INTEGER)")
        self.db.execute("INSERT OR IGNORE INTO revisions VALUES (1,0)")
        self.db.commit()

    def _inputs(self, symbol, left, right, width, now_ms):
        """Use complete closed rollups, then uncovered minutes and the live tail."""
        candidates = {}
        for interval, step in sorted(PERIODS.items(), key=lambda item: item[1], reverse=True):
            if not MINUTE < step < width or width % step:
                continue
            for row in self.db.execute(
                """SELECT start_ms,end_ms,open,high,low,close,volume,revision,updated_ms
                   FROM rollups WHERE symbol=? AND interval=? AND start_ms>=? AND end_ms<=?
                   AND is_complete=1 ORDER BY start_ms""",
                (symbol, interval, left, min(right, now_ms)),
            ):
                start, end, opening, high, low, close, volume, revision, updated = row
                if end - start != step or start % step:
                    continue
                candidates.setdefault(start, BarUpdate(symbol, interval, start, end, opening,
                                                       high, low, close, volume, True, True,
                                                       revision, updated))
        partial = self.forming.get(symbol)
        if partial and not (left <= partial.start_ms < right and partial.start_ms <= now_ms < partial.end_ms):
            partial = None
        result, sources = [], set()
        cursor = left
        while cursor < right:
            rolled = candidates.get(cursor)
            if rolled is not None:
                result.append(rolled)
                sources.add(rolled.interval)
                cursor = rolled.end_ms
                continue
            stop = min((stamp for stamp in candidates if stamp > cursor), default=right)
            minutes = list(self.store.read(symbol, cursor, stop))
            if minutes:
                sources.add("1m")
            if partial and cursor <= partial.start_ms < stop and all(
                minute.start_ms != partial.start_ms for minute in minutes
            ):
                minutes.append(partial)
                minutes.sort(key=lambda minute: minute.start_ms)
                sources.add("live_trades")
            result.extend(minutes)
            cursor = stop
        return result, tuple(sorted(sources))

    def update(self, bar, now_ms, *, is_closed=False, allow_new_risk=False):
        if not isinstance(bar, MinuteBar) or bar.symbol not in self.store.symbols:
            raise DataError("Unexpected rollup input")
        # Native trade-derived partials may arrive late; they never replace committed minutes.
        if not is_closed and (bar.start_ms < now_ms // MINUTE * MINUTE
                              or bar.start_ms > now_ms + 2000):
            return
        if is_closed:
            if bar.end_ms > now_ms:
                raise DataError("Cannot close a future minute")
            cached = self.forming.get(bar.symbol)
            if cached and cached.start_ms <= bar.start_ms:
                self.forming.pop(bar.symbol, None)
        else:
            previous = self.forming.get(bar.symbol)
            if previous and previous.start_ms > bar.start_ms:
                return
            self.forming[bar.symbol] = bar
        output = []
        for interval in self.intervals:
            width = PERIODS[interval]
            left = bar.start_ms // width * width
            right = left + width
            rows, sources = self._inputs(bar.symbol, left, right, width, now_ms)
            if not rows:
                continue
            closed = right <= now_ms
            complete = (closed and rows[0].start_ms == left and rows[-1].end_ms == right
                        and all(previous.end_ms == current.start_ms
                                for previous, current in pairwise(rows)))
            key = (bar.symbol, interval, left)
            old = self.db.execute(
                "SELECT revision FROM rollups WHERE symbol=? AND interval=? AND start_ms=?", key
            ).fetchone()
            prior = self.latest.get((bar.symbol, interval))
            with self.db:
                revision = max(self.db.execute("SELECT value FROM revisions WHERE id=1").fetchone()[0],
                               old[0] if old else 0,
                               prior.revision if prior and prior.start_ms == left else 0) + 1
                self.db.execute("UPDATE revisions SET value=? WHERE id=1", (revision,))
            value = BarUpdate(bar.symbol, interval, left, right, str(rows[0].open),
                              str(max(Decimal(row.high) for row in rows)), str(min(Decimal(row.low) for row in rows)),
                              str(rows[-1].close), str(sum((Decimal(row.volume) for row in rows), Decimal(0))),
                              closed, complete, revision, now_ms,
                              allow_new_risk and (complete if closed else False), sources)
            # A forming candle is deliberately incomplete until an authoritative close;
            # consumers may observe it but cannot infer complete trade coverage from it.
            if closed:
                with self.db:
                    self.db.execute("INSERT OR REPLACE INTO rollups VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (*key, right, value.open, value.high, value.low, value.close,
                                     value.volume, complete, revision, now_ms))
            if prior is None or left >= prior.start_ms:
                self.latest[(bar.symbol, interval)] = value
            output.append(value)
        if is_closed:
            with self.db:
                for interval in self.intervals:
                    self.db.execute("""INSERT INTO checkpoints VALUES (?,?,?)
                        ON CONFLICT(symbol,interval) DO UPDATE SET next_ms=max(next_ms,excluded.next_ms)""",
                                    (bar.symbol, interval, bar.end_ms))
        for value in output:
            if self.on_update:
                self.on_update(value)
        return output

    def recover(self, now_ms):
        # Source minutes and their contiguous checkpoint are authoritative. Replaying
        # them repairs derived writes interrupted before commit; it never grants risk.
        for symbol in self.store.symbols:
            checkpoints = [self.db.execute("SELECT next_ms FROM checkpoints WHERE symbol=? AND interval=?",
                                           (symbol, interval)).fetchone() for interval in self.intervals]
            start = max(self.store.start_ms, min(row[0] if row else self.store.start_ms
                        for row in checkpoints) - max(PERIODS[value] for value in self.intervals))
            for bar in self.store.read(symbol, start, self.store.next_ms(symbol)):
                self.update(bar, now_ms, is_closed=True)

    def snapshot(self, now_ms):
        return [value.public() for value in self.latest.values()
                if value.is_closed or value.start_ms <= now_ms < value.end_ms]

    def close(self):
        self.db.close()
