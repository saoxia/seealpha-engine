"""Atomic minute journal, contiguous checkpoints and persistent conflict evidence."""

import json
import sqlite3
from decimal import Decimal
from pathlib import Path

from .models import MINUTE, DataError, MinuteBar


class Store:
    def __init__(self, path: Path, symbols: tuple[str, ...], start_ms: int):
        if not symbols or len(set(symbols)) != len(symbols) or start_ms % MINUTE:
            raise ValueError("Invalid dataset configuration")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS bars (
                symbol TEXT NOT NULL, start_ms INTEGER NOT NULL,
                open TEXT NOT NULL, high TEXT NOT NULL, low TEXT NOT NULL,
                close TEXT NOT NULL, volume TEXT NOT NULL,
                source TEXT NOT NULL, received_ms INTEGER NOT NULL,
                PRIMARY KEY(symbol,start_ms));
            CREATE TABLE IF NOT EXISTS provenance (
                symbol TEXT, start_ms INTEGER, source TEXT, checked_ms INTEGER,
                PRIMARY KEY(symbol,start_ms,source));
            CREATE TABLE IF NOT EXISTS cursors (symbol TEXT PRIMARY KEY, next_ms INTEGER);
            CREATE TABLE IF NOT EXISTS incidents (
                id INTEGER PRIMARY KEY, at_ms INTEGER, kind TEXT, detail TEXT,
                UNIQUE(kind, detail));
        """)
        config = {r["key"]: json.loads(r["value"]) for r in self.db.execute("SELECT * FROM config")}
        if config and (config["symbols"] != list(symbols) or config["schema"] != 1):
            raise DataError("Dataset configuration changed; use a separate directory")
        self.symbols = symbols
        self.start_ms = config.get("start_ms", start_ms)
        with self.db:
            for key, value in {"symbols": symbols, "schema": 1, "start_ms": self.start_ms}.items():
                self.db.execute(
                    "INSERT OR IGNORE INTO config VALUES (?,?)", (key, json.dumps(value))
                )
            for symbol in symbols:
                self.db.execute(
                    "INSERT OR IGNORE INTO cursors VALUES (?,?)", (symbol, self.start_ms)
                )
        # Reconstruct the index from committed bars. Checkpoints are an optimization,
        # not proof of coverage, including after a copied/restored database.
        with self.db:
            for symbol in symbols:
                self.db.execute(
                    "UPDATE cursors SET next_ms=? WHERE symbol=?", (self.start_ms, symbol)
                )
                self._advance(symbol)

    @staticmethod
    def decode(row):
        return MinuteBar(
            row["symbol"],
            row["start_ms"],
            *(Decimal(row[k]) for k in ("open", "high", "low", "close", "volume")),
        )

    def next_ms(self, symbol):
        row = self.db.execute("SELECT next_ms FROM cursors WHERE symbol=?", (symbol,)).fetchone()
        if row is None:
            raise DataError("Unexpected symbol")
        return row[0]

    def _advance(self, symbol):
        expected = self.next_ms(symbol)
        for row in self.db.execute(
            "SELECT start_ms FROM bars WHERE symbol=? AND start_ms>=? ORDER BY start_ms",
            (symbol, expected),
        ):
            if row[0] != expected:
                break
            expected += MINUTE
        self.db.execute("UPDATE cursors SET next_ms=? WHERE symbol=?", (expected, symbol))

    def incident(self, kind, detail, now_ms):
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO incidents(at_ms,kind,detail) VALUES (?,?,?)",
                (now_ms, kind, json.dumps(detail, sort_keys=True)),
            )

    @property
    def conflicted(self):
        return (
            self.db.execute("SELECT 1 FROM incidents WHERE kind='conflict' LIMIT 1").fetchone()
            is not None
        )

    def put(self, bars, source, now_ms):
        bars = list(bars)
        if source not in {"catalog", "rest", "websocket"}:
            raise ValueError("Unknown source")
        try:
            with self.db:
                touched = set()
                for bar in bars:
                    if bar.symbol not in self.symbols or bar.end_ms > now_ms:
                        raise DataError("Unexpected symbol or unclosed/future bar")
                    if bar.start_ms < self.start_ms:
                        continue
                    old = self.db.execute(
                        "SELECT * FROM bars WHERE symbol=? AND start_ms=?",
                        (bar.symbol, bar.start_ms),
                    ).fetchone()
                    if old is not None and not self.decode(old).matches(bar):
                        conflict = {
                            "symbol": bar.symbol,
                            "start_ms": bar.start_ms,
                            "old": self.decode(old).values(),
                            "new": bar.values(),
                            "old_source": old["source"],
                            "new_source": source,
                        }
                        raise DataError("Conflicting closed bar: " + json.dumps(conflict))
                    self.db.execute(
                        "INSERT OR IGNORE INTO bars VALUES (?,?,?,?,?,?,?,?,?)",
                        (bar.symbol, bar.start_ms, *bar.values(), source, now_ms),
                    )
                    self.db.execute(
                        "INSERT OR IGNORE INTO provenance VALUES (?,?,?,?)",
                        (bar.symbol, bar.start_ms, source, now_ms),
                    )
                    touched.add(bar.symbol)
                for symbol in touched:
                    self._advance(symbol)
        except DataError:
            if "conflict" in locals():
                self.incident("conflict", conflict, now_ms)
            raise

    def read(self, symbol, start_ms, end_ms):
        for row in self.db.execute(
            "SELECT * FROM bars WHERE symbol=? AND start_ms>=? AND start_ms<? ORDER BY start_ms",
            (symbol, start_ms, end_ms),
        ):
            yield self.decode(row)

    def missing(self, symbol, start_ms, end_ms):
        """Yield exact half-open gaps, including interior holes, without loading a day list."""
        cursor = start_ms
        for row in self.db.execute(
            "SELECT start_ms FROM bars WHERE symbol=? AND start_ms>=? AND start_ms<? ORDER BY start_ms",
            (symbol, start_ms, end_ms),
        ):
            if row[0] > cursor:
                yield cursor, row[0]
            cursor = row[0] + MINUTE
        if cursor < end_ms:
            yield cursor, end_ms

    def close(self):
        self.db.close()
