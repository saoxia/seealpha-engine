"""CPython timings for the pre-migration hot-path algorithms.

Setup is outside the timer. Peak RSS is the process VmHWM after the workload.
Run one workload per process: ``python3 benchmarks/python_hot_path.py gaps 200000``.
"""

import json
import sqlite3
import sys
import tempfile
import time
from decimal import ROUND_DOWN, Decimal
from pathlib import Path


def peak_rss_kib():
    for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1])
    return 0


def gaps(n):
    intervals = [(index * 100, index * 100) for index in range(0, n, 2)]
    started = time.perf_counter()
    missing = missing_event_ranges(intervals, 0, n * 100, 100)
    return time.perf_counter() - started, len(missing)


def missing_event_ranges(intervals, required_start, required_end, step_ns):
    cursor = required_start
    missing = []
    for start, end in sorted(intervals):
        if end < cursor or start > required_end:
            continue
        if start > cursor:
            missing.append((cursor, min(start - step_ns, required_end)))
        cursor = max(cursor, end + step_ns)
        if cursor > required_end:
            break
    if cursor <= required_end:
        missing.append((cursor, required_end))
    return tuple(gap for gap in missing if gap[0] <= gap[1])


def net(n):
    increment = Decimal("0.001")
    started = time.perf_counter()
    acc = Decimal(0)
    out = Decimal(0)
    for index in range(n):
        qty = Decimal(index % 7) - Decimal(3)
        acc += qty
        if acc == 0:
            sized = Decimal(0)
        else:
            steps = (abs(acc) / increment).to_integral_value(rounding=ROUND_DOWN)
            sized = steps * increment
            if acc < 0:
                sized = -sized
        out += sized
    return time.perf_counter() - started, str(out)


def aggregate(n):
    trades = [
        (1_700_000_000_000 + index * 10, Decimal(1000 + (index % 50)), Decimal("0.01"))
        for index in range(n)
    ]
    started = time.perf_counter()
    interval_ms = 60_000
    bars = []
    bucket = None
    open_price = high = low = close = volume = None
    for timestamp, price, quantity in trades:
        current = (timestamp // interval_ms) * interval_ms
        if bucket != current:
            if bucket is not None:
                bars.append((open_price, high, low, close, volume))
            bucket = current
            open_price = high = low = close = price
            volume = quantity
        else:
            high = price if price > high else high
            low = price if price < low else low
            close = price
            volume += quantity
    if bucket is not None:
        bars.append((open_price, high, low, close, volume))
    return time.perf_counter() - started, len(bars)


def klines(n):
    rows = [(index * 60_000, index * 60_000 + 59_999, 100.0, 100.0, 100.0, 100.0, 1.0) for index in range(n)]
    started = time.perf_counter()
    previous = None
    interval = 60_000
    for open_time, close_time, open_, high, low, close, volume in rows:
        if previous is not None and (open_time <= previous or open_time - previous != interval):
            raise SystemExit("gap")
        if close_time != open_time + interval - 1:
            raise SystemExit("close")
        if not (low <= open_ <= high and low <= close <= high) or volume < 0:
            raise SystemExit("ohlc")
        previous = open_time
    return time.perf_counter() - started, n


def sqlite_reads(n):
    path = Path(tempfile.mkdtemp()) / "book.sqlite"
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(
        """CREATE TABLE order_requests (
            tenant_id TEXT, account_id TEXT, request_id TEXT, body_json TEXT, updated_at INTEGER,
            PRIMARY KEY (tenant_id, account_id, request_id))"""
    )
    connection.execute(
        "INSERT INTO order_requests VALUES (?, ?, ?, ?, ?)",
        ("1", "a", "req", json.dumps({"fingerprint": "abc", "response": {"status": "completed"}}), 1),
    )
    connection.commit()
    started = time.perf_counter()
    hits = 0
    for _ in range(n):
        row = connection.execute(
            "SELECT body_json FROM order_requests WHERE tenant_id=? AND account_id=? AND request_id=?",
            ("1", "a", "req"),
        ).fetchone()
        if row and json.loads(row[0])["fingerprint"] == "abc":
            hits += 1
    elapsed = time.perf_counter() - started
    connection.close()
    return elapsed, hits


WORK = {"gaps": gaps, "net": net, "aggregate": aggregate, "klines": klines, "sqlite": sqlite_reads}


def main():
    name = sys.argv[1]
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 200_000
    elapsed, result = WORK[name](count)
    print(json.dumps({
        "name": name,
        "n": count,
        "elapsed_s": elapsed,
        "peak_rss_kib": peak_rss_kib(),
        "result": result,
    }))


if __name__ == "__main__":
    main()
