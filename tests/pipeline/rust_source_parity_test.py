"""Cross-language offline parity for the source-owned SQLite trade journal.

Uses deterministic synthetic trades only. The Rust replay executable performs
no network activity and never touches production or persistent user data.
"""

import json
import os
import sqlite3
import subprocess
from pathlib import Path

import pytest

from engine.market_data.storage.pair_store import PairStore

BINARY = (
    Path(__file__).resolve().parents[2]
    / "engine/market_data/sources/binance_shared/rust/target/release/binance-journal-replay"
)


@pytest.mark.parametrize("base_level", ["tick", "1m"])
def test_rust_and_python_raw_trade_journal_parity(tmp_path, base_level):
    if not BINARY.is_file():
        pytest.skip("native offline journal replay binary has not been built")
    original = tmp_path / "python"
    native = tmp_path / "rust"
    original.mkdir()
    native.mkdir()
    events = [
        {"now_ms": 1700000002000, "row": {
            "id": 100, "time": 1700000000100, "price": "2000.010",
            "qty": "0.100", "isBuyerMaker": False}},
        {"now_ms": 1700000002000, "row": {
            "id": 101, "time": 1700000001200, "price": "2001.000",
            "qty": "0.200", "isBuyerMaker": True}},
        {"now_ms": 1700000003000, "row": {
            "id": 101, "time": 1700000001200, "price": "2001.000",
            "qty": "0.200", "isBuyerMaker": True}},
        {"now_ms": 1700000003000, "row": {
            "id": 102, "time": 1700000002200, "price": "0",
            "qty": "0.200", "isBuyerMaker": False}},
    ]
    store = PairStore(original / "collection.sqlite", "ETHUSDT", base_level, ())
    try:
        for event in events:
            store.ticks([event["row"]], event["now_ms"],
                        quarantine_invalid=True, calculate=False)
    finally:
        store.close()
    result = subprocess.run(
        [os.fspath(BINARY), "ETHUSDT", base_level, os.fspath(native)],
        input="".join(json.dumps(event) + "\n" for event in events),
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["accepted"] == 2
    assert report["duplicates"] == 1
    assert report["filtered_zero"] == 1
    assert report["conflicted"] is False

    def facts(path):
        with sqlite3.connect(path) as conn:
            return {
                "config": conn.execute("SELECT * FROM config").fetchall(),
                "trades": conn.execute("SELECT * FROM trades ORDER BY id").fetchall(),
                "pending": conn.execute(
                    "SELECT * FROM pending_updates ORDER BY source,start_ms"
                ).fetchall(),
                "runtime": conn.execute("SELECT * FROM runtime_values ORDER BY name").fetchall(),
                "revision": conn.execute("SELECT * FROM revisions").fetchall(),
                "filters": [
                    json.loads(item[0])["kind"]
                    for item in conn.execute("SELECT detail FROM conflicts")
                ],
            }

    assert facts(original / "collection.sqlite") == facts(native / "collection.sqlite")
