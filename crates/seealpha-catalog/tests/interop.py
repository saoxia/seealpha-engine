#!/usr/bin/env python3
"""Offline NautilusTrader RC9 fixtures for tests/native_parity.rs.

From the repository root:
    uv run python crates/seealpha-catalog/tests/interop.py prepare /tmp/sa-parity
    SEEALPHA_CATALOG_PARITY_ROOT=/tmp/sa-parity cargo test --locked -p seealpha-catalog \
        --test native_parity -- --ignored
    uv run python crates/seealpha-catalog/tests/interop.py verify /tmp/sa-parity

Never point this script at an existing historical or production Catalog.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from decimal import Decimal
from pathlib import Path


def _files(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*.parquet"))
    }


def prepare(root: Path) -> None:
    if root.exists() and any(root.iterdir()):
        raise RuntimeError("fixture root must be empty; will not modify historical data")
    root.mkdir(parents=True, exist_ok=True)

    import pandas as pd
    from nautilus_trader.model import (
        Bar, BarType, BookOrder, FundingRateUpdate, OrderBookDepth, OrderSide,
        QuoteTick, RecordFlag,
    )
    from nautilus_support import TestInstrumentProvider
    from engine.market_data.storage.catalog import ParquetDataCatalog, TradeTickDataWrangler

    catalog = ParquetDataCatalog(root / "python")
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    identifier = str(instrument.id)
    bar_type = BarType.from_str(f"{identifier}-1-MINUTE-LAST-EXTERNAL")
    start_ms = 1_704_067_200_000  # 2024-01-01T00:00:00Z
    start_ns = start_ms * 1_000_000

    bars = [
        Bar(
            bar_type, instrument.make_price(2000), instrument.make_price(2001),
            instrument.make_price(1999), instrument.make_price(2000),
            instrument.make_qty(Decimal("1.250")),
            (start_ms + (index + 1) * 60_000 - 1) * 1_000_000,
            (start_ms + (index + 1) * 60_000 - 1) * 1_000_000,
        )
        for index in range(2)
    ]
    catalog.write_data(bars)
    trade_times = [start_ns + 1, start_ns + 2]
    trades = TradeTickDataWrangler(instrument).process(
        pd.DataFrame(
            {
                "price": [Decimal("2000.01"), Decimal("2000.02")],
                "quantity": [Decimal("0.125"), Decimal("0.250")],
                "side": ["BUY", "SELL"],
                "trade_id": ["10001", "10002"],
            },
            index=pd.to_datetime(trade_times, unit="ns", utc=True),
        ),
    )
    catalog.write_data(trades)
    quotes = [
        QuoteTick(
            instrument.id, instrument.make_price(Decimal("2000.00")),
            instrument.make_price(Decimal("2000.05")), instrument.make_qty(Decimal("0.250")),
            instrument.make_qty(Decimal("0.375")), start_ns + 3, start_ns + 3,
        ),
        QuoteTick(
            instrument.id, instrument.make_price(Decimal("2000.01")),
            instrument.make_price(Decimal("2000.06")), instrument.make_qty(Decimal("0.250")),
            instrument.make_qty(Decimal("0.375")), start_ns + 4, start_ns + 4,
        ),
    ]
    catalog.write_data(quotes)
    funding = [
        FundingRateUpdate(
            instrument.id, Decimal("0.0001"), start_ns + 5, start_ns + 5,
            next_funding_ns=start_ns + 28_800_000_000_000,
        ),
    ]
    catalog.write_data(funding)
    depth = [
        OrderBookDepth(
            instrument.id,
            [BookOrder(OrderSide.BUY, instrument.make_price(Decimal("2000.00")),
                       instrument.make_qty(Decimal("0.250")), 0)],
            [BookOrder(OrderSide.SELL, instrument.make_price(Decimal("2000.05")),
                       instrument.make_qty(Decimal("0.375")), 0)],
            [0], [0],
            RecordFlag.F_SNAPSHOT.value | RecordFlag.F_LAST.value,
            1234, start_ns + 6, start_ns + 6,
        ),
    ]
    catalog.write_data(depth)

    entries = []
    for kind, values in [
        ("bars", bars), ("trades", trades), ("quotes", quotes),
        ("funding_rate_updates", funding), ("order_book_depths", depth),
    ]:
        item = {
            "kind": kind,
            "identifier": str(bar_type) if kind == "bars" else identifier,
            "start_ns": min(int(value.ts_event) for value in values),
            "end_ns": max(int(value.ts_event) for value in values) + 1,
        }
        if kind == "bars":
            item.update(
                symbol="ETHUSDT", product="futures_um", interval="1m",
                start_ms=start_ms, end_ms=start_ms + 120_000,
                price_precision=instrument.price_precision,
                size_precision=instrument.size_precision,
            )
        entries.append(item)

    (root / "manifest.json").write_text(
        json.dumps(entries, indent=2), encoding="utf-8",
    )
    (root / "original_sha256.json").write_text(
        json.dumps(_files(root / "python"), indent=2), encoding="utf-8",
    )
    print("Prepared Python NativeCatalog fixtures:", len(entries), "types")


def _signature(value):
    kind = type(value).__name__
    timestamp = int(value.ts_event)
    if kind == "Bar":
        return (timestamp, str(value.bar_type), *[
            str(getattr(value, name)) for name in
            ("open", "high", "low", "close", "volume")
        ])
    if kind == "TradeTick":
        return (
            timestamp, str(value.instrument_id), str(value.price), str(value.size),
            str(value.trade_id), str(value.aggressor_side),
        )
    if kind == "QuoteTick":
        return (
            timestamp, str(value.instrument_id), *[
                str(getattr(value, name)) for name in
                ("bid_price", "ask_price", "bid_size", "ask_size")
            ],
        )
    if kind == "FundingRateUpdate":
        return (timestamp, str(value.instrument_id), str(value.rate))
    if kind == "OrderBookDepth":
        return (
            timestamp, str(value.instrument_id), int(value.sequence),
            tuple((str(v.price), str(v.size)) for v in value.bids),
            tuple((str(v.price), str(v.size)) for v in value.asks),
        )
    raise AssertionError(f"unexpected native model: {kind}")


def verify(root: Path) -> None:
    from engine.market_data.storage.catalog import ParquetDataCatalog

    original_hashes = json.loads((root / "original_sha256.json").read_text())
    assert _files(root / "python") == original_hashes, "Python source was modified"
    catalog_python = ParquetDataCatalog(root / "python")
    catalog_rust = ParquetDataCatalog(root / "rust")
    entries = json.loads((root / "manifest.json").read_text())
    for item in entries:
        kind = item["kind"]
        identity = item["identifier"]
        start = item["start_ns"]
        end = item["end_ns"]
        def load(catalog):
            if kind == "bars":
                return catalog.bars(bar_types=[identity], start=start, end=end)
            if kind == "trades":
                return catalog.trade_ticks(instrument_ids=[identity], start=start, end=end)
            if kind == "quotes":
                return catalog.quote_ticks(instrument_ids=[identity], start=start, end=end)
            if kind == "funding_rate_updates":
                return catalog.funding_rate_updates([identity], start=start, end=end)
            if kind == "order_book_depths":
                return catalog.order_book_depths([identity], start=start, end=end)
            raise AssertionError(kind)
        python = [_signature(record) for record in load(catalog_python)]
        rust = [_signature(record) for record in load(catalog_rust)]
        assert python and python == rust, (kind, python, rust)
        print(f"Python Nautilus read parity: {kind}: {len(rust)} rows")
    print("All generated source hashes preserved; Python/Rust NativeCatalog parity OK")


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("mode", choices=("prepare", "verify"))
    cli.add_argument("root", type=Path)
    args = cli.parse_args()
    if args.mode == "prepare":
        prepare(args.root)
    else:
        verify(args.root)
