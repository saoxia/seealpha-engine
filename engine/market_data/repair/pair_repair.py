"""Hierarchical history checks and additive repairs inside the existing Pipeline."""

import asyncio
import json
import os
import subprocess
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq
from nautilus_trader.model import Bar, BarType, NautilusDataType, Price, Quantity

from core.market.instruments import bar_type_for, bar_window, instrument_id_for
from engine.market_data.repair.models import DataRequest
from engine.market_data.repair.prepare_data import ensure_instruments, prepare_data
from engine.market_data.storage.catalog import ParquetDataCatalog

SCAN_PATH = ("1d", "4h", "1h", "15m", "5m", "1m")
FIELDS = ("open", "high", "low", "close", "volume")


def local_klines(catalog, symbol, level, start_ms, end_ms, *, market="futures_um"):
    """Read saved source bars; Rust owns OHLCV aggregation and completeness."""
    bars = catalog.bars(bar_types=[bar_type_for(symbol, level, market)],
                        start=start_ms * 1000000, end=end_ms * 1000000 - 1)
    source = level
    if not bars and level != "1m":
        source = "1m"
        bars = catalog.bars(bar_types=[bar_type_for(symbol, source, market)],
                            start=start_ms * 1000000, end=end_ms * 1000000 - 1)
    binary = Path(os.environ.get("SEEALPHA_REPAIR_VERIFY_BINARY",
                                str(Path(__file__).resolve().parents[3] / "target/release/seealpha-repair-verify")))
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("Rust repair validator unavailable; build seealpha-repair-verify or configure SEEALPHA_REPAIR_VERIFY_BINARY")
    request = {"mode": "aggregate", "source_interval": source, "target_interval": level,
               "saved": [{"event_ns": bar.ts_event,
                          "values": [str(getattr(bar, field).as_decimal()) for field in FIELDS]}
                         for bar in bars]}
    try:
        result = subprocess.run([str(binary)], input=json.dumps(request), capture_output=True,
                                text=True, check=False, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ValueError("Rust saved K-line aggregation failed to execute") from exc
    if result.returncode != 0:
        raise ValueError("本地 K 线原生聚合失败：" + result.stderr.strip()[:200])
    try:
        aggregated = json.loads(result.stdout)
        if not isinstance(aggregated, dict):
            raise ValueError("Rust saved K-line aggregation returned invalid buckets")
        return {int(stamp): {"values": [Decimal(value) for value in bucket["values"]],
                             "end": bucket["end"], "complete": bucket["complete"]}
                for stamp, bucket in aggregated.items()}
    except (KeyError, TypeError, json.JSONDecodeError, InvalidOperation) as exc:
        raise ValueError("Rust saved K-line aggregation returned malformed evidence") from exc

async def remote_klines(rest, symbol, level, start_ms, end_ms, available_ms, *, market="futures_um", local=None, source=None):
    """Fetch public pages; Rust is the sole authority for repair evidence validation."""
    rows = []
    cursor = start_ms
    while cursor < end_ms:
        page = await rest.get("klines", symbol=symbol, interval=level,
                              startTime=cursor, endTime=end_ms - 1, limit=1000)
        if not page:
            raise ValueError(f"{level} 对照来源为空；不能声明核验通过")
        rows.extend(page)
        if len(rows) > 50_000:
            raise ValueError("交易所对照数据超出单次记录限制")
        try:
            next_cursor = int(page[-1][6]) + 1
        except (TypeError, ValueError, IndexError, OverflowError) as exc:
            raise ValueError("交易所 K 线结束时间无效") from exc
        if next_cursor <= cursor:
            raise ValueError("交易所 K 线分页未前进")
        cursor = next_cursor

    binary = Path(os.environ.get("SEEALPHA_REPAIR_VERIFY_BINARY",
                                str(Path(__file__).resolve().parents[3] / "target/release/seealpha-repair-verify")))
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("Rust repair validator unavailable; build seealpha-repair-verify or configure SEEALPHA_REPAIR_VERIFY_BINARY")
    request = {"interval": level, "product": market, "start_ms": start_ms,
               "end_ms": end_ms, "available_ms": available_ms, "rows": rows}
    if local is not None:
        if source is None:
            raise ValueError("Rust comparison requires a data source")
        request["source"] = source
        request["local"] = {str(stamp): {"values": [str(value) for value in item["values"]],
                                       "complete": item["complete"]} for stamp, item in local.items()}
    try:
        result = await asyncio.to_thread(subprocess.run, [str(binary)], input=json.dumps(request),
                                         capture_output=True, text=True, check=False, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ValueError("Rust repair validator failed to execute") from exc
    if result.returncode != 0:
        raise ValueError("交易所 K 线原生校验失败：" + result.stderr.strip()[:200])
    try:
        verified = json.loads(result.stdout)
        if not isinstance(verified, list) or len(verified) != len(rows):
            raise ValueError("Rust repair validator returned an invalid result")
        if local is not None and any(type(row.get("different")) is not bool for row in verified):
            raise ValueError("Rust validator omitted native difference verdict")
        return [{"start_ms": row["start_ms"], "end_ms": row["end_ms"],
                 "values": [Decimal(value) for value in row["values"]],
                 "count": row["count"], **({"different": row["different"]} if local is not None else {})}
                for row in verified]
    except (KeyError, TypeError, json.JSONDecodeError, InvalidOperation) as exc:
        raise ValueError("Rust repair validator returned malformed evidence") from exc


async def scan_day(rest, catalog, symbol, source, start_ms, end_ms, available_ms, *, market="futures_um"):
    """Only mismatching branches descend; retain inconsistent parent evidence."""
    candidates, unresolved = [], []

    async def descend(level_index, left, right):
        level = SCAN_PATH[level_index]
        local = await asyncio.to_thread(local_klines, catalog, symbol, level, left, right, market=market)
        remote = await remote_klines(rest, symbol, level, left, right, available_ms,
                                    market=market, local=local, source=source)
        found = False
        for item in remote:
            if not item["different"]:
                continue
            found = True
            if level == "1m":
                candidates.append({**item, "local": local.get(item["start_ms"])})
            elif not await descend(level_index + 1, item["start_ms"], item["end_ms"]):
                unresolved.append({"start_ms": item["start_ms"], "interval": level,
                                   "reason": "大周期与子周期对照不一致"})
        return found

    await descend(0, start_ms, end_ms)
    return candidates, unresolved


def install_tick_slice(canonical, catalog, symbol, start_ms, end_ms, *, market="futures_um"):
    """Add confirmed identities; same-ID conflicts never replace existing evidence."""
    identifier = instrument_id_for(symbol, market)
    expected = canonical.trade_ticks(instrument_ids=[identifier], start=start_ms * 1000000, end=end_ms * 1000000 - 1)
    actual = catalog.trade_ticks(instrument_ids=[identifier], start=start_ms * 1000000, end=end_ms * 1000000 - 1)
    existing = {str(tick.trade_id): tick for tick in actual}
    for tick in expected:
        old = existing.get(str(tick.trade_id))
        if old is not None and any(getattr(old, name) != getattr(tick, name)
                                   for name in ("ts_event", "price", "size", "aggressor_side")):
            raise ValueError("历史成交同编号内容冲突；保留原始数据")
    missing = [tick for tick in expected if str(tick.trade_id) not in existing]
    if not missing:
        return 0
    with TemporaryDirectory(prefix=".pair-ticks-", dir=catalog.path) as directory:
        staged = ParquetDataCatalog(directory)
        staged.write_data(missing)
        identities = {str(tick.trade_id) for tick in missing}
        # Install quote supplements first. Interrupted retries merge by identity,
        # so a receipt never causes a duplicate supplement or missing quote field.
        quotes = canonical.supplement_catalog(identifier).query_custom_data(
            NautilusDataType.Custom("TradeQuote"), start=start_ms * 1000000, end=end_ms * 1000000 - 1)
        selected = [quote for quote in quotes if str(quote.data.trade_id) in identities]
        if {str(quote.data.trade_id) for quote in selected} != identities:
            raise ValueError("成交来源缺少 quote 补充数据；不写入 Tick")
        staged.supplement_catalog(identifier).write_custom_data(selected)
        paths = sorted(Path(directory).rglob("*.parquet"), key=lambda path: "supplements" not in path.parts)
        for path in paths:
            destination = Path(catalog.path) / path.relative_to(directory)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                # A one-millisecond shard may already exist. Merge its identities
                # into the staged shard and atomically replace only this file.
                old, fresh = pq.read_table(destination), pq.read_table(path)
                old_rows = {str(row["trade_id"]): row for row in old.to_pylist()}
                rows = list(old_rows.values())
                for row in fresh.to_pylist():
                    identity = str(row["trade_id"])
                    if identity in old_rows and old_rows[identity] != row:
                        raise ValueError("成交分片内容冲突；保留原文件")
                    if identity not in old_rows:
                        rows.append(row)
                rows.sort(key=lambda row: (row["ts_event"], int(row["trade_id"])))
                pq.write_table(pa.Table.from_pylist(rows, schema=old.schema), path)
            with path.open("rb") as stream:
                os.fsync(stream.fileno())
            if destination.exists():
                os.replace(path, destination)
            else:
                os.link(path, destination)
    return len(missing)


def repair_candidates(policy, settings, catalog, source, candidates, day, instrument):
    """Use official minutes or checked daily trades; never reverse bars into trades."""
    from engine.market_data.repair.pair_data import install_bars

    written, conflicts = 0, []
    if source == "1m":
        bars = []
        for item in candidates:
            values = item["values"]
            price_step = Decimal(1).scaleb(-instrument.price_precision)
            size_step = Decimal(1).scaleb(-instrument.size_precision)
            if (any(value != value.quantize(price_step) for value in values[:4])
                    or values[4] != values[4].quantize(size_step)):
                raise ValueError("补齐来源精度与合约不一致")
            stamp = (item["end_ms"] - 1) * 1000000
            existing = catalog.bars(bar_types=[bar_type_for(policy["symbol"], "1m", policy.get("product", "futures_um"))], start=stamp, end=stamp)
            if existing:
                if any(getattr(existing[0], name).as_decimal() != value for name, value in zip(FIELDS, values, strict=True)):
                    conflicts.append({"start_ms": item["start_ms"], "interval": "1m", "reason": "已有分钟来源值冲突"})
                continue
            bars.append(Bar(BarType.from_str(bar_type_for(policy["symbol"], "1m", policy.get("product", "futures_um"))),
                            *(Price.from_str(str(value)) for value in values[:4]), Quantity.from_str(str(values[4])), stamp, stamp))
        install_bars(catalog, bars)
        return len(bars), conflicts
    if not candidates:
        return 0, conflicts
    with TemporaryDirectory(prefix=".pair-source-", dir=catalog.path) as directory:
        staging = Path(directory)
        canonical = ParquetDataCatalog(staging / "catalog")
        canonical.write_data([instrument])
        intervals = ("1s",) if source == "1s" else ()
        prepare_data(DataRequest((policy["symbol"],), day, day + timedelta(days=1), bar_intervals=intervals,
                                 market=policy.get("product", "futures_um")),
                     settings=replace(settings, market=policy.get("product", "futures_um"), catalog_dir=Path(canonical.path), state_dir=staging / "state",
                                      symbols=(policy["symbol"],), intervals=intervals))
        for item in candidates:
            if source == "tick":
                written += install_tick_slice(canonical, catalog, policy["symbol"], item["start_ms"], item["end_ms"],
                                              market=policy.get("product", "futures_um"))
            else:
                expected = canonical.bars(bar_types=[bar_type_for(policy["symbol"], "1s", policy.get("product", "futures_um"))],
                                          start=item["start_ms"] * 1000000, end=item["end_ms"] * 1000000 - 1)
                actual = {bar.ts_event: bar for bar in catalog.bars(bar_types=[bar_type_for(policy["symbol"], "1s", policy.get("product", "futures_um"))],
                           start=item["start_ms"] * 1000000, end=item["end_ms"] * 1000000 - 1)}
                if any(bar.ts_event in actual and any(getattr(bar, name) != getattr(actual[bar.ts_event], name)
                                                     for name in FIELDS) for bar in expected):
                    conflicts.append({"start_ms": item["start_ms"], "interval": "1s", "reason": "已有秒来源值冲突"})
                    continue
                fresh = [bar for bar in expected if bar.ts_event not in actual]
                install_bars(catalog, fresh)
                written += len(fresh)
    return written, conflicts


async def reconcile_day(policy, settings, rest, source, day, finish, available_ms, *, comparison_catalog=None):
    if source in {"tick", "1s"}:
        from engine.market_data.repair.pair_data import _history_archive_available
        if not await asyncio.to_thread(_history_archive_available, policy, settings, day, finish):
            return {"ready": False, "checked_at": datetime.now(UTC).isoformat(), "records_written": 0,
                    "archive_pending": True, "scope": "saved_kline_ohlcv"}
    comparison = ParquetDataCatalog(comparison_catalog or settings.catalog_dir)
    start_ms, end_ms = int(day.timestamp() * 1000), int(finish.timestamp() * 1000)
    scan_kwargs = {"market": "spot"} if policy.get("product") == "spot" else {}
    candidates, unresolved = await scan_day(
        rest, comparison, policy["symbol"], source, start_ms, end_ms, available_ms, **scan_kwargs,
    )
    written, conflicts = 0, []
    if candidates:
        catalog = ParquetDataCatalog(settings.catalog_dir)
        instrument = (await asyncio.to_thread(ensure_instruments, catalog, (policy["symbol"],), market=policy.get("product", "futures_um")))[policy["symbol"]]
        writing = asyncio.create_task(asyncio.to_thread(repair_candidates, policy, settings, catalog, source, candidates, day, instrument))
        try:
            written, conflicts = await asyncio.shield(writing)
        except asyncio.CancelledError:
            # Keep the caller's file lease until the worker has actually stopped writing.
            await writing
            raise
    remaining, after_errors = (
        await scan_day(
            rest, comparison, policy["symbol"], source, start_ms, end_ms, available_ms, **scan_kwargs,
        ) if written and source == "1m" else (candidates, [])
    )
    needs_calculation = (bool(candidates) and source != "1m" and not conflicts
                         or bool(written) and any(level != source for level in policy.get("intervals", [])))
    if not remaining and not unresolved and not conflicts and not after_errors:
        # A matching minute-only catalog is valid comparison evidence, but do not
        # finish an explicit managed policy before its requested outputs exist.
        active = comparison.bars(bar_types=[bar_type_for(policy["symbol"], "1m", policy.get("product", "futures_um")), bar_type_for(policy["symbol"], "1d", policy.get("product", "futures_um"))],
                                 start=start_ms * 1000000, end=end_ms * 1000000 - 1)
        if any(bar.volume.as_decimal() > 0 for bar in active):
            for level in policy.get("intervals", []):
                if level in {"1s", *SCAN_PATH} and not any(
                        first < end_ms * 1000000 and last >= start_ms * 1000000
                        for first, last in comparison.get_intervals(Bar, bar_type_for(policy["symbol"], level, policy.get("product", "futures_um")))):
                    needs_calculation = True
                    break
    return {"ready": not remaining and not unresolved and not conflicts and not after_errors and not needs_calculation,
            "checked_at": datetime.now(UTC).isoformat(), "records_written": written,
            "needs_calculation": needs_calculation,
            "candidates": len(candidates), "remaining_minutes": [item["start_ms"] for item in remaining],
            "conflicts": conflicts + unresolved + after_errors,
            "differences": [{"start_ms": item["start_ms"], "expected": dict(zip(FIELDS, map(str, item["values"]), strict=True)),
                             "expected_trade_count": item["count"],
                             "observed": dict(zip(FIELDS, map(str, item["local"]["values"]), strict=True)) if item["local"] else None,
                             "observed_complete": item["local"]["complete"] if item["local"] else False}
                            for item in remaining],
            "scope": "saved_kline_ohlcv"}
