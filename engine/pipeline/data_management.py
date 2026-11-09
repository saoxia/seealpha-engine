"""Bounded reads and fixed operations over the existing offline trades pipeline.

This module never starts a feed, changes pipeline configuration, forces a rebuild,
or treats file coverage as proof of Tick completeness.
"""

import argparse
import base64
import csv
import hashlib
import json
import os
import stat
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from itertools import islice, zip_longest
from pathlib import Path

import pandas as pd
import pyarrow as pa
from nautilus_trader.model import Bar, CryptoOption, NautilusDataType, TradeTick

from core.paths import PROJECT_ROOT
from engine.pipeline.catalog import (
    ParquetDataCatalog,
    TradeTickDataWrangler,
    _parse_filename_timestamps,
)
from engine.pipeline.config import load_settings
from engine.pipeline.convert_binance_bars import read_archive_checksum, read_record, record_path_for
from engine.pipeline.convert_binance_trades import (
    catalog_contains_record,
    convert_trade_archives,
    iter_trade_archive,
)
from engine.pipeline.models import (
    INTERVALS,
    DataRequest,
    SyncedArchive,
    bar_type_for,
    instrument_id_for,
)
from engine.pipeline.prepare_data import prepare_data, validate_request_against_settings
from engine.pipeline.sync_binance_batch import plan_daily_archives


def checked_request(symbol, start, end, intervals, settings, *, market="futures_um"):
    request = DataRequest((symbol,), start, end, bar_intervals=tuple(intervals), market=market)
    if market == "options":
        from engine.pipeline.options_data import check_request
        check_request(request)
    else:
        validate_request_against_settings(request, replace(settings, symbols=request.symbols, intervals=tuple(INTERVALS)))
    return request


def open_catalog(settings):
    # The catalog constructor creates directories: never call it on a missing volume.
    if not settings.catalog_dir.is_dir():
        raise FileNotFoundError("Catalog 尚不存在或数据存储不可用")
    return ParquetDataCatalog(str(settings.catalog_dir))


def archive_entries(request, settings):
    for spec in plan_daily_archives(request):
        yield spec, settings.raw_dir / spec.source_path.removeprefix("data/")


def coverage(request, settings):
    if request.market == "options":
        from engine.pipeline.options_data import coverage as option_coverage
        return option_coverage(request, settings)
    catalog = open_catalog(settings) if settings.catalog_dir.is_dir() else None
    days = []
    for spec, path in archive_entries(request, settings):
        record = read_record(record_path_for(path, settings.raw_dir, settings.state_dir))
        present = bool(record and record.get("status") == "ready" and catalog
                       and record.get("instrument_id") == instrument_id_for(spec.symbol)
                       and catalog_contains_record(catalog, instrument_id_for(spec.symbol),
                                                   record, request.bar_intervals))
        days.append({
            "day": spec.day.isoformat(), "symbol": spec.symbol,
            "archive_present": path.is_file(),
            "checksum_present": Path(f"{path}.CHECKSUM").is_file(),
            "catalog_present": present,
            "record_status": record.get("status") if record else None,
            "recorded_rows": record.get("rows") if record else None,
            "source_checksum": record.get("source_checksum") if record else None,
            "converter_version": record.get("converter_version") if record else None,
        })
    return {"checked_at": datetime.now(UTC).isoformat(), "days": days,
            "all_present": all(d["catalog_present"] for d in days),
            "quality": "unverified",
            "message": "文件覆盖及转换记录不代替本次逐笔校验；无成交时段不直接认定为缺口。"}


def dataset_inventory(settings, *, interval=None, all_levels=False):
    if not settings.catalog_dir.is_dir():
        return []
    catalog = open_catalog(settings)
    symbols = set()
    definitions = {}
    for instrument in catalog.instruments():
        identifier = str(instrument.id)
        if identifier.endswith(("USDT-PERP.BINANCE", "USDC-PERP.BINANCE")):
            symbols.add((identifier.removesuffix("-PERP.BINANCE"), "futures_um"))
        elif isinstance(instrument, CryptoOption) and identifier.endswith(".BINANCE"):
            symbol = identifier.removesuffix(".BINANCE")
            symbols.add((symbol, "options"))
            definitions[symbol] = instrument.info.get("binance_options", {})
    levels = (None, *settings.intervals) if all_levels else (interval,)
    items = []
    for symbol, market in sorted(symbols):
        for level in levels:
            ranges = catalog.get_intervals(Bar if level else TradeTick,
                                           bar_type_for(symbol, level, market) if level else instrument_id_for(symbol, market))
            if ranges:
                items.append({"symbol": symbol, "product": market, "instrument_id": instrument_id_for(symbol, market),
                              "level": level or "tick", "data_type": "bars" if level else "trades",
                              "quality": ("partial_recent_trades" if level is None else "exchange_klines") if market == "options" else "unverified",
                              **({"contract": {key: definitions[symbol].get(key) for key in
                                   ("underlying", "expiry", "strike", "right", "multiplier", "quote")}} if market == "options" else {}),
                              "event_ranges_ns": [[str(a), str(b)] for a, b in ranges]})
    return items


def deletion_plan(request, settings, *, delete_archives=False):
    """Enumerate exact files; never delete an overlapping file outside the selected days."""
    if any(value.time() != datetime.min.time() for value in (request.start, request.end)):
        raise ValueError("删除仅支持完整 UTC 日，起止时间必须为 00:00")
    start, end = pd.Timestamp(request.start).value, pd.Timestamp(request.end).value
    symbol = request.symbols[0]
    roots = {"catalog": settings.catalog_dir, "raw": settings.raw_dir, "state": settings.state_dir}
    files = []

    def add(path, root_name, group):
        root = roots[root_name]
        relative = path.relative_to(root)
        current = root
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                raise ValueError("删除范围包含符号链接，请先检查存储")
        if not path.exists():
            return
        value = path.stat()
        if not stat.S_ISREG(value.st_mode):
            raise ValueError("删除范围包含非普通文件")
        files.append({"root": root_name, "path": relative.as_posix(), "group": group,
                      "bytes": value.st_size, "mtime_ns": str(value.st_mtime_ns),
                      "ctime_ns": str(value.st_ctime_ns), "inode": str(value.st_ino),
                      "device": str(value.st_dev)})

    def parquet_files(folder, group, *, dependent=False):
        # Check parents before scanning, including folders containing no matching files.
        relative = folder.relative_to(settings.catalog_dir)
        current = settings.catalog_dir
        for part in relative.parts:
            current /= part
            if current.is_symlink():
                raise ValueError("删除范围包含符号链接，请先检查存储")
        for path in sorted(folder.glob("*")):
            name = path.name.removesuffix(".metadata.json") + ".parquet" if path.name.endswith(".metadata.json") else path.name
            bounds = _parse_filename_timestamps(name)
            if bounds is None:
                raise ValueError("数据目录包含无法识别的文件，请先检查存储")
            left, right = bounds
            if right < start or not dependent and left >= end:
                continue
            if left < start or not dependent and right >= end:
                raise ValueError("Catalog 文件跨越删除边界；请选择覆盖该文件的完整日期范围")
            add(path, "catalog", group)

    identifier = instrument_id_for(symbol, request.market)
    parquet_files(settings.catalog_dir / "data/trades" / identifier, "ticks")
    parquet_files(settings.catalog_dir / "supplements" / identifier / "data/custom/TradeQuote", "supplements")
    for interval in INTERVALS:
        bar_type = bar_type_for(symbol, interval, request.market)
        parquet_files(settings.catalog_dir / "data/bars" / bar_type, "bars")
        # Rolling indicators depend on preceding days, so invalidate the entire suffix.
        parquet_files(settings.catalog_dir / "features" / bar_type / "data/custom/BarFeatures",
                      "features", dependent=True)
    if request.market == "options":
        from engine.pipeline.options_data import source_paths
        if delete_archives:
            for path in source_paths(request, settings, include_pending=True):
                add(path, "raw", "archives")
    else:
        for _spec, path in archive_entries(request, settings):
            relative = path.relative_to(settings.raw_dir)
            for kind in ("converted", "failures"):
                add(settings.state_dir / kind / relative.parent / f"{relative.name}.json", "state", "state")
            if delete_archives:
                add(path, "raw", "archives")
                add(Path(f"{path}.CHECKSUM"), "raw", "archives")
    files.sort(key=lambda item: (0 if item["group"] == "state" else 1 if item["group"] == "features" else 3 if item["group"] == "archives" else 2,
                                 item["root"], item["path"]))
    identity = {"symbol": symbol, "product": request.market, "start": request.start.isoformat(), "end": request.end.isoformat(),
                "delete_archives": delete_archives, "roots": {key: str(path.resolve()) for key, path in roots.items()},
                "files": files}
    snapshot = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    groups = {}
    for item in files:
        group = groups.setdefault(item["group"], {"files": 0, "bytes": 0})
        group["files"] += 1
        group["bytes"] += item["bytes"]
    return {"snapshot": snapshot, "file_count": len(files), "total_bytes": sum(item["bytes"] for item in files),
            "groups": groups, "files": files, "symbol": symbol, "product": request.market,
            "start": request.start.isoformat(), "end": request.end.isoformat(),
            "delete_archives": delete_archives,
            "message": "删除选定日期的 Tick、所有周期 K 线、成交补充数据及转换记录；"
                       "从开始日期起的后续指标缓存一并失效清理。合约定义、连续行情、账户及研究报告保留。"
                       "删除不可撤销，中断可能已删除部分文件；请先停止手动 Pipeline 和使用这些数据的回测。"
                       if request.market != "options" else
                       "删除该合约选定 UTC 日期的本地 Tick 和全部周期 K 线；可同时删除来源快照。合约定义保留；不影响永续数据。删除不可撤销。"}


def delete_data(request, settings, *, expected_snapshot, delete_archives=False):
    plan = deletion_plan(request, settings, delete_archives=delete_archives)
    if plan["snapshot"] != expected_snapshot:
        raise ValueError("数据已变化，请重新预览删除范围")
    if not plan["file_count"]:
        raise ValueError("选定范围没有可删除文件")
    roots = {"catalog": settings.catalog_dir, "raw": settings.raw_dir, "state": settings.state_dir}
    return delete_planned_files(plan, roots)


def delete_planned_files(plan, roots):
    """Apply an already verified file manifest with per-file identity checks."""
    for item in plan["files"]:
        path = verify_planned_file(item, roots)
        path.unlink()
        print(f"已删除 {item['root']}/{item['path']} ({item['bytes']} bytes)", flush=True)
    return {"deleted_files": plan["file_count"], "deleted_bytes": plan["total_bytes"],
            "groups": plan["groups"], "snapshot": plan["snapshot"]}


def verify_planned_file(item, roots):
    """Recheck a planned file before deletion or an atomic range rewrite."""
    path = roots[item["root"]] / item["path"]
    # Recheck identity immediately before mutation; abort on a concurrent change.
    current = roots[item["root"]]
    for part in Path(item["path"]).parts:
        current /= part
        if current.is_symlink():
            raise ValueError("删除期间路径发生变化，已停止；请检查已执行日志")
    value = path.stat()
    if (str(value.st_ino), str(value.st_dev), value.st_size, str(value.st_mtime_ns), str(value.st_ctime_ns)) != (
            item["inode"], item["device"], item["bytes"], item["mtime_ns"], item["ctime_ns"]):
        raise ValueError("删除期间文件发生变化，已停止；请检查已执行日志")
    return path


def fingerprint(settings, identifier, data_type):
    folder = settings.catalog_dir / "data" / data_type / identifier.replace("/", "")
    digest = hashlib.sha256()
    for path in sorted(folder.glob("*.parquet")):
        stat = path.stat()
        digest.update(f"{path.name}:{stat.st_size}:{stat.st_mtime_ns}".encode())
    return digest.hexdigest()


def rows(settings, symbol, start, end, *, interval=None, market="futures_um"):
    catalog = open_catalog(settings)
    identifier = bar_type_for(symbol, interval, market) if interval else instrument_id_for(symbol, market)
    dtype = NautilusDataType.Bar if interval else NautilusDataType.TradeTick
    stream = catalog.native.query_data_arrow_stream(
        dtype, [identifier], pd.Timestamp(start).value, pd.Timestamp(end).value - 1,
        display=False,
    )
    reader = pa.RecordBatchReader._import_from_c_capsule(stream)
    names = (["open", "high", "low", "close", "volume"] if interval
             else ["price", "size", "aggressor_side", "trade_id"])
    try:
        for batch in reader:
            # Native batches can span a full day. Slice before creating Python row objects.
            for offset in range(0, batch.num_rows, 1000):
                piece = batch.slice(offset, 1000)
                times = piece.column("ts_event").cast(pa.int64()).to_pylist()
                for index, row in enumerate(piece.to_pylist()):
                    metadata = batch.schema.metadata or {}
                    values = {}
                    for name in names:
                        precision = metadata.get(b"size_precision" if name in {"size", "volume"}
                                                 else b"price_precision")
                        values[name] = (format(row[name], f".{int(precision)}f")
                                        if precision and name in {"price", "size", "open", "high",
                                                                  "low", "close", "volume"}
                                        else str(row[name]))
                    yield {"ts_event_ns": str(times[index]),
                           **values}
    finally:
        reader.close()


def preview(settings, symbol, start, end, *, limit=100, cursor=None, interval=None, market="futures_um"):
    if end - start > timedelta(minutes=15):
        raise ValueError("预览范围不能超过 15 分钟；大范围请使用导出")
    identifier = bar_type_for(symbol, interval, market) if interval else instrument_id_for(symbol, market)
    kind = "bars" if interval else "trades"
    snapshot = fingerprint(settings, identifier, kind)
    query = [market, symbol, start.isoformat(), end.isoformat(), interval, snapshot]
    offset = 0
    if cursor:
        try:
            token = json.loads(base64.b64decode(cursor, altchars=b"-_", validate=True))
            offset = token["offset"]
            if token["query"] != query or type(offset) is not int or not 0 <= offset <= 2_000_000:
                raise ValueError
        except (ValueError, TypeError, KeyError):
            raise ValueError("游标无效或数据已变化，请重新查询") from None
    source = rows(settings, symbol, start, end, interval=interval, market=market)
    try:
        records = list(islice(source, offset, offset + limit + 1))
    finally:
        source.close()
    if fingerprint(settings, identifier, kind) != snapshot:
        raise ValueError("查询期间数据发生变化，请重新查询")
    next_cursor = None
    if len(records) > limit:
        next_cursor = base64.urlsafe_b64encode(json.dumps(
            {"query": query, "offset": offset + limit}, separators=(",", ":")
        ).encode()).decode()
    return {"records": records[:limit], "next_cursor": next_cursor,
            "snapshot": snapshot, "quality": "unverified"}


def validate(request, settings):
    if request.market == "options":
        from engine.pipeline.options_data import validate as option_validate
        return option_validate(request, settings)
    """Hash archives, apply existing normalization, compare every canonical Tick.

    Duplicate resolution follows the existing converter; this verifies its canonical
    output, rather than making claims about unobserved exchange history.
    """
    catalog = open_catalog(settings)
    instruments = catalog.instruments(instrument_ids=[instrument_id_for(request.symbols[0])])
    if not instruments:
        raise ValueError("Catalog 缺少合约定义")
    instrument = max(instruments, key=lambda item: item.ts_init)
    results = []
    for spec, path in archive_entries(request, settings):
        print(f"校验 {spec.symbol} {spec.day}：归档 SHA-256、规范化 Tick 与 Catalog 逐笔比较", flush=True)
        item = {"symbol": spec.symbol, "day": spec.day.isoformat(), "status": "failed"}
        try:
            checksum = read_archive_checksum(path)
            begin = datetime.combine(spec.day, datetime.min.time(), UTC)
            finish = begin + timedelta(days=1)
            before = fingerprint(settings, str(instrument.id), "trades")

            def expected_rows(path=path, begin=begin, finish=finish):
                previous_time = previous_id = None
                wrangler = TradeTickDataWrangler(instrument)
                for frame in iter_trade_archive(path):
                    if (previous_time is not None and int(frame.iloc[0].timestamp) < previous_time
                            or previous_id is not None and int(frame.iloc[0].trade_id) <= previous_id):
                        raise ValueError("原始成交跨块倒序或重复")
                    previous_time = int(frame.iloc[-1].timestamp)
                    previous_id = int(frame.iloc[-1].trade_id)
                    normalized = pd.DataFrame({
                        "price": frame.price.to_numpy(), "quantity": frame.quantity.to_numpy(),
                        "side": frame.buyer_maker.map({True: "SELL", False: "BUY"}).to_numpy(),
                        "trade_id": frame.trade_id.astype(str).to_numpy(),
                    }, index=pd.to_datetime(frame.timestamp.to_numpy(), unit="ms", utc=True))
                    for tick in wrangler.process(normalized):
                        if not pd.Timestamp(begin).value <= tick.ts_event < pd.Timestamp(finish).value:
                            raise ValueError("原始成交超出归档日期")
                        yield {"ts_event_ns": str(tick.ts_event), "price": str(tick.price),
                               "size": str(tick.size), "trade_id": str(tick.trade_id),
                               "aggressor_side": tick.aggressor_side.name}

            count = 0
            actual = rows(settings, spec.symbol, begin, finish)
            try:
                for expected, observed in zip_longest(expected_rows(), actual):
                    # Decimal string formatting may differ between Arrow and the model.
                    if expected is None or observed is None:
                        raise ValueError("原始归档与 Catalog 成交数量不一致")
                    from decimal import Decimal
                    if any((Decimal(expected[k]) != Decimal(observed[k]) if k in {"price", "size"}
                            else expected[k] != observed[k]) for k in expected):
                        raise ValueError("原始归档与 Catalog 存在成交差异")
                    count += 1
            finally:
                actual.close()
            if not count:
                raise ValueError("归档无成交")
            if before != fingerprint(settings, str(instrument.id), "trades"):
                raise ValueError("校验期间数据发生变化")
            item.update(status="verified", rows=count, source_checksum=checksum,
                        catalog_fingerprint=before)
        except (OSError, ValueError, RuntimeError) as exc:
            item["error"] = str(exc)
        results.append(item)
    return {"ready": all(item["status"] == "verified" for item in results),
            "checked_at": datetime.now(UTC).isoformat(), "days": results,
            "scope": "canonical_archive_ticks_equal_catalog"}


def execute(action, request, settings, output, *, expected_snapshot=None, delete_archives=False):
    settings = replace(settings, symbols=request.symbols,
                       intervals=tuple(dict.fromkeys((*settings.intervals, *(request.bar_intervals or ())))))
    if action in {"prepare", "materialize"} and any(level in {"3d", "1w", "1M"} for level in request.bar_intervals or ()):
        raise ValueError("跨日周期请使用交易对配置的历史追加任务")
    if request.market == "options" and action != "delete":
        from engine.pipeline.options_data import execute as option_execute
        return option_execute(action, request, settings)
    if action == "delete":
        return delete_data(request, settings, expected_snapshot=expected_snapshot,
                           delete_archives=delete_archives)
    if action == "prepare":
        print("准备归档、转换 Tick / K 线并校验；仅补缺失，不强制重建", flush=True)
        # Keep the frozen preparation entry point unchanged. For an explicit Tick-only
        # task, its existing default-period fallback receives an empty local default.
        task_settings = replace(settings, intervals=()) if request.bar_intervals == () else settings
        dataset = prepare_data(request, settings=task_settings)
        return {"ready": dataset.ready, "coverage": coverage(request, settings)}
    if action == "validate":
        return validate(request, settings)
    if action == "materialize":
        catalog = open_catalog(settings)
        local = []
        for spec, path in archive_entries(request, settings):
            read_archive_checksum(path)
            local.append(SyncedArchive(spec, path))
        instruments = {request.symbols[0]: max(
            catalog.instruments(instrument_ids=[instrument_id_for(request.symbols[0])]),
            key=lambda item: item.ts_init,
        )}
        print("从本地已校验 trades 归档补齐派生 K 线；不下载、不强制重建", flush=True)
        convert_trade_archives(local, instruments, catalog, raw_dir=settings.raw_dir,
                               state_dir=settings.state_dir, bar_intervals=request.bar_intervals)
        return {"coverage": coverage(request, settings)}
    if action == "export":
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(".part")
        before = fingerprint(settings, instrument_id_for(request.symbols[0]), "trades")
        count = 0
        try:
            with temporary.open("x", encoding="utf-8", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=["ts_event_ns", "price", "size",
                                                           "aggressor_side", "trade_id"])
                writer.writeheader()
                for row in rows(settings, request.symbols[0], request.start, request.end):
                    writer.writerow(row)
                    count += 1
                target.flush()
                os.fsync(target.fileno())
            if before != fingerprint(settings, instrument_id_for(request.symbols[0]), "trades"):
                raise ValueError("导出期间数据发生变化，请重试")
            os.replace(temporary, output)
        finally:
            temporary.unlink(missing_ok=True)
        return {"rows": count, "catalog_fingerprint": before, "quality": "unverified"}
    raise ValueError("未知操作")


def main():
    parser = argparse.ArgumentParser(description="数据管理固定任务入口")
    parser.add_argument("action", choices=("prepare", "validate", "materialize", "export", "delete", "delete_pair", "trim"))
    parser.add_argument("--request", required=True)
    parser.add_argument("--job-id", required=True)
    args = parser.parse_args()
    import re
    if not re.fullmatch(r"[0-9a-f]{32}", args.job_id):
        parser.error("无效任务编号")
    payload = json.loads(args.request)
    settings = load_settings()
    result_path = Path(os.environ.get(
        "ENGINE_DATA_JOBS", str(PROJECT_ROOT / ".local/state/api/data_jobs"),
    )) / f"{args.job_id}.json"
    output = PROJECT_ROOT / "data/exports/ticks" / f"{args.job_id}.csv"
    if args.action == "trim":
        from engine.pipeline.pair_deletion import trim_pair_history
        result = trim_pair_history(payload["symbol"], payload["product"], datetime.fromisoformat(payload["policy"]["history_start"]),
                                   settings, expected_snapshot=payload["deletion_snapshot"])
    elif args.action == "delete_pair":
        from engine.pipeline.pair_deletion import delete_pair_data
        result = delete_pair_data(payload["symbol"], payload["product"], settings,
                                  expected_snapshot=payload["deletion_snapshot"])
    else:
        request = checked_request(payload["symbol"], datetime.fromisoformat(payload["start"]),
                                  datetime.fromisoformat(payload["end"]), payload["intervals"], settings, market=payload.get("product", "futures_um"))
        if request.market == "futures_um":
            plan_daily_archives(request)
        result = execute(args.action, request, settings, output,
                         expected_snapshot=payload.get("deletion_snapshot"),
                         delete_archives=payload.get("delete_archives", False))
    result_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = result_path.with_suffix(".tmp")
    temporary.write_text(json.dumps(result, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    os.replace(temporary, result_path)
    if result.get("ready") is False:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
