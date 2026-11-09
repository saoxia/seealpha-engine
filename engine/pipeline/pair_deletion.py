"""Full pair deletion over existing catalog, source and managed-feed storage."""

import fcntl
import hashlib
import json
import os
import re
import sqlite3
import stat
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from core.market.instruments import (
    INTERVALS,
    bar_type_for,
    bar_window,
    instrument_id_for,
    normalize_symbol,
)
from core.paths import PROJECT_ROOT

from .data_management import delete_planned_files, verify_planned_file


def pair_deletion_roots(settings, project):
    current = project / "data"
    for part in ("market_data", "binance_um_1m", "managed"):
        current /= part
        if current.is_symlink():
            raise ValueError("实时数据目录包含符号链接，请先检查存储")
    return {"catalog": settings.catalog_dir, "raw": settings.raw_dir,
            "state": settings.state_dir, "feed": project / "data/market_data/binance_um_1m/managed"}


def pair_deletion_plan(symbol, product, settings, *, project=PROJECT_ROOT):
    if product == "options" and "-" not in symbol:
        symbol = normalize_symbol(symbol)
        roots = pair_deletion_roots(settings, project)
        contracts = set()
        underlyings = {}
        if settings.catalog_dir.is_dir():
            from .catalog import ParquetDataCatalog
            for instrument in ParquetDataCatalog(settings.catalog_dir).instruments():
                metadata = (instrument.info or {}).get("binance_options", {})
                if metadata.get("underlying"):
                    underlyings[str(instrument.id).removesuffix(".BINANCE")] = metadata["underlying"]
        folders = [roots["raw"] / "options", roots["state"] / "options", roots["feed"],
                   roots["catalog"] / "data/trades", roots["catalog"] / "data/bars",
                   roots["catalog"] / "features", roots["catalog"] / "supplements"]
        for folder in folders:
            if folder.is_symlink():
                raise ValueError("删除范围包含符号链接，请先检查存储")
            if not folder.is_dir():
                continue
            for entry in folder.iterdir():
                name = entry.name.split(".BINANCE", 1)[0]
                if underlyings.get(name, name.split("-", 1)[0] + "USDT") != symbol:
                    continue
                try:
                    contracts.add(normalize_symbol(name, "options"))
                except ValueError:
                    continue
        plans = [pair_deletion_plan(contract, product, settings, project=project) for contract in sorted(contracts)]
        files = [item for plan in plans for item in plan["files"]]
        snapshot = hashlib.sha256(json.dumps({"symbol": symbol, "contracts": [(p["symbol"], p["snapshot"]) for p in plans],
                    "roots": {k: str(v.resolve()) for k, v in roots.items()}}, sort_keys=True).encode()).hexdigest()
        groups = {}
        for item in files:
            group = groups.setdefault(item["group"], {"files": 0, "bytes": 0})
            group["files"] += 1
            group["bytes"] += item["bytes"]
        return {"symbol": symbol, "product": product, "snapshot": snapshot, "files": files,
                "file_count": len(files), "total_bytes": sum(item["bytes"] for item in files), "groups": groups,
                "contracts": {p["symbol"]: p["snapshot"] for p in plans},
                "start": "", "end": "", "delete_archives": True,
                "message": "删除该标的所有到期日、方向及行权价的期权行情、来源归档和托管数据；保留合约定义、账户、交易记录及研究报告。"}
    symbol = normalize_symbol(symbol, product)
    roots = pair_deletion_roots(settings, project)
    identifier = instrument_id_for(symbol, product)
    folders = [("catalog", Path("data/trades") / identifier, "ticks"),
               ("catalog", Path("supplements") / identifier, "supplements")]
    for interval in INTERVALS:
        bar_type = bar_type_for(symbol, interval, product)
        folders.extend([("catalog", Path("data/bars") / bar_type, "bars"),
                        ("catalog", Path("features") / bar_type, "features")])
    if product == "options":
        folders.extend([("raw", Path("options") / symbol, "archives"),
                        ("state", Path("options") / symbol, "state"),
                        ("feed", Path(symbol), "feed")])
    else:
        for cadence in ("daily", "monthly"):
            for kind in ("trades", "aggTrades", "klines"):
                relative = Path("futures/um") / cadence / kind / symbol
                folders.append(("raw", relative, "archives"))
                for state_kind in ("converted", "failures"):
                    folders.append(("state", Path(state_kind) / relative, "state"))
        folders.append(("feed", Path(symbol), "feed"))
    files = []
    for root_name, relative, group in folders:
        root = roots[root_name]
        current = root
        # The configured data volume itself may be a mount symlink; descendants may not.
        for part in relative.parts:
            current /= part
            if current.is_symlink():
                raise ValueError("删除范围包含符号链接，请先检查存储")
        if not current.exists():
            continue
        if not current.is_dir():
            raise ValueError("交易对数据目录不是文件夹")
        for directory, subdirs, names in os.walk(current, followlinks=False):
            for name in subdirs + names:
                path = Path(directory) / name
                value = path.lstat()
                if stat.S_ISLNK(value.st_mode):
                    raise ValueError("删除范围包含符号链接，请先检查存储")
                if stat.S_ISDIR(value.st_mode):
                    continue
                if not stat.S_ISREG(value.st_mode):
                    raise ValueError("删除范围包含非普通文件")
                files.append({"root": root_name, "path": path.relative_to(root).as_posix(), "group": group,
                              "bytes": value.st_size, "mtime_ns": str(value.st_mtime_ns),
                              "ctime_ns": str(value.st_ctime_ns), "inode": str(value.st_ino), "device": str(value.st_dev)})
    files.sort(key=lambda item: (item["root"], item["path"]))
    # SQLite readers update WAL read marks in shared memory without changing
    # market data. Keep its identity/size, but fingerprint the database and WAL
    # with their full timestamps. The deletion manifest remains unmodified for
    # strict per-file checks immediately before unlinking.
    identities = [{key: value for key, value in item.items()
                   if not (item["root"] == "feed" and item["path"].endswith(".sqlite-shm")
                           and key in {"mtime_ns", "ctime_ns"})} for item in files]
    identity = {"symbol": symbol, "product": product, "roots": {key: str(path.resolve()) for key, path in roots.items()}, "files": identities}
    snapshot = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    groups = {}
    for item in files:
        group = groups.setdefault(item["group"], {"files": 0, "bytes": 0})
        group["files"] += 1
        group["bytes"] += item["bytes"]
    return {"symbol": symbol, "product": product, "snapshot": snapshot, "files": files,
            "file_count": len(files), "total_bytes": sum(item["bytes"] for item in files), "groups": groups,
            "start": "", "end": "", "delete_archives": True,
            "message": "删除全部日期的 Tick、K 线、来源归档、转换记录、指标缓存和该交易对的托管实时行情。"
                       "成功后移除配置；合约定义、账户、操作记录和研究报告保留。请先停止手动采集、Pipeline 和相关回测。"}


def delete_pair_data(symbol, product, settings, *, expected_snapshot, project=PROJECT_ROOT):
    if product == "options" and "-" not in symbol:
        plan = pair_deletion_plan(symbol, product, settings, project=project)
        if plan["snapshot"] != expected_snapshot:
            raise ValueError("数据已变化，请重新预览删除范围")
        count, size = 0, 0
        # Lock one concrete contract at a time, retaining the exact preview for
        # every contract. This avoids opening thousands of inherited descriptors.
        for contract, snapshot in plan["contracts"].items():
            with ExitStack() as leases:
                for kind in ("job", "access"):
                    path = project / ".local/run/api/data/options" / f"{contract}.{kind}.lock"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    lease = leases.enter_context(path.open("a"))
                    try:
                        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        raise ValueError("期权合约仍有采集或数据任务运行，请停止后重新预览") from None
                result = delete_pair_data(contract, product, settings, expected_snapshot=snapshot, project=project)
            count += result["deleted_files"]
            size += result["deleted_bytes"]
        if pair_deletion_plan(symbol, product, settings, project=project)["file_count"]:
            raise ValueError("删除期间出现新数据，配置保留；请停止其他写入并重新预览")
        return {"deleted_files": count, "deleted_bytes": size, "groups": plan["groups"], "snapshot": plan["snapshot"]}
    with ExitStack() as stack:
        plan = pair_deletion_plan(symbol, product, settings, project=project)
        # Refuse deletion of a manually running managed collector, even with a stale API state.
        for item in plan["files"]:
            if item["root"] == "feed" and Path(item["path"]).name == "writer.lock":
                lease = stack.enter_context((pair_deletion_roots(settings, project)["feed"] / item["path"]).open("r"))
                try:
                    fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise ValueError("该交易对仍有采集进程运行，请先停止后重新预览") from None
        if plan["snapshot"] != expected_snapshot:
            raise ValueError("数据已变化，请重新预览删除范围")
        result = delete_planned_files(plan, pair_deletion_roots(settings, project))
        if pair_deletion_plan(symbol, product, settings, project=project)["file_count"]:
            raise ValueError("删除期间出现新数据，配置保留；请停止其他写入并重新预览")
        return result


def _trim_sqlite(db, symbol, cutoff, *, apply=False):
    """Count or remove prefix rows in the existing managed journal schemas."""
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    count, retained_evidence = 0, 0
    for table, column in (("trades", "timestamp_ms"), ("candles", "start_ms"), ("bars", "start_ms"),
                          ("provenance", "start_ms"), ("rollups", "start_ms"), ("pending_updates", "start_ms"),
                          ("gaps", "end_ms")):
        if table not in tables:
            continue
        columns = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
        where = f"{column}<?" + (" AND symbol=?" if "symbol" in columns else "")
        parameters = (cutoff, symbol) if "symbol" in columns else (cutoff,)
        count += db.execute(f"SELECT count(*) FROM {table} WHERE {where}", parameters).fetchone()[0]
        if apply:
            db.execute(f"DELETE FROM {table} WHERE {where}", parameters)
    if "conflicts" in tables:
        for (detail,) in db.execute("SELECT detail FROM conflicts").fetchall():
            value = json.loads(detail)
            stamp = (value[1] if isinstance(value, list) and len(value) == 5 else
                     value.get("start_ms", value.get("row", {}).get("time")) if isinstance(value, dict) else None)
            if type(stamp) is not int:
                retained_evidence += 1
            if type(stamp) is int and stamp < cutoff:
                count += 1
                if apply:
                    db.execute("DELETE FROM conflicts WHERE detail=?", (detail,))
    if "runtime_values" in tables:
        count += db.execute("SELECT count(*) FROM runtime_values WHERE name='started_ms' AND value<?", (cutoff,)).fetchone()[0]
        if apply:
            db.execute("UPDATE runtime_values SET value=max(value,?) WHERE name='started_ms'", (cutoff,))
    if "config" in tables:
        columns = {row[1] for row in db.execute("PRAGMA table_info(config)")}
        if "symbol" in columns:
            if any(row[0] != symbol for row in db.execute("SELECT symbol FROM config")):
                raise ValueError("行情日志交易对与删除范围不一致")
        elif "key" in columns:
            symbols = db.execute("SELECT value FROM config WHERE key='symbols'").fetchone()
            if symbols and json.loads(symbols[0]) != [symbol]:
                raise ValueError("旧分钟日志包含其他交易对，不能修改共享起点")
            row = db.execute("SELECT value FROM config WHERE key='start_ms'").fetchone()
            if row and int(json.loads(row[0])) < cutoff:
                count += 1
                if apply:
                    db.execute("UPDATE config SET value=? WHERE key='start_ms'", (json.dumps(cutoff),))
    for table in ("cursors", "checkpoints"):
        if table in tables:
            count += db.execute(f"SELECT count(*) FROM {table} WHERE symbol=? AND next_ms<?", (symbol, cutoff)).fetchone()[0]
            if apply:
                db.execute(f"UPDATE {table} SET next_ms=? WHERE symbol=? AND next_ms<?", (cutoff, symbol, cutoff))
    if "incidents" in tables:
        retained_evidence += db.execute("SELECT count(*) FROM incidents").fetchone()[0]
    return count, retained_evidence


def _parquet_event_bounds(path):
    """Use actual timestamp statistics, with bounded reads when statistics are absent."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    source = pq.ParquetFile(path)
    index = source.schema_arrow.get_field_index("ts_event")
    if index < 0:
        raise ValueError("行情文件缺少事件时间，不能安全裁剪")
    statistics = [source.metadata.row_group(i).column(index).statistics for i in range(source.metadata.num_row_groups)]
    if statistics and all(item is not None and item.has_min_max and item.null_count == 0 for item in statistics):
        return min(int(item.min_raw) for item in statistics), max(int(item.max_raw) for item in statistics)
    first, last = None, None
    for batch in source.iter_batches(batch_size=65536, columns=["ts_event"]):
        values = batch.column(0).cast(pa.int64())
        if values.null_count:
            raise ValueError("行情存在空事件时间，不能安全裁剪")
        low, high = pc.min(values).as_py(), pc.max(values).as_py()
        if low is not None:
            first, last = min(first if first is not None else low, low), max(last if last is not None else high, high)
    return first, last


def history_trim_plan(symbol, product, start, settings, *, project=PROJECT_ROOT):
    """Plan deletion before a UTC day, including row trimming across file boundaries."""
    from .catalog import _parse_filename_timestamps

    if product != "futures_um" or start.tzinfo is None:
        raise ValueError("配置范围裁剪当前仅支持 USD-M 和明确 UTC 日期")
    start = start.astimezone(UTC)
    if start.time() != datetime.min.time():
        raise ValueError("数据开始日期必须为 UTC 零点")
    cutoff = int(start.timestamp() * 1000)
    complete = pair_deletion_plan(symbol, product, settings, project=project)
    roots = pair_deletion_roots(settings, project)
    files, retained = [], []
    with ExitStack() as stack:
        for item in complete["files"]:
            path = roots[item["root"]] / item["path"]
            if item["root"] == "feed" and path.name == "writer.lock":
                lease = stack.enter_context(path.open("r"))
                try:
                    fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise ValueError("该交易对仍有采集或计算进程，请先停止后预览") from None
            operation, threshold = None, cutoff * 1000000
            parts = Path(item["path"]).parts
            if item["group"] == "features" or item["root"] == "feed" and "features" in parts:
                operation = "delete"
            elif path.suffix == ".parquet":
                if "bars" not in parts and "trades" not in parts and "TradeQuote" not in parts:
                    continue
                bounds = _parse_filename_timestamps(path.name)
                if bounds is None:
                    raise ValueError("行情文件名无法识别，不能安全裁剪")
                if "bars" in parts:
                    identifier = path.parent.name
                    intervals = [level for level in INTERVALS if bar_type_for(symbol, level) == identifier]
                    if not intervals:
                        raise ValueError("K 线周期无法识别，不能安全裁剪")
                    left, right = bar_window(intervals[0], cutoff)
                    threshold = (right - 1 if left == cutoff else bar_window(intervals[0], right)[1] - 1) * 1000000
                elif "trades" not in parts and "TradeQuote" not in parts:
                    continue  # Retain instrument definitions in an internal source Catalog.
                if bounds[0] < threshold:
                    first, last = _parquet_event_bounds(path)
                    operation = "delete" if first is None or last < threshold else "parquet"
            elif item["root"] == "feed":
                if path.suffix == ".sqlite":
                    db = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
                    try:
                        rows, evidence = _trim_sqlite(db, symbol, cutoff)
                    except sqlite3.DatabaseError as exc:
                        raise ValueError("行情日志结构无法安全裁剪，请检查来源") from exc
                    finally:
                        db.close()
                    if evidence:
                        retained.append({"path": item["path"], "reason": f"保留 {evidence} 条无法确认日期范围的旧异常证据"})
                    if rows:
                        operation = "sqlite"
                elif path.suffix == ".json":
                    if path.name in {"checkpoint.json", "status.json"} or "changes" in parts:
                        operation = "delete"
                    elif "audits" in parts:
                        try:
                            if datetime.fromisoformat(path.stem).replace(tzinfo=UTC) < start:
                                operation = "delete"
                        except ValueError:
                            retained.append({"path": item["path"], "reason": "无法确认日期的核验证据保留"})
            elif item["root"] in {"raw", "state"}:
                match = re.search(r"-(\d{4}-\d{2}(?:-\d{2})?)\.zip", path.name)
                if match:
                    value = match[1]
                    begin = datetime.fromisoformat(value + ("-01" if len(value) == 7 else "")).replace(tzinfo=UTC)
                    finish = (begin + timedelta(days=1) if len(value) == 10 else
                              begin.replace(year=begin.year + 1, month=1) if begin.month == 12 else begin.replace(month=begin.month + 1))
                    if finish <= start:
                        operation = "delete"
                    elif begin < start:
                        retained.append({"path": item["path"], "reason": "完整月归档跨越开始日期，保留官方文件及校验"})
                else:
                    retained.append({"path": item["path"], "reason": "无法确认日期的来源文件保留"})
            if operation:
                files.append({**item, "operation": operation, "threshold_ns": str(threshold)})
    # Read-only SQLite can create ephemeral WAL bookkeeping. It contains no new
    # market rows; actual database files and nonempty WAL retain strict identities.
    latest = pair_deletion_plan(symbol, product, settings, project=project)
    identities = [item for item in latest["files"] if not item["path"].endswith(".sqlite-shm")
                  and not (item["path"].endswith(".sqlite-wal") and not item["bytes"])]
    snapshot = hashlib.sha256(json.dumps({"files": identities, "start": start.isoformat(),
                                         "roots": {key: str(path.resolve()) for key, path in roots.items()},
                                         "symbol": symbol, "product": product}, sort_keys=True).encode()).hexdigest()
    current = {(item["root"], item["path"]): item for item in latest["files"]}
    files = [{**item, **current[(item["root"], item["path"])], "operation": item["operation"]} for item in files]
    files.sort(key=lambda item: (0 if item["group"] in {"state", "features"} or item["operation"] == "delete" and item["root"] == "feed" else 1,
                                 item["root"], item["path"]))
    groups = {}
    for item in files:
        group = groups.setdefault(item["group"], {"files": 0, "bytes": 0})
        group["files"] += 1
        group["bytes"] += item["bytes"]
    return {"symbol": symbol, "product": product, "snapshot": snapshot, "files": files, "groups": groups,
            "file_count": len(files), "total_bytes": sum(item["bytes"] for item in files),
            "start": "", "end": start.isoformat(), "delete_archives": True, "retained_files": retained,
            "message": "删除开始日期之前的 Tick、K 线、相关缓存和完整日归档；跨界行情文件仅裁掉旧记录。"
                       "周/月 K 线若包含日期之前的来源则整根移除。完整月归档跨界时保留，合约定义、账户及研究报告保留。"
                       "中断可能完成部分删除，服务保持暂停，须重新预览。"}


def trim_pair_history(symbol, product, start, settings, *, expected_snapshot, project=PROJECT_ROOT):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from .catalog import _timestamps_to_filename

    plan = history_trim_plan(symbol, product, start, settings, project=project)
    if plan["snapshot"] != expected_snapshot:
        raise ValueError("数据已变化，请重新预览配置删除范围")
    roots = pair_deletion_roots(settings, project)
    with ExitStack() as leases:
        for item in pair_deletion_plan(symbol, product, settings, project=project)["files"]:
            if item["root"] == "feed" and Path(item["path"]).name == "writer.lock":
                lease = leases.enter_context((roots["feed"] / item["path"]).open("r"))
                try:
                    fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise ValueError("该交易对仍有行情进程，请先停止后重新预览") from None
        changed = 0
        for item in plan["files"]:
            path = verify_planned_file(item, roots)
            if item["operation"] == "delete":
                path.unlink()
            else:
                with TemporaryDirectory(prefix=f".{symbol}-range-trim-", dir=roots[item["root"]]) as directory:
                    staged = Path(directory) / path.name
                    if item["operation"] == "parquet":
                        table = pq.read_table(path)
                        stamps = table.column("ts_event").cast(pa.int64())
                        table = table.filter(pc.greater_equal(stamps, int(item["threshold_ns"])))
                        if not table.num_rows:
                            raise ValueError("跨界文件内容与时间范围不一致，请检查来源")
                        values = table.column("ts_event").cast(pa.int64())
                        target = path.with_name(_timestamps_to_filename(pc.min(values).as_py(), pc.max(values).as_py()))
                        if target != path and target.exists():
                            raise ValueError("裁剪目标文件已存在，请核对重叠来源")
                        pq.write_table(table, staged)
                    else:
                        source = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
                        destination = sqlite3.connect(staged)
                        try:
                            source.backup(destination)
                            destination.execute("PRAGMA journal_mode=DELETE")
                            with destination:
                                _trim_sqlite(destination, symbol, int(start.timestamp() * 1000), apply=True)
                            destination.execute("VACUUM")
                        finally:
                            source.close()
                            destination.close()
                        target = path
                    with staged.open("rb") as stream:
                        os.fsync(stream.fileno())
                    verify_planned_file(item, roots)
                    os.replace(staged, path)
                    if target != path:
                        os.replace(path, target)
                    if item["operation"] == "sqlite":
                        for suffix in ("-wal", "-shm"):
                            sidecar = Path(str(path) + suffix)
                            if sidecar.is_symlink():
                                raise ValueError("日志侧文件变为符号链接，已停止")
                            sidecar.unlink(missing_ok=True)
            changed += 1
            print(f"已裁剪 {item['root']}/{item['path']}：{item['operation']}", flush=True)
    return {"changed_files": changed, "start": start.isoformat(), "snapshot": expected_snapshot,
            "retained_files": plan["retained_files"]}
