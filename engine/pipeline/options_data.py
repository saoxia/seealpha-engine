"""Option data tasks using public EAPI evidence and the native Catalog.

Klines remain exchange bars; recent trades remain partial Tick observations.
Immutable source snapshots precede atomic Parquet installation, so an interrupted
write is detectable and the same evidence can be installed again on retry.
"""

import fcntl
import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

from nautilus_trader.model import (
    Bar,
    CryptoOption,
    TradeTick,
)

from core.market.instruments import (
    bar_type_for,
    instrument_id_for,
    interval_delta,
    normalize_symbol,
)
from engine.connectors.binance import options
from engine.connectors.binance.options_parsing import bar as parse_bar
from engine.connectors.binance.options_parsing import depth as parse_depth
from engine.connectors.binance.options_parsing import trade as parse_trade
from engine.connectors.binance.queries import QueryError
from engine.pipeline.catalog import ParquetDataCatalog

PERIODS = ("1m", "5m", "15m")
DEFINITION_FIELDS = ("underlying", "quote_currency", "settlement_currency", "option_kind", "strike_price",
                     "expiration_ns", "multiplier", "price_precision", "size_precision")


def check_request(request):
    if any(period not in PERIODS for period in request.bar_intervals or ()):
        raise ValueError("期权历史 K 线仅支持 1m、5m、15m；Tick 为独立的近期成交来源")


def check_download(request):
    check_request(request)
    now = datetime.now(UTC)
    if request.bar_intervals:
        if (request.start.time() != datetime.min.time() or request.end.time() != datetime.min.time()
                or request.end > now.replace(hour=0, minute=0, second=0, microsecond=0)):
            raise ValueError("期权历史 K 线按已完成的完整 UTC 日获取")
    elif (request.start.date() != now.date() or request.end.date() != now.date()
          or request.start < now - timedelta(minutes=16) or request.end > now + timedelta(seconds=5)
          or request.end < now - timedelta(minutes=2)):
        raise ValueError("官方仅提供近期成交；请提交当前获取任务，不能指定任意历史 Tick 区间")


def _storage(settings, device=None):
    # Never recreate the shared storage base when an external volume disappears.
    root = Path(os.path.commonpath((settings.raw_dir, settings.catalog_dir)))
    if not root.is_dir() or (device is not None and root.stat().st_dev != device):
        raise OSError("期权数据存储已不可用或挂载发生变化，不会自动重建目录")
    return root.stat().st_dev


def _safe(root, path):
    current = root
    for part in path.relative_to(root).parts:
        current /= part
        if current.is_symlink():
            raise ValueError("期权数据路径包含符号链接")
    return path


def source_paths(request, settings, *, include_pending=False):
    root = settings.raw_dir / "options" / request.symbols[0]
    _safe(settings.raw_dir, root)
    day, last = request.start.date(), (request.end - timedelta(microseconds=1)).date()
    result = []
    while day <= last:
        for level in ("tick", "depth", *PERIODS):
            folder = _safe(settings.raw_dir, root / level / day.isoformat())
            if folder.exists():
                for path in folder.iterdir():
                    _safe(settings.raw_dir, path)
                    if path.name.startswith(".source-") and path.is_file():
                        if include_pending:
                            result.append(path)
                        continue
                    if not path.is_file() or len(path.stem) != 64 or path.suffix != ".json":
                        raise ValueError("期权来源目录包含无法识别的文件")
                    result.append(path)
        day += timedelta(days=1)
    return sorted(result)


def _decode(snapshot):
    instrument = CryptoOption.from_dict(snapshot["instrument"])
    symbol, level = str(instrument.raw_symbol), snapshot["level"]
    sources = {"tick": ("eapi/v1/trades", "ws/optionTrade"), "depth": ("ws/depth",)}
    if (snapshot["product"] != "options" or level not in ("tick", "depth", *PERIODS)
            or str(instrument.id) != instrument_id_for(symbol, "options")
            or snapshot["source"] not in sources.get(level, ("eapi/v1/klines", "ws/kline"))):
        raise ValueError("无效的期权来源类型")
    first, last = snapshot["start_ms"], snapshot["end_ms"]
    if type(first) is not int or type(last) is not int or first < 0 or last <= first:
        raise ValueError("期权来源时间范围无效")
    result = {}
    payload = snapshot["payload"]
    if not isinstance(payload, list):
        raise TypeError("无效的期权行情响应")
    for row in payload:
        if level == "depth":
            if type(row["received_ns"]) is not int or not 0 <= row["received_ns"] < 2**64:
                raise ValueError("期权盘口接收时间无效")
            item = parse_depth(instrument, row, 10, row["received_ns"])
            if not first * 1000000 <= item.ts_event < last * 1000000 or item.ts_event > instrument.expiration_ns:
                raise ValueError("期权盘口超出时间范围或到期边界")
            key = f"{item.ts_event}:{item.sequence}"
            record = {"ts_event_ns": str(item.ts_event), "sequence": item.sequence,
                      "bids": row["b"], "asks": row["a"]}
        elif level == "tick":
            if row.get("symbol", symbol) != symbol or type(row["time"]) is not int:
                raise ValueError("期权成交合约或时间不一致")
            stamp = row["time"]
            if not first <= stamp < last:
                continue
            tick = parse_trade(instrument, row)
            if tick.ts_event > instrument.expiration_ns:
                raise ValueError("期权成交超过到期边界")
            identity = tick.trade_id
            key = str(identity)
            record = {"ts_event_ns": str(tick.ts_event), "price": str(tick.price), "size": str(tick.size),
                      "aggressor_side": str(tick.aggressor_side), "trade_id": key}
            item = tick
        else:
            item = parse_bar(instrument, level, row)
            if item.ts_event > instrument.expiration_ns:
                raise ValueError("期权 K 线超过到期边界")
            duration = int(interval_delta(level).total_seconds() * 1000)
            opened = item.ts_event // 1_000_000 + 1 - duration
            if not first <= opened < last:
                raise ValueError("期权 K 线超出时间范围")
            key = str(item.ts_event)
            record = {"ts_event_ns": key, **{field: str(getattr(item, field)) for field in ("open", "high", "low", "close", "volume")}}
        if key in result and result[key][1] != record:
            raise ValueError("期权来源包含冲突的重复记录")
        result[key] = (item, record)
    return instrument, sorted(result.values(), key=lambda pair: (pair[0].ts_event, int(pair[1].get("trade_id", "0"))))


def _load(path):
    with path.open("rb") as source:
        raw = source.read(20 * 1024 * 1024 + 1)
    if len(raw) > 20 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != path.stem:
        raise ValueError("期权来源快照校验失败")
    snapshot = json.loads(raw)
    day = datetime.fromtimestamp(snapshot["start_ms"] / 1000, UTC)
    if (path.parent.name != day.date().isoformat() or path.parent.parent.name != snapshot["level"]
            or path.parent.parent.parent.name != snapshot["instrument"]["raw_symbol"]
            or day.time() != datetime.min.time() or snapshot["end_ms"] != snapshot["start_ms"] + 86400000):
        raise ValueError("期权来源路径与内容范围不一致")
    return snapshot


def _save(snapshot, settings, device):
    _storage(settings, device)
    instrument, values = _decode(snapshot)
    folder = settings.raw_dir / "options" / str(instrument.raw_symbol) / snapshot["level"] / datetime.fromtimestamp(snapshot["start_ms"] / 1000, UTC).date().isoformat()
    _safe(settings.raw_dir, folder)
    folder.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(snapshot, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()
    destination = folder / (hashlib.sha256(raw).hexdigest() + ".json")
    descriptor, temporary = tempfile.mkstemp(prefix=".source-", dir=folder)
    try:
        with os.fdopen(descriptor, "wb") as target:
            target.write(raw); target.flush(); os.fsync(target.fileno())
        try:
            os.link(temporary, destination)
        except FileExistsError:
            if destination.read_bytes() != raw:
                raise ValueError("期权来源发生冲突") from None
    finally:
        Path(temporary).unlink(missing_ok=True)
    return destination, len(values)


def _existing(settings, instrument, level, first, last):
    from engine.pipeline.data_management import rows
    if not settings.catalog_dir.is_dir():
        return {}
    records = rows(settings, str(instrument.raw_symbol), datetime.fromtimestamp(first / 1000, UTC),
                   datetime.fromtimestamp(last / 1000, UTC), interval=None if level == "tick" else level,
                   market="options")
    result = {}
    try:
        for record in records:
            key = record["trade_id" if level == "tick" else "ts_event_ns"]
            if key in result:
                raise ValueError("本地期权数据存在重复编号或时间")
            result[key] = record
        return result
    finally:
        records.close()


def _ensure_definition(instrument, settings, device):
    """Retain public contract metadata even when an option has no observations yet."""
    catalog = ParquetDataCatalog(settings.catalog_dir)
    existing_instrument = catalog.instruments(instrument_ids=[str(instrument.id)])
    if not existing_instrument:
        with tempfile.TemporaryDirectory(prefix=".options-stage-", dir=settings.catalog_dir.parent) as stage:
            staged = ParquetDataCatalog(stage)
            staged.write_data([instrument])
            for source in Path(stage).rglob("*.parquet"):
                _storage(settings, device)
                target = _safe(settings.catalog_dir, settings.catalog_dir / source.relative_to(stage))
                target.parent.mkdir(parents=True, exist_ok=True)
                os.link(source, target)

    else:
        original = max(existing_instrument, key=lambda value: value.ts_init).to_dict()
        supplied = instrument.to_dict()
        if any(original[key] != supplied[key] for key in DEFINITION_FIELDS):
            raise ValueError("期权合约定义与本地定义冲突")
    return catalog


def _install(path, settings, device, *, merge=False):
    _storage(settings, device)
    snapshot = _load(path)
    instrument, values = _decode(snapshot)
    catalog = _ensure_definition(instrument, settings, device)
    level = snapshot["level"]
    if level == "depth":
        # Depth history is the immutable raw snapshot; there is no derived Bar/Tick.
        return 0
    existing = _existing(settings, instrument, level, snapshot["start_ms"], snapshot["end_ms"])
    missing = []
    for item, record in values:
        key = record["trade_id" if level == "tick" else "ts_event_ns"]
        if key in existing and existing[key] != record:
            raise ValueError("期权来源与已有数据冲突，不会覆盖已有数据")
        if key not in existing:
            missing.append(item)
    if not missing:
        return 0
    identifier = str(instrument.id) if level == "tick" else bar_type_for(str(instrument.raw_symbol), level, "options")
    ranges = catalog.get_intervals(TradeTick if level == "tick" else Bar, identifier)
    if merge:
        return _merge_missing(catalog, settings, instrument, level, missing, device)
    if level == "tick" and ranges and missing[0].ts_event <= max(right for _left, right in ranges):
        raise ValueError("近期成交出现未保存的旧时间戳，无法安全追加；请校验来源或预览后清理该日期再获取")
    if any(left <= item.ts_event <= right for item in missing for left, right in ranges):
        raise ValueError("已有文件跨越待补数据，无法安全追加；请先校验或清理明确范围")
    with tempfile.TemporaryDirectory(prefix=".options-stage-", dir=settings.catalog_dir.parent) as stage:
        staged = ParquetDataCatalog(stage)
        staged.write_data(missing)
        kind = "trades" if level == "tick" else "bars"
        relative = Path("data") / kind / identifier
        target = _safe(settings.catalog_dir, settings.catalog_dir / relative)
        target.mkdir(parents=True, exist_ok=True)
        for source in (Path(stage) / relative).glob("*.parquet"):
            os.link(source, target / source.name)
    return len(missing)


def _definition(symbol, settings, definition=None, *, local_only=False):
    if definition is not None:
        value = CryptoOption.from_dict(definition)
        if str(value.raw_symbol) != normalize_symbol(symbol, "options"):
            raise ValueError("期权批量定义与合约不一致")
        return value
    if settings.catalog_dir.is_dir():
        values = ParquetDataCatalog(settings.catalog_dir).instruments(instrument_ids=[instrument_id_for(symbol, "options")])
        if values:
            return max(values, key=lambda item: item.ts_init)
    if local_only:
        raise ValueError("本地无期权定义，等待公开目录刷新")
    for item in options.markets():
        if item["symbol"] == symbol:
            return options.instrument(item)
    raise ValueError("当前期权目录未找到合约，且本地无合约定义")


def _capture_recent(request, settings, instrument, device, *, merge=False):
    from dataclasses import replace

    payload = options.get("trades", symbol=str(instrument.raw_symbol), limit=500)
    if not isinstance(payload, list) or len(payload) > 500:
        raise ValueError("近期成交返回格式或数量无效")
    moment = int(datetime.now(UTC).timestamp() * 1000)
    groups = {}
    for row in payload:
        stamp = row.get("time") if isinstance(row, dict) else None
        if type(stamp) is not int or stamp < 0 or stamp > moment + 5000 or stamp * 1_000_000 > instrument.expiration_ns:
            raise ValueError("近期成交时间无效或超过合约到期日")
        day = datetime.fromtimestamp(stamp / 1000, UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        groups.setdefault(day, []).append(row)
    if payload:
        _decode({"product": "options", "level": "tick", "start_ms": min(row["time"] for row in payload),
                 "end_ms": max(row["time"] for row in payload) + 1, "source": "eapi/v1/trades",
                 "fetched_ms": moment, "instrument": instrument.to_dict(), "payload": payload})
    written, observed, times = 0, 0, []
    for day, records in sorted(groups.items()):
        part = replace(request, start=day, end=day + timedelta(days=1))
        # Source windows use observed event dates, not the time the capture was submitted.
        # This preserves all returned trades even for infrequently traded contracts.
        for path in sorted(source_paths(part, settings), key=lambda value: _load(value)["fetched_ms"]):
            if _load(path)["level"] == "tick":
                written += _install(path, settings, device, merge=merge)
        snapshot = {"product": "options", "level": "tick", "start_ms": int(day.timestamp() * 1000),
                    "end_ms": int((day + timedelta(days=1)).timestamp() * 1000), "fetched_ms": moment,
                    "source": "eapi/v1/trades", "instrument": instrument.to_dict(), "payload": records}
        path, count = _save(snapshot, settings, device)
        written += _install(path, settings, device, merge=merge)
        observed += count
        times.extend(row["time"] for row in records)
        print(f"{day.date()} 近期接口返回 {count} 笔；不证明当日或历史完整", flush=True)
    return {"ready": bool(observed), "records_written": written, "records_fetched": observed,
            "observed_start": datetime.fromtimestamp(min(times) / 1000, UTC).isoformat() if times else None,
            "observed_end": datetime.fromtimestamp(max(times) / 1000, UTC).isoformat() if times else None,
            "scope": "options_recent_trades", "historical_tick_complete": False,
            "message": "保存官方近期接口实际返回的最多 500 笔；实际日期以记录为准，不提供任意历史 Tick 补齐。"}


def _prepare(request, settings, *, managed=False, definition=None):
    check_download(request)
    device = _storage(settings)
    instrument = _definition(request.symbols[0], settings, definition)
    if not request.bar_intervals:
        return _capture_recent(request, settings, instrument, device, merge=managed)
    written, fetched, empty = 0, 0, 0
    paths = source_paths(request, settings)
    relevant = [(path, _load(path)) for path in paths]
    relevant = [(path, raw) for path, raw in relevant if raw["level"] in request.bar_intervals]
    for path, _raw in sorted(relevant, key=lambda pair: (pair[1]["start_ms"], pair[1]["fetched_ms"])):
        written += _install(path, settings, device, merge=managed)
    periods = request.bar_intervals
    for level in periods:
        day = request.start
        while day < request.end:
            end = min(day + timedelta(days=1), request.end)
            first, last = int(day.timestamp() * 1000), int(end.timestamp() * 1000)
            if not managed and any(raw["level"] == level and raw["start_ms"] == first
                                       and raw["end_ms"] == last and raw["source"] == "eapi/v1/klines"
                                       and raw.get("history_request_complete") is True
                                       and _decode(raw)[1] for _p, raw in relevant):
                print(f"{day.date()} {level} 复用本地期权来源", flush=True)
                day = end
                continue
            print(f"{instrument.raw_symbol} {level} 获取 Binance 期权公开行情 {day.isoformat()}", flush=True)
            payload = []
            cursor, duration = first, int(interval_delta(level).total_seconds() * 1000)
            while cursor < last:
                page = options.get("klines", symbol=str(instrument.raw_symbol), interval=level,
                                   startTime=cursor, endTime=last - 1, limit=1500)
                if not isinstance(page, list) or len(page) > 1500:
                    raise ValueError("期权 K 线返回格式或数量无效")
                if not page:
                    break
                stamps = [row["openTime"] if isinstance(row, dict) else row[0] for row in page]
                if any(type(stamp) is not int or stamp < cursor or stamp >= last for stamp in stamps):
                    raise ValueError("期权 K 线分页未前进或超出范围")
                payload.extend(page)
                if len(payload) > 1500:
                    raise ValueError("单日期权 K 线数量超出周期上限")
                cursor = max(stamps) + duration
            snapshot = {"product": "options", "level": level, "start_ms": first, "end_ms": last,
                        "fetched_ms": int(datetime.now(UTC).timestamp() * 1000),
                        "source": "eapi/v1/klines",
                        "history_request_complete": True,
                        "instrument": instrument.to_dict(), "payload": payload}
            _instrument, decoded = _decode(snapshot)
            if not decoded:
                empty += 1
                print(f"{day.date()} {level} 官方未返回此范围内记录；不写入空数据或声明完整", flush=True)
                day = end
                continue
            path, count = _save(snapshot, settings, device)
            written += _install(path, settings, device, merge=managed)
            fetched += count
            empty += not bool(count)
            print(f"{day.date()} {level} 来源含 {count} 条记录；不声明历史 Tick 完整", flush=True)
            day = end
    return {"ready": empty == 0 and (written > 0 or bool(relevant)), "records_written": written,
            "records_fetched": fetched, "empty_batches": empty,
            "scope": "options_exchange_klines",
            "historical_tick_complete": False,
            "message": "仅保存官方返回的记录；近期成交与交易所 K 线均不证明历史 Tick 完整。"}


def validate(request, settings):
    wanted = request.bar_intervals or ("tick",)
    expected = {level: {} for level in wanted}
    errors = []
    first, last = int(request.start.timestamp() * 1000) * 1_000_000, int(request.end.timestamp() * 1000) * 1_000_000
    instrument = None
    definitions = (ParquetDataCatalog(settings.catalog_dir).instruments(
        instrument_ids=[instrument_id_for(request.symbols[0], "options")]) if settings.catalog_dir.is_dir() else [])
    saved_definition = max(definitions, key=lambda item: item.ts_init).to_dict() if definitions else None
    for path in source_paths(request, settings):
        try:
            snapshot = _load(path)
            if snapshot["level"] not in wanted:
                continue
            instrument, values = _decode(snapshot)
            supplied = instrument.to_dict()
            if saved_definition is None or any(saved_definition[key] != supplied[key] for key in DEFINITION_FIELDS):
                raise ValueError("本地期权合约单位、精度或定义与来源不一致")
            for _item, record in values:
                if first <= int(record["ts_event_ns"]) < last:
                    level = snapshot["level"]
                    key = record["trade_id" if level == "tick" else "ts_event_ns"]
                    if key in expected[level] and expected[level][key] != record:
                        raise ValueError("期权来源之间存在冲突")
                    expected[level][key] = record
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append(str(exc))
    if instrument is not None:
        for level in wanted:
            actual = _existing(settings, instrument, level, first // 1_000_000, last // 1_000_000)
            if actual != expected[level]:
                errors.append(f"{level} 本地 Catalog 与来源快照不一致")
    else:
        errors.append("此范围没有可校验的期权来源")
    return {"ready": not errors and all(expected.values()), "scope": "options_local_source_consistency",
            "records": sum(len(values) for values in expected.values()), "errors": errors,
            "historical_tick_complete": False,
            "message": "校验本地保存与已捕获来源的一致性，不证明交易所历史完整。"}


def coverage(request, settings):
    paths = source_paths(request, settings)
    days, day = [], request.start.date()
    catalog = ParquetDataCatalog(settings.catalog_dir) if settings.catalog_dir.is_dir() else None
    levels = request.bar_intervals or ("tick",)
    ranges = {level: catalog.get_intervals(TradeTick if level == "tick" else Bar,
              instrument_id_for(request.symbols[0], "options") if level == "tick" else
              bar_type_for(request.symbols[0], level, "options")) if catalog else [] for level in levels}
    while day <= (request.end - timedelta(microseconds=1)).date():
        present = any(path.parent.name == day.isoformat() and path.parent.parent.name in (request.bar_intervals or ("tick",)) for path in paths)
        begin = int(datetime.combine(day, datetime.min.time(), UTC).timestamp()) * 1_000_000_000
        finish = begin + 86400 * 1_000_000_000
        stored = all(any(left < finish and right >= begin for left, right in ranges[level]) for level in levels)
        days.append({"day": day.isoformat(), "symbol": request.symbols[0], "archive_present": present,
                     "catalog_present": stored, "recorded_rows": None})
        day += timedelta(days=1)
    return {"days": days, "all_present": False, "quality": "source_consistency_check_required",
            "historical_tick_complete": False,
            "message": "期权来源快照不代表完整历史覆盖；获取仅复用本地来源或补充官方可返回数据。"}


def execute(action, request, settings):
    if action == "prepare":
        return prepare(request, settings)
    if action == "validate":
        return validate(request, settings)
    raise ValueError("期权暂不支持从不完整近期 Tick 派生历史 K 线或使用永续导出入口")


@contextmanager
def writer(symbol, settings):
    """Serialize live, history and explicit writers for the same option."""
    symbol = normalize_symbol(symbol, "options")
    _storage(settings)
    root = _safe(settings.state_dir, settings.state_dir / "options" / symbol)
    root.mkdir(parents=True, exist_ok=True)
    with _safe(settings.state_dir, root / "writer.lock").open("a") as lease:
        fcntl.flock(lease, fcntl.LOCK_EX)
        yield


def prepare(request, settings, *, managed=False, definition=None):
    with writer(request.symbols[0], settings):
        return _prepare(request, settings, managed=managed, definition=definition)


def _merge_missing(catalog, settings, instrument, level, missing, device):
    """Fill holes with atomic single-file replacement, preserving known records."""
    from engine.pipeline.catalog import _parse_filename_timestamps

    identifier = str(instrument.id) if level == "tick" else bar_type_for(str(instrument.raw_symbol), level, "options")
    kind = "trades" if level == "tick" else "bars"
    folder = _safe(settings.catalog_dir, settings.catalog_dir / "data" / kind / identifier)
    folder.mkdir(parents=True, exist_ok=True)
    remaining = list(missing)
    boundaries = []
    for path in sorted(folder.glob("*.parquet")):
        _safe(settings.catalog_dir, path)
        bounds = _parse_filename_timestamps(path.name)
        if bounds is None:
            raise ValueError("期权文件时间范围无法识别")
        left, right = bounds
        additions = [value for value in remaining if left <= value.ts_event <= right]
        boundaries.append((left, right))
        if not additions:
            continue
        original = (catalog.trade_ticks(instrument_ids=[identifier], start=left, end=right) if level == "tick" else
                    catalog.bars(bar_types=[identifier], start=left, end=right))
        merged = sorted([*original, *additions], key=lambda value: (value.ts_event, int(str(value.trade_id)) if level == "tick" else 0))
        with tempfile.TemporaryDirectory(prefix=".options-stage-", dir=settings.catalog_dir.parent) as stage:
            ParquetDataCatalog(stage).write_data(merged)
            source = Path(stage) / "data" / kind / identifier / path.name
            if not source.is_file():
                raise ValueError("期权合并改变原文件边界，拒绝替换")
            _storage(settings, device)
            with source.open("rb") as stream:
                os.fsync(stream.fileno())
            os.replace(source, path)
        remaining = [value for value in remaining if not left <= value.ts_event <= right]
    # Separate new ranges at existing files; no new file may span retained data.
    groups = {}
    for value in remaining:
        index = sum(right < value.ts_event for _left, right in boundaries)
        groups.setdefault(index, []).append(value)
    for values in groups.values():
        with tempfile.TemporaryDirectory(prefix=".options-stage-", dir=settings.catalog_dir.parent) as stage:
            ParquetDataCatalog(stage).write_data(values)
            for source in (Path(stage) / "data" / kind / identifier).glob("*.parquet"):
                _storage(settings, device)
                with source.open("rb") as stream:
                    os.fsync(stream.fileno())
                os.link(source, folder / source.name)
    return len(missing)


def persist_live(symbol, events, settings, *, recover=False, definition=None):
    """Persist native stream evidence through the existing Pipeline storage boundary."""
    with writer(symbol, settings):
        device = _storage(settings)
        instrument = _definition(symbol, settings, definition)
        _ensure_definition(instrument, settings, device)
        written = 0
        if recover:
            root = settings.raw_dir / "options" / symbol
            for path in sorted(root.glob("*/*/*.json")):
                _safe(settings.raw_dir, path)
                snapshot = _load(path)
                if snapshot["source"].startswith("ws/"):
                    written += _install(path, settings, device, merge=True)
        groups = {}
        now = int(datetime.now(UTC).timestamp() * 1000)
        for event in events:
            level, row = event["level"], event["row"]
            stamp = row["time"] if level == "tick" else row["T"] if level == "depth" else row[0]
            closed = stamp if level in {"tick", "depth"} else row[6] + 1
            if type(stamp) is not int or closed > now or closed * 1000000 > instrument.expiration_ns:
                raise ValueError("期权实时来源未闭合、时间超前或超过到期边界")
            day = stamp // 86400000 * 86400000
            groups.setdefault((level, day), []).append(row)
        for (level, day), rows in sorted(groups.items()):
            snapshot = {"product": "options", "level": level, "start_ms": day, "end_ms": day + 86400000,
                        "fetched_ms": now, "source": {"tick": "ws/optionTrade", "depth": "ws/depth"}.get(level, "ws/kline"),
                        "instrument": instrument.to_dict(), "payload": rows}
            path, _count = _save(snapshot, settings, device)
            written += _install(path, settings, device, merge=True)
        return {"records_written": written, "expiry_ms": instrument.expiration_ns // 1000000,
                "historical_tick_complete": False}


def repair_live(symbol, start_ms, settings, *, definition=None):
    """Recover exchange closed bars since disconnect; Tick recovery stays partial."""
    with writer(symbol, settings):
        device = _storage(settings)
        instrument = _definition(symbol, settings, definition)
        now = min(int(datetime.now(UTC).timestamp() * 1000), instrument.expiration_ns // 1000000)
        for level in PERIODS:
            duration = int(interval_delta(level).total_seconds() * 1000)
            end = now // duration * duration
            cursor = start_ms // duration * duration
            while cursor < end:
                last = min((cursor // 86400000 + 1) * 86400000, end)
                payload = options.get("klines", symbol=symbol, interval=level,
                                      startTime=cursor, endTime=last - 1, limit=1500)
                if not isinstance(payload, list) or len(payload) > 1500:
                    raise ValueError("期权恢复来源格式无效")
                stamps = [row["openTime"] if isinstance(row, dict) else row[0] for row in payload]
                if any(type(stamp) is not int or not cursor <= stamp < last for stamp in stamps):
                    raise ValueError("期权恢复分页越界或未推进")
                day = cursor // 86400000 * 86400000
                snapshot = {"product": "options", "level": level, "start_ms": day, "end_ms": day + 86400000,
                            "fetched_ms": int(datetime.now(UTC).timestamp() * 1000), "source": "eapi/v1/klines",
                            "instrument": instrument.to_dict(), "payload": payload}
                _instrument, decoded = _decode(snapshot)
                if any(item.ts_event >= now * 1000000 for item, _record in decoded):
                    raise ValueError("期权恢复包含未闭合 K 线")
                if payload:
                    path, _count = _save(snapshot, settings, device)
                    _install(path, settings, device, merge=True)
                cursor = max(stamps) + duration if stamps else last
        return now


async def serve_history(policy, settings, root, *, definition=None, limiter=None, heartbeat_seconds=5):
    """Option history uses existing process supervision and file checkpoints."""
    import asyncio

    from engine.pipeline.convert_binance_bars import write_record
    from engine.pipeline.models import DataRequest

    limiter = limiter or asyncio.Semaphore(1)

    async def run(function, *args, **kwargs):
        async with limiter:
            task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
            try:
                return await asyncio.shield(task)
            finally:
                if not task.done():
                    await task

    device = _storage(settings)
    root.mkdir(parents=True, exist_ok=True)
    checkpoint = root / "checkpoint.json"
    configuration = hashlib.sha256(json.dumps(policy, sort_keys=True).encode()).hexdigest()
    saved = json.loads(checkpoint.read_text()) if checkpoint.exists() else {}
    if saved.get("configuration") != configuration:
        saved = {}
    state = {"state": "WARMING_UP", "backlog": None, "last_error": None,
             "historical_tick_complete": False, "allow_new_risk": False}

    async def heartbeat():
        while True:
            now = int(datetime.now(UTC).timestamp() * 1000)
            _storage(settings, device)
            write_record(root / "status.json", {**state, "checked_ms": now, "valid_until_ms": now + heartbeat_seconds * 3000})
            await asyncio.sleep(heartbeat_seconds)

    beat = asyncio.create_task(heartbeat())
    try:
        while True:
            try:
                _storage(settings, device)
                instrument = await run(_definition, policy["symbol"], settings, definition)
                # The directory drops expired contracts; preserve its real units first.
                await run(persist_live, policy["symbol"], (), settings, definition=instrument.to_dict())
                expiry = datetime.fromtimestamp(instrument.expiration_ns / 1e9, UTC)
                today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
                end = min(today, expiry.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
                if not saved:
                    rows = await run(options.get, "klines", symbol=policy["symbol"],
                                                   interval="1m", startTime=0, limit=1)
                    if not rows:
                        raise ValueError("官方未返回最早历史，检查点未推进")
                    stamp = rows[0]["openTime"] if isinstance(rows[0], dict) else rows[0][0]
                    if (type(stamp) is not int or stamp < 0 or stamp % 60000
                            or stamp > int(datetime.now(UTC).timestamp() * 1000)
                            or stamp >= instrument.expiration_ns // 1000000):
                        raise ValueError("官方最早 K 线时间无效，检查点未推进")
                    available = datetime.fromtimestamp(stamp / 1000, UTC)
                    if policy["history_mode"] == "start":
                        start = max(datetime.fromisoformat(policy["history_start"]).astimezone(UTC), available)
                    elif policy["history_mode"] == "duration":
                        start = max(today - timedelta(days=policy["history_days"]), available)
                    else:
                        start = available
                    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
                    saved = {"configuration": configuration, "start": start.isoformat(), "next_day": start.isoformat()}
                day = datetime.fromisoformat(saved["next_day"])
                state.update(backlog=max(0, (end - day).days), expiry_ms=instrument.expiration_ns // 1000000)
                if day >= end:
                    state.update(state="READY", last_error=None, reason="caught_up")
                    write_record(checkpoint, saved)
                    await asyncio.sleep(30)
                    continue
                state.update(state="WARMING_UP", current_day=str(day.date()))
                request = DataRequest((policy["symbol"],), day, day + timedelta(days=1),
                                      bar_intervals=PERIODS, market="options")
                # One UTC day per commit; failed/empty days are retried, never skipped.
                result = await run(prepare, request, settings, managed=True, definition=definition)
                if result["empty_batches"]:
                    raise ValueError("官方存在空来源，保留此日待重试；不声明完整")
                saved["next_day"] = request.end.isoformat()
                write_record(checkpoint, saved)
                state.update(state="READY", last_error=None, last_processed_ms=int(request.end.timestamp() * 1000))
            except (OSError, ValueError, QueryError) as exc:
                state.update(state="DEGRADED", last_error=str(exc))
                await asyncio.sleep(30)
    finally:
        beat.cancel()
        await asyncio.gather(beat, return_exceptions=True)


async def serve_history_batch(manifest, settings, managed_root, *, project=None):
    """One supervised history process; per-contract locks/checkpoints, two active writers."""
    import asyncio
    import time

    from core.paths import PROJECT_ROOT
    from engine.pipeline.convert_binance_bars import write_record

    project = project or PROJECT_ROOT
    limiter = asyncio.Semaphore(2)
    jobs, definitions, retry_at = {}, {}, {}
    fetch, fetched_at = None, float("-inf")

    async def run(policy, definition):
        symbol = normalize_symbol(policy["symbol"], "options")
        root = managed_root / symbol / policy["base_level"] / "history"
        access = project / ".local/run/api/data/options" / f"{symbol}.access.lock"
        access.parent.mkdir(parents=True, exist_ok=True)
        with access.open("a") as lease:
            fcntl.flock(lease, fcntl.LOCK_SH | fcntl.LOCK_NB)
            _storage(settings)
            if not managed_root.is_dir():
                raise OSError("行情存储不可用，不重建挂载目录")
            root.mkdir(parents=True, exist_ok=True)
            with (root / "writer.lock").open("a") as writer_lease:
                fcntl.flock(writer_lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                try:
                    await serve_history(policy, settings, root, definition=definition, limiter=limiter, heartbeat_seconds=15)
                except (OSError, ValueError, QueryError) as exc:
                    if root.is_dir():
                        stamp = int(time.time() * 1000)
                        write_record(root / "status.json", {"state": "DEGRADED", "last_error": str(exc),
                                     "checked_ms": stamp, "valid_until_ms": stamp + 45000})
                    raise

    try:
        while True:
            now = time.monotonic()
            if fetch is None and now - fetched_at >= 300:
                fetch = asyncio.create_task(asyncio.to_thread(options.markets))
            if fetch is not None and fetch.done():
                try:
                    definitions.update({row["symbol"]: options.instrument(row).to_dict() for row in fetch.result()})
                    fetched_at = now
                except (OSError, ValueError, QueryError):
                    fetched_at = now - 270
                fetch = None
            try:
                if manifest.stat().st_size > 5 * 1024 * 1024:
                    raise ValueError("期权运行配置过大")
                value = json.loads(manifest.read_text())
                if value.get("version") != 1 or not isinstance(value.get("policies"), list):
                    raise ValueError("期权运行配置异常")
                desired = {}
                for policy in value["policies"]:
                    symbol = normalize_symbol(policy["symbol"], "options")
                    if policy["product"] != "options" or policy["base_level"] not in {"tick", *PERIODS}:
                        raise ValueError("期权运行范围异常")
                    if symbol in desired or type(policy.get("history_enabled")) is not bool:
                        raise ValueError("期权运行配置重复或开关无效")
                    if policy["history_enabled"]:
                        desired[symbol] = {**policy, "symbol": symbol}
            except (OSError, ValueError, KeyError, TypeError):
                # A broken snapshot never means an empty desired set.
                await asyncio.sleep(1)
                continue
            removed = [symbol for symbol, (policy, task) in jobs.items() if desired.get(symbol) != policy]
            for symbol in removed:
                jobs[symbol][1].cancel()
            if removed:
                await asyncio.gather(*(jobs[symbol][1] for symbol in removed), return_exceptions=True)
                for symbol in removed:
                    del jobs[symbol]
            for symbol, policy in desired.items():
                if symbol in jobs and jobs[symbol][1].done():
                    task = jobs.pop(symbol)[1]
                    if not task.cancelled():
                        task.exception()  # Consume a worker failure before its bounded retry.
                    retry_at[symbol] = now + 30
                if symbol not in jobs and now >= retry_at.get(symbol, 0):
                    definition = definitions.get(symbol)
                    if definition is None:
                        if fetch is not None:
                            continue
                        try:
                            definition = (await asyncio.to_thread(_definition, symbol, settings, local_only=True)).to_dict()
                        except (OSError, ValueError, QueryError):
                            fetched_at = now - 300
                            retry_at[symbol] = now + 5
                            access = project / ".local/run/api/data/options" / f"{symbol}.access.lock"
                            access.parent.mkdir(parents=True, exist_ok=True)
                            try:
                                with access.open("a") as lease:
                                    fcntl.flock(lease, fcntl.LOCK_SH | fcntl.LOCK_NB)
                                    _storage(settings)
                                    stamp = int(time.time() * 1000)
                                    write_record(managed_root / symbol / policy["base_level"] / "history/status.json",
                                                 {"state": "DEGRADED", "reason": "waiting_definition", "last_error": "等待官方或本地合约定义，历史检查点未推进",
                                                  "checked_ms": stamp, "valid_until_ms": stamp + 45000})
                            except OSError:
                                pass
                            continue
                    jobs[symbol] = (policy, asyncio.create_task(run(policy, definition)))
            await asyncio.sleep(1)
    finally:
        for _policy, task in jobs.values():
            task.cancel()
        await asyncio.gather(*(task for _policy, task in jobs.values()), return_exceptions=True)
        if fetch is not None:
            await asyncio.gather(fetch, return_exceptions=True)
