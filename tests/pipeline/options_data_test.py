"""Option REST evidence, native storage, retries and isolated deletion."""

import json
from datetime import UTC, datetime, timedelta

import pytest
from nautilus_trader.model import AggressorSide, InstrumentId, Price, Quantity, TradeId, TradeTick

from core.market.instruments import instrument_id_for, normalize_symbol
from engine.connectors.binance import options
from engine.pipeline.catalog import ParquetDataCatalog
from engine.pipeline.data_management import (
    checked_request,
    dataset_inventory,
    deletion_plan,
    execute,
    preview,
)
from engine.pipeline.models import DataRequest, PipelineSettings
from engine.pipeline.options_data import prepare, validate
from tests.connectors.binance.options_queries_test import definition


@pytest.fixture
def context(tmp_path, monkeypatch):
    item = definition(monkeypatch)
    monkeypatch.setattr(options, "markets", lambda: [item])
    settings = PipelineSettings("futures_um", "trades", ("BTCUSDT",), ("1s", "1m", "5m", "15m"),
                                tmp_path / "raw", tmp_path / "catalog", tmp_path / "state")
    return settings, item


def recent_request():
    end = datetime.now(UTC)
    start = max(end - timedelta(minutes=15), end.replace(hour=0, minute=0, second=0, microsecond=0))
    return DataRequest(("BTC-261225-85000-C",), start, end, bar_intervals=(), market="options")


def trade(identity, stamp, price="3000.000"):
    return {"tradeId": identity, "symbol": "BTC-261225-85000-C", "time": stamp,
            "price": price, "qty": "0.01", "side": 1}


def bars_request():
    start = datetime(2026, 8, 1, tzinfo=UTC)
    return DataRequest(("BTC-261225-85000-C",), start, start + timedelta(days=1),
                       bar_intervals=("1m",), market="options")


def fake_klines(monkeypatch, request, calls):
    stamp = int(request.start.timestamp() * 1000)

    def get(endpoint, **params):
        assert endpoint == "klines"
        calls.append(params)
        if params["startTime"] > stamp:
            return []
        return [[stamp, "3000.000", "3010.000", "2990.000", "3005.000", "0.03", stamp + 59999],
                [stamp + 60000, "3005.000", "3010.000", "2990.000", "3005.000", "0.00", stamp + 119999]]

    monkeypatch.setattr(options, "get", get)


def test_option_model_and_perpetual_identity_are_separate():
    assert normalize_symbol("btc-261225-85000-c", "options") == "BTC-261225-85000-C"
    assert instrument_id_for("BTC-261225-85000-C", "options") == "BTC-261225-85000-C.BINANCE"
    assert instrument_id_for("BTCUSDC") == "BTCUSDC-PERP.BINANCE"
    assert bars_request().data_type == "klines"
    for symbol in ["BTC-260231-85000-C", "BTC-261225-0-C", "../../BTC-261225-85000-C", "BTC-261225-85000-X"]:
        with pytest.raises(ValueError):
            normalize_symbol(symbol, "options")


def test_recent_tick_capture_deduplicates_and_does_not_claim_full_history(context, monkeypatch):
    settings, _item = context
    request = recent_request()
    stamp = int(request.start.timestamp() * 1000) + 1000
    packet = [trade(1, stamp), trade(2, stamp), trade(1, stamp)]
    monkeypatch.setattr(options, "get", lambda *args, **kwargs: packet)
    first = prepare(request, settings)
    assert first["ready"] and first["records_written"] == 2
    assert not first["historical_tick_complete"]
    assert prepare(request, settings)["records_written"] == 0
    assert validate(request, settings)["ready"]
    data = preview(settings, request.symbols[0], request.start, request.end, market="options")
    assert [row["trade_id"] for row in data["records"]] == ["1", "2"]
    assert data["records"][0]["aggressor_side"] == "BUY"
    packet.append(trade(3, stamp + 1000))
    assert prepare(request, settings)["records_written"] == 1
    assert validate(request, settings)["ready"]
    items = dataset_inventory(settings, all_levels=True)
    assert [(row["product"], row["level"]) for row in items] == [("options", "tick")]
    assert items[0]["quality"] == "partial_recent_trades"
    assert items[0]["contract"]["multiplier"] == "10"


def test_option_bars_source_closed_times_retry_and_delete_isolation(context, monkeypatch, tmp_path):
    settings, _item = context
    request, calls = bars_request(), []
    fake_klines(monkeypatch, request, calls)
    result = execute("prepare", request, settings, tmp_path / "unused")
    assert result["ready"] and result["records_written"] == 2
    assert not result["historical_tick_complete"]
    assert validate(request, settings)["ready"]
    assert len(calls) == 2
    assert prepare(request, settings)["records_written"] == 0
    assert len(calls) == 2
    data = preview(settings, request.symbols[0], request.start, request.start + timedelta(minutes=2),
                   interval="1m", market="options")
    assert int(data["records"][0]["ts_event_ns"]) == int(request.start.timestamp()) * 1_000_000_000 + 60_000_000_000 - 1
    catalog = ParquetDataCatalog(settings.catalog_dir)
    catalog.write_data([TradeTick(InstrumentId.from_str("BTCUSDT-PERP.BINANCE"), Price.from_str("85000.00"),
                                  Quantity.from_str("0.01"), AggressorSide.BUY, TradeId("99"),
                                  int(request.start.timestamp()) * 1_000_000_000, 0)])
    plan = deletion_plan(request, settings)
    assert plan["file_count"] == 1
    execute("delete", request, settings, tmp_path / "unused", expected_snapshot=plan["snapshot"])
    assert len(catalog.trade_ticks(instrument_ids=["BTCUSDT-PERP.BINANCE"])) == 1
    monkeypatch.setattr(options, "get", lambda *args, **kwargs: pytest.fail("should restore from local evidence"))
    monkeypatch.setattr(options, "markets", lambda: pytest.fail("local instrument is retained"))
    assert prepare(request, settings)["ready"]
    plan = deletion_plan(request, settings, delete_archives=True)
    execute("delete", request, settings, tmp_path / "unused", expected_snapshot=plan["snapshot"], delete_archives=True)
    assert not list(settings.raw_dir.rglob("*.json"))
    assert len(catalog.trade_ticks(instrument_ids=["BTCUSDT-PERP.BINANCE"])) == 1


def test_interrupted_install_replays_immutable_sources(context, monkeypatch):
    import os

    settings, _item = context
    request, calls = bars_request(), []
    fake_klines(monkeypatch, request, calls)
    original = os.link

    def interrupted(source, destination):
        if ".options-stage-" in str(source):
            raise OSError("fixture interruption")
        return original(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(os, "link", interrupted)
        with pytest.raises(OSError, match="interruption"):
            prepare(request, settings)
    assert list(settings.raw_dir.rglob("*.json"))
    assert not validate(request, settings)["ready"]
    count = len(calls)
    assert prepare(request, settings)["ready"]
    assert len(calls) == count
    assert validate(request, settings)["ready"]


def test_source_corruption_conflict_and_precision_do_not_overwrite(context, monkeypatch):
    settings, _item = context
    request = recent_request()
    stamp = int(request.start.timestamp() * 1000) + 1000
    packet = [trade(1, stamp)]
    monkeypatch.setattr(options, "get", lambda *args, **kwargs: packet)
    prepare(request, settings)
    packet[0]["price"] = "3000.0001"
    with pytest.raises(ValueError, match="精度"):
        prepare(request, settings)
    packet[0]["price"] = "3010.000"
    with pytest.raises(ValueError, match="冲突"):
        prepare(request, settings)
    assert not validate(request, settings)["ready"]
    source = next(settings.raw_dir.rglob("*.json"))
    value = json.loads(source.read_text())
    value["fetched_ms"] += 1
    source.write_text(json.dumps(value))
    assert not validate(request, settings)["ready"]
    assert preview(settings, request.symbols[0], request.start, request.end, market="options")["records"][0]["price"] == "3000.000"


def test_unsupported_period_and_historical_tick_download_are_rejected(context):
    settings, _item = context
    request = bars_request()
    with pytest.raises(ValueError, match="1m"):
        checked_request(request.symbols[0], request.start, request.end, ["1s"], settings, market="options")
    old = DataRequest(request.symbols, request.start, request.end, bar_intervals=(), market="options")
    with pytest.raises(ValueError, match="近期"):
        prepare(old, settings)


def test_empty_response_and_nonadvancing_pages_never_create_fake_data(context, monkeypatch):
    settings, _item = context
    request = bars_request()
    monkeypatch.setattr(options, "get", lambda *args, **kwargs: [])
    assert not prepare(request, settings)["ready"]
    assert not settings.catalog_dir.exists()
    assert not list(settings.raw_dir.rglob("*.json"))
    stamp = int(request.start.timestamp() * 1000)
    monkeypatch.setattr(options, "get", lambda *args, **kwargs:
                        [[stamp, "3000.000", "3000.000", "3000.000", "3000.000", "0.01", stamp + 59999]])
    with pytest.raises(ValueError, match="分页"):
        prepare(request, settings)
    assert not settings.catalog_dir.exists()


def test_worker_entry_point_carries_option_product(context, monkeypatch, tmp_path):
    import sys

    from engine.pipeline import data_management

    settings, _item = context
    request, calls = bars_request(), []
    fake_klines(monkeypatch, request, calls)
    monkeypatch.setattr(data_management, "load_settings", lambda: settings)
    monkeypatch.setattr(data_management, "PROJECT_ROOT", tmp_path)
    payload = {"symbol": request.symbols[0], "product": "options", "start": request.start.isoformat(),
               "end": request.end.isoformat(), "intervals": ["1m"]}
    identity = "f" * 32
    monkeypatch.setattr(sys, "argv", ["data_management", "prepare", "--request", json.dumps(payload),
                                      "--job-id", identity])
    data_management.main()
    result = json.loads((tmp_path / ".local/state/api/data_jobs" / f"{identity}.json").read_text())
    assert result["ready"] and result["scope"] == "options_exchange_klines"
    assert dataset_inventory(settings, all_levels=True)[0]["product"] == "options"


def test_recent_capture_keeps_returned_trades_with_older_observed_dates(context, monkeypatch):
    settings, _item = context
    request = recent_request()
    yesterday = request.start.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    first = int(yesterday.timestamp() * 1000) + 1000
    last = int(request.start.timestamp() * 1000) + 1000
    packet = [trade(10, first), trade(11, last)]
    monkeypatch.setattr(options, "get", lambda *args, **kwargs: packet)
    result = prepare(request, settings)
    assert result["records_written"] == 2
    assert result["observed_start"].startswith(yesterday.date().isoformat())
    full = DataRequest(request.symbols, yesterday, yesterday + timedelta(days=2),
                       bar_intervals=(), market="options")
    assert validate(full, settings)["ready"]
    assert not result["historical_tick_complete"]


def test_disappearing_storage_is_not_recreated(context, monkeypatch):
    settings, _item = context
    request = bars_request()
    root = settings.raw_dir.parent
    moved = root.with_name(root.name + "-detached")
    stamp = int(request.start.timestamp() * 1000)

    def lost_volume(endpoint, **params):
        if root.exists():
            root.rename(moved)
        if params["startTime"] > stamp:
            return []
        return [[stamp, "3000.000", "3000.000", "3000.000", "3000.000", "0.01", stamp + 59999]]

    monkeypatch.setattr(options, "get", lost_volume)
    try:
        with pytest.raises(OSError, match="存储"):
            prepare(request, settings)
        assert not root.exists()
    finally:
        moved.rename(root)
    assert not list(settings.raw_dir.rglob("*.json"))


def test_local_validation_checks_contract_multiplier_and_precision(context, monkeypatch):
    from nautilus_trader.model import CryptoOption

    settings, _item = context
    request = recent_request()
    stamp = int(request.start.timestamp() * 1000) + 1000
    monkeypatch.setattr(options, "get", lambda *args, **kwargs: [trade(1, stamp)])
    assert prepare(request, settings)["ready"]
    catalog = ParquetDataCatalog(settings.catalog_dir)
    original = catalog.instruments()[0].to_dict()
    original["multiplier"] = "1"
    original["ts_init"] += 1
    catalog.write_data([CryptoOption.from_dict(original)])
    result = validate(request, settings)
    assert not result["ready"]
    assert any("合约单位" in error for error in result["errors"])


def test_managed_live_late_trades_and_historical_holes_merge_atomically(context, monkeypatch):
    from engine.pipeline.options_data import persist_live

    settings, item = context
    symbol = item["symbol"]
    stamp = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    events = [{"level": "tick", "row": trade(identity, stamp + offset)}
              for identity, offset in ((1, 1000), (3, 3000))]
    assert persist_live(symbol, events, settings)["records_written"] == 2
    assert persist_live(symbol, [{"level": "tick", "row": trade(2, stamp + 2000)}], settings)["records_written"] == 1
    assert persist_live(symbol, [{"level": "tick", "row": trade(4, stamp + 3000)}], settings)["records_written"] == 1
    assert persist_live(symbol, events, settings)["records_written"] == 0
    records = ParquetDataCatalog(settings.catalog_dir).trade_ticks(instrument_ids=[instrument_id_for(symbol, "options")])
    assert [str(row.trade_id) for row in records] == ["1", "2", "3", "4"]
    with pytest.raises(ValueError, match="冲突"):
        persist_live(symbol, [{"level": "tick", "row": trade(2, stamp + 2000, "3001.000")}], settings)
    # A stream can arrive before backfill; history fills the same day's interior hole.
    bars = [[stamp + i * 60000, "3000", "3010", "2990", "3005", "0.03", stamp + (i + 1) * 60000 - 1]
            for i in range(3)]
    persist_live(symbol, [{"level": "1m", "row": row} for row in (bars[0], bars[2])], settings)
    monkeypatch.setattr(options, "get", lambda endpoint, **kw: bars if kw["startTime"] == stamp else [])
    result = prepare(bars_request(), settings, managed=True)
    assert result["records_written"] == 1
    assert not result["historical_tick_complete"]
    assert len(ParquetDataCatalog(settings.catalog_dir).bars()) == 3
    assert prepare(bars_request(), settings, managed=True)["records_written"] == 0


def test_managed_live_recovers_source_after_interrupted_install(context, monkeypatch):
    from engine.pipeline import options_data

    settings, item = context
    stamp = int(datetime(2026, 8, 1, tzinfo=UTC).timestamp() * 1000)
    original = options_data._install
    def interrupted(*args, **kwargs):
        raise OSError("interrupted")
    monkeypatch.setattr(options_data, "_install", interrupted)
    with pytest.raises(OSError, match="interrupted"):
        options_data.persist_live(item["symbol"], [{"level": "tick", "row": trade(1, stamp)}], settings)
    assert list(settings.raw_dir.rglob("*.json"))
    monkeypatch.setattr(options_data, "_install", original)
    assert options_data.persist_live(item["symbol"], [], settings, recover=True)["records_written"] == 1
    assert options_data.persist_live(item["symbol"], [], settings, recover=True)["records_written"] == 0
    with pytest.raises(ValueError, match="未闭合"):
        options_data.persist_live(item["symbol"], [{"level": "1m", "row": [9999999960000, "1", "1", "1", "1", "0", 9999999960000 + 59999]}], settings)


@pytest.mark.parametrize("source", ["ws/kline", "eapi/v1/klines"])
def test_history_fetches_rest_despite_existing_partial_day(context, monkeypatch, source):
    from engine.pipeline import options_data

    settings, item = context
    request, calls = bars_request(), []
    stamp = int(request.start.timestamp() * 1000)
    options_data.persist_live(item["symbol"], [{"level": "1m", "row":
        [stamp, "3000.000", "3010.000", "2990.000", "3005.000", "0.03", stamp + 59999]}], settings)
    if source == "eapi/v1/klines":
        snapshot = options_data._load(next(settings.raw_dir.rglob("*.json")))
        snapshot["source"] = source
        options_data._save(snapshot, settings, options_data._storage(settings))
    fake_klines(monkeypatch, request, calls)

    assert prepare(request, settings)["records_written"] == 1
    assert len(calls) == 2
    assert all(call["symbol"] == item["symbol"] and call["interval"] == "1m" for call in calls)
    assert validate(request, settings)["ready"]
    assert len(ParquetDataCatalog(settings.catalog_dir).bars()) == 2
    assert prepare(request, settings)["records_written"] == 0
    assert len(calls) == 2


def test_option_history_checkpoint_retries_empty_day_and_resumes(context, monkeypatch, tmp_path):
    import asyncio

    from engine.pipeline import options_data

    settings, item = context
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start = today - timedelta(days=1)
    policy = {"symbol": item["symbol"], "product": "options", "base_level": "tick", "intervals": [],
              "history_mode": "start", "history_start": start.isoformat()}
    calls = []
    monkeypatch.setattr(options, "get", lambda *args, **kw: [[int(start.timestamp() * 1000)]])
    empty = True
    def prepare_day(request, settings, *, managed, definition=None):
        assert managed and request.bar_intervals == ("1m", "5m", "15m")
        calls.append(request.start)
        return {"empty_batches": int(empty)}
    monkeypatch.setattr(options_data, "prepare", prepare_day)
    original_sleep = asyncio.sleep
    async def stop_after_attempt(seconds):
        if seconds == 30:
            raise asyncio.CancelledError
        await original_sleep(0)
    monkeypatch.setattr(asyncio, "sleep", stop_after_attempt)
    root = tmp_path / "history"
    async def run():
        with pytest.raises(asyncio.CancelledError):
            await options_data.serve_history(policy, settings, root)
    asyncio.run(run())
    assert not (root / "checkpoint.json").exists()
    empty = False
    asyncio.run(run())
    saved = json.loads((root / "checkpoint.json").read_text())
    assert saved["next_day"] == today.isoformat()
    assert calls == [start, start]
    asyncio.run(run())
    assert calls == [start, start]


def test_batch_history_limits_writers_and_releases_only_stopped_contract(context, monkeypatch, tmp_path):
    import asyncio
    import fcntl
    import threading
    import time

    from engine.pipeline import options_data

    settings, template = context
    rows = [{**template, "symbol": f"BTC-261225-{85000 + index * 1000}-C", "strike": str(85000 + index * 1000)}
            for index in range(3)]
    monkeypatch.setattr(options, "markets", lambda: rows)
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    stamp = int((today - timedelta(days=1)).timestamp() * 1000)
    monkeypatch.setattr(options, "get", lambda *args, **kwargs: [[stamp]])
    active, peak = 0, 0
    lock = threading.Lock()
    def prepare_day(request, settings, *, managed, definition):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(.1)
        with lock:
            active -= 1
        return {"empty_batches": 0}
    monkeypatch.setattr(options_data, "prepare", prepare_day)
    policies = [{"symbol": row["symbol"], "product": "options", "base_level": "1m", "history_enabled": True,
                 "history_mode": "all", "realtime_enabled": False} for row in rows]
    manifest = tmp_path / "runtime.json"
    manifest.write_text(json.dumps({"version": 1, "policies": policies}))
    managed = tmp_path / "managed"
    managed.mkdir()
    async def scenario():
        task = asyncio.create_task(options_data.serve_history_batch(manifest, settings, managed, project=tmp_path))
        try:
            async with asyncio.timeout(8):
                while not all((managed / p["symbol"] / "1m/history/checkpoint.json").exists() for p in policies):
                    await asyncio.sleep(.05)
            assert peak == 2
            for p in policies:
                saved = json.loads((managed / p["symbol"] / "1m/history/checkpoint.json").read_text())
                assert saved["next_day"] == today.isoformat()
            policies[0]["history_enabled"] = False
            manifest.write_text(json.dumps({"version": 1, "policies": policies}))
            access = tmp_path / ".local/run/api/data/options" / f"{policies[0]['symbol']}.access.lock"
            async with asyncio.timeout(5):
                while True:
                    with access.open("a") as lease:
                        try:
                            fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            break
                        except BlockingIOError:
                            pass
                    await asyncio.sleep(.05)
            other = tmp_path / ".local/run/api/data/options" / f"{policies[1]['symbol']}.access.lock"
            with other.open("a") as lease, pytest.raises(BlockingIOError):
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


def test_history_batch_entry_point_uses_shared_runtime_and_mounted_root(context, monkeypatch, tmp_path):
    from engine.pipeline import options_data, pair_data

    settings, _item = context
    (tmp_path / "data").mkdir()
    root = tmp_path / "data/managed/.options"
    manifest = tmp_path / "runtime.json"
    manifest.write_text('{"version":1,"policies":[]}')
    called = []
    async def serve_batch(path, settings, managed_root):
        called.append((path, settings, managed_root))
    monkeypatch.setattr(pair_data, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(pair_data, "load_settings", lambda: settings)
    monkeypatch.setattr(options_data, "serve_history_batch", serve_batch)
    monkeypatch.setattr("sys.argv", ["engine.pipeline.pair_data", "--service", "--options-config", str(manifest), "--root", str(root)])
    pair_data.main()
    assert called == [(manifest, settings, root.parent)]


def test_batch_remembers_contract_units_before_expiry_without_inventing_data(context, monkeypatch):
    from engine.pipeline.options_data import persist_live

    settings, item = context
    result = persist_live(item["symbol"], [], settings, definition=options.instrument(item).to_dict())
    assert result["records_written"] == 0
    catalog = ParquetDataCatalog(settings.catalog_dir)
    assert len(catalog.instruments()) == 1
    assert dataset_inventory(settings, all_levels=True) == []
    def unavailable():
        raise AssertionError("Expired contracts must recover their preserved local definition")
    monkeypatch.setattr(options, "markets", unavailable)
    recovered = persist_live(item["symbol"], [], settings, recover=True)
    assert recovered["expiry_ms"] == item["expiry"]
    assert not recovered["historical_tick_complete"]


def test_live_depth_persists_exact_raw_snapshots_and_participates_in_deletion(context):
    from engine.pipeline import options_data

    settings, item = context
    request = bars_request()
    stamp = int(request.start.timestamp() * 1000) + 1000
    row = {"s": item["symbol"], "T": stamp, "u": 42, "received_ns": stamp * 1000000 + 123,
           "b": [["3000.000", "0.10"], ["2990.000", "0.20"]], "a": [["3010.000", "0.30"]]}
    events = [{"level": "depth", "row": row}, {"level": "tick", "row": trade(1, stamp)}]
    result = options_data.persist_live(item["symbol"], events, settings)
    assert result["records_written"] == 1
    paths = options_data.source_paths(request, settings)
    depth = next(path for path in paths if path.parent.parent.name == "depth")
    snapshot = options_data._load(depth)
    assert snapshot["source"] == "ws/depth"
    assert snapshot["payload"] == [row]
    _, decoded = options_data._decode(snapshot)
    assert decoded[0][0].sequence == 42
    assert decoded[0][0].ts_init == row["received_ns"]
    assert str(decoded[0][0].bids[0].price) == "3000.000"
    assert options_data.persist_live(item["symbol"], (), settings, recover=True)["records_written"] == 0
    plan = deletion_plan(request, settings, delete_archives=True)
    assert any(entry["path"] == depth.relative_to(settings.raw_dir).as_posix() for entry in plan["files"])
    assert depth.exists()
    with pytest.raises(ValueError, match="交叉"):
        options_data.persist_live(item["symbol"], [{"level": "depth", "row": {**row, "a": [["2990.000", "0.10"]]}}], settings)
    assert options_data._load(depth)["payload"] == [row]
    assert len(list(depth.parent.glob("*.json"))) == 1
