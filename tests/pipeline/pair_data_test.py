"""Pair backfill uses calendar windows and respects existing Catalog ranges."""

import asyncio
from datetime import UTC, datetime, timedelta
from itertools import pairwise

from nautilus_trader.model import Bar

from engine.pipeline.catalog import ParquetDataCatalog
from engine.pipeline.models import PipelineSettings
from engine.pipeline.pair_data import backfill, history_bounds
from nautilus_support import TestInstrumentProvider


def test_history_modes_use_retained_anchor_and_explicit_start(tmp_path):
    catalog = ParquetDataCatalog(tmp_path / "catalog")
    now = datetime(2026, 10, 29, tzinfo=UTC)
    policy = {"symbol": "ETHUSDT", "base_level": "1m", "history_mode": "duration", "history_days": 30}
    assert history_bounds(policy, catalog, now) == (now - timedelta(days=30), now)
    assert history_bounds({**policy, "history_mode": "all"}, catalog, now) == (None, now)
    first = datetime(2026, 8, 1, tzinfo=UTC)
    assert history_bounds({**policy, "history_mode": "start", "history_start": first.isoformat()}, catalog, now) == (first, now)


def test_exchange_history_installs_selected_bars_without_ticks_and_retries_idempotently(tmp_path, monkeypatch):
    import engine.pipeline.pair_data as module
    (tmp_path / "data").mkdir()
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    settings = PipelineSettings("futures_um", "trades", ("ETHUSDT",), ("1m",), tmp_path / "data/raw", tmp_path / "data/catalog", tmp_path / "data/state")
    catalog = ParquetDataCatalog(settings.catalog_dir)
    catalog.write_data([TestInstrumentProvider.ethusdt_perp_binance()])
    first = datetime(2026, 10, 28, tzinfo=UTC)
    left = int(first.timestamp() * 1000)

    class Rest:
        async def get(self, endpoint, **params):
            assert endpoint == "klines"
            if params.get("limit") == 1:
                return [[left, "10", "10", "10", "10", "2", left + 59999]]
            assert params["interval"] == "1d"
            return [[left, "10", "12", "9", "11", "2", left + 86400000 - 1]]

    policy = {"symbol": "ETHUSDT", "product": "futures_um", "base_level": "1d", "intervals": [], "history_mode": "all"}
    result = asyncio.run(backfill(policy, settings, rest=Rest(), now=first + timedelta(days=1)))
    assert result["ready"] and result["records_written"] == 1
    assert len(catalog.bars()) == 1
    assert not catalog.trade_ticks()
    repeated = asyncio.run(backfill(policy, settings, rest=Rest(), now=first + timedelta(days=1)))
    assert repeated["records_written"] == 0 and len(catalog.bars()) == 1
    start, _ = history_bounds({**policy, "history_mode": "duration", "history_days": 3}, catalog, first + timedelta(days=1))
    assert start == first - timedelta(days=3)
    assert catalog.get_intervals(Bar)


def test_duration_uses_retained_realtime_base_and_calendar_open(tmp_path):
    now = datetime(2026, 10, 29, tzinfo=UTC)
    first = datetime(2026, 8, 1, tzinfo=UTC)
    policy = {"symbol": "ETHUSDT", "base_level": "1M", "history_mode": "duration", "history_days": 30}
    # Catalog event convention points to the close; the anchor is the opening month.
    closed = datetime(2026, 8, 31, 23, 59, 59, tzinfo=UTC)
    class Catalog:
        def get_intervals(self, *args):
            stamp = int(closed.timestamp() * 1e9)
            return [(stamp, stamp)]
    start, _ = history_bounds(policy, Catalog(), now)
    assert start == first - timedelta(days=30)
    retained = int(datetime(2026, 7, 15, tzinfo=UTC).timestamp() * 1e9)
    start, _ = history_bounds({**policy, "base_level": "1m"}, None, now, retained_start_ns=retained)
    assert start == datetime(2026, 6, 15, tzinfo=UTC)


def test_history_calculation_derives_complete_windows_and_preserves_conflicts(tmp_path):
    from nautilus_trader.model import BarType, Price, Quantity

    from core.market.instruments import bar_type_for
    from engine.pipeline.pair_data import calculate_history_change
    source = ParquetDataCatalog(tmp_path / "source")
    source.write_data([TestInstrumentProvider.ethusdt_perp_binance()])
    start = datetime(2026, 10, 28, tzinfo=UTC)
    left = int(start.timestamp() * 1000)
    bars = [Bar(BarType.from_str(bar_type_for("ETHUSDT", "1m")), *(Price.from_str(v) for v in ("10", "12", "9", "11")),
                Quantity.from_str("2"), (left + (i + 1) * 60000 - 1) * 1000000,
                (left + (i + 1) * 60000 - 1) * 1000000) for i in range(120)]
    source.write_data(bars)
    event = {"policy": {"symbol": "ETHUSDT", "base_level": "1m", "intervals": ["1h", "1d"]},
             "source": "1m", "source_catalog": source.path, "start": start.isoformat(),
             "coverage_start": start.isoformat(), "end": (start + timedelta(days=1)).isoformat()}
    destination = tmp_path / "destination"
    assert calculate_history_change(event, destination, completed_until=event["end"]) == 2
    catalog = ParquetDataCatalog(destination)
    result = catalog.bars()
    assert len(result) == 2 and all(bar.volume.as_decimal() == 120 for bar in result)
    assert calculate_history_change(event, destination, completed_until=event["end"]) == 0
    altered = ParquetDataCatalog(tmp_path / "altered")
    altered.write_data([TestInstrumentProvider.ethusdt_perp_binance()])
    altered.write_data([Bar(bar.bar_type, bar.open, Price.from_str("13"), bar.low, bar.close, bar.volume,
                            bar.ts_event, bar.ts_init) for bar in bars])
    import pytest
    with pytest.raises(ValueError, match="冲突"):
        calculate_history_change({**event, "source_catalog": altered.path}, destination, completed_until=event["end"])
    assert catalog.bars() == result


def test_history_restart_replays_unfinished_day_and_keeps_fixed_duration_anchor(tmp_path, monkeypatch):
    import json

    import engine.pipeline.pair_data as module
    from engine.pipeline.convert_binance_bars import write_record
    root = tmp_path / "data/history"
    root.mkdir(parents=True)
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    settings = PipelineSettings("futures_um", "trades", ("ETHUSDT",), ("1m",), tmp_path / "data/raw", tmp_path / "data/catalog", tmp_path / "data/state")
    now = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    first = now - timedelta(days=1)
    policy = {"symbol": "ETHUSDT", "product": "futures_um", "base_level": "1m", "intervals": ["1h"],
              "history_mode": "duration", "history_days": 1}
    state = {"configuration": module.history_configuration(policy), "start": first.isoformat(), "next_day": first.isoformat(),
             "available_ms": int(first.timestamp() * 1000), "verification_version": 2}
    write_record(root / "checkpoint.json", state)
    calls = []
    async def unavailable(*args, **kwargs):
        calls.append(args[0])
        raise OSError("offline")
    async def stop(_seconds):
        raise asyncio.CancelledError
    monkeypatch.setattr("engine.pipeline.pair_repair.reconcile_day", unavailable)
    monkeypatch.setattr(module.asyncio, "sleep", stop)
    import pytest
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(module.serve_history(policy, settings, root, rest=object()))
    assert json.loads((root / "checkpoint.json").read_text()) == state
    async def recovered(*args, **kwargs):
        calls.append(args[0])
        return {"ready": True, "checked_at": "now", "records_written": 0}
    monkeypatch.setattr("engine.pipeline.pair_repair.reconcile_day", recovered)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(module.serve_history(policy, settings, root, rest=object()))
    restored = json.loads((root / "checkpoint.json").read_text())
    assert not json.loads(next((root / "audits").glob("*.json")).read_text())["verified"]
    # An interrupted source write requires the existing calculation handoff to
    # finish before saved bars can be accepted as final verification evidence.
    pending = next((root / "changes").glob("*.json"))
    assert json.loads(pending.read_text())["state"] == "ready"
    pending.unlink()  # Calculation completed successfully.
    from types import SimpleNamespace
    monkeypatch.setattr(module, "time", SimpleNamespace(time=lambda: restored["next_scan_at"] + 1))
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(module.serve_history(policy, settings, root, rest=object()))
    restored = json.loads((root / "checkpoint.json").read_text())
    assert restored["start"] == first.isoformat() and restored["next_day"] == now.isoformat()
    assert "next_scan_at" not in restored  # Completed dates are not put back in the scan queue.
    assert calls[0]["symbol"] == calls[1]["symbol"] == "ETHUSDT"
    assert json.loads(next((root / "audits").glob("*.json")).read_text())["verified"]
    assert not list((root / "changes").glob("*.json"))


def test_failed_source_page_install_leaves_no_partial_public_file(tmp_path, monkeypatch):
    import pytest

    import engine.pipeline.pair_data as module
    catalog = ParquetDataCatalog(tmp_path / 'catalog')
    class InterruptedWriter:
        def __init__(self, path):
            self.path = path
        def write_data(self, _bars):
            from pathlib import Path
            partial = Path(self.path) / 'data/bars/interrupted.parquet'
            partial.parent.mkdir(parents=True)
            partial.write_bytes(b'incomplete page')
            raise OSError('writer interrupted')
    monkeypatch.setattr(module, 'ParquetDataCatalog', InterruptedWriter)
    with pytest.raises(OSError, match='interrupted'):
        module.install_bars(catalog, [object()])
    from pathlib import Path
    assert not list(Path(catalog.path).rglob('*.parquet'))


def test_explicit_history_dates_are_aligned_to_utc_days():
    from datetime import timezone
    now = datetime(2026, 10, 30, 3, 45, tzinfo=timezone(timedelta(hours=8)))
    policy = {"symbol": "ETHUSDT", "base_level": "1m", "history_mode": "start",
              "history_start": "2026-08-02T03:45:12+08:00"}
    assert history_bounds(policy, None, now) == (
        datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 10, 29, tzinfo=UTC))


def test_tick_history_reuses_seconds_for_all_periods_and_keeps_trade_order(tmp_path, monkeypatch):
    import shutil
    from decimal import Decimal
    from pathlib import Path
    from types import SimpleNamespace

    from nautilus_trader.model import AggressorSide, Price, Quantity, TradeId, TradeTick

    import engine.pipeline.pair_data as module
    from core.market.instruments import bar_type_for, bar_window

    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    source = ParquetDataCatalog(tmp_path / "ticks")
    source.write_data([instrument])
    start = datetime(2026, 10, 1, tzinfo=UTC)
    left = int(start.timestamp() * 1000)
    # More than one conversion chunk; equal timestamps, hour boundaries and
    # empty seconds must preserve the direct-Tick result without invented bars.
    ticks = [TradeTick(instrument.id, Price.from_str(f"{10 + i % 7}.00"), Quantity.from_str("0.001"),
                       AggressorSide.BUY, TradeId(str(i + 1)),
                       (left + (i // 1200) * 3600000 + i % 3) * 1000000,
                       (left + (i // 1200) * 3600000 + i % 3) * 1000000) for i in range(2400)]
    source.write_data(sorted(ticks, key=lambda tick: tick.ts_event))
    def append_shard(tick, name):
        staged = ParquetDataCatalog(tmp_path / name)
        staged.write_data([tick])
        for path in Path(staged.path).rglob("*.parquet"):
            destination = Path(source.path) / path.relative_to(staged.path)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, destination)
    # A later file adds a trade at the same timestamp, with a smaller ID than
    # trades already present there. File order cannot determine the open/close.
    extra = TradeTick(instrument.id, Price.from_str("9.00"), Quantity.from_str("0.002"),
                      AggressorSide.BUY, TradeId("0"), left * 1000000, left * 1000000)
    append_shard(extra, "repair")
    ticks.append(extra)
    queries = []
    original = module._history_inputs

    def tracked(catalog, *args, **kwargs):
        native = catalog.native
        def query(kind, identifiers, first, last, **options):
            queries.append((first, last))
            return native.query_data_arrow_stream(kind, identifiers, first, last, **options)
        catalog.native = SimpleNamespace(query_data_arrow_stream=query)
        yield from original(catalog, *args, **kwargs)

    monkeypatch.setattr(module, "_history_inputs", tracked)
    levels = ["1s", "1m", "3m", "5m", "1h", "1d", "1w", "1M"]
    event = {"policy": {"symbol": "ETHUSDT", "base_level": "tick", "intervals": levels},
             "source": "tick", "source_catalog": source.path, "start": start.isoformat(),
             "end": (start + timedelta(days=1)).isoformat()}
    destination = tmp_path / "derived"
    module.calculate_history_change(event, destination, completed_until=event["end"])
    assert len(queries) == 24  # One pass over the day, shared by every target.
    assert queries[0][0] == left * 1000000
    assert queries[-1][1] == (left + 86400000) * 1000000 - 1
    assert all(last - first + 1 == 3600000 * 1000000 for first, last in queries)
    assert all(a[1] + 1 == b[0] for a, b in pairwise(queries))
    result = ParquetDataCatalog(destination)
    for level in levels:
        if level in {"1w", "1M"}:
            assert not result.bars(bar_types=[bar_type_for("ETHUSDT", level)])
            continue
        expected = {}
        for tick in sorted(ticks, key=lambda tick: (tick.ts_event, int(str(tick.trade_id)))):
            opening, _ = bar_window(level, tick.ts_event // 1000000)
            expected.setdefault(opening, []).append(tick)
        bars = result.bars(bar_types=[bar_type_for("ETHUSDT", level)])
        assert len(bars) == len(expected)
        for bar in bars:
            trades = expected[bar_window(level, bar.ts_event // 1000000)[0]]
            prices = [trade.price.as_decimal() for trade in trades]
            assert [value.as_decimal() for value in (bar.open, bar.high, bar.low, bar.close, bar.volume)] == [
                prices[0], max(prices), min(prices), prices[-1],
                sum((trade.size.as_decimal() for trade in trades), Decimal(0))]
    assert module.calculate_history_change(event, destination, completed_until=event["end"]) == 0
    # Changed raw evidence must still reject an existing closed derived value.
    append_shard(TradeTick(instrument.id, Price.from_str("20.00"), Quantity.from_str("0.001"),
                          AggressorSide.BUY, TradeId("9999"),
                          (left + 100) * 1000000, (left + 100) * 1000000), "changed")
    import pytest
    with pytest.raises(ValueError, match="冲突"):
        module.calculate_history_change(event, destination, completed_until=event["end"])


def test_history_does_not_read_unclosed_or_partially_covered_calendar_windows(tmp_path, monkeypatch):
    import engine.pipeline.pair_data as module

    source = ParquetDataCatalog(tmp_path / "source")
    source.write_data([TestInstrumentProvider.ethusdt_perp_binance()])
    def forbidden(*args, **kwargs):
        raise AssertionError("No complete target window: source data must not be read")
    monkeypatch.setattr(module, "_history_inputs", forbidden)
    for first, last, coverage in (("2026-10-22", "2026-10-23", "2026-10-01"),
                                  ("2026-10-31", "2026-11-01", "2026-10-02")):
        event = {"policy": {"symbol": "ETHUSDT", "base_level": "tick", "intervals": ["1w", "1M"]},
                 "source": "tick", "source_catalog": source.path,
                 "start": first + "T00:00:00+00:00", "end": last + "T00:00:00+00:00",
                 "coverage_start": coverage + "T00:00:00+00:00"}
        assert module.calculate_history_change(event, tmp_path / "target", completed_until=event["end"]) == 0


def test_minute_source_gap_still_blocks_high_period(tmp_path):
    from nautilus_trader.model import BarType, Price, Quantity

    from core.market.instruments import bar_type_for
    from engine.pipeline.pair_data import calculate_history_change

    source = ParquetDataCatalog(tmp_path / "source")
    source.write_data([TestInstrumentProvider.ethusdt_perp_binance()])
    start = datetime(2026, 10, 1, tzinfo=UTC)
    left = int(start.timestamp() * 1000)
    source.write_data([Bar(BarType.from_str(bar_type_for("ETHUSDT", "1m")),
                           *(Price.from_str("10.00") for _ in range(4)), Quantity.from_str("0.001"),
                           (left + (minute + 1) * 60000 - 1) * 1000000,
                           (left + (minute + 1) * 60000 - 1) * 1000000)
                       for minute in range(120) if minute != 60])
    event = {"policy": {"symbol": "ETHUSDT", "base_level": "1m", "intervals": ["2h"]},
             "source": "1m", "source_catalog": source.path, "start": start.isoformat(),
             "end": (start + timedelta(hours=2)).isoformat()}
    import pytest
    with pytest.raises(ValueError, match="缺口或重复"):
        calculate_history_change(event, tmp_path / "target", completed_until=event["end"])


def test_kline_only_seconds_history_uses_private_tick_source(tmp_path, monkeypatch):
    import json

    import pytest

    import engine.pipeline.pair_data as module
    from engine.pipeline.convert_binance_bars import write_record

    root = tmp_path / "data/managed/history"
    root.mkdir(parents=True)
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    settings = PipelineSettings("futures_um", "trades", ("ETHUSDT",), ("1m",),
                                tmp_path / "data/raw", tmp_path / "data/catalog", tmp_path / "data/state")
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start = today - timedelta(days=2)
    policy = {"symbol": "ETHUSDT", "product": "futures_um", "base_level": "1m", "intervals": ["1s", "1m", "1h"],
              "history_mode": "start", "history_start": start.isoformat()}
    write_record(root / "checkpoint.json", {"configuration": module.history_configuration(policy),
                 "start": start.isoformat(), "next_day": start.isoformat(),
                 "available_ms": int(start.timestamp() * 1000), "verification_version": 2})
    calls = []
    async def repair(source_policy, source_settings, rest, source, *args, **kwargs):
        assert kwargs["comparison_catalog"] == settings.catalog_dir
        calls.append((source_policy, source_settings.catalog_dir, source))
        return {"ready": False, "needs_calculation": True, "checked_at": "now", "records_written": 0}
    async def stop(_delay):
        raise asyncio.CancelledError
    monkeypatch.setattr("engine.pipeline.pair_repair.reconcile_day", repair)
    monkeypatch.setattr(module.asyncio, "sleep", stop)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(module.serve_history(policy, settings, root, rest=object()))
    assert calls[0][0]["base_level"] == "1m"
    assert calls[0][1] == root / "source_catalog" and calls[0][2] == "tick"
    event = json.loads(next((root / "changes").glob("*.json")).read_text())
    assert event["source"] == "tick" and event["source_catalog"] == str(root / "source_catalog")
    assert not settings.catalog_dir.exists()  # Working Tick is not a public dataset.


def test_archive_publication_wait_is_persisted_and_skips_scanning_until_retry(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace

    import pytest

    import engine.pipeline.pair_data as module
    from engine.pipeline.convert_binance_bars import write_record

    root = tmp_path / "data/history"
    root.mkdir(parents=True)
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    settings = PipelineSettings("futures_um", "trades", ("ETHUSDT",), ("1s", "1m"),
                                tmp_path / "data/raw", tmp_path / "data/catalog", tmp_path / "data/state")
    start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    policy = {"symbol": "ETHUSDT", "product": "futures_um", "base_level": "tick", "intervals": ["1s", "1m"],
              "history_mode": "start", "history_start": start.isoformat()}
    initial = {"configuration": module.history_configuration(policy), "start": start.isoformat(),
               "next_day": start.isoformat(), "available_ms": int(start.timestamp() * 1000), "verification_version": 2}
    checkpoint = root / "checkpoint.json"
    write_record(checkpoint, initial)
    clock, available, probes, scans, checks = [1000], [False], [], [], []
    def probe(*args):
        probes.append(args)
        return available[0]
    async def repair(*args, **kwargs):
        checks.append(args)
        if not probe(*args):
            return {"ready": False, "archive_pending": True, "checked_at": "now", "records_written": 0}
        scans.append(args)
        return {"ready": True, "checked_at": "now", "records_written": 0}
    async def stop(_delay):
        raise asyncio.CancelledError
    monkeypatch.setattr(module, "time", SimpleNamespace(time=lambda: clock[0]))
    monkeypatch.setattr(module, "_history_archive_available", probe)
    monkeypatch.setattr("engine.pipeline.pair_repair.reconcile_day", repair)
    monkeypatch.setattr(module.asyncio, "sleep", stop)
    def run():
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(module.serve_history(policy, settings, root, rest=object()))
    run()
    saved = json.loads(checkpoint.read_text())
    assert saved["next_day"] == initial["next_day"]
    assert saved["archive_wait"] == {"day": start.isoformat(), "retry_at": 2800}
    assert len(probes) == 1 and not scans
    assert len(checks) == 1
    assert not list((root / "changes").glob("*.json"))
    available[0] = True
    # Restart within the waiting interval must not probe or run K-line scans.
    run()
    assert json.loads(checkpoint.read_text()) == saved
    assert len(probes) == 1 and not scans
    assert len(checks) == 1
    clock[0] = 2801
    run()
    assert len(probes) == 2 and len(scans) == 1
    assert "archive_wait" not in json.loads(checkpoint.read_text())
    assert json.loads(next((root / "audits").glob("*.json")).read_text())["verified"]


def test_archive_day_becomes_eligible_at_utc_midnight_two_dates_later(tmp_path, monkeypatch):
    import pytest

    import engine.pipeline.pair_data as module
    from engine.pipeline.convert_binance_bars import write_record

    for at, expected in ((datetime(2026, 11, 7, 23, 59, 59, tzinfo=UTC), 0),
                         (datetime(2026, 11, 8, tzinfo=UTC), 1)):
        with monkeypatch.context() as patch:
            class FrozenDatetime(datetime):
                @classmethod
                def now(cls, tz=None, _value=at):
                    return _value
            patch.setattr(module, "datetime", FrozenDatetime)
            project = tmp_path / str(expected)
            root = project / "data/history"
            root.mkdir(parents=True)
            patch.setattr(module, "PROJECT_ROOT", project)
            start = datetime(2026, 11, 6, tzinfo=UTC)
            policy = {"symbol": "ETHUSDT", "base_level": "tick", "product": "futures_um", "intervals": ["1s"],
                      "history_mode": "start", "history_start": start.isoformat()}
            write_record(root / "checkpoint.json", {"configuration": module.history_configuration(policy),
                         "start": start.isoformat(), "next_day": start.isoformat(),
                         "available_ms": int(start.timestamp() * 1000), "verification_version": 2})
            settings = PipelineSettings("futures_um", "trades", ("ETHUSDT",), ("1s",),
                                        project / "data/raw", project / "data/catalog", project / "data/state")
            probes, scans = [], []
            def probe(*args, _probes=probes):
                _probes.append(args)
                return True
            async def repair(*args, _scans=scans, **kwargs):
                probe(*args)
                _scans.append(args)
                return {"ready": True, "checked_at": "now", "records_written": 0}
            async def stop(_delay):
                raise asyncio.CancelledError
            patch.setattr(module, "_history_archive_available", probe)
            patch.setattr("engine.pipeline.pair_repair.reconcile_day", repair)
            patch.setattr(module.asyncio, "sleep", stop)
            with pytest.raises(asyncio.CancelledError):
                asyncio.run(module.serve_history(policy, settings, root, rest=object()))
            assert len(probes) == len(scans) == expected


def test_archive_probe_checks_checksum_and_zip_without_download_and_reuses_local(tmp_path, monkeypatch):
    from contextlib import nullcontext
    from urllib.error import HTTPError

    import pytest

    import engine.pipeline.sync_binance_batch as batch
    import engine.pipeline.sync_binance_file as download
    from engine.pipeline.pair_data import _history_archive_available

    day = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    settings = PipelineSettings("futures_um", "trades", ("ETHUSDT",), (), tmp_path / "raw", tmp_path / "catalog", tmp_path / "state")
    calls = []
    monkeypatch.setattr(batch, "local_archive_is_verified", lambda *args: None)
    for failure, expected in (("checksum404", False), ("zip404", False), ("none", True), ("checksum403", None)):
        calls.clear()
        def checksum(url, case=failure, **kwargs):
            calls.append("checksum")
            if case.startswith("checksum"):
                code = 404 if case == "checksum404" else 403
                raise download.SyncError("unavailable") from HTTPError(url, code, "missing", {}, None)
            return "valid", b"valid"
        def head(request, case=failure, **kwargs):
            assert request.get_method() == "HEAD"
            calls.append("zip-head")
            if case == "zip404":
                raise HTTPError(request.full_url, 404, "missing", {}, None)
            return nullcontext()
        monkeypatch.setattr(download, "fetch_checksum", checksum)
        monkeypatch.setattr("urllib.request.urlopen", head)
        if expected is None:
            with pytest.raises(download.SyncError):
                _history_archive_available({"symbol": "ETHUSDT"}, settings, day, day + timedelta(days=1))
        else:
            assert _history_archive_available({"symbol": "ETHUSDT"}, settings, day, day + timedelta(days=1)) is expected
        assert calls == (["checksum"] if failure.startswith("checksum") else ["checksum", "zip-head"])
    calls.clear()
    monkeypatch.setattr(batch, "local_archive_is_verified", lambda *args: tmp_path / "verified.zip")
    assert _history_archive_available({"symbol": "ETHUSDT"}, settings, day, day + timedelta(days=1))
    assert not calls


def test_verified_day_is_never_automatically_rescanned_even_after_cursor_replay(tmp_path, monkeypatch):
    import json

    import pytest

    import engine.pipeline.pair_data as module
    from engine.pipeline.convert_binance_bars import write_record

    root = tmp_path / "data/history"
    root.mkdir(parents=True)
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    settings = PipelineSettings("futures_um", "trades", ("ETHUSDT",), ("1s", "1m"),
                                tmp_path / "data/raw", tmp_path / "data/catalog", tmp_path / "data/state")
    start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    policy = {"symbol": "ETHUSDT", "product": "futures_um", "base_level": "tick", "intervals": ["1s", "1m"],
              "history_mode": "start", "history_start": start.isoformat()}
    initial = {"configuration": module.history_configuration(policy), "start": start.isoformat(),
               "next_day": start.isoformat(), "available_ms": int(start.timestamp() * 1000), "verification_version": 2}
    checkpoint = root / "checkpoint.json"
    write_record(checkpoint, initial)
    checks = []
    async def verified(*args, **kwargs):
        checks.append(args)
        return {"ready": True, "scope": "saved_kline_ohlcv", "checked_at": "now", "records_written": 0}
    async def stop(_delay):
        raise asyncio.CancelledError
    monkeypatch.setattr("engine.pipeline.pair_repair.reconcile_day", verified)
    monkeypatch.setattr(module.asyncio, "sleep", stop)
    def run():
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(module.serve_history(policy, settings, root, rest=object()))
    run()
    audit = next((root / "audits").glob("*.json"))
    evidence = audit.read_bytes()
    assert json.loads(evidence)["verified"] is True
    assert json.loads(evidence)["configuration"] == initial["configuration"]
    assert len(checks) == 1
    assert json.loads(checkpoint.read_text())["next_day"] == (start + timedelta(days=1)).isoformat()
    run()  # Restart while caught up: do not reset to the beginning.
    assert len(checks) == 1 and audit.read_bytes() == evidence
    write_record(checkpoint, initial)  # Replay an older cursor after a crash.
    run()
    assert len(checks) == 1 and audit.read_bytes() == evidence
    assert not list((root / "changes").glob("*.json"))


def test_old_or_other_configuration_audits_cannot_complete_a_date(tmp_path, monkeypatch):
    import json

    import pytest

    import engine.pipeline.pair_data as module
    from engine.pipeline.convert_binance_bars import write_record

    start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    policy = {"symbol": "ETHUSDT", "product": "futures_um", "base_level": "tick", "intervals": ["1s", "1m"],
              "history_mode": "start", "history_start": start.isoformat()}
    digest = module.history_configuration(policy)
    for index, prior in enumerate(({"ready": True, "scope": "hierarchical_ohlcv_and_available_trade_count"},
                                    {"ready": True, "verified": True, "scope": "saved_kline_ohlcv", "configuration": "other", "verification_version": 2},
                                    {"ready": False, "verified": False, "scope": "saved_kline_ohlcv", "configuration": digest, "verification_version": 2})):
        root = tmp_path / str(index) / "data/history"
        root.mkdir(parents=True)
        monkeypatch.setattr(module, "PROJECT_ROOT", root.parent.parent)
        settings = PipelineSettings("futures_um", "trades", ("ETHUSDT",), ("1s",),
                                    root.parent / "raw", root.parent / "catalog", root.parent / "state")
        write_record(root / "checkpoint.json", {"configuration": digest, "start": start.isoformat(),
                     "next_day": start.isoformat(), "available_ms": int(start.timestamp() * 1000), "verification_version": 2})
        audit = root / "audits" / f"{start.date()}.json"
        write_record(audit, prior)
        calls = []
        async def conflict(*args, _calls=calls, **kwargs):
            _calls.append(args)
            return {"ready": False, "scope": "saved_kline_ohlcv", "checked_at": "now", "records_written": 0,
                    "conflicts": [{"reason": "闭合数据冲突"}]}
        async def stop(_delay):
            raise asyncio.CancelledError
        monkeypatch.setattr("engine.pipeline.pair_repair.reconcile_day", conflict)
        monkeypatch.setattr(module.asyncio, "sleep", stop)
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(module.serve_history(policy, settings, root, rest=object()))
        assert len(calls) == 1
        result = json.loads(audit.read_text())
        assert result["verified"] is False and result["configuration"] == digest
