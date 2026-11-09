"""Full pair deletion never crosses product/symbol or follows storage links."""

import fcntl
import os

import pytest

from core.market.instruments import bar_type_for, instrument_id_for
from engine.pipeline.models import PipelineSettings
from engine.pipeline.pair_deletion import delete_pair_data, pair_deletion_plan


@pytest.fixture
def storage(tmp_path):
    settings = PipelineSettings("futures_um", "trades", ("ETHUSDT",), ("1m",),
                                tmp_path / "data/raw", tmp_path / "data/catalog", tmp_path / "data/state")
    return settings, tmp_path


@pytest.mark.parametrize("product,symbol", [("futures_um", "ETHUSDT"), ("options", "ETH-261225-3000-C")])
def test_full_pair_manifest_deletes_all_dates_and_keeps_other_data(storage, product, symbol):
    settings, root = storage
    identifier = instrument_id_for(symbol, product)
    paths = [settings.catalog_dir / "data/trades" / identifier / "old.parquet",
             settings.catalog_dir / "data/bars" / bar_type_for(symbol, "1M", product) / "today.parquet",
             settings.catalog_dir / "features" / bar_type_for(symbol, "1m", product) / "data/custom/BarFeatures/cache.json"]
    if product == "futures_um":
        paths.extend([settings.raw_dir / "futures/um/daily/trades" / symbol / "archive.zip.part",
                      settings.state_dir / "converted/futures/um/daily/trades" / symbol / "archive.zip.json",
                      root / "data/market_data/binance_um_1m/managed" / symbol / "tick/collection.sqlite",
                      root / "data/market_data/binance_um_1m/managed" / symbol / "1m/rollups.sqlite-wal"])
    else:
        paths.append(settings.raw_dir / "options" / symbol / "tick/2026-10-29/source.json")
    keep = [settings.raw_dir / "futures/um/daily/trades/BTCUSDT/keep.zip",
            root / "data/reports/keep.html", settings.catalog_dir / "data/crypto_option/definition.parquet"]
    for path in paths + keep:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"test data")
    plan = pair_deletion_plan(symbol, product, settings, project=root)
    assert plan["file_count"] == len(paths)
    result = delete_pair_data(symbol, product, settings, expected_snapshot=plan["snapshot"], project=root)
    assert result["deleted_files"] == len(paths)
    assert not any(path.exists() for path in paths)
    assert all(path.exists() for path in keep)
    assert pair_deletion_plan(symbol, product, settings, project=root)["file_count"] == 0


def test_changed_manifest_symlink_and_active_writer_are_rejected(storage):
    settings, root = storage
    folder = root / "data/market_data/binance_um_1m/managed/ETHUSDT/tick"
    folder.mkdir(parents=True)
    lock = folder / "writer.lock"
    lock.touch()
    data = folder / "collection.sqlite"
    data.write_bytes(b"before")
    plan = pair_deletion_plan("ETHUSDT", "futures_um", settings, project=root)
    data.write_bytes(b"after")
    with pytest.raises(ValueError, match="数据已变化"):
        delete_pair_data("ETHUSDT", "futures_um", settings, expected_snapshot=plan["snapshot"], project=root)
    plan = pair_deletion_plan("ETHUSDT", "futures_um", settings, project=root)
    with lock.open() as lease:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="采集进程"):
            delete_pair_data("ETHUSDT", "futures_um", settings, expected_snapshot=plan["snapshot"], project=root)
    assert data.exists()
    (folder / "escape").symlink_to(root, target_is_directory=True)
    with pytest.raises(ValueError, match="符号链接"):
        pair_deletion_plan("ETHUSDT", "futures_um", settings, project=root)


def test_empty_pair_and_invalid_symbol(storage):
    settings, root = storage
    plan = pair_deletion_plan("ETHUSDT", "futures_um", settings, project=root)
    assert delete_pair_data("ETHUSDT", "futures_um", settings, expected_snapshot=plan["snapshot"], project=root)["deleted_files"] == 0
    with pytest.raises(ValueError):
        pair_deletion_plan("../BTCUSDT", "futures_um", settings, project=root)


def test_sqlite_read_marks_do_not_invalidate_confirmation_but_data_changes_do(storage):
    settings, root = storage
    folder = root / "data/market_data/binance_um_1m/managed/ETHUSDT/tick"
    folder.mkdir(parents=True)
    for name in ("collection.sqlite", "collection.sqlite-wal", "collection.sqlite-shm"):
        (folder / name).write_bytes(b"data")
    plan = pair_deletion_plan("ETHUSDT", "futures_um", settings, project=root)
    shm = folder / "collection.sqlite-shm"
    os.utime(shm, ns=(shm.stat().st_atime_ns, shm.stat().st_mtime_ns + 1000000000))
    assert pair_deletion_plan("ETHUSDT", "futures_um", settings, project=root)["snapshot"] == plan["snapshot"]
    for name in ("collection.sqlite", "collection.sqlite-wal"):
        path = folder / name
        path.write_bytes(b"changed")
        with pytest.raises(ValueError, match="数据已变化"):
            delete_pair_data("ETHUSDT", "futures_um", settings, expected_snapshot=plan["snapshot"], project=root)
        plan = pair_deletion_plan("ETHUSDT", "futures_um", settings, project=root)
    result = delete_pair_data("ETHUSDT", "futures_um", settings, expected_snapshot=plan["snapshot"], project=root)
    assert result["deleted_files"] == 3


def test_option_underlying_deletion_groups_expiries_and_preserves_other_products(storage, monkeypatch):
    from types import SimpleNamespace

    settings, root = storage
    symbols = ("BTC-261225-85000-C", "BTC-270129-90000-P")
    monkeypatch.setattr("engine.pipeline.catalog.ParquetDataCatalog.instruments", lambda self: [
        SimpleNamespace(id=instrument_id_for(symbol, "options"), info={"binance_options": {"underlying": "BTCUSDT"}})
        for symbol in symbols])
    paths = [settings.raw_dir / "options" / symbols[0] / "source.json",
             settings.catalog_dir / "data/bars" / bar_type_for(symbols[1], "1m", "options") / "bars.parquet"]
    keep = [settings.raw_dir / "options/ETH-261225-3000-C/source.json",
            settings.raw_dir / "futures/um/daily/trades/BTCUSDT/archive.zip",
            settings.catalog_dir / "data/crypto_option" / instrument_id_for(symbols[0], "options") / "definition.parquet"]
    for path in paths + keep:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"data")
    plan = pair_deletion_plan("BTCUSDT", "options", settings, project=root)
    assert plan["file_count"] == 2 and set(plan["contracts"]) == set(symbols)
    paths[1].write_bytes(b"changed")
    with pytest.raises(ValueError, match="数据已变化"):
        delete_pair_data("BTCUSDT", "options", settings, expected_snapshot=plan["snapshot"], project=root)
    plan = pair_deletion_plan("BTCUSDT", "options", settings, project=root)
    lock = root / ".local/run/api/data/options" / f"{symbols[0]}.access.lock"
    lock.parent.mkdir(parents=True)
    with lock.open("a") as lease:
        fcntl.flock(lease, fcntl.LOCK_SH | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="仍有采集"):
            delete_pair_data("BTCUSDT", "options", settings, expected_snapshot=plan["snapshot"], project=root)
    assert all(p.exists() for p in paths)
    result = delete_pair_data("BTCUSDT", "options", settings, expected_snapshot=plan["snapshot"], project=root)
    assert result["deleted_files"] == 2 and all(p.exists() for p in keep)
    assert pair_deletion_plan("BTCUSDT", "options", settings, project=root)["file_count"] == 0


def test_option_underlying_deletion_uses_venue_settlement_metadata(storage, monkeypatch):
    from types import SimpleNamespace

    settings, root = storage
    symbols = {"BTC-261225-85000-C": "BTCUSDT", "BTC-270129-90000-P": "BTCUSDC"}
    monkeypatch.setattr("engine.pipeline.catalog.ParquetDataCatalog.instruments", lambda self: [
        SimpleNamespace(id=instrument_id_for(symbol, "options"), info={"binance_options": {"underlying": pair}})
        for symbol, pair in symbols.items()])
    settings.catalog_dir.mkdir(parents=True)
    for symbol in symbols:
        path = settings.raw_dir / "options" / symbol / "source.json"
        path.parent.mkdir(parents=True)
        path.write_text("evidence")
    for pair in ("BTCUSDT", "BTCUSDC"):
        plan = pair_deletion_plan(pair, "options", settings, project=root)
        assert set(plan["contracts"]) == {symbol for symbol, underlying in symbols.items() if underlying == pair}
