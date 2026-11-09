from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from engine.pipeline import sync_binance_batch
from engine.pipeline.config import load_settings
from engine.pipeline.models import (
    ArchiveSpec,
    DataRequest,
    PipelineConfigError,
    bar_type_for,
    instrument_id_for,
    normalize_symbol,
)
from engine.pipeline.sync_binance_batch import archive_path, plan_daily_archives


def test_default_settings_define_three_trade_level_usdt_perpetuals() -> None:
    settings = load_settings()

    assert settings.market == "futures_um"
    assert settings.data_type == "trades"
    assert settings.symbols == ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    assert settings.intervals == ("1s", "1m", "5m", "15m")
    assert settings.raw_dir.is_absolute()
    assert settings.raw_dir.parts[-2:] == ("raw", "binance")
    assert settings.catalog_dir.parts[-2:] == ("data", "catalog")
    assert settings.state_dir.parts[-3:] == ("data", "pipeline_state", "binance")


def test_load_settings_resolves_relative_paths_from_project(tmp_path: Path) -> None:
    config = tmp_path / "sources.toml"
    config.write_text(
        """
[dataset]
market = "futures_um"
data_type = "trades"
symbols = ["eth-usdt"]

[bars]
intervals = ["15m"]

[paths]
raw = "data/test-raw"
catalog = "data/test-catalog"
state = "data/test-state"
""".strip(),
        encoding="utf-8",
    )

    settings = load_settings(config)

    assert settings.symbols == ("ETHUSDT",)
    assert settings.raw_dir.parts[-2:] == ("data", "test-raw")


def test_binance_and_nautilus_names_are_consistent() -> None:
    assert normalize_symbol("ETH-USDT") == "ETHUSDT"
    assert normalize_symbol("ETH/USDC") == "ETHUSDC"
    assert instrument_id_for("ethusdc") == "ETHUSDC-PERP.BINANCE"
    assert instrument_id_for("ethusdt") == "ETHUSDT-PERP.BINANCE"
    assert bar_type_for("ethusdt", "15m") == "ETHUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL"
    assert bar_type_for("ethusdt", "1s") == "ETHUSDT-PERP.BINANCE-1-SECOND-LAST-EXTERNAL"
    assert (
        archive_path("ETHUSDT", "klines", date(2026, 8, 1), "15m")
        == "data/futures/um/daily/klines/ETHUSDT/15m/ETHUSDT-15m-2026-08-01.zip"
    )
    assert (
        archive_path("ETHUSDT", "trades", date(2026, 8, 1))
        == "data/futures/um/daily/trades/ETHUSDT/ETHUSDT-trades-2026-08-01.zip"
    )


def test_daily_plan_expands_symbols_and_left_closed_range() -> None:
    request = DataRequest(
        symbols=("ETH-USDT", "BTC-USDT"),
        start=datetime(2026, 8, 1, tzinfo=UTC),
        end=datetime(2026, 8, 3, tzinfo=UTC),
    )

    specs = plan_daily_archives(request, today=date(2026, 8, 31))

    assert len(specs) == 4
    assert [(spec.symbol, spec.day) for spec in specs] == [
        ("ETHUSDT", date(2026, 8, 1)),
        ("BTCUSDT", date(2026, 8, 1)),
        ("ETHUSDT", date(2026, 8, 2)),
        ("BTCUSDT", date(2026, 8, 2)),
    ]


def test_archive_plan_rejects_current_utc_day() -> None:
    request = DataRequest(
        symbols=("ETHUSDT",),
        start=datetime(2026, 8, 30, tzinfo=UTC),
        end=datetime(2026, 8, 31, 0, 15, tzinfo=UTC),
    )

    with pytest.raises(PipelineConfigError, match="REST"):
        plan_daily_archives(request, today=date(2026, 8, 31))


def test_request_rejects_kline_archive_source() -> None:
    with pytest.raises(PipelineConfigError, match="暂不支持数据类型"):
        DataRequest(
            symbols=("ETHUSDT",),
            data_type="klines",
            start=datetime(2026, 8, 1, 0, 1, tzinfo=UTC),
            end=datetime(2026, 8, 2, tzinfo=UTC),
        )


def test_refresh_rechecks_remote_without_forcing_download(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = ArchiveSpec(
        symbol="ETHUSDT",
        data_type="trades",
        day=date(2026, 8, 1),
        source_path=archive_path("ETHUSDT", "trades", date(2026, 8, 1)),
    )
    target = tmp_path / "archive.zip"
    captured_force: list[bool] = []

    def fake_sync_file(*_args, **kwargs) -> Path:
        captured_force.append(kwargs["force"])
        return target

    monkeypatch.setattr(sync_binance_batch, "sync_file", fake_sync_file)

    synced = sync_binance_batch.sync_archives([spec], tmp_path, refresh=True)

    assert captured_force == [False]
    assert synced[0].path == target
