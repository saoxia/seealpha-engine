"""Spot and USD-M share plumbing but not instruments, storage or order semantics."""

from datetime import UTC, date, datetime
from pathlib import Path
from zipfile import ZipFile

import pytest
from pydantic import ValidationError

from core.market.instruments import bar_type_for, instrument_id_for
from core.trading_contracts import SubmitOrderValues
from engine.api.app import SpotCancelValues, SpotCommandBody, SpotSubmitValues, SpotTradingStateBody
from engine.market_data.repair.convert_binance_trades import iter_trade_archive
from engine.market_data.repair.models import DataRequest
from engine.market_data.repair.pair_deletion import pair_deletion_roots
from engine.market_data.repair.sync_binance_batch import archive_path, plan_daily_archives


def test_spot_archive_identity_and_source_are_separate():
    day = date(2026, 8, 1)
    assert archive_path("BTCUSDT", "trades", day, market="spot") == (
        "data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2026-08-01.zip"
    )
    assert archive_path("BTCUSDT", "trades", day) != archive_path(
        "BTCUSDT", "trades", day, market="spot"
    )
    request = DataRequest(
        ("BTCUSDT",), datetime(2026, 8, 1, tzinfo=UTC),
        datetime(2026, 8, 2, tzinfo=UTC), market="spot",
    )
    specs = plan_daily_archives(request, today=date(2026, 11, 19))
    assert len(specs) == 1
    assert specs[0].market == "spot"
    assert "data/spot/" in specs[0].source_path
    assert instrument_id_for("BTCUSDT", "spot") == "BTCUSDT.BINANCE"
    assert instrument_id_for("BTCUSDT") == "BTCUSDT-PERP.BINANCE"
    assert bar_type_for("BTCUSDT", "1m", "spot") != bar_type_for("BTCUSDT", "1m")


def test_spot_trade_zip_reads_seven_columns_and_microsecond_timestamps(tmp_path: Path):
    archive = tmp_path / "spot-trades.zip"
    micro = 1785542400000123
    with ZipFile(archive, "w") as zipped:
        zipped.writestr("BTCUSDT-trades.csv", (
            f"1,100.01,0.5,50.005,{micro},true,true\n"
            f"2,100.02,0.6,60.012,{micro+100},false,true\n"
        ))
    rows = list(iter_trade_archive(archive, market="spot", microseconds=True))
    assert sum(len(frame) for frame in rows) == 2
    assert int(rows[0].iloc[0].timestamp) == micro
    with pytest.raises(ValueError):
        list(iter_trade_archive(archive, market="spot", microseconds=False))


def test_spot_feed_deletion_roots_are_not_perpetual_roots(tmp_path: Path):
    settings = type("Settings", (), {
        "raw_dir": tmp_path / "raw", "state_dir": tmp_path / "state",
        "catalog_dir": tmp_path / "catalog",
    })()
    spot = pair_deletion_roots(settings, tmp_path, "spot")
    futures = pair_deletion_roots(settings, tmp_path, "futures_um")
    assert spot["feed"] == tmp_path / "data/market_data/binance_spot_1m/managed"
    assert futures["feed"] == tmp_path / "data/market_data/binance_um_1m/managed"
    assert spot["feed"] != futures["feed"]


def test_spot_orders_have_separate_fail_closed_contract():
    standard = {
        "account_revision": "a" * 64,
        "request_id": "123e4567-e89b-42d3-a456-426614174000",
        "environment": "sandbox",
        "symbol": "BTCUSDT",
        "side": "buy",
        "order_type": "limit",
        "quantity": "0.1",
        "price": "20000",
    }
    assert SpotSubmitValues.model_validate(standard).symbol == "BTCUSDT"
    with pytest.raises(ValidationError):
        SpotSubmitValues.model_validate({**standard, "reduce_only": True})
    with pytest.raises(ValidationError):
        SpotSubmitValues.model_validate({**standard, "symbol": "BTCUSDT-PERP"})
    with pytest.raises(ValidationError):
        SpotSubmitValues.model_validate({**standard, "order_type": "market", "price": None,
                                         "post_only": True})
    with pytest.raises(ValidationError):
        SubmitOrderValues.model_validate(standard)
    assert SpotCommandBody.model_validate({
        "environment": "sandbox", "api_key": "fake", "api_secret": "fake",
        "action": "submit", "values": standard,
    }).values["symbol"] == "BTCUSDT"
    assert SpotTradingStateBody.model_validate({
        "environment": "sandbox", "api_key": "fake", "api_secret": "fake",
        "symbol": "BTCUSDT",
    }).symbol == "BTCUSDT"
    with pytest.raises(ValidationError):
        SpotCancelValues.model_validate({
            "account_revision": "a" * 64,
            "request_id": "123e4567-e89b-42d3-a456-426614174000",
            "environment": "sandbox", "symbol": "BTCUSDT",
            "client_order_id": "../../etc",
        })
