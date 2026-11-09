"""为回测、研究和后续实盘预热提供统一的历史数据准备入口。"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from nautilus_trader.adapters.binance import (
    BinanceDataClientConfig,
    BinanceEnvironment,
    BinanceInstrumentProviderConfig,
    BinanceProductType,
    load_binance_instruments,
)

from engine.pipeline.bar_features import prepare_bar_features
from engine.pipeline.catalog import ParquetDataCatalog
from engine.pipeline.config import DEFAULT_CONFIG_PATH, load_settings
from engine.pipeline.convert_binance_bars import ConversionError
from engine.pipeline.convert_binance_trades import (
    convert_trade_archives,
    validate_converted_trade_archives,
)
from engine.pipeline.models import (
    DataRequest,
    PipelineConfigError,
    PipelineSettings,
    PreparedDataset,
    bar_type_for,
    instrument_id_for,
)
from engine.pipeline.sync_binance_batch import (
    BatchSyncError,
    parse_utc_datetime,
    plan_daily_archives,
    sync_archives,
)
from engine.pipeline.validate_data import DataValidationError
from nautilus_support import Instrument


class DataNotReadyError(RuntimeError):
    """表示请求的数据完成处理后仍未达到严格就绪条件。"""


async def load_missing_instruments(symbols: list[str]) -> dict[str, Instrument]:
    """从 Binance 公开接口读取缺失的 USD-M 永续合约定义。"""
    config = BinanceDataClientConfig(
        product_type=BinanceProductType.USD_M,
        environment=BinanceEnvironment.LIVE,
        instrument_provider=BinanceInstrumentProviderConfig(
            load_all=False, load_ids=[instrument_id_for(symbol) for symbol in symbols],
            log_warnings=False,
        ),
    )
    loaded = await load_binance_instruments(config)
    by_id = {str(item.id): item for item in loaded}
    result = {}
    for symbol in symbols:
        instrument = by_id.get(instrument_id_for(symbol))
        if instrument is None:
            raise DataNotReadyError(f"Binance 未返回合约定义：{instrument_id_for(symbol)}")
        result[symbol] = instrument
    return result


def ensure_instruments(
    catalog: ParquetDataCatalog,
    symbols: tuple[str, ...],
) -> dict[str, Instrument]:
    """优先使用 Catalog 中的定义，只为缺失交易对访问 Binance。"""
    expected_ids = {symbol: instrument_id_for(symbol) for symbol in symbols}
    existing = catalog.instruments(instrument_ids=list(expected_ids.values()))
    by_id = {str(instrument.id): instrument for instrument in existing}
    instruments = {
        symbol: by_id[instrument_id]
        for symbol, instrument_id in expected_ids.items()
        if instrument_id in by_id
    }
    missing = [symbol for symbol in symbols if symbol not in instruments]
    if missing:
        downloaded = asyncio.run(load_missing_instruments(missing))
        catalog.write_data(list(downloaded.values()))
        instruments.update(downloaded)
    return instruments


def validate_request_against_settings(
    request: DataRequest,
    settings: PipelineSettings,
) -> None:
    """确保调用方只请求配置明确允许的数据集。"""
    unknown_symbols = sorted(set(request.symbols) - set(settings.symbols))
    if unknown_symbols:
        raise PipelineConfigError(
            f"交易对未在 binance_sources.toml 中声明：{', '.join(unknown_symbols)}",
        )
    requested_intervals = request.bar_intervals or settings.intervals
    unknown_intervals = sorted(set(requested_intervals) - set(settings.intervals))
    if unknown_intervals:
        raise PipelineConfigError(
            f"K 线周期未在 [bars] 中声明：{', '.join(unknown_intervals)}",
        )
    if request.market != settings.market:
        raise PipelineConfigError("请求市场与配置不一致")


def prepare_data(
    request: DataRequest,
    *,
    settings: PipelineSettings | None = None,
    refresh_archives: bool = False,
    force: bool = False,
) -> PreparedDataset:
    """下载、转换并严格验证请求数据，返回轻量 Catalog 描述。"""
    settings = settings or load_settings()
    validate_request_against_settings(request, settings)
    settings.catalog_dir.mkdir(parents=True, exist_ok=True)
    settings.raw_dir.mkdir(parents=True, exist_ok=True)
    settings.state_dir.mkdir(parents=True, exist_ok=True)

    specs = plan_daily_archives(request)
    archives = sync_archives(
        specs,
        settings.raw_dir,
        refresh=refresh_archives,
    )
    catalog = ParquetDataCatalog(str(settings.catalog_dir))
    instruments = ensure_instruments(catalog, request.symbols)
    requested_bar_intervals = request.bar_intervals or settings.intervals
    convert_trade_archives(
        archives,
        instruments,
        catalog,
        raw_dir=settings.raw_dir,
        state_dir=settings.state_dir,
        bar_intervals=requested_bar_intervals,
        force=force,
    )
    failures = validate_converted_trade_archives(
        archives,
        instruments,
        catalog,
        raw_dir=settings.raw_dir,
        state_dir=settings.state_dir,
        bar_intervals=requested_bar_intervals,
    )
    if failures:
        raise DataNotReadyError(f"Trade 归档未完成转换：{', '.join(failures)}")
    prepare_bar_features(
        archives,
        catalog,
        raw_dir=settings.raw_dir,
        state_dir=settings.state_dir,
        bar_intervals=requested_bar_intervals,
        force=force,
    )
    bar_types = tuple(
        bar_type_for(symbol, interval, request.market)
        for symbol in request.symbols
        for interval in requested_bar_intervals
    )

    return PreparedDataset(
        catalog_path=settings.catalog_dir,
        data_type=request.data_type,
        instrument_ids=tuple(
            instrument_id_for(symbol, request.market) for symbol in request.symbols
        ),
        bar_types=bar_types,
        start=request.start,
        end=request.end,
        ready=True,
        feature_bar_types=bar_types,
    )


def build_argument_parser() -> argparse.ArgumentParser:
    """创建统一数据准备命令行参数。"""
    parser = argparse.ArgumentParser(description="准备并验证 Nautilus 历史行情 Catalog")
    parser.add_argument("--start", required=True, type=parse_utc_datetime)
    parser.add_argument("--end", required=True, type=parse_utc_datetime)
    parser.add_argument("--symbols", nargs="+", help="默认使用配置中的全部交易对")
    parser.add_argument(
        "--bar-intervals",
        nargs="+",
        help="本次准备的 K 线周期；默认准备 [bars].intervals 中的全部周期",
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--refresh-archives", action="store_true")
    parser.add_argument("--force", action="store_true", help="重建请求日期的 TradeTick 和指定周期")
    return parser


def main() -> None:
    """运行统一数据准备命令。"""
    args = build_argument_parser().parse_args()
    try:
        settings = load_settings(args.config)
        request = DataRequest(
            symbols=tuple(args.symbols or settings.symbols),
            start=args.start,
            end=args.end,
            data_type=settings.data_type,
            bar_intervals=tuple(args.bar_intervals) if args.bar_intervals else None,
            market=settings.market,
        )
        dataset = prepare_data(
            request,
            settings=settings,
            refresh_archives=args.refresh_archives,
            force=args.force,
        )
        print(f"数据已就绪：{dataset.catalog_path}")
        print(f"覆盖范围：{dataset.start.isoformat()} 至 {dataset.end.isoformat()}")
        print(f"数据类型：{dataset.data_type}")
        if dataset.bar_types:
            print(f"BarType：{', '.join(dataset.bar_types)}")
    except (
        BatchSyncError,
        ConversionError,
        DataNotReadyError,
        DataValidationError,
        PipelineConfigError,
    ) as exc:
        print(f"数据准备失败：{exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
