"""数据管道共享的数据模型和 Binance/Nautilus 命名规则。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from core.market.instruments import (
    INTERVALS,
    SUPPORTED_MARKET,
    as_utc,
    bar_type_for,
    instrument_id_for,
    interval_delta,
    normalize_symbol,
)
from core.market.instruments import MarketConfigError as PipelineConfigError

__all__ = [
    "INTERVALS",
    "SUPPORTED_DATA_TYPES",
    "SUPPORTED_MARKET",
    "ArchiveSpec",
    "DataRequest",
    "PipelineConfigError",
    "PipelineSettings",
    "PreparedDataset",
    "SyncedArchive",
    "as_utc",
    "bar_type_for",
    "instrument_id_for",
    "interval_delta",
    "normalize_symbol",
]

SUPPORTED_DATA_TYPES = ("trades",)


@dataclass(frozen=True)
class PipelineSettings:
    """从 TOML 读取的项目级管道设置。"""

    market: str
    data_type: str
    symbols: tuple[str, ...]
    intervals: tuple[str, ...]
    raw_dir: Path
    catalog_dir: Path
    state_dir: Path


@dataclass(frozen=True)
class DataRequest:
    """一次历史数据准备请求，时间范围采用左闭右开语义。"""

    symbols: tuple[str, ...]
    start: datetime
    end: datetime
    data_type: str = "trades"
    bar_intervals: tuple[str, ...] | None = None
    market: str = SUPPORTED_MARKET

    def __post_init__(self) -> None:
        normalized_symbols = tuple(
            dict.fromkeys(normalize_symbol(symbol, self.market) for symbol in self.symbols)
        )
        object.__setattr__(self, "symbols", normalized_symbols)
        object.__setattr__(self, "start", as_utc(self.start))
        object.__setattr__(self, "end", as_utc(self.end))
        if self.bar_intervals is not None:
            normalized_intervals = tuple(dict.fromkeys(self.bar_intervals))
            for interval in normalized_intervals:
                if interval != "1M":
                    interval_delta(interval)
            object.__setattr__(self, "bar_intervals", normalized_intervals)

        if not normalized_symbols:
            raise PipelineConfigError("至少需要一个交易对")
        if self.start >= self.end:
            raise PipelineConfigError("开始时间必须早于结束时间")
        if self.market not in {SUPPORTED_MARKET, "options"}:
            raise PipelineConfigError("数据请求仅支持 futures_um 或 options")
        if self.market == "options":
            if self.data_type not in {"trades", "klines"}:
                raise PipelineConfigError("期权仅支持近期 trades 或交易所 klines")
            object.__setattr__(self, "data_type", "klines" if self.bar_intervals else "trades")
        elif self.data_type not in SUPPORTED_DATA_TYPES:
            raise PipelineConfigError(
                f"暂不支持数据类型 {self.data_type!r}；可用值：{', '.join(SUPPORTED_DATA_TYPES)}",
            )


@dataclass(frozen=True)
class ArchiveSpec:
    """一个 Binance 日归档及其覆盖范围。"""

    symbol: str
    data_type: str
    day: date
    source_path: str
    interval: str | None = None


@dataclass(frozen=True)
class SyncedArchive:
    """已同步到本地的归档。"""

    spec: ArchiveSpec
    path: Path


@dataclass(frozen=True)
class PreparedDataset:
    """已通过准备和校验、可从 Catalog 加载的数据集描述。"""

    catalog_path: Path
    data_type: str
    instrument_ids: tuple[str, ...]
    bar_types: tuple[str, ...]
    start: datetime
    end: datetime
    ready: bool
    feature_bar_types: tuple[str, ...] = ()
