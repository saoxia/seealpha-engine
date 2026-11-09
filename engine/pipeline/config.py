"""读取和验证数据管道的 TOML 配置。"""

from __future__ import annotations

import tomllib
from pathlib import Path

from core.paths import PROJECT_ROOT
from engine.pipeline.models import (
    SUPPORTED_DATA_TYPES,
    PipelineConfigError,
    PipelineSettings,
    interval_delta,
    normalize_symbol,
)

DEFAULT_CONFIG_PATH = Path(__file__).with_name("binance_sources.toml")


def resolve_project_path(value: object, field: str) -> Path:
    """把配置中的相对路径解析为项目内绝对路径。"""
    if not isinstance(value, str) or not value.strip():
        raise PipelineConfigError(f"paths.{field} 必须是非空字符串")
    path = Path(value)
    return path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()


def string_tuple(value: object, field: str) -> tuple[str, ...]:
    """读取非空字符串数组。"""
    if not isinstance(value, list) or not value:
        raise PipelineConfigError(f"dataset.{field} 必须是非空数组")
    if not all(isinstance(item, str) and item.strip() for item in value):
        raise PipelineConfigError(f"dataset.{field} 只能包含非空字符串")
    return tuple(dict.fromkeys(item.strip() for item in value))


def load_settings(path: Path = DEFAULT_CONFIG_PATH) -> PipelineSettings:
    """加载项目管道设置。"""
    try:
        with path.open("rb") as file:
            content = tomllib.load(file)
    except FileNotFoundError as exc:
        raise PipelineConfigError(f"找不到管道配置：{path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise PipelineConfigError(f"无法解析管道配置 {path}：{exc}") from exc

    dataset = content.get("dataset")
    bars = content.get("bars", {})
    paths = content.get("paths")
    if not isinstance(dataset, dict) or not isinstance(bars, dict) or not isinstance(paths, dict):
        raise PipelineConfigError("配置必须包含 [dataset] 和 [paths]")

    market = dataset.get("market")
    if market != "futures_um":
        raise PipelineConfigError("首版管道仅支持 dataset.market = 'futures_um'")

    data_type = dataset.get("data_type")
    if data_type not in SUPPORTED_DATA_TYPES:
        raise PipelineConfigError(
            f"dataset.data_type 必须是以下值之一：{', '.join(SUPPORTED_DATA_TYPES)}",
        )

    symbols = tuple(
        normalize_symbol(symbol) for symbol in string_tuple(dataset.get("symbols"), "symbols")
    )
    intervals_value = bars.get("intervals", [])
    intervals = string_tuple(intervals_value, "intervals") if intervals_value else ()
    if not intervals:
        raise PipelineConfigError("bars.intervals 至少需要一个由 trades 聚合的 K 线周期")
    for interval in intervals:
        interval_delta(interval)

    return PipelineSettings(
        market=market,
        data_type=data_type,
        symbols=symbols,
        intervals=intervals,
        raw_dir=resolve_project_path(paths.get("raw"), "raw"),
        catalog_dir=resolve_project_path(paths.get("catalog"), "catalog"),
        state_dir=resolve_project_path(paths.get("state"), "state"),
    )
