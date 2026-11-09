"""调用 Rust 流式转换器并校验其 staging manifest。"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from engine.pipeline.validate_data import DataValidationError

DEFAULT_BINARY = (
    Path(__file__).resolve().parent
    / "rust_converter"
    / "target"
    / "release"
    / "nautilus-data-converter"
)


class RustConversionError(RuntimeError):
    """表示 Rust 转换器启动或输出格式异常。"""


@dataclass(frozen=True)
class RustOutput:
    path: Path
    rows: int
    first_ts: int
    last_ts: int


@dataclass(frozen=True)
class RustBarOutput(RustOutput):
    interval: str
    bar_type: str


@dataclass(frozen=True)
class RustManifest:
    rows: int
    trade: RustOutput
    bars: tuple[RustBarOutput, ...]
    quote: RustOutput | None = None


def find_rust_converter() -> Path | None:
    """返回可执行转换器；设置环境变量为 ``off`` 可强制使用 Python 回退。"""
    configured = os.environ.get("NAUTILUS_RUST_CONVERTER")
    if configured is not None:
        if configured.strip().lower() in {"", "0", "false", "off"}:
            return None
        path = Path(configured).expanduser()
    else:
        path = DEFAULT_BINARY
    return path.resolve() if path.is_file() and os.access(path, os.X_OK) else None


def _integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RustConversionError(f"Rust manifest 的 {name} 无效")
    return value


def _staged_path(value: object, staging_dir: Path, name: str) -> Path:
    if not isinstance(value, str):
        raise RustConversionError(f"Rust manifest 的 {name}.path 无效")
    path = Path(value).resolve()
    try:
        path.relative_to(staging_dir.resolve())
    except ValueError as exc:
        raise RustConversionError(f"Rust manifest 的 {name}.path 越出 staging 目录") from exc
    if not path.is_file():
        raise RustConversionError(f"Rust 转换结果不存在：{path}")
    return path


def _output(value: object, staging_dir: Path, name: str) -> RustOutput:
    if not isinstance(value, dict):
        raise RustConversionError(f"Rust manifest 的 {name} 无效")
    output = RustOutput(
        path=_staged_path(value.get("path"), staging_dir, name),
        rows=_integer(value.get("rows"), f"{name}.rows"),
        first_ts=_integer(value.get("first_ts"), f"{name}.first_ts"),
        last_ts=_integer(value.get("last_ts"), f"{name}.last_ts"),
    )
    if output.rows == 0 or output.first_ts > output.last_ts:
        raise RustConversionError(f"Rust manifest 的 {name} 区间无效")
    return output


def run_rust_trade_converter(
    binary: Path,
    archive: Path,
    staging_dir: Path,
    *,
    instrument_id: str,
    price_precision: int,
    size_precision: int,
    day_start_ns: int,
    day_end_ns: int,
    bar_types: dict[str, str],
    batch_rows: int,
) -> RustManifest:
    """运行 Rust 核心并返回经过边界检查的 manifest。"""
    command = [
        str(binary),
        "--input",
        str(archive),
        "--output-dir",
        str(staging_dir),
        "--instrument-id",
        instrument_id,
        "--price-precision",
        str(price_precision),
        "--size-precision",
        str(size_precision),
        "--day-start-ms",
        str(day_start_ns // 1_000_000),
        "--day-end-ms",
        str(day_end_ns // 1_000_000),
        "--batch-rows",
        str(batch_rows),
    ]
    for interval, bar_type in bar_types.items():
        command.extend(("--bar", f"{interval}={bar_type}"))
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode == 2:
        raise DataValidationError(completed.stderr.strip() or "Rust 数据校验失败")
    if completed.returncode != 0:
        detail = completed.stderr.strip() or f"退出码 {completed.returncode}"
        raise RustConversionError(f"Rust 转换失败：{detail}")
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RustConversionError("Rust 转换器未返回合法 JSON manifest") from exc
    if not isinstance(value, dict):
        raise RustConversionError("Rust manifest 顶层必须是对象")

    trade = _output(value.get("trade"), staging_dir, "trade")
    rows = _integer(value.get("rows"), "rows")
    raw_bars = value.get("bars")
    if rows != trade.rows or not isinstance(raw_bars, list):
        raise RustConversionError("Rust manifest 行数或 bars 无效")
    bars: list[RustBarOutput] = []
    for index, raw_bar in enumerate(raw_bars):
        output = _output(raw_bar, staging_dir, f"bars[{index}]")
        if not isinstance(raw_bar.get("interval"), str) or not isinstance(
            raw_bar.get("bar_type"),
            str,
        ):
            raise RustConversionError(f"Rust manifest 的 bars[{index}] 标识无效")
        bars.append(
            RustBarOutput(
                path=output.path,
                rows=output.rows,
                first_ts=output.first_ts,
                last_ts=output.last_ts,
                interval=raw_bar["interval"],
                bar_type=raw_bar["bar_type"],
            ),
        )
    if {bar.interval: bar.bar_type for bar in bars} != bar_types:
        raise RustConversionError("Rust manifest 的 Bar 集合与请求不一致")
    quote = _output(value["quote"], staging_dir, "quote") if "quote" in value else None
    if quote is not None and (quote.rows != rows or quote.first_ts != trade.first_ts or quote.last_ts != trade.last_ts):
        raise RustConversionError("Quote supplement does not align with native trades")
    return RustManifest(rows=rows, trade=trade, bars=tuple(bars), quote=quote)
