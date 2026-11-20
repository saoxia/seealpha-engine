"""Read-only legacy CSV source for offline feature generation.

No TradeTick, quote, or Catalog writes occur here. Archive installation
and journal conversion are owned by the Rust native converter.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from pathlib import Path
from zipfile import BadZipFile, ZipFile

import pandas as pd

from engine.market_data.quality.validate_data import (
    TRADE_COLUMNS,
    DataValidationError,
    normalize_trade_frame,
    validate_normalized_trade_frame,
)

CSV_CHUNK_ROWS = 250_000


def iter_trade_archive(path: Path, chunk_rows: int = CSV_CHUNK_ROWS, *, market="futures_um",
                       microseconds=False) -> Iterator[pd.DataFrame]:
    """逐块读取 trades CSV；重复 Trade ID 保留源文件中靠后的记录。"""
    try:
        with ZipFile(path) as archive:
            csv_members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if len(csv_members) != 1:
                raise DataValidationError(
                    f"归档应包含且只包含一个 CSV，实际为 {len(csv_members)} 个",
                )
            with archive.open(csv_members[0]) as file:
                # 先识别表头，避免首块的几十万行因混入表头而退回字符串解析。
                header_reader = csv.reader(line.decode("utf-8-sig") for line in file)
                first_row = next(header_reader, [])
                while not first_row or (len(first_row) == 1 and not first_row[0].strip()):
                    first_row = next(header_reader, None)
                    if first_row is None:
                        break
                has_header = bool(first_row) and first_row[0].strip().lower() in {
                    "id",
                    "trade id",
                    "trade_id",
                }
                skiprows = header_reader.line_num if has_header else 0
                file.seek(0)
                chunks = pd.read_csv(
                    file,
                    header=None,
                    names=(*TRADE_COLUMNS, "is_best_match") if market == "spot" else TRADE_COLUMNS,
                    # 让 CSV 解析器直接生成数值列，避免先创建逐行字符串再解析。
                    # 混合内容的块仍由下方规范化函数严格校验。
                    # pandas 3.1 起各 float_precision 模式共用同一转换器，再传该参数只会告警。
                    low_memory=False,
                    skiprows=skiprows,
                    chunksize=chunk_rows,
                )
                pending = None
                for chunk in chunks:
                    if chunk.empty:
                        continue
                    combined = normalize_trade_frame(chunk, microseconds=microseconds)
                    if pending is not None:
                        combined = pd.concat([pending, combined], ignore_index=True)
                    combined = validate_normalized_trade_frame(combined)
                    ready = combined.iloc[:-1]
                    pending = combined.iloc[-1:].copy()
                    if not ready.empty:
                        yield ready
                if pending is not None:
                    yield pending
    except (BadZipFile, OSError) as exc:
        raise DataValidationError(f"无法读取 Trade ZIP 归档：{path}") from exc


