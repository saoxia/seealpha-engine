"""按交易对、周期和 UTC 日期范围批量同步 Binance 日归档。"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from engine.pipeline.config import DEFAULT_CONFIG_PATH, load_settings
from engine.pipeline.models import ArchiveSpec, DataRequest, PipelineConfigError, SyncedArchive
from engine.pipeline.sync_binance_file import (
    SyncError,
    destination_for,
    normalize_source,
    parse_checksum,
    sha256_file,
    sync_file,
)


class BatchSyncError(RuntimeError):
    """表示一批归档中至少有一个同步失败。"""


def parse_utc_datetime(value: str) -> datetime:
    """解析 ISO 日期或时间为 UTC。"""
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def archive_path(
    symbol: str,
    data_type: str,
    day: date,
    interval: str | None = None,
) -> str:
    """生成 USD-M 永续 trades 或 klines 日归档的站内路径。"""
    if data_type == "trades":
        filename = f"{symbol}-trades-{day.isoformat()}.zip"
        return f"data/futures/um/daily/trades/{symbol}/{filename}"
    if data_type == "klines" and interval is not None:
        filename = f"{symbol}-{interval}-{day.isoformat()}.zip"
        return f"data/futures/um/daily/klines/{symbol}/{interval}/{filename}"
    raise PipelineConfigError(f"无法生成归档路径：data_type={data_type!r}, interval={interval!r}")


def plan_daily_archives(request: DataRequest, *, today: date | None = None) -> list[ArchiveSpec]:
    """将左闭右开请求展开为去重后的 Binance trades UTC 日归档。"""
    today = today or datetime.now(UTC).date()
    last_included = (request.end - timedelta(microseconds=1)).date()
    if last_included >= today:
        raise PipelineConfigError(
            "data.binance.vision 只用于已完成 UTC 日；当日缺口应由 Binance REST 补齐",
        )

    specs: list[ArchiveSpec] = []
    day = request.start.date()
    while day <= last_included:
        for symbol in request.symbols:
            specs.append(
                ArchiveSpec(
                    symbol=symbol,
                    data_type="trades",
                    day=day,
                    source_path=archive_path(symbol, "trades", day),
                ),
            )
        day += timedelta(days=1)
    return specs


def local_archive_is_verified(spec: ArchiveSpec, output_dir: Path) -> Path | None:
    """如果本地归档及官方校验均有效，则返回文件路径。"""
    source_url = normalize_source(spec.source_path)
    target = destination_for(source_url, output_dir.resolve())
    checksum_path = Path(f"{target}.CHECKSUM")
    if not target.is_file() or not checksum_path.is_file():
        return None
    try:
        expected = parse_checksum(checksum_path.read_bytes(), target.name)
    except (OSError, SyncError):
        return None
    return target if sha256_file(target) == expected else None


def sync_archives(
    specs: list[ArchiveSpec],
    output_dir: Path,
    *,
    refresh: bool = False,
    timeout: float = 30.0,
    retries: int = 3,
) -> list[SyncedArchive]:
    """可靠同步一批归档；默认离线复用已经通过校验的本地文件。"""
    synced: list[SyncedArchive] = []
    failures: list[str] = []
    for index, spec in enumerate(specs, start=1):
        label = spec.data_type if spec.interval is None else f"{spec.data_type}/{spec.interval}"
        print(f"[{index}/{len(specs)}] {spec.symbol} {label} {spec.day}")
        local = None if refresh else local_archive_is_verified(spec, output_dir)
        if local is not None:
            print(f"本地归档校验通过，跳过网络：{local}")
            synced.append(SyncedArchive(spec=spec, path=local))
            continue
        try:
            path = sync_file(
                spec.source_path,
                output_dir,
                verify_checksum=True,
                force=False,
                timeout=timeout,
                retries=retries,
            )
            synced.append(SyncedArchive(spec=spec, path=path))
        except SyncError as exc:
            failures.append(f"{spec.source_path}: {exc}")

    if failures:
        details = "\n".join(f"- {failure}" for failure in failures)
        raise BatchSyncError(f"{len(failures)} 个归档同步失败：\n{details}")
    return synced


def build_argument_parser() -> argparse.ArgumentParser:
    """创建批量同步命令行参数。"""
    parser = argparse.ArgumentParser(description="批量同步 Binance USD-M 永续日归档")
    parser.add_argument("--start", required=True, type=parse_utc_datetime, help="UTC 开始时间")
    parser.add_argument(
        "--end", required=True, type=parse_utc_datetime, help="UTC 结束时间（不含）"
    )
    parser.add_argument("--symbols", nargs="+", help="交易对；默认读取配置中的全部交易对")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--refresh", action="store_true", help="重新检查远端 CHECKSUM")
    parser.add_argument("--plan-only", action="store_true", help="只打印归档计划，不下载")
    return parser


def main() -> None:
    """运行批量同步命令。"""
    args = build_argument_parser().parse_args()
    try:
        settings = load_settings(args.config)
        request = DataRequest(
            symbols=tuple(args.symbols or settings.symbols),
            start=args.start,
            end=args.end,
            data_type=settings.data_type,
            market=settings.market,
        )
        specs = plan_daily_archives(request)
        if args.plan_only:
            for spec in specs:
                print(spec.source_path)
            return
        sync_archives(specs, settings.raw_dir, refresh=args.refresh)
    except (BatchSyncError, PipelineConfigError) as exc:
        print(f"批量同步失败：{exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
