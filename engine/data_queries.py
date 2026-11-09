"""Tenant-scoped, read-only market data APIs backed by the resident Engine volume.

No caller-supplied filesystem paths are accepted. All parquet/journal I/O happens
inside the Engine process under its own tenant-mounted data directory.
"""
from __future__ import annotations

import fcntl
import json
import time
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from nautilus_trader.model import NautilusDataType
from pydantic import BaseModel, Field

from core.market.instruments import INTERVALS, bar_type_for, bar_window, normalize_symbol
from engine.feed.pair_store import journal_chart, journal_inventory, journal_page
from engine.pipeline.config import load_settings
from engine.pipeline.data_management import (
    checked_request,
    coverage,
    dataset_inventory,
    open_catalog,
    preview,
    rows,
)
from engine.pipeline.models import DataRequest
from engine.pipeline.sync_binance_batch import plan_daily_archives

router = APIRouter(prefix="/tenants/{tenant_id}/data", tags=["engine-market-data"])
_LEVELS = ("tick", *INTERVALS)


class Range(BaseModel):
    symbol: str = Field(min_length=1, max_length=30)
    product: Literal["futures_um", "options"] = "futures_um"
    start: datetime
    end: datetime
    intervals: list[str] = Field(default_factory=lambda: ["1m"], max_length=16)


class Page(Range):
    interval: str | None = None
    limit: int = Field(default=100, ge=1, le=500)
    cursor: str | None = Field(default=None, max_length=2000)
    journal: bool = False


class Inventory(BaseModel):
    interval: str | None = None
    all_levels: bool = False
    symbols: list[str] = Field(default_factory=list, max_length=200)


class Chart(BaseModel):
    symbol: str
    interval: Literal["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"] = "1m"
    base_level: str | None = None


def checked_tenant(request, tenant_id):
    runtime = request.app.state.runtime
    row = runtime.tenant_row(tenant_id)
    if row is None or row["status"] != "active":
        raise HTTPException(409, "租户不存在或已归档。")
    if not (runtime.project / "data").is_dir():
        raise HTTPException(503, "租户数据目录不可用。")
    return runtime.project


def journal_paths(project: Path, symbol: str):
    symbol = normalize_symbol(symbol, "futures_um")
    base = project / "data/market_data/binance_um_1m/managed" / symbol
    return [path for path in (
        base / "collection.sqlite",
        *(base / level / "collection.sqlite" for level in _LEVELS),
    ) if path.is_file() and not path.is_symlink()]


@contextmanager
def read_lease(project: Path, symbol: str, product: str):
    symbol = normalize_symbol(symbol, product)
    path = project / "data/.locks" / product / f"{symbol}.access.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            raise HTTPException(409, "该交易对正在删除，请稍后操作。") from None
        yield


def selection(body):
    settings = load_settings()
    if body.start.tzinfo is None or body.end.tzinfo is None:
        raise ValueError("时间必须带有时区。")
    start = body.start.astimezone(UTC)
    end = body.end.astimezone(UTC)
    query = checked_request(body.symbol, start, end, body.intervals, settings, market=body.product)
    if query.market == "futures_um":
        plan_daily_archives(query)
    return query, settings


def safe(func):
    try:
        return func()
    except FileNotFoundError:
        raise HTTPException(404, "Catalog 尚不存在，请先准备数据。") from None
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, str(exc)) from None


@router.post("/datasets")
def datasets(tenant_id: str, body: Inventory, request: Request):
    project = checked_tenant(request, tenant_id)

    def run():
        if body.interval is not None and body.interval not in INTERVALS:
            raise ValueError("K 线周期未配置")
        settings = replace(load_settings(), intervals=tuple(INTERVALS))
        items = dataset_inventory(settings, interval=body.interval, all_levels=body.all_levels)
        for symbol in dict.fromkeys(body.symbols):
            with read_lease(project, symbol, "futures_um"):
                found = [entry for path in journal_paths(project, symbol)
                         for entry in journal_inventory(path, symbol)]
            for item in found:
                if not body.all_levels and item["level"] != (body.interval or "tick"):
                    continue
                existing = next((row for row in items
                                 if row["symbol"] == item["symbol"] and row["level"] == item["level"]
                                 and row.get("product", "futures_um") == "futures_um"), None)
                if existing:
                    existing["event_ranges_ns"] = sorted(
                        [*existing["event_ranges_ns"], *item["event_ranges_ns"]],
                        key=lambda bounds: int(bounds[0]),
                    )
                else:
                    items.append(item)
        return {"items": items}

    return safe(run)


@router.post("/latest")
def latest(tenant_id: str, body: Inventory, request: Request):
    project = checked_tenant(request, tenant_id)

    def run():
        symbols = list(dict.fromkeys(normalize_symbol(symbol, "futures_um") for symbol in body.symbols))
        settings = load_settings()
        if not symbols or not settings.catalog_dir.is_dir():
            return dict.fromkeys(symbols)
        catalog = open_catalog(settings)
        result = {}
        for symbol in symbols:
            with read_lease(project, symbol, "futures_um"):
                stamp = catalog.native.query_last_timestamp(
                    NautilusDataType.Bar, bar_type_for(symbol, "1s"),
                )
            result[symbol] = str(stamp) if stamp is not None else None
        return result

    return safe(run)


@router.post("/coverage")
def data_coverage(tenant_id: str, body: Range, request: Request):
    project = checked_tenant(request, tenant_id)

    def run():
        query, settings = selection(body)
        with read_lease(project, body.symbol, body.product):
            return coverage(query, settings)

    return safe(run)


@router.post("/prepare-plan")
def prepare_plan(tenant_id: str, body: Range, request: Request):
    project = checked_tenant(request, tenant_id)

    def run():
        query, settings = selection(body)
        if query.market == "options":
            from engine.pipeline.options_data import check_download

            check_download(query)
        with read_lease(project, body.symbol, body.product):
            result = coverage(query, settings)
        return {
            **result,
            "archive_days": len(result["days"]),
            "missing_days": sum(not item["catalog_present"] for item in result["days"]),
            "request": body.model_dump(mode="json", exclude={"delete_archives"}),
            "message": (
                "期权 Tick 获取官方近期接口返回的最多 500 笔，实际时间以记录为准；不代表完整历史。"
                if query.market == "options" and not query.bar_intervals else
                "期权历史 K 线直接来自 Binance；不从 K 线反推 Tick，不证明历史逐笔完整。"
                if query.market == "options" else
                "按完整 UTC 日归档处理；只补缺失，不强制重建。下载体积与耗时未知。"
            ),
        }

    return safe(run)


@router.post("/page")
def data_page(tenant_id: str, body: Page, request: Request):
    project = checked_tenant(request, tenant_id)

    def run():
        # Small live previews are not UTC-day archive preparation requests.
        if body.start.tzinfo is None or body.end.tzinfo is None:
            raise ValueError("时间必须带有时区。")
        query = DataRequest(
            (body.symbol,), body.start, body.end,
            bar_intervals=tuple(body.intervals), market=body.product,
        )
        settings = load_settings()
        if body.interval and body.interval not in INTERVALS:
            raise ValueError("K 线周期未配置")
        with read_lease(project, body.symbol, body.product):
            if body.journal and body.product == "futures_um":
                for path in journal_paths(project, query.symbols[0]):
                    try:
                        result = journal_page(
                            path, body.interval or "tick", int(query.start.timestamp() * 1000),
                            int(query.end.timestamp() * 1000), body.limit,
                            body.cursor if body.cursor and body.cursor.startswith("journal.") else None,
                        )
                    except ValueError:
                        if body.cursor and body.cursor.startswith("journal."):
                            continue
                        raise
                    if result["records"] or body.cursor and body.cursor.startswith("journal."):
                        return result
                if body.cursor and body.cursor.startswith("journal."):
                    raise ValueError("游标无效或数据已变化，请重新查询")
            return preview(
                settings, query.symbols[0], query.start, query.end, limit=body.limit,
                cursor=body.cursor, interval=body.interval, market=body.product,
            )

    return safe(run)


def _status(path):
    try:
        if path.stat().st_size > 131072:
            return None
        value = json.loads(path.read_text())
        if (not isinstance(value, dict) or value.get("state") != "READY"
                or type(value.get("valid_until_ms")) is not int
                or int(time.time() * 1000) > value["valid_until_ms"]):
            return None
        return value
    except (OSError, ValueError, TypeError):
        return None


@router.post("/chart")
def chart(tenant_id: str, body: Chart, request: Request):
    project = checked_tenant(request, tenant_id)

    def run():
        symbol = normalize_symbol(body.symbol, "futures_um")
        settings = load_settings()
        now = int(time.time() * 1000)
        left = bar_window(body.interval, now)[0]
        width = int(INTERVALS[body.interval][2].total_seconds() * 1000)
        start = left - 299 * width
        candles = {}
        live = False
        with read_lease(project, symbol, "futures_um"):
            if settings.catalog_dir.is_dir():
                source = rows(
                    settings, symbol, datetime.fromtimestamp(start / 1000, UTC),
                    datetime.fromtimestamp(now / 1000, UTC), interval=body.interval,
                )
                try:
                    for row in source:
                        stamp = int(row["ts_event_ns"]) // 1000000
                        opening, _ = bar_window(body.interval, stamp)
                        candles[opening] = {
                            "time": opening // 1000, "is_closed": True,
                            **{key: row[key] for key in ("open", "high", "low", "close", "volume")},
                        }
                finally:
                    source.close()
            if body.base_level is not None:
                for path in reversed(journal_paths(project, symbol)):
                    for value in journal_chart(path, body.interval, start, now + 1):
                        opening = value["start_ms"]
                        candles[opening] = {
                            "time": opening // 1000,
                            "is_complete": value["is_complete"],
                            "is_closed": value["is_closed"],
                            **{key: value[key] for key in ("open", "high", "low", "close", "volume")},
                        }
                base = project / "data/market_data/binance_um_1m/managed" / symbol
                if body.base_level != "auto":
                    base = base / body.base_level
                states = [_status(base / "status.json"),
                          _status(base / "calculation/status.json")]
                live = all(states)
                latest_updates = {}
                for state in states:
                    if state is None:
                        continue
                    for update in state.get("bar_updates", []):
                        if (update.get("interval") != body.interval
                                or type(update.get("start_ms")) is not int
                                or not start <= update["start_ms"] <= now
                                or type(update.get("updated_ms")) is not int):
                            continue
                        existing = latest_updates.get(update["start_ms"])
                        if existing and (existing["updated_ms"] > update["updated_ms"]
                                         or existing["is_closed"] and not update.get("is_closed")):
                            continue
                        latest_updates[update["start_ms"]] = update
                for opening, value in latest_updates.items():
                    if not all(key in value for key in (
                        "open", "high", "low", "close", "volume", "is_closed", "is_complete"
                    )):
                        continue
                    candles[opening] = {
                        "time": opening // 1000,
                        "is_complete": value["is_complete"],
                        "is_closed": value["is_closed"],
                        **{key: value[key] for key in ("open", "high", "low", "close", "volume")},
                    }
        return {
            "symbol": symbol, "interval": body.interval, "checked_at": now,
            "live": live, "candles": [candles[key] for key in sorted(candles)[-300:]],
            "quality": "unverified",
        }

    return safe(run)
