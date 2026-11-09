"""Bounded immutable Parquet research exports from existing tenant Catalog.

A queued snapshot is never presented as ready before the file and metadata
are durably written. No filesystem path or redirect comes from the caller.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json
import os
from pathlib import Path
from threading import BoundedSemaphore, RLock
from uuid import uuid4

from fastapi import HTTPException
import pyarrow as pa
import pyarrow.parquet as pq

from core.market.instruments import INTERVALS
from engine.data_queries import read_lease
from engine.pipeline.config import load_settings
from engine.pipeline.data_management import dataset_inventory, rows
from engine.research_auth import identifier, require_op

_MAX_WINDOW = timedelta(days=31)
_MAX_BYTES = 128 * 1024 * 1024
_MAX_ROWS = 3_000_000
_CHUNK_ROWS = 4096
_COLUMNS = {
    "tick": ("ts_event_ns", "price", "size", "aggressor_side", "trade_id"),
    "bar": ("ts_event_ns", "open", "high", "low", "close", "volume"),
}
_STATUSES = {"pending", "running", "ready", "failed", "interrupted"}


def parse_range(start: str, end: str) -> tuple[datetime, datetime]:
    try:
        a = datetime.fromisoformat(start.replace("Z", "+00:00"))
        b = datetime.fromisoformat(end.replace("Z", "+00:00"))
        if a.tzinfo is None:
            a = a.replace(tzinfo=UTC)
        if b.tzinfo is None:
            b = b.replace(tzinfo=UTC)
        a, b = a.astimezone(UTC), b.astimezone(UTC)
        if not a < b or b - a > _MAX_WINDOW:
            raise ValueError
        return a, b
    except (AttributeError, TypeError, ValueError, OverflowError):
        raise HTTPException(422, "Snapshot interval must be increasing and at most 31 days") from None


def _write_json(file: Path, value: dict) -> None:
    staging = file.with_suffix(".json.tmp")
    with staging.open("x", encoding="utf-8") as handle:
        os.fchmod(handle.fileno(), 0o600)
        json.dump(value, handle, ensure_ascii=False, allow_nan=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(staging, file)


def _row_schema(level: str) -> pa.Schema:
    keys = _COLUMNS["tick" if level == "tick" else "bar"]
    return pa.schema([pa.field(key, pa.string(), nullable=True) for key in keys])


class SnapshotService:
    """Single-Engine bounded queue, not a distributed scheduler."""

    def __init__(self, runtime, root: Path, tenant_id: str):
        self.runtime = runtime
        self.tenant_id = identifier(tenant_id)
        self.root = Path(root) / "research" / "snapshots"
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = RLock()
        self.slots = BoundedSemaphore(2)
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="research-snapshot")
        self._recover()

    def _recover(self):
        for path in self.root.glob("*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if data.get("status") in {"pending", "running"}:
                data["status"] = "interrupted"
                data["error"] = "snapshot_process_restarted"
                _write_json(path, data)

    def close(self):
        self.pool.shutdown(wait=False, cancel_futures=True)

    def _require_volume(self):
        tenant = self.runtime.tenant_row(self.tenant_id)
        if (tenant is None or tenant["status"] != "active"
                or tenant["purpose"] != "development"):
            raise HTTPException(403, "Development tenant not active")
        if not (self.runtime.project / "data" / "catalog").is_dir():
            raise HTTPException(503, "Tenant Catalog volume unavailable")

    def _settings(self):
        # Always use the Engine tenant volume, not an arbitrary pipeline config path.
        settings = load_settings()
        return replace(
            settings,
            catalog_dir=self.runtime.project / "data" / "catalog",
            intervals=tuple(INTERVALS),
        )

    def _discover(self) -> list[dict]:
        self._require_volume()
        try:
            found = dataset_inventory(self._settings(), all_levels=True)
        except (OSError, ValueError) as exc:
            raise HTTPException(503, "Tenant Catalog discovery unavailable") from exc
        datasets = []
        for entry in found:
            if not isinstance(entry, dict):
                continue
            product, symbol, level = (
                entry.get("product"), entry.get("symbol"), entry.get("level"),
            )
            if not all(isinstance(item, str) for item in (product, symbol, level)):
                continue
            try:
                dataset_id = identifier(f"{product}:{symbol}:{level}")
            except ValueError:
                continue
            datasets.append({
                "id": dataset_id, "product": product, "symbol": symbol,
                "level": level, "quality": entry.get("quality", "unverified"),
                "event_ranges_ns": entry.get("event_ranges_ns", []),
            })
        return datasets

    def discover(self, grant: dict) -> list[dict]:
        require_op(grant, "datasets:read")
        return [d for d in self._discover() if d["id"] in grant["datasets"]]

    def _meta(self, snapshot_id: str) -> Path:
        snapshot_id = identifier(snapshot_id)
        if len(snapshot_id) != 32 or any(c not in "0123456789abcdef" for c in snapshot_id):
            raise HTTPException(404, "Research snapshot not found")
        return self.root / f"{snapshot_id}.json"

    def _read(self, snapshot_id: str) -> dict:
        path = self._meta(snapshot_id)
        if path.is_symlink():
            raise HTTPException(404, "Research snapshot not found")
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            raise HTTPException(404, "Research snapshot not found") from None
        except (OSError, ValueError):
            raise HTTPException(503, "Research snapshot journal unavailable") from None
        if (
            data.get("snapshot_id") != snapshot_id
            or data.get("tenant_id") != self.tenant_id
            or data.get("status") not in _STATUSES
        ):
            raise HTTPException(503, "Research snapshot journal corrupted")
        return data

    def get(self, snapshot_id: str, grant: dict) -> dict:
        require_op(grant, "snapshots:read")
        with self.lock:
            data = self._read(snapshot_id)
        if data["task_id"] != grant["task_id"]:
            raise HTTPException(404, "Research snapshot not found")
        require_op(grant, "snapshots:read", dataset=data["dataset_id"])
        return data

    def file(self, snapshot_id: str, grant: dict) -> Path:
        data = self.get(snapshot_id, grant)
        if data["status"] != "ready":
            raise HTTPException(409, "Research snapshot is not ready")
        path = self.root / f"{snapshot_id}.parquet"
        if path.is_symlink() or not path.is_file() or path.stat().st_size != data["size_bytes"]:
            raise HTTPException(503, "Research snapshot bytes unavailable")
        return path

    def create(self, dataset_id: str, start: str, end: str, grant: dict) -> dict:
        require_op(grant, "snapshots:create", dataset=identifier(dataset_id))
        a, b = parse_range(start, end)
        datasets = {item["id"]: item for item in self._discover()}
        if dataset_id not in datasets:
            raise HTTPException(404, "Research dataset not found")
        if not self.slots.acquire(blocking=False):
            raise HTTPException(429, "Research snapshot queue full")
        snapshot_id = uuid4().hex
        dataset = datasets[dataset_id]
        metadata = {
            "snapshot_id": snapshot_id, "tenant_id": self.tenant_id,
            "task_id": grant["task_id"], "dataset_id": dataset_id,
            "format": "parquet", "status": "pending",
            "start": a.isoformat(), "end": b.isoformat(),
            "size_bytes": None, "sha256": None, "row_count": None,
            "quality": dataset["quality"],
            "source_ranges_ns": dataset["event_ranges_ns"],
        }
        try:
            with self.lock:
                _write_json(self._meta(snapshot_id), metadata)
            future = self.pool.submit(self._build, snapshot_id, dataset, a, b)
            future.add_done_callback(lambda _future: self.slots.release())
        except Exception:
            self.slots.release()
            raise
        return metadata

    def _build(self, snapshot_id: str, dataset: dict, start: datetime, end: datetime):
        target = self.root / f"{snapshot_id}.parquet"
        staged = self.root / f"{snapshot_id}.parquet.tmp"
        count = 0
        writer = None
        schema = _row_schema(dataset["level"])
        try:
            with self.lock:
                current = self._read(snapshot_id)
                current["status"] = "running"
                _write_json(self._meta(snapshot_id), current)
            with read_lease(self.runtime.project, dataset["symbol"], dataset["product"]):
                reader = rows(
                    self._settings(), dataset["symbol"], start, end,
                    interval=None if dataset["level"] == "tick" else dataset["level"],
                    market=dataset["product"],
                )
                with closing(reader):
                    buffer = []
                    for row in reader:
                        count += 1
                        if count > _MAX_ROWS:
                            raise ValueError("Research snapshot row quota exceeded")
                        buffer.append({key: str(row[key]) if row.get(key) is not None else None
                                       for key in schema.names})
                        if len(buffer) == _CHUNK_ROWS:
                            if writer is None:
                                writer = pq.ParquetWriter(staged, schema, compression="zstd")
                            writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
                            buffer.clear()
                            if staged.stat().st_size > _MAX_BYTES:
                                raise ValueError("Research snapshot size quota exceeded")
                    if writer is None:
                        writer = pq.ParquetWriter(staged, schema, compression="zstd")
                    if buffer:
                        writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
                    writer.close()
                    writer = None
            size = staged.stat().st_size
            if size > _MAX_BYTES:
                raise ValueError("Research snapshot size quota exceeded")
            checksum = sha256()
            with staged.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    checksum.update(block)
            os.replace(staged, target)
            with self.lock:
                current = self._read(snapshot_id)
                current.update(
                    status="ready", size_bytes=size, sha256=checksum.hexdigest(),
                    row_count=count,
                )
                _write_json(self._meta(snapshot_id), current)
        except Exception:
            with self.lock:
                current = self._read(snapshot_id)
                current.update(status="failed", error="snapshot_generation_failed")
                _write_json(self._meta(snapshot_id), current)
        finally:
            if writer is not None:
                writer.close()
            staged.unlink(missing_ok=True)
