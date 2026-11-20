"""Durable, research-only persistence for non-executable basket phase journals.

No order API or production entitlement is provided here. Persist a phase's
"begin" checkpoint *before* the caller invokes any external operation.
Only a trusted caller may provide subsequent fill and position evidence.
"""
import fcntl
import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from engine.portfolio.basket_reconciliation import (
    BasketCheckpoint,
    BasketReconciliationError,
    checkpoint_to_record,
    restore_checkpoint,
)
from engine.portfolio.basket_targets import BasketPlan

_BASKET_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
_MAX_RECORD_BYTES = 1024 * 1024
_FILE_FLAGS = os.O_NOFOLLOW | os.O_CLOEXEC


class BasketJournalError(BasketReconciliationError):
    """Journal data or its durable state cannot be trusted."""


class BasketJournalConflict(BasketJournalError):
    """Another writer changed a checkpoint; reload before advancing."""


@dataclass(frozen=True)
class StoredBasketCheckpoint:
    checkpoint: BasketCheckpoint
    revision: str


def _canonical(value: dict) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _basket_id(value: str) -> str:
    if not isinstance(value, str) or _BASKET_ID.fullmatch(value) is None:
        raise BasketJournalError("无效的组合记录标识")
    return value


def _private_regular_file(fd: int) -> None:
    meta = os.fstat(fd)
    if (
        not stat.S_ISREG(meta.st_mode)
        or meta.st_uid != os.getuid()
        or meta.st_nlink != 1
        or meta.st_mode & 0o077
    ):
        raise BasketJournalError("组合记录必须是当前用户独占的普通文件")


class BasketJournalStore:
    """POSIX private-directory, atomic, revision-checked checkpoint store.

    The SHA-256 revision detects inadvertent record corruption and supports
    compare-and-swap, but is NOT an authenticity signature against a user
    already able to modify this directory. This does not replace a trusted
    transactional live-order ledger, worker isolation, or venue reconciliation.
    """

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(mode=0o700, parents=False, exist_ok=True)
        self._require_private_directory()

    def _require_private_directory(self) -> None:
        meta = self.directory.lstat()
        if (
            not stat.S_ISDIR(meta.st_mode)
            or meta.st_uid != os.getuid()
            or meta.st_mode & 0o077
        ):
            raise BasketJournalError("组合记录目录必须为当前用户私有且非符号链接")

    @contextmanager
    def _locked(self, basket_id: str) -> Iterator[Path]:
        self._require_private_directory()
        lock_path = self.directory / f"{basket_id}.lock"
        try:
            fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | _FILE_FLAGS, 0o600)
            try:
                _private_regular_file(fd)
                fcntl.flock(fd, fcntl.LOCK_EX)
                yield self.directory / f"{basket_id}.json"
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
        except OSError as exc:
            raise BasketJournalError("组合记录锁不可用，禁止继续") from exc

    def _read(self, path: Path, plan: BasketPlan) -> StoredBasketCheckpoint | None:
        try:
            fd = os.open(path, os.O_RDONLY | _FILE_FLAGS)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise BasketJournalError("组合记录读取失败，禁止继续") from exc
        try:
            _private_regular_file(fd)
            with os.fdopen(fd, "rb") as stream:
                fd = -1
                raw = stream.read(_MAX_RECORD_BYTES + 1)
        finally:
            if fd >= 0:
                os.close(fd)
        if len(raw) > _MAX_RECORD_BYTES:
            raise BasketJournalError("组合记录大小超出上限")
        try:
            envelope = json.loads(raw)
            if not isinstance(envelope, dict) or set(envelope) != {
                "version", "payload", "revision"
            } or type(envelope["version"]) is not int or envelope["version"] != 1:
                raise BasketJournalError("组合记录封装版本无效")
            payload = envelope["payload"]
            if (
                not isinstance(payload, dict)
                or set(payload) != {"basket_id", "checkpoint"}
                or payload["basket_id"] != path.stem
                or not isinstance(envelope["revision"], str)
            ):
                raise BasketJournalError("组合记录作用域无效")
            revision = hashlib.sha256(_canonical(payload)).hexdigest()
            if revision != envelope["revision"]:
                raise BasketJournalError("组合记录损坏，必须人工核对")
            checkpoint = restore_checkpoint(plan, payload["checkpoint"])
        except (ValueError, TypeError, UnicodeDecodeError, OverflowError) as exc:
            raise BasketJournalError("组合记录无法通过恢复校验") from exc
        return StoredBasketCheckpoint(checkpoint, revision)

    def load(self, basket_id: str, plan: BasketPlan) -> StoredBasketCheckpoint | None:
        """Load and replay; awaiting_confirmation remains pending on restart."""
        basket_id = _basket_id(basket_id)
        with self._locked(basket_id) as path:
            return self._read(path, plan)

    def save(
        self,
        basket_id: str,
        checkpoint: BasketCheckpoint,
        *,
        expected_revision: str | None,
    ) -> StoredBasketCheckpoint:
        """Atomically commit only the expected prior revision.

        None means a fresh record. A matching current record is idempotent;
        all divergent concurrent updates fail closed. No orders are sent.
        """
        basket_id = _basket_id(basket_id)
        if expected_revision is not None and (
            not isinstance(expected_revision, str)
            or re.fullmatch(r"[0-9a-f]{64}", expected_revision) is None
        ):
            raise BasketJournalError("组合记录预期版本无效")
        try:
            record = checkpoint_to_record(checkpoint)
            # Verify event replay before committing even if a caller mutates a
            # dataclass using replace() to fabricate a "completed" status.
            if restore_checkpoint(checkpoint.plan, record) != checkpoint:
                raise BasketJournalError("组合内存状态无法从原始事件完整恢复")
            payload = {"basket_id": basket_id, "checkpoint": record}
            revision = hashlib.sha256(_canonical(payload)).hexdigest()
            serialized = _canonical({
                "version": 1, "payload": payload, "revision": revision,
            }) + b"\n"
        except (ValueError, TypeError, OverflowError) as exc:
            raise BasketJournalError("组合记录序列化或恢复校验失败") from exc
        if len(serialized) > _MAX_RECORD_BYTES:
            raise BasketJournalError("组合记录超过大小上限")
        with self._locked(basket_id) as path:
            existing = self._read(path, checkpoint.plan)
            if existing is not None and existing.revision == revision:
                return existing
            if (existing.revision if existing else None) != expected_revision:
                raise BasketJournalConflict("组合记录已变化，请重新读取并核对")
            temp_name = None
            try:
                fd, temp_name = tempfile.mkstemp(
                    prefix=f".{basket_id}.", suffix=".tmp", dir=self.directory,
                )
                with os.fdopen(fd, "wb") as stream:
                    stream.write(serialized)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temp_name, path)
                temp_name = None
                dir_fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY | _FILE_FLAGS)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError as exc:
                raise BasketJournalError(
                    "组合记录持久化失败；禁止执行后续动作，重新读取并核对"
                ) from exc
            finally:
                if temp_name is not None:
                    os.unlink(temp_name)
            return StoredBasketCheckpoint(checkpoint, revision)
