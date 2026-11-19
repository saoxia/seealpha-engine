"""Validate an AI-written full Python strategy package without importing it.

Platform selects the tenant Docker and the candidate revision. Engine only checks
the fixed development research contract before the existing Backtest Worker runs
the original strategy package using the established NautilusTrader loader.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import io
import re
import stat
import zipfile
from decimal import Decimal, InvalidOperation

from engine.strategies.package import (
    ALLOWED_SUFFIXES,
    MAX_ARCHIVE_BYTES,
    MAX_MEMBERS,
    PackageError,
    parse_metadata,
)

_MAX_ENCODED = 400_000
_DECIMAL = re.compile(r"(?:0|[1-9][0-9]{0,20})(?:\.[0-9]{1,8})?\Z")
_SYMBOL = re.compile(r"[A-Z0-9]{3,20}\Z")
_SECRET_NAMES = frozenset({
    "api_key", "secret", "passphrase", "password", "token",
    "credential", "credentials",
})


def inspect_python_candidate(encoded: str, *, package_hash: str, symbol: str) -> dict:
    """No code execution here; executable Python is loaded only by the Worker."""
    if (not isinstance(encoded, str) or not 1 <= len(encoded) <= _MAX_ENCODED
            or not isinstance(package_hash, str)
            or re.fullmatch(r"[a-f0-9]{64}", package_hash) is None):
        raise ValueError("Invalid Python candidate envelope")
    try:
        data = base64.b64decode(encoded, validate=True)
        if not 1 <= len(data) <= MAX_ARCHIVE_BYTES:
            raise ValueError
        if hashlib.sha256(data).hexdigest() != package_hash:
            raise ValueError
        metadata = parse_metadata(data)
        module, _, _symbol = metadata.entry_point.partition(":")
        entry_file = module.replace(".", "/") + ".py"
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            infos = archive.infolist()
            names = [item.filename for item in infos]
            if (not 1 <= len(names) <= MAX_MEMBERS
                    or len(names) != len(set(names))
                    or "seealpha-package.json" not in names
                    or entry_file not in names):
                raise ValueError
            total = 0
            for item in infos:
                filename = item.filename
                path = filename.split("/")
                if (not filename or filename.startswith("/")
                        or "\\" in filename or ".." in path
                        or any(part.startswith("strategy_python") for part in path)
                        or item.flag_bits & 1
                        or stat.S_IFMT(item.external_attr >> 16) == stat.S_IFLNK):
                    raise ValueError
                if not item.is_dir() and filename.rsplit(".", 1)[-1].lower() not in {
                    ext.lstrip(".") for ext in ALLOWED_SUFFIXES
                }:
                    raise ValueError
                total += item.file_size
                if total > MAX_ARCHIVE_BYTES:
                    raise ValueError
            if any("strategy_python" in archive.read(n).decode("utf-8")
                   for n in names if n.endswith(".py")):
                raise ValueError
        parameters = metadata.parameters
        if (not isinstance(parameters, dict) or not parameters
                or len(parameters) > 24 or set(parameters) & _SECRET_NAMES
                or parameters.get("symbol") != symbol
                or not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None):
            raise ValueError
        for name, value in parameters.items():
            if (not isinstance(name, str) or re.fullmatch(r"[a-z][a-z0-9_]{0,40}", name) is None
                    or type(value) not in (str, int)
                    or (isinstance(value, str) and len(value) > 128)
                    or (type(value) is int and not -1_000_000_000 <= value <= 1_000_000_000)):
                raise ValueError
        for field in ("allocation", "risk_limit"):
            value = parameters.get(field)
            if not isinstance(value, str) or not _DECIMAL.fullmatch(value):
                raise ValueError
            try:
                amount = Decimal(value)
            except (InvalidOperation, ValueError):
                raise ValueError from None
            if not amount.is_finite() or amount <= 0:
                raise ValueError
        return {
            "name": metadata.name, "title": metadata.title,
            "version": metadata.version, "entry_point": metadata.entry_point,
            "parameters": parameters, "package_hash": package_hash,
        }
    except (PackageError, ValueError, TypeError, OSError, OverflowError,
            UnicodeError, binascii.Error, zipfile.BadZipFile, RuntimeError, MemoryError):
        raise ValueError("Invalid AI Python strategy package") from None
