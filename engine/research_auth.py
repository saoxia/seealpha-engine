"""Engine-only research grant validation. No HTTP endpoint issues grants.

Platform may sign a short-lived grant after user authorization. The signing
key must NEVER be exposed to an Agent, Harness Shell, or general Engine client.
"""
from __future__ import annotations

import base64
import binascii
import hmac
import json
import re
import time

from fastapi import HTTPException

AUDIENCE = "seealpha-engine-research"
RESEARCH_HEADER = "X-SeeAlpha-Research-Contract-Version"
RESEARCH_VERSION = "1"
ENGINE_HEADER = "X-SeeAlpha-Engine-Contract-Version"
ENGINE_VERSION = "2"
_IDENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_OPS = frozenset({
    "datasets:read", "snapshots:create", "snapshots:read",
    "backtests:submit", "backtests:read", "backtests:cancel",
})


def identifier(value: str) -> str:
    if not isinstance(value, str) or not _IDENT.fullmatch(value) or ".." in value:
        raise ValueError("Invalid research identifier")
    return value


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _unb64(data: str) -> bytes:
    return base64.b64decode(data + "=" * (-len(data) % 4), altchars=b"-_", validate=True)


def _validate_claims(claims: object, *, now: int) -> dict:
    if not isinstance(claims, dict) or set(claims) != {
        "version", "audience", "tenant_id", "task_id", "ops",
        "datasets", "strategies", "not_before", "expires_at",
    }:
        raise ValueError("Invalid research grant shape")
    if type(claims["version"]) is not int or claims["version"] != 1:
        raise ValueError("Invalid research grant version")
    if claims["audience"] != AUDIENCE:
        raise ValueError("Invalid research grant audience")
    identifier(claims["tenant_id"])
    identifier(claims["task_id"])
    for key, allowed in (
        ("ops", _OPS), ("datasets", None), ("strategies", None),
    ):
        values = claims[key]
        if (not isinstance(values, list) or len(values) > 100
                or len(set(map(str, values))) != len(values)):
            raise ValueError("Invalid research scope list")
        for item in values:
            if key == "ops":
                if not isinstance(item, str) or item not in allowed:
                    raise ValueError("Invalid research operation")
            else:
                identifier(item)
    left, right = claims["not_before"], claims["expires_at"]
    if (
        type(left) is not int or type(right) is not int
        or not 0 < right - left <= 3600 or left > now or right <= now
    ):
        raise ValueError("Expired or invalid research grant time")
    return claims


def sign_research_grant(key: str, claims: dict, *, now: int | None = None) -> str:
    """Sign an *already authorized* scope. Never callable by untrusted clients."""
    if not isinstance(key, str) or len(key) < 32:
        raise ValueError("Research signing key must be at least 32 characters")
    _validate_claims(claims, now=int(time.time()) if now is None else now)
    payload = _b64(json.dumps(
        claims, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8"))
    signature = _b64(hmac.digest(key.encode(), payload.encode("ascii"), "sha256"))
    return payload + "." + signature


def verify_research_grant(
    key: str, token: str, *, now: int | None = None,
) -> dict:
    try:
        if not isinstance(key, str) or len(key) < 32:
            raise ValueError
        if not isinstance(token, str) or len(token) > 4096:
            raise ValueError
        payload, signature = token.split(".")
        if not payload or not signature:
            raise ValueError
        expected = _b64(hmac.digest(key.encode(), payload.encode("ascii"), "sha256"))
        if not hmac.compare_digest(expected, signature):
            raise ValueError
        claims = json.loads(_unb64(payload))
        return _validate_claims(claims, now=int(time.time()) if now is None else now)
    except (ValueError, TypeError, UnicodeError, KeyError, OverflowError, binascii.Error):
        raise HTTPException(401, "Research authorization rejected") from None


def require_op(grant: dict, operation: str, *, dataset: str | None = None,
               strategy: str | None = None) -> None:
    if (
        operation not in grant["ops"]
        or dataset is not None and dataset not in grant["datasets"]
        or strategy is not None and strategy not in grant["strategies"]
    ):
        raise HTTPException(403, "Research operation outside granted scope")
