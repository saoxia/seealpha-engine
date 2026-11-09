"""Lightweight shared API/Engine result and error contracts.

These definitions deliberately have no NautilusTrader, connector or Engine
runtime dependencies. They can become a separately versioned contract package
when SeeAlpha Engine moves into its own repository.
"""

from dataclasses import dataclass

ENGINE_CONTRACT_HEADER = "X-SeeAlpha-Engine-Contract-Version"
ENGINE_CONTRACT_VERSION = "2"


class QueryError(RuntimeError):
    """A sanitized connector/Engine query error safe for an API caller."""


class EngineConflict(Exception):
    """A rejected Engine business operation (HTTP 409)."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


@dataclass
class AccountSnapshot:
    balances: dict[str, str]
    info: dict
    positions: list[dict]
    observed_at: int
