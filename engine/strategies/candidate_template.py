"""Trusted Engine-owned executor for restricted AI research candidate recipes.

The candidate's Python source is NEVER imported or evaluated by the Engine.
Only an allowlisted, validated recipe drives this known-good strategy class.
"""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

from nautilus_trader.indicators import ExponentialMovingAverage, SimpleMovingAverage
from nautilus_trader.trading import Strategy

_ALLOWED = {"ema_cross", "ema_revert", "sma_cross", "sma_revert"}
_FIELDS = {"kind", "symbol", "fast_period", "slow_period", "allocation", "risk_limit"}


def validate_candidate(recipe: object, *, symbol: str | None = None) -> dict:
    if not isinstance(recipe, dict) or set(recipe) != _FIELDS:
        raise ValueError("Candidate recipe has an unsupported shape")
    kind, market = recipe["kind"], recipe["symbol"]
    if kind not in _ALLOWED or not isinstance(market, str) or not (
        3 <= len(market) <= 20 and market.isascii() and market.isalnum()
        and market.upper() == market
    ):
        raise ValueError("Unsupported candidate rule or symbol")
    if symbol is not None and symbol != market:
        raise ValueError("Candidate symbol does not match approved data")
    fast, slow = recipe["fast_period"], recipe["slow_period"]
    if (type(fast) is not int or type(slow) is not int
            or not 2 <= fast < slow <= 500):
        raise ValueError("Invalid candidate indicator periods")
    amounts = {}
    for field in ("allocation", "risk_limit"):
        value = recipe[field]
        if (not isinstance(value, str)
                or not re.fullmatch(r"(?:0|[1-9][0-9]{0,20})(?:\.[0-9]{1,8})?", value)):
            raise ValueError("Invalid candidate position value")
        try:
            quantity = Decimal(value)
        except (InvalidOperation, ValueError):
            raise ValueError("Invalid candidate position value") from None
        digits = quantity.as_tuple()
        if (not quantity.is_finite() or quantity <= 0 or digits.exponent < -8
                or len(digits.digits) > 30):
            raise ValueError("Invalid candidate position value")
        amounts[field] = format(quantity, "f")
    return {
        "kind": kind, "symbol": market, "fast_period": fast,
        "slow_period": slow, **amounts,
    }


class CandidateSignal(Strategy):
    """Engine-owned single-product target publisher, not arbitrary user code."""

    def __init__(self, config):
        super().__init__(config)
        self.fast = None
        self.slow = None
        self.previous = None

    def on_start(self):
        kind = self.parameters["kind"]
        indicator = (
            ExponentialMovingAverage if kind.startswith("ema_")
            else SimpleMovingAverage
        )
        self.fast = indicator(self.parameters["fast_period"])
        self.slow = indicator(self.parameters["slow_period"])
        self.register_indicator_for_bars(self.bar_type, self.fast)
        self.register_indicator_for_bars(self.bar_type, self.slow)
        self.subscribe_bars(self.bar_type)

    def on_bar(self, bar):
        if bar.bar_type != self.bar_type or not (self.fast.initialized and self.slow.initialized):
            return
        current = (float(self.fast.value), float(self.slow.value))
        previous = self.previous
        self.previous = current
        if previous is None:
            return
        was_above = previous[0] > previous[1]
        above = current[0] > current[1]
        if was_above == above:
            return
        long_when_above = self.parameters["kind"].endswith("_cross")
        long_target = above if long_when_above else not above
        quantity = Decimal(self.parameters["allocation"]) if long_target else Decimal(0)
        self.publish(symbol=self.parameters["symbol"], quantity=quantity)


def candidate_load(recipe: dict, *, tenant_id: str, account_id: str,
                   candidate_id: str, package_hash: str) -> dict:
    validated = validate_candidate(recipe)
    return {
        "tenant_id": str(tenant_id),
        "account_id": str(account_id),
        "issuance_id": "candidate_" + candidate_id,
        "code_key": "research_candidate",
        "grant_purpose": "development",
        "parameters": validated,
        "package_hash": package_hash,
        "_trusted_candidate": True,
    }
