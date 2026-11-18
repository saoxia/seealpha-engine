"""Opt-in RC9 native research helpers: no live APIs or production mutations."""

from decimal import Decimal
from types import SimpleNamespace

import pytest
from nautilus_trader.model import InstrumentId, OptionGreeks

from engine.strategies.research_native import greeks_exposures, protective_bracket


def test_native_greeks_are_contract_adjusted_and_grouped_per_underlying():
    iid = InstrumentId.from_str("BTC-261225-85000-C.BINANCE")
    observation = OptionGreeks(
        instrument_id=iid, delta=0.5, gamma=0.02, vega=1.5, theta=-0.25,
        ts_event=100_000, ts_init=100_000,
    )
    result = greeks_exposures(
        [(iid, "BTC", Decimal(2), Decimal("0.1"))], [observation],
        now_ns=110_000, max_age_ns=20_000,
    )
    assert result["BTC"]["delta"] == Decimal("0.10")
    assert result["BTC"]["gamma"] == Decimal("0.004")
    with pytest.raises(ValueError, match="Stale"):
        greeks_exposures(
            [(iid, "BTC", Decimal(2), Decimal("0.1"))], [observation],
            now_ns=200_000, max_age_ns=20_000,
        )


def test_bracket_helper_requires_both_native_exits_reduce_only():
    orders = [SimpleNamespace(is_reduce_only=False),
              SimpleNamespace(is_reduce_only=True),
              SimpleNamespace(is_reduce_only=True)]
    calls = []
    fake = SimpleNamespace(order_factory=SimpleNamespace(
        bracket=lambda **kwargs: (calls.append(kwargs), orders)[1],
    ))
    assert protective_bracket(
        fake, instrument_id="ETHUSDT-PERP.BINANCE", order_side="BUY",
        quantity="1", tp_price="2200.00", sl_trigger_price="1800.00",
    ) is orders
    orders[-1].is_reduce_only = False
    with pytest.raises(ValueError, match="reduce-only"):
        protective_bracket(
            fake, instrument_id="ETHUSDT-PERP.BINANCE", order_side="BUY",
            quantity="1", tp_price="2200.00", sl_trigger_price="1800.00",
        )
