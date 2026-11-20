"""Generic, non-executable multi-symbol portfolio targets use existing core contracts."""
from decimal import Decimal

import pytest

from engine.strategies.basket_targets import (
    BasketPlanError,
    LegEvidence,
    LegId,
    MemberLegTarget,
    plan_basket,
)

NOW = 1_000_000_000_000


def leg(product, symbol, account="account"):
    return LegId("tenant", account, product, symbol)


def plan(rows, positions, cap="5000", evidence=None):
    if evidence is None:
        evidence = {
            key: LegEvidence(Decimal(1000), Decimal("0.1"), Decimal(1), NOW)
            for key in positions
        }
    return plan_basket(
        rows, positions, evidence, now_ns=NOW, max_age_ns=1_000_000_000,
        max_gross_notional=Decimal(cap), policy="generic",
    )


def test_multiple_symbols_and_products_share_one_target_snapshot():
    eth_spot = leg("spot", "ETHUSDT")
    eth_um = leg("futures_um", "ETHUSDT")
    btc_um = leg("futures_um", "BTCUSDT")
    sol_spot = leg("spot", "SOLUSDT")
    current = {key: Decimal(0) for key in (eth_spot, eth_um, btc_um, sol_spot)}
    targets = [
        MemberLegTarget("alpha", eth_spot, Decimal("0.3")),
        MemberLegTarget("alpha", eth_um, Decimal("-0.3")),
        MemberLegTarget("alpha", btc_um, Decimal("0.2")),
        MemberLegTarget("beta", btc_um, Decimal("-0.1")),
        MemberLegTarget("beta", sol_spot, Decimal("0.1")),
    ]
    result = plan(targets, current)
    assert dict(result.targets) == {
        eth_spot: Decimal("0.3"),
        eth_um: Decimal("-0.3"),
        btc_um: Decimal("0.1"),
        sol_spot: Decimal("0.1"),
    }
    assert result.gross_target_notional == Decimal("800.0")
    assert len(result.phases) == 1
    assert result.phases[0].name == "increase_new_exposure"
    assert not result.executable


def test_reversal_closes_first_and_does_not_net_other_instruments():
    eth = leg("futures_um", "ETHUSDT")
    btc = leg("futures_um", "BTCUSDT")
    current = {eth: Decimal("0.3"), btc: Decimal("-0.1")}
    rows = [
        MemberLegTarget("alpha", eth, Decimal("-0.2")),
        MemberLegTarget("alpha", btc, Decimal("0.2")),
    ]
    result = plan(rows, current)
    phases = result.phases
    assert [p.name for p in phases] == [
        "reduce_existing_exposure", "increase_new_exposure",
    ]
    assert {v.leg: v.quantity_delta for v in phases[0].changes} == {
        eth: Decimal("-0.3"), btc: Decimal("0.1"),
    }
    assert {v.leg: v.quantity_delta for v in phases[1].changes} == {
        eth: Decimal("-0.2"), btc: Decimal("0.2"),
    }


def test_missing_leg_cross_account_and_stale_evidence_fail_closed():
    eth = leg("spot", "ETHUSDT")
    btc = leg("futures_um", "BTCUSDT")
    rows = [MemberLegTarget("x", eth, Decimal("0.1")),
            MemberLegTarget("x", btc, Decimal("-0.2"))]
    current = {eth: Decimal(0), btc: Decimal(0)}
    with pytest.raises(BasketPlanError, match="实际仓位"):
        plan(rows, {eth: Decimal(0)})
    foreign = leg("futures_um", "BTCUSDT", account="other")
    with pytest.raises(BasketPlanError, match="同一租户"):
        plan([rows[0], MemberLegTarget("x", foreign, Decimal("-0.2"))],
             {eth: Decimal(0), foreign: Decimal(0)})
    quotes = {
        eth: LegEvidence(Decimal(1000), Decimal("0.1"), Decimal(1), NOW - 5_000_000_000),
        btc: LegEvidence(Decimal(1000), Decimal("0.1"), Decimal(1), NOW),
    }
    with pytest.raises(BasketPlanError, match="过期"):
        plan(rows, current, evidence=quotes)


def test_shared_gross_cap_and_reduce_only_when_over_budget():
    eth = leg("futures_um", "ETHUSDT")
    btc = leg("futures_um", "BTCUSDT")
    current = {eth: Decimal("0.4"), btc: Decimal("0.4")}
    flat = [MemberLegTarget("x", leg_, Decimal(0)) for leg_ in current]
    reduced = plan(flat, current, cap="500")
    assert [phase.name for phase in reduced.phases] == ["reduce_existing_exposure"]
    new = [MemberLegTarget("x", eth, Decimal("0.2")),
           MemberLegTarget("x", btc, Decimal("0.5"))]
    with pytest.raises(BasketPlanError, match="预算"):
        plan(new, current, cap="500")
    with pytest.raises(BasketPlanError, match="预算"):
        plan(new, {eth: Decimal(0), btc: Decimal(0)}, cap="500")


def test_short_spot_naked_option_and_quantization_rejected():
    spot = leg("spot", "ETHUSDT")
    option = leg("options", "ETH-261225-1800-P")
    positions = {spot: Decimal(0), option: Decimal(0)}
    with pytest.raises(BasketPlanError, match="裸期权"):
        plan([MemberLegTarget("x", spot, Decimal(0)),
              MemberLegTarget("x", option, Decimal("-0.1"))], positions)
    with pytest.raises(BasketPlanError, match="裸期权"):
        plan([MemberLegTarget("x", spot, Decimal("-0.1")),
              MemberLegTarget("x", option, Decimal(0))], positions)
    with pytest.raises(BasketPlanError, match="最小步长"):
        plan([MemberLegTarget("x", spot, Decimal("0.05")),
              MemberLegTarget("x", option, Decimal(0))], positions)
