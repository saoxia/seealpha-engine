"""Research-only multi-product basket planning contracts."""
from decimal import Decimal

import pytest

from engine.strategies.basket_targets import (
    BasketPlanError,
    LegEvidence,
    LegId,
    MemberLegTarget,
    plan_basket,
)


def _leg(product, symbol):
    return LegId("desk", product, symbol)


def _evidence(legs, *, now=1_000_000_000_000):
    return {
        leg: LegEvidence(
            Decimal("2000") if leg.product != "options" else Decimal("2"),
            Decimal("0.1"), Decimal("1"), now - 1_000_000_000,
        )
        for leg in legs
    }


def _plan(targets, positions, *, policy, **kwargs):
    return plan_basket(
        targets, positions, _evidence(positions),
        now_ns=1_000_000_000_000,
        max_age_ns=5_000_000_000,
        max_gross_notional=Decimal("2000"),
        policy=policy,
        **kwargs,
    )


def test_net_only_same_account_product_and_instrument():
    spot = _leg("spot", "ETHUSDT")
    perp = _leg("futures_um", "ETHUSDT")
    targets = [
        MemberLegTarget("one", spot, Decimal("0.2")),
        MemberLegTarget("two", spot, Decimal("0.1")),
        MemberLegTarget("one", perp, Decimal("-0.3")),
    ]
    result = _plan(targets, {spot: Decimal(0), perp: Decimal(0)},
                   policy="funding_carry")
    assert dict(result.targets) == {spot: Decimal("0.3"), perp: Decimal("-0.3")}
    assert len(result.phases) == 1
    assert result.phases[0].name == "coordinated_spot_perpetual_required"
    assert result.executable is False
    assert {row.leg.product for row in result.phases[0].changes} == {"spot", "futures_um"}


def test_option_wings_are_purchased_before_perpetual_open():
    put = _leg("options", "ETH-261225-1800-P")
    call = _leg("options", "ETH-261225-2200-C")
    perp = _leg("futures_um", "ETHUSDT")
    targets = [
        MemberLegTarget("one", put, Decimal("0.2")),
        MemberLegTarget("one", call, Decimal("0.2")),
        MemberLegTarget("one", perp, Decimal("0.2")),
    ]
    positions = {leg: Decimal(0) for leg in (put, call, perp)}
    planned = _plan(targets, positions, policy="option_protected")
    assert [phase.name for phase in planned.phases] == [
        "acquire_and_confirm_protection",
        "increase_directional_risk_after_protection",
    ]
    assert len(planned.phases[0].changes) == 2
    assert planned.phases[1].changes[0].leg == perp

    positions = {put: Decimal("0.2"), call: Decimal("0.2"), perp: Decimal("0.2")}
    flat = [
        MemberLegTarget("one", leg, Decimal(0))
        for leg in (put, call, perp)
    ]
    exit_plan = _plan(flat, positions, policy="option_protected")
    assert [phase.name for phase in exit_plan.phases] == [
        "reduce_directional_risk", "release_protection_after_risk_reduction",
    ]


def test_fail_closed_on_missing_position_budget_precision_and_naked_options():
    spot = _leg("spot", "ETHUSDT")
    perp = _leg("futures_um", "ETHUSDT")
    targets = [MemberLegTarget("one", spot, Decimal("0.2")),
               MemberLegTarget("one", perp, Decimal("-0.2"))]
    positions = {spot: Decimal(0), perp: Decimal(0)}
    with pytest.raises(BasketPlanError, match="实际仓位"):
        _plan(targets, {spot: Decimal(0)}, policy="funding_carry")
    with pytest.raises(BasketPlanError, match="预算"):
        plan_basket(targets, positions, _evidence(positions),
                    now_ns=1_000_000_000_000, max_age_ns=5_000_000_000,
                    max_gross_notional=Decimal("1"), policy="funding_carry")
    with pytest.raises(BasketPlanError, match="最小步长"):
        _plan(targets, {spot: Decimal("0.01"), perp: Decimal(0)}, policy="funding_carry")
    with pytest.raises(BasketPlanError, match="重复"):
        _plan(targets + [targets[0]], positions, policy="funding_carry")
    with pytest.raises(BasketPlanError, match="偏差"):
        _plan(targets, {spot: Decimal("0.2"), perp: Decimal(0)},
              policy="funding_carry", max_leg_drift=Decimal("0.1"))


def test_option_hedge_coverage_and_identity_fail_closed():
    put = _leg("options", "ETH-261225-1800-P")
    call = _leg("options", "ETH-261225-2200-C")
    perp = _leg("futures_um", "ETHUSDT")
    current = {put: Decimal(0), call: Decimal(0), perp: Decimal(0)}
    targets = [MemberLegTarget("x", put, Decimal("0.1")),
               MemberLegTarget("x", call, Decimal("0.1")),
               MemberLegTarget("x", perp, Decimal("0.2"))]
    with pytest.raises(BasketPlanError, match="保护数量不足"):
        _plan(targets, current, policy="option_protected")
    different = _leg("options", "ETH-261226-2200-C")
    with pytest.raises(BasketPlanError, match="相同到期日"):
        _plan([targets[0], MemberLegTarget("x", different, Decimal("0.2")), targets[2]],
              {put: Decimal(0), different: Decimal(0), perp: Decimal(0)},
              policy="option_protected")
    with pytest.raises(BasketPlanError, match="保护数量不足"):
        _plan([MemberLegTarget("x", put, Decimal("0.2")),
               MemberLegTarget("x", call, Decimal("0.2")),
               MemberLegTarget("x", perp, Decimal("0.2"))],
              {put: Decimal(0), call: Decimal(0), perp: Decimal("0.1")},
              policy="option_protected")
