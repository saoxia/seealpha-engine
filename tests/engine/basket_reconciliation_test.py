"""Fail-closed offline basket phase and restart tests; no exchange credentials."""
from decimal import Decimal

import pytest

from engine.strategies.basket_reconciliation import BasketReconciliation
from engine.strategies.basket_targets import (
    BasketPlanError, LegEvidence, LegId, MemberLegTarget, plan_basket,
)


def _plan():
    perp = LegId("dev", "desk", "futures_um", "ETHUSDT")
    put = LegId("dev", "desk", "options", "ETH-261225-1800-P")
    call = LegId("dev", "desk", "options", "ETH-261225-2200-C")
    legs = (perp, put, call)
    positions = {leg: Decimal(0) for leg in legs}
    evidence = {
        leg: LegEvidence(Decimal(2000) if leg.product == "futures_um" else Decimal(2),
                         Decimal("0.1"), Decimal(1), 100)
        for leg in legs
    }
    targets = [MemberLegTarget("one", leg, Decimal("0.2")) for leg in legs]
    plan = plan_basket(
        targets, positions, evidence, now_ns=100, max_age_ns=10,
        max_gross_notional=Decimal(1000), policy="option_protected",
    )
    return plan, positions, perp, put, call


def _open(tracker, now=101):
    phase = tracker.next_phase
    assert phase is not None
    ids = {change.leg: f"order-{tracker.phase_index}-{i}"
           for i, change in enumerate(phase.changes)}
    tracker.open_phase(ids, observed_positions=dict(tracker.positions), now_ns=now)
    return phase, ids


def test_hedge_first_requires_complete_fills_and_independent_confirmation():
    plan, positions, perp, put, call = _plan()
    tracker = BasketReconciliation(plan, positions, run_id="research-1")
    phase, ids = _open(tracker)
    assert phase.name == "acquire_and_confirm_protection"
    for leg in (put, call):
        tracker.record_fill(ids[leg], Decimal("0.1"), fill_id=leg.symbol, now_ns=102)
    assert not tracker.needs_confirmation
    assert tracker.next_phase is None
    with pytest.raises(BasketPlanError, match="未全部确认"):
        tracker.confirm_phase(dict(tracker.positions))
    assert tracker.frozen_reason is not None


def test_multi_phase_partial_fills_idempotency_and_hedge_before_perpetual():
    plan, positions, perp, put, call = _plan()
    tracker = BasketReconciliation(plan, positions, run_id="research-2")
    phase, ids = _open(tracker)
    assert phase.name == "acquire_and_confirm_protection"
    for leg in (put, call):
        tracker.record_fill(ids[leg], Decimal("0.1"), fill_id=f"{leg.symbol}-1", now_ns=102)
        tracker.record_fill(ids[leg], Decimal("0.1"), fill_id=f"{leg.symbol}-1", now_ns=102)
        tracker.record_fill(ids[leg], Decimal("0.1"), fill_id=f"{leg.symbol}-2", now_ns=103)
    assert tracker.needs_confirmation
    assert tracker.positions[perp] == 0
    tracker.confirm_phase(dict(tracker.positions))
    assert tracker.next_phase.name == "increase_directional_risk_after_protection"
    _, ids = _open(tracker, now=104)
    tracker.record_fill(ids[perp], Decimal("0.2"), fill_id="future-fill", now_ns=105)
    tracker.confirm_phase(dict(tracker.positions))
    assert tracker.complete
    assert tracker.positions == dict(plan.targets)


def test_overfill_modified_duplicate_unknown_fill_and_invalid_phase_freeze():
    plan, positions, _, put, _ = _plan()
    tracker = BasketReconciliation(plan, positions, run_id="research-3")
    _, ids = _open(tracker)
    with pytest.raises(BasketPlanError, match="超过"):
        tracker.record_fill(ids[put], Decimal("0.3"), fill_id="bad", now_ns=102)
    assert tracker.frozen_reason is not None
    with pytest.raises(BasketPlanError, match="冻结"):
        tracker.record_fill(ids[put], Decimal("0.1"), fill_id="later", now_ns=103)

    tracker = BasketReconciliation(plan, positions, run_id="research-4")
    _, ids = _open(tracker)
    tracker.record_fill(ids[put], Decimal("0.1"), fill_id="same", now_ns=102)
    with pytest.raises(BasketPlanError, match="不同内容"):
        tracker.record_fill(ids[put], Decimal("0.2"), fill_id="same", now_ns=103)

    tracker = BasketReconciliation(plan, positions, run_id="research-5")
    with pytest.raises(BasketPlanError, match="不匹配"):
        tracker.open_phase({put: "wrong"}, observed_positions=positions, now_ns=101)
    assert tracker.next_phase is None


def test_mismatched_actual_position_blocks_stage_and_frozen_run():
    plan, positions, _, put, _ = _plan()
    tracker = BasketReconciliation(plan, positions, run_id="research-6")
    _, ids = _open(tracker)
    for leg, order_id in ids.items():
        tracker.record_fill(order_id, Decimal("0.2"), fill_id=leg.symbol, now_ns=102)
    wrong = dict(tracker.positions)
    wrong[put] = Decimal(0)
    with pytest.raises(BasketPlanError, match="不一致"):
        tracker.confirm_phase(wrong)
    assert tracker.frozen_reason
    assert tracker.next_phase is None


def test_timeout_unknown_and_inflight_restart_always_freeze():
    plan, positions, _, _, _ = _plan()
    tracker = BasketReconciliation(
        plan, positions, run_id="research-7", max_phase_age_ns=10,
    )
    _open(tracker)
    tracker.check_timeout(111)
    with pytest.raises(BasketPlanError, match="超时"):
        tracker.check_timeout(112)
    assert tracker.frozen_reason

    tracker = BasketReconciliation(plan, positions, run_id="research-8")
    _, ids = _open(tracker)
    checkpoint = tracker.checkpoint()
    restored = BasketReconciliation.restore(
        plan, positions, run_id="research-8", checkpoint=checkpoint,
        observed_positions=positions,
    )
    assert restored.frozen_reason
    assert restored.next_phase is None
    with pytest.raises(BasketPlanError, match="冻结"):
        restored.open_phase(ids, observed_positions=positions, now_ns=200)

    tracker.reject_or_unknown(next(iter(ids.values())), "venue rejected")
    assert tracker.frozen_reason


def test_clean_checkpoint_resume_requires_matching_plan_and_native_positions():
    plan, initial, _, _, _ = _plan()
    tracker = BasketReconciliation(plan, initial, run_id="research-9")
    _, ids = _open(tracker)
    for leg, order_id in ids.items():
        tracker.record_fill(order_id, Decimal("0.2"), fill_id=leg.symbol, now_ns=102)
    tracker.confirm_phase(dict(tracker.positions))
    checkpoint = tracker.checkpoint()
    restored = BasketReconciliation.restore(
        plan, initial, run_id="research-9", checkpoint=checkpoint,
        observed_positions=dict(tracker.positions),
    )
    assert not restored.frozen_reason
    assert restored.phase_index == 1
    assert restored.next_phase.name == "increase_directional_risk_after_protection"
    inconsistent = dict(tracker.positions)
    inconsistent[next(iter(inconsistent))] = Decimal(100)
    blocked = BasketReconciliation.restore(
        plan, initial, run_id="research-9", checkpoint=checkpoint,
        observed_positions=inconsistent,
    )
    assert blocked.frozen_reason is not None
    with pytest.raises(BasketPlanError, match="身份"):
        BasketReconciliation.restore(
            plan, initial, run_id="different", checkpoint=checkpoint,
            observed_positions=dict(tracker.positions),
        )
