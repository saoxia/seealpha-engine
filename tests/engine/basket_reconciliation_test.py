"""Fail-closed execution journal tests. No external orders or credentials are used."""
import json
from decimal import Decimal

import pytest

from engine.strategies.basket_reconciliation import (
    BasketReconciliationError,
    begin_phase,
    checkpoint_to_record,
    confirm_phase,
    freeze_checkpoint,
    new_checkpoint,
    record_cumulative_fill,
    restore_checkpoint,
)
from engine.strategies.basket_targets import (
    LegEvidence,
    LegId,
    MemberLegTarget,
    plan_basket,
)


def _legs():
    spot = LegId("dev", "virtual", "spot", "ETHUSDT")
    perp = LegId("dev", "virtual", "futures_um", "ETHUSDT")
    put = LegId("dev", "virtual", "options", "ETH-261225-1800-P")
    call = LegId("dev", "virtual", "options", "ETH-261225-2200-C")
    return spot, perp, put, call


def _plan(policy="funding_carry"):
    spot, perp, put, call = _legs()
    legs = (spot, perp) if policy == "funding_carry" else (perp, put, call)
    desired = (
        {spot: Decimal("0.3"), perp: Decimal("-0.3")}
        if policy == "funding_carry"
        else {perp: Decimal("0.2"), put: Decimal("0.2"), call: Decimal("0.2")}
    )
    start = {leg: Decimal(0) for leg in legs}
    now = 1_000_000_000_000
    evidence = {
        leg: LegEvidence(
            Decimal(2) if leg.product == "options" else Decimal(2000),
            Decimal("0.1"), Decimal(1), now - 1_000,
        ) for leg in legs
    }
    plan = plan_basket(
        [MemberLegTarget("a", leg, value) for leg, value in desired.items()],
        start, evidence, now_ns=now, max_age_ns=10_000,
        max_gross_notional=Decimal(5000), policy=policy,
    )
    return plan, start, desired


def _fill(checkpoint, request_id, event_id, leg, qty):
    return record_cumulative_fill(
        checkpoint, request_id=request_id, event_id=event_id,
        leg=leg, cumulative=Decimal(qty),
    )


def test_funding_pair_requires_all_terminal_fills_and_authoritative_positions():
    plan, start, target = _plan()
    spot, perp, _, _ = _legs()
    state = new_checkpoint(plan, start)
    assert state.status == "ready" and not state.plan.executable
    pending = begin_phase(state, "carry-1")
    assert pending == begin_phase(pending, "carry-1")
    partial = _fill(pending, "carry-1", "trade-1", spot, "0.1")
    assert partial == _fill(partial, "carry-1", "trade-1", spot, "0.1")
    with pytest.raises(BasketReconciliationError, match="未完全成交"):
        confirm_phase(
            partial, request_id="carry-1",
            terminal_statuses={spot: "FILLED", perp: "FILLED"},
            observed_positions=start,
        )
    with pytest.raises(BasketReconciliationError, match="最终成交"):
        confirm_phase(
            partial, request_id="carry-1",
            terminal_statuses={spot: "PARTIALLY_FILLED", perp: "FILLED"},
            observed_positions=start,
        )
    partial = _fill(partial, "carry-1", "trade-2", spot, "0.3")
    partial = _fill(partial, "carry-1", "trade-3", perp, "-0.3")
    with pytest.raises(BasketReconciliationError, match="实际仓位"):
        confirm_phase(
            partial, request_id="carry-1",
            terminal_statuses={spot: "FILLED", perp: "FILLED"},
            observed_positions=start,
        )
    done = confirm_phase(
        partial, request_id="carry-1",
        terminal_statuses={spot: "FILLED", perp: "FILLED"},
        observed_positions=target,
    )
    assert done.status == "completed"
    assert dict(done.confirmed_positions) == target
    with pytest.raises(BasketReconciliationError):
        begin_phase(done, "second")
    assert restore_checkpoint(plan, json.loads(json.dumps(checkpoint_to_record(done)))) == done


def test_protection_must_finish_before_directional_expansion():
    plan, start, target = _plan("option_protected")
    _, perp, put, call = _legs()
    state = new_checkpoint(plan, start)
    assert [phase.name for phase in plan.phases] == [
        "acquire_and_confirm_protection", "increase_directional_risk_after_protection",
    ]
    first = begin_phase(state, "protection")
    first = _fill(first, "protection", "put-fill", put, "0.2")
    with pytest.raises(BasketReconciliationError, match="未完全成交"):
        confirm_phase(
            first, request_id="protection",
            terminal_statuses={put: "FILLED", call: "FILLED"},
            observed_positions={**start, put: Decimal("0.2")},
        )
    with pytest.raises(BasketReconciliationError, match="未授权"):
        _fill(first, "protection", "perp-fill", perp, "0.2")
    first = _fill(first, "protection", "call-fill", call, "0.2")
    second = confirm_phase(
        first, request_id="protection",
        terminal_statuses={put: "FILLED", call: "FILLED"},
        observed_positions={**start, put: Decimal("0.2"), call: Decimal("0.2")},
    )
    assert second.status == "ready" and second.phase_index == 1
    with pytest.raises(BasketReconciliationError, match="重复"):
        begin_phase(second, "protection")
    second = begin_phase(second, "perpetual")
    second = _fill(second, "perpetual", "new-perp-fill", perp, "0.2")
    done = confirm_phase(
        second, request_id="perpetual",
        terminal_statuses={perp: "FILLED"},
        observed_positions=target,
    )
    assert done.status == "completed"
    assert restore_checkpoint(plan, checkpoint_to_record(done)) == done


def test_unknown_result_freezes_across_restart_and_requires_explicit_replan():
    plan, start, _ = _plan()
    state = begin_phase(new_checkpoint(plan, start), "ambiguous")
    state = _fill(state, "ambiguous", "part-1", _legs()[0], "0.1")
    state = freeze_checkpoint(state, "order timeout, outcome unknown")
    restored = restore_checkpoint(plan, checkpoint_to_record(state))
    assert restored.status == "frozen"
    with pytest.raises(BasketReconciliationError):
        begin_phase(restored, "resend")
    with pytest.raises(BasketReconciliationError):
        confirm_phase(
            restored, request_id="ambiguous",
            terminal_statuses={_legs()[0]: "FILLED", _legs()[1]: "FILLED"},
            observed_positions=start,
        )
    # Recovery starts with a verified new venue snapshot and a newly validated plan.
    new_state = new_checkpoint(plan, start)
    assert new_state.status == "ready"
    assert state.status == "frozen"


def test_reject_out_of_order_overfill_conflict_and_wrong_scope():
    plan, start, _ = _plan()
    spot, perp, _, _ = _legs()
    state = begin_phase(new_checkpoint(plan, start), "p1")
    state = _fill(state, "p1", "f1", spot, "0.2")
    with pytest.raises(BasketReconciliationError, match="倒退"):
        _fill(state, "p1", "f2", spot, "0.1")
    with pytest.raises(BasketReconciliationError, match="超量"):
        _fill(state, "p1", "f2", spot, "0.4")
    with pytest.raises(BasketReconciliationError, match="身份冲突"):
        _fill(state, "p1", "f1", perp, "-0.3")
    with pytest.raises(BasketReconciliationError, match="不匹配"):
        _fill(state, "wrong", "f2", spot, "0.3")
    with pytest.raises(BasketReconciliationError, match="权威仓位"):
        confirm_phase(
            _fill(state, "p1", "f2", spot, "0.3"), request_id="p1",
            terminal_statuses={spot: "FILLED", perp: "FILLED"},
            observed_positions={spot: Decimal("0.3")},
        )


def test_restore_rejects_tampered_plan_incomplete_journal_and_invalid_records():
    plan, start, _ = _plan()
    state = begin_phase(new_checkpoint(plan, start), "start")
    record = checkpoint_to_record(state)
    restored = restore_checkpoint(plan, json.loads(json.dumps(record)))
    assert restored.status == "awaiting_confirmation"
    assert restored.pending_phase == plan.phases[0]
    record["fingerprint"] = "wrong"
    with pytest.raises(BasketReconciliationError, match="指纹"):
        restore_checkpoint(plan, record)
    record = checkpoint_to_record(state)
    record["events"].append({"kind": "confirm", "request_id": "start", "positions": []})
    with pytest.raises(BasketReconciliationError):
        restore_checkpoint(plan, record)
    with pytest.raises(BasketReconciliationError, match="阶段数量"):
        new_checkpoint(plan, {**start, _legs()[0]: Decimal("0.1")})
