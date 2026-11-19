"""Pure risk monitoring scenarios, no real trading or invented fills."""
from decimal import Decimal

import pytest

from engine.strategies.basket_reconciliation import (
    BasketReconciliationError,
    begin_phase,
    checkpoint_to_record,
    new_checkpoint,
    record_cumulative_fill,
    restore_checkpoint,
)
from engine.strategies.basket_risk_monitor import observe_basket_phase
from engine.strategies.basket_targets import (
    LegEvidence,
    LegId,
    MemberLegTarget,
    plan_basket,
)


NOW = 1_000_000_000_000


def _state():
    spot = LegId("dev", "virtual", "spot", "ETHUSDT")
    perp = LegId("dev", "virtual", "futures_um", "ETHUSDT")
    positions = {spot: Decimal(0), perp: Decimal(0)}
    evidence = {
        leg: LegEvidence(Decimal(2000), Decimal("0.1"), Decimal(1), NOW - 1000)
        for leg in positions
    }
    targets = [
        MemberLegTarget("a", spot, Decimal("0.2")),
        MemberLegTarget("a", perp, Decimal("-0.2")),
    ]
    plan = plan_basket(
        targets, positions, evidence, now_ns=NOW,
        max_age_ns=100_000, max_gross_notional=Decimal(1000),
        max_leg_drift=Decimal("0.1"), policy="funding_carry",
    )
    return new_checkpoint(plan, positions), positions, evidence, spot, perp


def _observe(state, positions, evidence, **kwargs):
    return observe_basket_phase(
        state, positions, evidence,
        now_ns=kwargs.pop("now_ns", NOW),
        max_age_ns=kwargs.pop("max_age_ns", 10_000),
        max_gross_notional=kwargs.pop("max_gross_notional", Decimal(1000)),
        max_leg_drift=kwargs.pop("max_leg_drift", Decimal("0.1")),
        max_pending_ns=kwargs.pop("max_pending_ns", 100_000),
        **kwargs,
    )


def test_ready_stale_free_snapshot_returns_non_executable_safe_observation():
    checkpoint, positions, evidence, _, _ = _state()
    verdict = _observe(checkpoint, positions, evidence)
    assert verdict.state == "safe" and not verdict.executable
    assert verdict.gross_notional == Decimal(0)
    assert verdict.leg_drift == Decimal(0)
    assert verdict.checkpoint == checkpoint


def test_partial_one_leg_fill_freezes_and_replays_fail_closed():
    checkpoint, positions, evidence, spot, _ = _state()
    checkpoint = begin_phase(checkpoint, "phase-1")
    checkpoint = record_cumulative_fill(
        checkpoint, request_id="phase-1", event_id="fill-spot",
        leg=spot, cumulative=Decimal("0.2"),
    )
    current = {**positions, spot: Decimal("0.2")}
    verdict = _observe(checkpoint, current, evidence, pending_since_ns=NOW - 1000)
    assert verdict.state == "frozen"
    assert verdict.leg_drift == Decimal("0.2")
    assert verdict.checkpoint.status == "frozen"
    restored = restore_checkpoint(checkpoint.plan, checkpoint_to_record(verdict.checkpoint))
    assert restored == verdict.checkpoint
    with pytest.raises(BasketReconciliationError, match="冻结"):
        begin_phase(restored, "no-blind-resend")


def test_unsupported_missing_leg_and_forged_snapshot_are_rejected():
    checkpoint, positions, evidence, spot, perp = _state()
    pending = begin_phase(checkpoint, "phase-1")
    with pytest.raises(BasketReconciliationError, match="完整"):
        _observe(pending, {spot: Decimal(0)}, evidence, pending_since_ns=NOW)
    mismatch = {spot: Decimal("0.1"), perp: Decimal(0)}
    verdict = _observe(pending, mismatch, evidence, pending_since_ns=NOW)
    assert verdict.state == "frozen"
    assert "权威仓位" in verdict.reason


def test_pending_timeout_and_missing_timing_evidence_freeze():
    checkpoint, positions, evidence, _, _ = _state()
    pending = begin_phase(checkpoint, "phase-1")
    assert _observe(pending, positions, evidence).state == "frozen"
    assert _observe(
        pending, positions, evidence, pending_since_ns=NOW - 100,
        max_pending_ns=10,
    ).state == "frozen"


def test_quote_expiry_and_runtime_gross_exposure_violation_freeze():
    checkpoint, positions, evidence, spot, perp = _state()
    stale = {
        **evidence,
        spot: LegEvidence(Decimal(2000), Decimal("0.1"), Decimal(1), NOW - 20000),
    }
    assert _observe(checkpoint, positions, stale).state == "frozen"
    pending = begin_phase(checkpoint, "phase-2")
    pending = record_cumulative_fill(
        pending, request_id="phase-2", event_id="fill-s",
        leg=spot, cumulative=Decimal("0.2"),
    )
    pending = record_cumulative_fill(
        pending, request_id="phase-2", event_id="fill-p",
        leg=perp, cumulative=Decimal("-0.2"),
    )
    current = {spot: Decimal("0.2"), perp: Decimal("-0.2")}
    verdict = _observe(
        pending, current, evidence, pending_since_ns=NOW,
        max_gross_notional=Decimal(700),
    )
    assert verdict.state == "frozen"
    assert verdict.gross_notional == Decimal(800)


def test_options_incomplete_protection_freezes_after_verified_snapshot():
    perp = LegId("dev", "virtual", "futures_um", "ETHUSDT")
    put = LegId("dev", "virtual", "options", "ETH-261225-1800-P")
    call = LegId("dev", "virtual", "options", "ETH-261225-2200-C")
    positions = {put: Decimal(0), call: Decimal(0), perp: Decimal(0)}
    evidence = {
        leg: LegEvidence(
            Decimal(2) if leg.product == "options" else Decimal(2000),
            Decimal("0.1"), Decimal(1), NOW - 1000,
        ) for leg in positions
    }
    targets = [
        MemberLegTarget("a", put, Decimal("0.2")),
        MemberLegTarget("a", call, Decimal("0.2")),
        MemberLegTarget("a", perp, Decimal("0.2")),
    ]
    plan = plan_basket(
        targets, positions, evidence, now_ns=NOW, max_age_ns=10000,
        max_gross_notional=Decimal(1000), policy="option_protected",
    )
    checkpoint = begin_phase(new_checkpoint(plan, positions), "wing-phase")
    assert _observe(
        checkpoint, positions, evidence, pending_since_ns=NOW,
    ).state == "safe"
    # External account snapshot differs from fully replayed cumulative fills:
    # the pending leg did not produce a trusted fill, so it cannot be assumed safe.
    suspicious = {**positions, perp: Decimal("0.1")}
    verdict = _observe(
        checkpoint, suspicious, evidence, pending_since_ns=NOW,
    )
    assert verdict.state == "frozen"
    assert "保护" in verdict.reason or "权威" in verdict.reason
