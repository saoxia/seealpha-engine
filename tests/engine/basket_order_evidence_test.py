"""Authenticated-order-evidence contract tests without any exchange traffic."""
from dataclasses import replace
from decimal import Decimal

import pytest

from engine.strategies.basket_order_evidence import (
    AuthoritativeOrderEvidence,
    bind_phase_orders,
    reconcile_order_evidence,
)
from engine.strategies.basket_reconciliation import (
    BasketReconciliationError,
    begin_phase,
    checkpoint_to_record,
    new_checkpoint,
    restore_checkpoint,
)
from engine.strategies.basket_targets import (
    LegEvidence,
    LegId,
    MemberLegTarget,
    plan_basket,
)


NOW = 1_000_000_000_000


def _pending(policy="funding_carry"):
    perp = LegId("dev", "account", "futures_um", "ETHUSDT")
    spot = LegId("dev", "account", "spot", "ETHUSDT")
    put = LegId("dev", "account", "options", "ETH-261225-1800-P")
    call = LegId("dev", "account", "options", "ETH-261225-2200-C")
    targets = (
        {spot: Decimal("0.2"), perp: Decimal("-0.2")}
        if policy == "funding_carry"
        else {put: Decimal("0.2"), call: Decimal("0.2"), perp: Decimal("0.2")}
    )
    initial = {leg: Decimal(0) for leg in targets}
    evidence = {
        leg: LegEvidence(
            Decimal(2) if leg.product == "options" else Decimal(2000),
            Decimal("0.1"), Decimal(1), NOW - 10,
        )
        for leg in initial
    }
    plan = plan_basket(
        [MemberLegTarget("strategy", leg, amount) for leg, amount in targets.items()],
        initial, evidence, now_ns=NOW, max_age_ns=100,
        max_gross_notional=Decimal(5000), policy=policy,
    )
    checkpoint = begin_phase(new_checkpoint(plan, initial), "research-phase-1")
    return checkpoint, initial, targets


def _proofs(manifest, *, status="FILLED", quantity=None, stale=False):
    return tuple(
        AuthoritativeOrderEvidence(
            leg=order.leg,
            client_order_id=order.client_order_id,
            # Order/trade identity collisions *across products* are legal.
            venue_order_id="123456",
            execution_id="987654",
            filled_quantity=order.quantity if quantity is None else quantity,
            status=status,
            as_of_ns=NOW - (5000 if stale else 10),
        ) for order in manifest.orders
    )


def _reconcile(state, manifest, proofs, positions=None, **options):
    return reconcile_order_evidence(
        state, manifest, proofs,
        positions if positions is not None else dict(state.confirmed_positions),
        now_ns=options.pop("now_ns", NOW),
        positions_ts_ns=options.pop("positions_ts_ns", NOW - 10),
        max_age_ns=options.pop("max_age_ns", 1000),
        **options,
    )


def test_funding_phase_ids_reconstruct_after_restart_and_require_complete_fills():
    state, _, goal = _pending()
    manifest = bind_phase_orders(state)
    assert not manifest.executable
    assert bind_phase_orders(
        restore_checkpoint(state.plan, checkpoint_to_record(state))
    ) == manifest
    assert len(manifest.orders) == 2
    assert len({order.client_order_id for order in manifest.orders}) == 2
    assert all(len(order.client_order_id) == 32 for order in manifest.orders)
    done = _reconcile(state, manifest, _proofs(manifest), goal)
    assert done.status == "completed"
    assert dict(done.confirmed_positions) == goal
    assert restore_checkpoint(done.plan, checkpoint_to_record(done)) == done
    with pytest.raises(BasketReconciliationError, match="待核对"):
        bind_phase_orders(done)


def test_missing_or_nonterminal_exchanges_freeze_without_blind_retries():
    state, _, goal = _pending()
    manifest = bind_phase_orders(state)
    unknown = _proofs(manifest, status="UNKNOWN", quantity=Decimal(0))
    unknown = tuple(replace(row, venue_order_id="", execution_id="") for row in unknown)
    frozen = _reconcile(state, manifest, unknown, goal)
    assert frozen.status == "frozen"
    assert "未完成" in frozen.reason or "状态" in frozen.reason
    assert restore_checkpoint(state.plan, checkpoint_to_record(frozen)) == frozen
    pending = _proofs(manifest, status="PARTIALLY_FILLED", quantity=Decimal("0.1"))
    frozen_partial = _reconcile(state, manifest, pending, {
        leg: Decimal("0.1") if leg.product == "spot" else Decimal("-0.1")
        for leg in goal
    })
    assert frozen_partial.status == "frozen"
    assert dict(frozen_partial.fills) == {
        order.leg: Decimal("0.1") if order.side == "BUY" else Decimal("-0.1")
        for order in manifest.orders
    }


def test_order_id_mismatch_and_untrusted_wide_snapshot_do_not_pass():
    state, initial, goal = _pending()
    manifest = bind_phase_orders(state)
    wrong = list(_proofs(manifest))
    wrong[0] = replace(wrong[0], client_order_id="intruder")
    with pytest.raises(BasketReconciliationError, match="不属于"):
        _reconcile(state, manifest, tuple(wrong), goal)
    with pytest.raises(BasketReconciliationError, match="完整"):
        _reconcile(state, manifest, _proofs(manifest), {next(iter(initial)): Decimal(0)})
    with pytest.raises(BasketReconciliationError, match="完整"):
        _reconcile(state, manifest, _proofs(manifest)[:1], goal)
    stale = _reconcile(state, manifest, _proofs(manifest), goal,
                       positions_ts_ns=NOW - 5000)
    assert stale.status == "frozen"


def test_rejected_or_expired_order_and_conflicting_fill_are_frozen():
    state, _, goal = _pending()
    manifest = bind_phase_orders(state)
    rejected = _reconcile(
        state, manifest, _proofs(manifest, status="REJECTED", quantity=Decimal(0)),
    )
    assert rejected.status == "frozen"
    stale_order = _reconcile(state, manifest, _proofs(manifest, stale=True), goal)
    assert stale_order.status == "frozen"
    contradictory = _reconcile(
        state, manifest, _proofs(manifest, status="FILLED", quantity=Decimal("0.1")), goal,
    )
    assert contradictory.status == "frozen"


def test_options_protection_first_phase_reconciles_without_opening_directional_risk():
    state, initial, goal = _pending("option_protected")
    manifest = bind_phase_orders(state)
    assert len(manifest.orders) == 2
    assert {item.leg.product for item in manifest.orders} == {"options"}
    after_wings = {**initial, **{
        item.leg: Decimal("0.2") for item in manifest.orders
    }}
    next_phase = _reconcile(state, manifest, _proofs(manifest), after_wings)
    assert next_phase.status == "ready"
    next_phase = begin_phase(next_phase, "research-phase-2")
    perp_manifest = bind_phase_orders(next_phase)
    assert len(perp_manifest.orders) == 1
    assert perp_manifest.orders[0].leg.product == "futures_um"
    final = _reconcile(next_phase, perp_manifest, _proofs(perp_manifest), goal)
    assert final.status == "completed"
    assert dict(final.confirmed_positions) == goal
