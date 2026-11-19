"""Filesystem replay and concurrency guards for the research-only basket journal."""
import json
import os
from dataclasses import replace
from decimal import Decimal

import pytest

from engine.strategies.basket_journal_store import (
    BasketJournalConflict,
    BasketJournalError,
    BasketJournalStore,
)
from engine.strategies.basket_reconciliation import (
    begin_phase,
    confirm_phase,
    freeze_checkpoint,
    new_checkpoint,
    record_cumulative_fill,
)
from engine.strategies.basket_targets import (
    LegEvidence,
    LegId,
    MemberLegTarget,
    plan_basket,
)


def _plan():
    spot = LegId("development", "virtual", "spot", "ETHUSDT")
    perp = LegId("development", "virtual", "futures_um", "ETHUSDT")
    positions = {spot: Decimal(0), perp: Decimal(0)}
    now = 1_000_000_000_000
    plan = plan_basket(
        [
            MemberLegTarget("strategy-a", spot, Decimal("0.3")),
            MemberLegTarget("strategy-a", perp, Decimal("-0.3")),
        ],
        positions,
        {
            leg: LegEvidence(Decimal(2000), Decimal("0.1"), Decimal(1), now - 100)
            for leg in positions
        },
        now_ns=now,
        max_age_ns=1_000,
        max_gross_notional=Decimal(4000),
        policy="funding_carry",
    )
    return plan, positions, spot, perp


def _fill(checkpoint, request_id, event_id, leg, cumulative):
    return record_cumulative_fill(
        checkpoint, request_id=request_id, event_id=event_id,
        leg=leg, cumulative=Decimal(cumulative),
    )


def test_fresh_store_replays_pending_and_complete_across_restart(tmp_path):
    plan, positions, spot, perp = _plan()
    directory = tmp_path / "private"
    store = BasketJournalStore(directory)
    assert store.load("carry-A", plan) is None
    fresh = store.save("carry-A", new_checkpoint(plan, positions), expected_revision=None)
    assert fresh.checkpoint.status == "ready"
    assert store.save("carry-A", fresh.checkpoint, expected_revision=None) == fresh

    pending = begin_phase(fresh.checkpoint, "intent-0001")
    saved = store.save("carry-A", pending, expected_revision=fresh.revision)
    reopened = BasketJournalStore(directory).load("carry-A", plan)
    assert reopened == saved
    assert reopened.checkpoint.status == "awaiting_confirmation"
    assert reopened.checkpoint.request_id == "intent-0001"

    partially_filled = _fill(pending, "intent-0001", "fill-a", spot, "0.1")
    partial = store.save(
        "carry-A", partially_filled, expected_revision=reopened.revision,
    )
    assert BasketJournalStore(directory).load("carry-A", plan) == partial

    all_filled = _fill(partial.checkpoint, "intent-0001", "fill-b", spot, "0.3")
    all_filled = _fill(all_filled, "intent-0001", "fill-c", perp, "-0.3")
    saved = store.save("carry-A", all_filled, expected_revision=partial.revision)
    complete = confirm_phase(
        saved.checkpoint,
        request_id="intent-0001",
        terminal_statuses={spot: "FILLED", perp: "FILLED"},
        observed_positions={spot: Decimal("0.3"), perp: Decimal("-0.3")},
    )
    committed = store.save("carry-A", complete, expected_revision=saved.revision)
    assert BasketJournalStore(directory).load("carry-A", plan) == committed
    assert committed.checkpoint.status == "completed"
    assert (directory / "carry-A.json").stat().st_mode & 0o077 == 0


def test_conflict_prevents_lost_updates_and_idempotent_replay(tmp_path):
    plan, positions, _spot, _perp = _plan()
    first_writer = BasketJournalStore(tmp_path / "private")
    second_writer = BasketJournalStore(tmp_path / "private")
    initial = first_writer.save("b1", new_checkpoint(plan, positions), expected_revision=None)
    other_observation = second_writer.load("b1", plan)
    assert other_observation == initial

    ready = first_writer.save(
        "b1", begin_phase(initial.checkpoint, "phase-1"),
        expected_revision=initial.revision,
    )
    assert ready == first_writer.save(
        "b1", ready.checkpoint, expected_revision=initial.revision,
    )
    with pytest.raises(BasketJournalConflict, match="已变化"):
        second_writer.save(
            "b1", freeze_checkpoint(initial.checkpoint, "stale decision"),
            expected_revision=other_observation.revision,
        )
    with pytest.raises(BasketJournalConflict, match="已变化"):
        second_writer.save("b1", initial.checkpoint, expected_revision=None)
    assert first_writer.load("b1", plan) == ready


def test_rejects_corruption_scope_mismatch_and_unsafe_files(tmp_path):
    plan, positions, _spot, _perp = _plan()
    store = BasketJournalStore(tmp_path / "private")
    record = store.save("b2", new_checkpoint(plan, positions), expected_revision=None)
    path = tmp_path / "private" / "b2.json"

    envelope = json.loads(path.read_text())
    envelope["payload"]["basket_id"] = "another"
    path.write_text(json.dumps(envelope))
    with pytest.raises(BasketJournalError):
        store.load("b2", plan)

    path.write_bytes(b"{not-json")
    with pytest.raises(BasketJournalError):
        store.load("b2", plan)

    store.save("b3", new_checkpoint(plan, positions), expected_revision=None)
    other_path = tmp_path / "private" / "b3.json"
    other_path.chmod(0o644)
    with pytest.raises(BasketJournalError, match="独占"):
        store.load("b3", plan)

    other_path.unlink()
    other_path.symlink_to(path)
    with pytest.raises(BasketJournalError):
        store.load("b3", plan)

    with pytest.raises(BasketJournalError, match="标识"):
        store.load("../escape", plan)
    with pytest.raises(BasketJournalError, match="标识"):
        store.save("a/b", record.checkpoint, expected_revision=None)

    public_dir = tmp_path / "not-private"
    public_dir.mkdir(mode=0o755)
    public_dir.chmod(0o755)
    with pytest.raises(BasketJournalError, match="私有"):
        BasketJournalStore(public_dir)


def test_rejects_forged_checkpoint_and_mismatching_plan(tmp_path):
    plan, positions, _spot, _perp = _plan()
    store = BasketJournalStore(tmp_path / "private")
    pending = begin_phase(new_checkpoint(plan, positions), "phase-a")
    with pytest.raises(BasketJournalError, match="恢复"):
        store.save(
            "forged", replace(pending, status="completed"), expected_revision=None,
        )

    initial = store.save("proper", new_checkpoint(plan, positions), expected_revision=None)
    assert initial.revision
    wrong_plan = replace(plan, gross_target_notional=Decimal(100))
    with pytest.raises(BasketJournalError):
        store.load("proper", wrong_plan)
    with pytest.raises(BasketJournalError, match="版本"):
        store.save("proper", pending, expected_revision="wrong")


def test_frozen_state_survives_restart_and_never_reopens(tmp_path):
    plan, positions, _spot, _perp = _plan()
    store = BasketJournalStore(tmp_path / "private")
    initial = store.save("unknown-order", new_checkpoint(plan, positions), expected_revision=None)
    pending = begin_phase(initial.checkpoint, "unknown-0001")
    saved = store.save("unknown-order", pending, expected_revision=initial.revision)
    frozen = freeze_checkpoint(saved.checkpoint, "order outcome unknown")
    store.save("unknown-order", frozen, expected_revision=saved.revision)
    recovered = BasketJournalStore(tmp_path / "private").load("unknown-order", plan)
    assert recovered.checkpoint.status == "frozen"
    with pytest.raises(Exception, match="冻结"):
        begin_phase(recovered.checkpoint, "automatic-retry")


def test_journal_is_private_and_no_external_side_effects(tmp_path, monkeypatch):
    plan, positions, _spot, _perp = _plan()
    store = BasketJournalStore(tmp_path / "private")
    def refuse_external_process(*_args, **_kwargs):
        raise AssertionError("no process should be launched")
    monkeypatch.setattr(os, "system", refuse_external_process)
    saved = store.save("local-only", new_checkpoint(plan, positions), expected_revision=None)
    assert store.load("local-only", plan) == saved
    assert (tmp_path / "private").stat().st_mode & 0o077 == 0
