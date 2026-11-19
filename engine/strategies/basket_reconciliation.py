"""Research-only replayable guard for multi-product basket phase confirmations.

This module never submits an order or infers that an accepted order has filled.
The caller must durably write its journal before any external side effect and
obtain authoritative terminal order/position evidence before advancing phases.
"""
import hashlib
import json
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Literal

from engine.strategies.basket_targets import BasketPlan, BasketPlanError, LegId


class BasketReconciliationError(BasketPlanError):
    """A basket phase cannot safely advance on the supplied evidence."""


Status = Literal["ready", "awaiting_confirmation", "frozen", "completed"]
_VALID_ID = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:")
_MAX_EVENTS = 4096


@dataclass(frozen=True)
class JournalEvent:
    kind: Literal["begin", "fill", "confirm", "freeze"]
    request_id: str = ""
    event_id: str = ""
    leg: LegId | None = None
    cumulative: Decimal | None = None
    positions: tuple[tuple[LegId, Decimal], ...] = ()
    reason: str = ""


@dataclass(frozen=True)
class BasketCheckpoint:
    """Immutable journal state. It grants no trading or production authority."""

    plan: BasketPlan
    fingerprint: str
    initial_positions: tuple[tuple[LegId, Decimal], ...]
    confirmed_positions: tuple[tuple[LegId, Decimal], ...]
    phase_index: int
    status: Status
    request_id: str = ""
    fills: tuple[tuple[LegId, Decimal], ...] = ()
    used_request_ids: tuple[str, ...] = ()
    seen_events: tuple[tuple[str, str, LegId, Decimal], ...] = ()
    events: tuple[JournalEvent, ...] = ()
    reason: str = ""

    @property
    def pending_phase(self):
        if self.phase_index >= len(self.plan.phases):
            return None
        return self.plan.phases[self.phase_index]


def _quantity(value: Decimal) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise BasketReconciliationError("仓位和成交累计值必须为有限 Decimal")
    return value


def _id(value: str, label: str) -> str:
    if (not isinstance(value, str) or not 1 <= len(value) <= 128
            or not all(char in _VALID_ID for char in value)):
        raise BasketReconciliationError(f"{label}无效")
    return value


def _pairs(value: dict[LegId, Decimal], expected: set[LegId]) -> tuple:
    if not isinstance(value, dict) or set(value) != expected:
        raise BasketReconciliationError("必须提交全部组合腿的权威仓位快照")
    return tuple(sorted((leg, _quantity(quantity)) for leg, quantity in value.items()))


def _leg_key(leg: LegId) -> list[str]:
    return [leg.tenant_id, leg.account_id, leg.product, leg.symbol]


def _pairs_wire(pairs) -> list[dict]:
    return [{"leg": _leg_key(leg), "quantity": str(quantity)} for leg, quantity in pairs]


def _unwire_pairs(rows: list) -> dict[LegId, Decimal]:
    if not isinstance(rows, list) or len(rows) > 64:
        raise BasketReconciliationError("恢复仓位记录无效")
    result = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"leg", "quantity"}:
            raise BasketReconciliationError("恢复仓位条目无效")
        raw_leg = row["leg"]
        if not isinstance(raw_leg, list) or len(raw_leg) != 4:
            raise BasketReconciliationError("恢复组合腿无效")
        leg = LegId(*raw_leg)
        if leg in result or not isinstance(row["quantity"], str):
            raise BasketReconciliationError("恢复仓位重复或数量无效")
        try:
            result[leg] = _quantity(Decimal(row["quantity"]))
        except (ValueError, ArithmeticError) as exc:
            raise BasketReconciliationError("恢复仓位数量格式无效") from exc
    return result


def _fingerprint(plan: BasketPlan) -> str:
    payload = {
        "targets": _pairs_wire(plan.targets),
        "phases": [
            {"name": phase.name, "changes": _pairs_wire(
                (change.leg, change.quantity_delta) for change in phase.changes
            )}
            for phase in plan.phases
        ],
        "gross_target_notional": str(plan.gross_target_notional),
        "executable": plan.executable,
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def new_checkpoint(
    plan: BasketPlan, starting_positions: dict[LegId, Decimal],
) -> BasketCheckpoint:
    """Verify that all phases sum exactly to the planned signed targets."""
    if not isinstance(plan, BasketPlan) or plan.executable is not False:
        raise BasketReconciliationError("只允许研究态、不可执行的组合计划")
    if len(plan.targets) > 64 or len(plan.phases) > 32 or not plan.targets:
        raise BasketReconciliationError("组合计划规模无效")
    targets = dict(plan.targets)
    if len(targets) != len(plan.targets) or not all(isinstance(leg, LegId) for leg in targets):
        raise BasketReconciliationError("组合目标腿重复或无效")
    initial = _pairs(starting_positions, set(targets))
    delta = {leg: Decimal(0) for leg in targets}
    for phase in plan.phases:
        if not phase.name or not phase.changes:
            raise BasketReconciliationError("组合阶段必须有名称和变动")
        seen = set()
        for change in phase.changes:
            if change.leg not in targets or change.leg in seen:
                raise BasketReconciliationError("阶段包含未知或重复组合腿")
            seen.add(change.leg)
            delta[change.leg] += _quantity(change.quantity_delta)
    if any(_quantity(targets[leg]) != dict(initial)[leg] + delta[leg] for leg in targets):
        raise BasketReconciliationError("阶段数量之和与目标仓位不一致")
    status: Status = "ready" if plan.phases else "completed"
    return BasketCheckpoint(
        plan=plan,
        fingerprint=_fingerprint(plan),
        initial_positions=initial,
        confirmed_positions=initial,
        phase_index=0,
        status=status,
    )


def _event(checkpoint: BasketCheckpoint, event: JournalEvent) -> tuple[JournalEvent, ...]:
    if len(checkpoint.events) >= _MAX_EVENTS:
        raise BasketReconciliationError("组合事件超过上限，必须冻结并人工核对")
    return checkpoint.events + (event,)


def begin_phase(checkpoint: BasketCheckpoint, request_id: str) -> BasketCheckpoint:
    """Register a durable phase identity; this does NOT transmit any order."""
    request_id = _id(request_id, "阶段请求标识")
    if checkpoint.status == "awaiting_confirmation" and checkpoint.request_id == request_id:
        return checkpoint  # idempotent retry of an already recorded intent
    if checkpoint.status != "ready" or request_id in checkpoint.used_request_ids:
        raise BasketReconciliationError("阶段未就绪、请求重复或组合已冻结")
    if checkpoint.pending_phase is None:
        raise BasketReconciliationError("没有可登记的下一阶段")
    return replace(
        checkpoint,
        status="awaiting_confirmation",
        request_id=request_id,
        fills=(),
        used_request_ids=checkpoint.used_request_ids + (request_id,),
        events=_event(checkpoint, JournalEvent("begin", request_id=request_id)),
    )


def record_cumulative_fill(
    checkpoint: BasketCheckpoint,
    *,
    request_id: str,
    event_id: str,
    leg: LegId,
    cumulative: Decimal,
) -> BasketCheckpoint:
    """Apply a trusted, signed cumulative fill; duplicates cannot double-count."""
    _id(event_id, "成交事件标识")
    if checkpoint.status != "awaiting_confirmation" or request_id != checkpoint.request_id:
        raise BasketReconciliationError("成交事件与进行中的阶段不匹配")
    if not isinstance(leg, LegId):
        raise BasketReconciliationError("成交组合腿无效")
    cumulative = _quantity(cumulative)
    for old_request, old_id, old_leg, old_quantity in checkpoint.seen_events:
        if old_id == event_id:
            if old_request == request_id and old_leg == leg and old_quantity == cumulative:
                return checkpoint
            raise BasketReconciliationError("成交事件身份冲突，必须冻结核对")
    phase_deltas = {change.leg: change.quantity_delta for change in checkpoint.pending_phase.changes}
    if leg not in phase_deltas:
        raise BasketReconciliationError("收到未授权组合腿的成交")
    expected = phase_deltas[leg]
    old = dict(checkpoint.fills).get(leg, Decimal(0))
    if (
        not expected
        or cumulative * expected < 0
        or abs(cumulative) > abs(expected)
        or abs(cumulative) < abs(old)
    ):
        raise BasketReconciliationError("成交累计数量倒退、超量或方向冲突")
    fills = {**dict(checkpoint.fills), leg: cumulative}
    event = JournalEvent("fill", request_id, event_id, leg, cumulative)
    return replace(
        checkpoint,
        fills=tuple(sorted(fills.items())),
        seen_events=checkpoint.seen_events + ((request_id, event_id, leg, cumulative),),
        events=_event(checkpoint, event),
    )


def confirm_phase(
    checkpoint: BasketCheckpoint,
    *,
    request_id: str,
    terminal_statuses: dict[LegId, str],
    observed_positions: dict[LegId, Decimal],
) -> BasketCheckpoint:
    """Require complete fills, verified terminal order status and exact venue positions.

    A caller must use authoritative order and account snapshots; no inferred fills,
    stale market data or accepted-only acknowledgments can satisfy this gate.
    """
    if checkpoint.status != "awaiting_confirmation" or request_id != checkpoint.request_id:
        raise BasketReconciliationError("没有对应的待核对阶段")
    phase = checkpoint.pending_phase
    planned = {change.leg: change.quantity_delta for change in phase.changes}
    if (
        not isinstance(terminal_statuses, dict)
        or set(terminal_statuses) != set(planned)
        or any(value != "FILLED" for value in terminal_statuses.values())
    ):
        raise BasketReconciliationError("全部阶段委托必须取得可信的最终成交状态")
    if any(dict(checkpoint.fills).get(leg, Decimal(0)) != delta
           for leg, delta in planned.items()):
        raise BasketReconciliationError("存在未完全成交的组合腿，禁止进入下一阶段")
    expected = dict(checkpoint.confirmed_positions)
    for leg, delta in planned.items():
        expected[leg] += delta
    actual = _pairs(observed_positions, set(expected))
    if actual != tuple(sorted(expected.items())):
        raise BasketReconciliationError("实际仓位与已确认成交不一致，禁止继续")
    index = checkpoint.phase_index + 1
    if index == len(checkpoint.plan.phases) and actual != checkpoint.plan.targets:
        raise BasketReconciliationError("最终仓位未达到组合目标")
    return replace(
        checkpoint,
        confirmed_positions=actual,
        phase_index=index,
        status="completed" if index == len(checkpoint.plan.phases) else "ready",
        request_id="",
        fills=(),
        events=_event(checkpoint, JournalEvent(
            "confirm", request_id=request_id, positions=actual,
        )),
    )


def freeze_checkpoint(checkpoint: BasketCheckpoint, reason: str) -> BasketCheckpoint:
    """Fail closed. Replanning requires a fresh, complete external snapshot."""
    if checkpoint.status == "completed":
        raise BasketReconciliationError("已完成计划不能冻结或重用")
    if not isinstance(reason, str) or not 1 <= len(reason) <= 512:
        raise BasketReconciliationError("必须记录明确的冻结原因")
    if checkpoint.status == "frozen":
        return checkpoint
    return replace(
        checkpoint,
        status="frozen",
        reason=reason,
        events=_event(checkpoint, JournalEvent("freeze", reason=reason)),
    )


def checkpoint_to_record(checkpoint: BasketCheckpoint) -> dict:
    """JSON-safe audit checkpoint; persisting it transactionally is caller-owned."""
    result = {
        "version": 1,
        "fingerprint": checkpoint.fingerprint,
        "initial_positions": _pairs_wire(checkpoint.initial_positions),
        "events": [],
    }
    for entry in checkpoint.events:
        row = {"kind": entry.kind}
        if entry.kind == "begin":
            row["request_id"] = entry.request_id
        elif entry.kind == "fill":
            row.update({
                "request_id": entry.request_id, "event_id": entry.event_id,
                "leg": _leg_key(entry.leg), "cumulative": str(entry.cumulative),
            })
        elif entry.kind == "confirm":
            row.update({"request_id": entry.request_id, "positions": _pairs_wire(entry.positions)})
        else:
            row["reason"] = entry.reason
        result["events"].append(row)
    return result


def restore_checkpoint(plan: BasketPlan, record: dict) -> BasketCheckpoint:
    """Replay the durable journal; a missing/unknown terminal outcome stays pending.

    Replayed state NEVER causes a new order. An unresolved phase must be checked
    against authoritative order and position evidence or frozen for replan.
    """
    if not isinstance(record, dict) or set(record) != {
        "version", "fingerprint", "initial_positions", "events"
    } or record["version"] != 1 or record["fingerprint"] != _fingerprint(plan):
        raise BasketReconciliationError("组合恢复记录版本或计划指纹不匹配")
    initial = _unwire_pairs(record["initial_positions"])
    checkpoint = new_checkpoint(plan, initial)
    if not isinstance(record["events"], list) or len(record["events"]) > _MAX_EVENTS:
        raise BasketReconciliationError("组合恢复事件记录无效")
    for row in record["events"]:
        if not isinstance(row, dict):
            raise BasketReconciliationError("恢复事件无效")
        kind = row.get("kind")
        try:
            if kind == "begin" and set(row) == {"kind", "request_id"}:
                checkpoint = begin_phase(checkpoint, row["request_id"])
            elif kind == "fill" and set(row) == {
                "kind", "request_id", "event_id", "leg", "cumulative"
            }:
                if not isinstance(row["leg"], list) or len(row["leg"]) != 4:
                    raise BasketReconciliationError("恢复事件中的组合腿无效")
                checkpoint = record_cumulative_fill(
                    checkpoint, request_id=row["request_id"], event_id=row["event_id"],
                    leg=LegId(*row["leg"]), cumulative=Decimal(row["cumulative"]),
                )
            elif kind == "confirm" and set(row) == {"kind", "request_id", "positions"}:
                phase = checkpoint.pending_phase
                if phase is None:
                    raise BasketReconciliationError("恢复确认没有对应阶段")
                checkpoint = confirm_phase(
                    checkpoint, request_id=row["request_id"],
                    terminal_statuses={change.leg: "FILLED" for change in phase.changes},
                    observed_positions=_unwire_pairs(row["positions"]),
                )
            elif kind == "freeze" and set(row) == {"kind", "reason"}:
                checkpoint = freeze_checkpoint(checkpoint, row["reason"])
            else:
                raise BasketReconciliationError("恢复事件类型或字段无效")
        except (TypeError, ValueError, ArithmeticError) as exc:
            raise BasketReconciliationError("恢复事件验证失败") from exc
    if checkpoint_to_record(checkpoint) != record:
        raise BasketReconciliationError("恢复事件不规范或记录不一致")
    return checkpoint
