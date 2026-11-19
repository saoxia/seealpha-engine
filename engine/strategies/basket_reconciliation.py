"""Research-only, fail-closed reconciliation of multi-product basket phases.

No exchange orders are submitted here. The caller owns native order creation,
independent portfolio observations and permanent storage of the checkpoint.
"""
import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal

from engine.strategies.basket_targets import BasketPlan, BasketPlanError, LegId


def _key(leg: LegId) -> tuple[str, str, str, str]:
    return leg.tenant_id, leg.account_id, leg.product, leg.symbol


def _fingerprint(plan: BasketPlan) -> str:
    payload = {
        "targets": [(_key(leg), format(qty, "f")) for leg, qty in plan.targets],
        "phases": [
            (phase.name, [(_key(change.leg), format(change.quantity_delta, "f"))
                          for change in phase.changes])
            for phase in plan.phases
        ],
        "gross": format(plan.gross_target_notional, "f"),
    }
    canonical = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass
class _Pending:
    leg: LegId
    signed_request: Decimal
    remaining: Decimal


class BasketReconciliation:
    """Only real fills plus independent position checks may advance a phase.

    Unknown results, duplicate order submissions, timeouts and inconsistent
    evidence freeze the run rather than retrying. In-flight restores stay frozen.
    """

    def __init__(self, plan: BasketPlan, initial_positions: dict[LegId, Decimal],
                 *, run_id: str, max_phase_age_ns: int = 60_000_000_000) -> None:
        if not isinstance(plan, BasketPlan) or plan.executable is not False:
            raise BasketPlanError("仅接受不可实盘执行的组合研究计划")
        if not isinstance(run_id, str) or not 1 <= len(run_id) <= 128 or not run_id.strip():
            raise BasketPlanError("组合运行身份无效")
        if type(max_phase_age_ns) is not int or max_phase_age_ns <= 0:
            raise BasketPlanError("组合阶段超时无效")
        self.plan = plan
        self.run_id = run_id
        self.max_phase_age_ns = max_phase_age_ns
        self.plan_hash = _fingerprint(plan)
        self.positions = dict(initial_positions)
        self.phase_index = 0
        self._active = False
        self._started_ns: int | None = None
        self._pending: dict[str, _Pending] = {}
        self._used_ids: set[str] = set()
        self._fills: dict[str, tuple[str, Decimal]] = {}
        self.frozen_reason: str | None = None

        targets = dict(plan.targets)
        if len(targets) != len(plan.targets) or set(self.positions) != set(targets):
            raise BasketPlanError("组合的仓位身份与目标不一致")
        for quantity in (*targets.values(), *self.positions.values()):
            if not isinstance(quantity, Decimal) or not quantity.is_finite():
                raise BasketPlanError("组合数量必须为有限 Decimal")
        expected = dict(self.positions)
        for phase in plan.phases:
            if not phase.changes:
                raise BasketPlanError("组合阶段不得为空")
            in_phase = set()
            for change in phase.changes:
                if change.leg not in expected or change.leg in in_phase:
                    raise BasketPlanError("阶段腿身份错误或重复")
                in_phase.add(change.leg)
                if (not isinstance(change.quantity_delta, Decimal)
                        or not change.quantity_delta.is_finite()
                        or change.quantity_delta == 0):
                    raise BasketPlanError("阶段目标差额无效")
                expected[change.leg] += change.quantity_delta
        if expected != targets:
            raise BasketPlanError("组合计划阶段不能完整收敛至目标仓位")

    @property
    def next_phase(self):
        if self.frozen_reason is not None or self._active:
            return None
        if self.phase_index < len(self.plan.phases):
            return self.plan.phases[self.phase_index]
        return None

    @property
    def complete(self) -> bool:
        return (
            self.frozen_reason is None
            and not self._active
            and self.phase_index == len(self.plan.phases)
            and self.positions == dict(self.plan.targets)
        )

    @property
    def needs_confirmation(self) -> bool:
        return self._active and not self._pending and self.frozen_reason is None

    def freeze(self, reason: str) -> None:
        if self.frozen_reason is None:
            self.frozen_reason = reason or "组合状态未知，必须人工核对"

    def _deny(self, reason: str):
        self.freeze(reason)
        raise BasketPlanError(reason)

    def _verify_positions(self, actual_positions: dict[LegId, Decimal]) -> None:
        if not isinstance(actual_positions, dict) or actual_positions != self.positions:
            self._deny("原生成交仓位与组合账本不一致，冻结组合")

    def open_phase(self, order_ids: dict[LegId, str], *,
                   observed_positions: dict[LegId, Decimal], now_ns: int) -> None:
        """Register all client IDs before the caller submits any native orders."""
        if self.frozen_reason is not None:
            raise BasketPlanError("组合已冻结：" + self.frozen_reason)
        if self._active or self.next_phase is None:
            self._deny("组合阶段尚未确认或已完成，不得再次提交")
        self._verify_positions(observed_positions)
        if type(now_ns) is not int or now_ns <= 0:
            self._deny("组合阶段时间无效")
        phase = self.plan.phases[self.phase_index]
        expected = {change.leg: change for change in phase.changes}
        if not isinstance(order_ids, dict) or set(order_ids) != set(expected):
            self._deny("组合阶段订单与计划腿不匹配")
        ids = list(order_ids.values())
        if (any(not isinstance(x, str) or not 1 <= len(x) <= 128 or not x.strip()
                for x in ids) or len(ids) != len(set(ids)) or set(ids) & self._used_ids):
            self._deny("组合订单请求身份无效或重复")
        self._pending = {
            order_ids[leg]: _Pending(leg, change.quantity_delta, abs(change.quantity_delta))
            for leg, change in expected.items()
        }
        self._used_ids.update(ids)
        self._started_ns = now_ns
        self._active = True

    def record_fill(self, client_order_id: str, quantity: Decimal, *,
                    fill_id: str, now_ns: int) -> None:
        """Accept a unique native fill ID, not an accepted order or target."""
        if self.frozen_reason is not None:
            raise BasketPlanError("组合已冻结：" + self.frozen_reason)
        if (not isinstance(fill_id, str) or not 1 <= len(fill_id) <= 128
                or not fill_id.strip()):
            self._deny("组合成交事件身份无效")
        if not isinstance(quantity, Decimal) or not quantity.is_finite() or quantity <= 0:
            self._deny("组合成交数量无效")
        old = self._fills.get(fill_id)
        if old is not None:
            if old == (client_order_id, quantity):
                return
            self._deny("相同成交事件身份对应不同内容")
        if not self._active or client_order_id not in self._pending:
            self._deny("未知或重复的组合成交订单")
        if type(now_ns) is not int or self._started_ns is None or now_ns < self._started_ns:
            self._deny("组合成交事件时间无效")
        pending = self._pending[client_order_id]
        if quantity > pending.remaining:
            self._deny("组合成交量超过尚未成交数量")
        sign = Decimal(1) if pending.signed_request > 0 else Decimal(-1)
        self.positions[pending.leg] += sign * quantity
        pending.remaining -= quantity
        self._fills[fill_id] = (client_order_id, quantity)
        if pending.remaining == 0:
            del self._pending[client_order_id]

    def confirm_phase(self, observed_positions: dict[LegId, Decimal]) -> None:
        """Require an independent full account-position observation."""
        if self.frozen_reason is not None:
            raise BasketPlanError("组合已冻结：" + self.frozen_reason)
        if not self.needs_confirmation:
            self._deny("阶段未全部确认成交")
        self._verify_positions(observed_positions)
        self.phase_index += 1
        self._active = False
        self._started_ns = None

    def reject_or_unknown(self, client_order_id: str, reason: str) -> None:
        if client_order_id not in self._used_ids:
            self._deny("无法识别的组合订单状态报告")
        self.freeze("组合订单未确认：" + reason)

    def check_timeout(self, now_ns: int) -> None:
        if self.frozen_reason is not None:
            raise BasketPlanError("组合已冻结：" + self.frozen_reason)
        if type(now_ns) is not int or now_ns <= 0:
            self._deny("组合时间无效")
        if (self._active and self._started_ns is not None
                and now_ns - self._started_ns > self.max_phase_age_ns):
            self._deny("组合阶段超时，禁止盲目重发")

    def checkpoint(self) -> dict:
        """Serializable state; a restart with an in-flight phase freezes."""
        return {
            "version": 1, "run_id": self.run_id, "plan_hash": self.plan_hash,
            "phase_index": self.phase_index, "active": self._active,
            "positions": [format(self.positions[leg], "f") for leg, _ in self.plan.targets],
            "used_order_ids": sorted(self._used_ids),
            "pending_order_ids": sorted(self._pending),
            "frozen_reason": self.frozen_reason,
        }

    @classmethod
    def restore(cls, plan: BasketPlan, initial_positions: dict[LegId, Decimal], *,
                run_id: str, checkpoint: dict,
                observed_positions: dict[LegId, Decimal]) -> "BasketReconciliation":
        """Only resume a verified phase boundary; never replay in-flight orders."""
        tracker = cls(plan, initial_positions, run_id=run_id)
        if not isinstance(checkpoint, dict) or any(
            checkpoint.get(field) != value for field, value in (
                ("version", 1), ("run_id", run_id), ("plan_hash", tracker.plan_hash),
            )
        ):
            tracker._deny("组合检查点与计划身份不一致")
        index = checkpoint.get("phase_index")
        if type(index) is not int or not 0 <= index <= len(plan.phases):
            tracker._deny("组合检查点阶段无效")
        ids = checkpoint.get("used_order_ids")
        if (not isinstance(ids, list) or len(ids) != len(set(ids))
                or any(not isinstance(x, str) or not x for x in ids)):
            tracker._deny("组合检查点订单身份无效")
        tracker._used_ids = set(ids)
        tracker.phase_index = index
        for phase in plan.phases[:index]:
            for change in phase.changes:
                tracker.positions[change.leg] += change.quantity_delta
        if checkpoint.get("positions") != [
            format(tracker.positions[leg], "f") for leg, _ in plan.targets
        ]:
            tracker._deny("组合检查点仓位与已确认阶段不匹配")
        if checkpoint.get("active") is not False:
            tracker.freeze("恢复时存在未确认的组合订单，必须独立对账")
        if checkpoint.get("frozen_reason"):
            tracker.freeze(str(checkpoint["frozen_reason"]))
        if not isinstance(observed_positions, dict) or observed_positions != tracker.positions:
            tracker.freeze("恢复时原生账户仓位不一致，必须独立对账")
        return tracker
