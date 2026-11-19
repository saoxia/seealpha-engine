"""Offline phase-risk observations; never authorizes a venue order.

Consumers must supply *authoritative* position evidence and fresh market data.
An alarm returns a frozen research checkpoint that the caller must persist
with the existing CAS journal store before performing any later action.
"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from engine.strategies.basket_reconciliation import (
    BasketCheckpoint,
    BasketReconciliationError,
    freeze_checkpoint,
)
from engine.strategies.basket_targets import LegEvidence, LegId


@dataclass(frozen=True)
class BasketRiskObservation:
    state: Literal["safe", "frozen"]
    gross_notional: Decimal
    leg_drift: Decimal | None
    reason: str
    checkpoint: BasketCheckpoint
    executable: bool = False


def observe_basket_phase(
    checkpoint: BasketCheckpoint,
    observed_positions: dict[LegId, Decimal],
    evidence: dict[LegId, LegEvidence],
    *,
    now_ns: int,
    max_age_ns: int,
    max_gross_notional: Decimal,
    max_leg_drift: Decimal,
    max_pending_ns: int,
    pending_since_ns: int | None = None,
) -> BasketRiskObservation:
    """Fail closed on inconsistent fills, hedge gaps and an overlong phase.

    Do not infer venue fills from prices or from a pending order acceptance.
    No state transition can create an order or restore a frozen checkpoint.
    """
    if not isinstance(checkpoint, BasketCheckpoint) or checkpoint.plan.executable is not False:
        raise BasketReconciliationError("只能监督不可执行的研究组合阶段")
    if (
        type(now_ns) is not int or now_ns <= 0
        or type(max_age_ns) is not int or max_age_ns <= 0
        or type(max_pending_ns) is not int or max_pending_ns <= 0
        or not isinstance(max_gross_notional, Decimal)
        or not max_gross_notional.is_finite() or max_gross_notional <= 0
        or not isinstance(max_leg_drift, Decimal)
        or not max_leg_drift.is_finite() or max_leg_drift < 0
    ):
        raise BasketReconciliationError("研究阶段风控阈值无效")
    expected_legs = {leg for leg, _ in checkpoint.plan.targets}
    if (
        not isinstance(observed_positions, dict)
        or not isinstance(evidence, dict)
        or set(observed_positions) != expected_legs
        or set(evidence) != expected_legs
    ):
        raise BasketReconciliationError("研究监督必须接收完整的权威仓位及市场证据")

    reason = ""
    gross = Decimal(0)
    drift = None
    for leg in sorted(expected_legs):
        current = observed_positions[leg]
        quote = evidence[leg]
        if (
            not isinstance(current, Decimal) or not current.is_finite()
            or not isinstance(quote, LegEvidence)
        ):
            raise BasketReconciliationError("组合仓位或市场证据无效")
        if not 0 <= now_ns - quote.ts_ns <= max_age_ns:
            reason = "研究阶段行情过期或来自未来"
        if current % quote.quantity_increment:
            reason = "组合实际仓位不满足合约数量步长"
        if leg.product in {"spot", "options"} and current < 0:
            reason = "现有研究仓位出现不支持的卖空或裸卖期权"
        gross += abs(current) * quote.price * quote.multiplier

    if checkpoint.status == "frozen":
        reason = checkpoint.reason or "组合已被冻结"
    elif checkpoint.status == "awaiting_confirmation":
        if type(pending_since_ns) is not int or not 0 < pending_since_ns <= now_ns:
            reason = "未提供可信阶段开始时间"
        elif now_ns - pending_since_ns > max_pending_ns:
            reason = "待确认组合超过最大裸露/未知订单时长"
        # The trusted checkpoint's cumulative fills must reconcile with the
        # full external account snapshot before risk gates can pass.
        projected = dict(checkpoint.confirmed_positions)
        for leg, delta in checkpoint.fills:
            projected[leg] += delta
        if projected != observed_positions:
            reason = "权威仓位与可重放累计成交不一致"
    elif checkpoint.status in {"ready", "completed"}:
        if dict(checkpoint.confirmed_positions) != observed_positions:
            reason = "权威仓位与已确认阶段不一致"
    else:
        raise BasketReconciliationError("未知组合阶段状态")

    futures = [leg for leg in expected_legs if leg.product == "futures_um"]
    if len(futures) != 1:
        raise BasketReconciliationError("组合研究监督需要唯一 USD-M 永续腿")
    perp = futures[0]
    spots = [leg for leg in expected_legs if leg.product == "spot"]
    options = [leg for leg in expected_legs if leg.product == "options"]
    if len(spots) == 1 and not options and len(expected_legs) == 2:
        drift = abs(observed_positions[spots[0]] + observed_positions[perp])
        if drift > max_leg_drift:
            reason = "组合现货与永续腿间偏差超过风险上限"
    elif len(options) == 2 and not spots and len(expected_legs) == 3:
        if any(
            observed_positions[leg] * evidence[leg].multiplier <
            abs(observed_positions[perp])
            for leg in options
        ):
            reason = "组合永续仓位未获已确认的期权双翼完全保护"
    else:
        raise BasketReconciliationError("研究风险监督不支持此组合结构")
    if gross > max_gross_notional:
        reason = "组合当前毛名义风险超过预算"

    if reason:
        if checkpoint.status == "completed":
            # A completed historical journal is immutable. Reject external
            # discrepancies instead of returning an impossible frozen state.
            raise BasketReconciliationError("已完成组合出现新的风险或仓位异常：" + reason)
        frozen = (
            checkpoint if checkpoint.status == "frozen"
            else freeze_checkpoint(checkpoint, reason)
        )
        return BasketRiskObservation("frozen", gross, drift, reason, frozen)
    return BasketRiskObservation("safe", gross, drift, "", checkpoint)
