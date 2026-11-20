"""Fail-closed signed, order-bound exchange evidence gate for a basket phase.

Only accepts trusted data supplied by a future *separate* venue adapter; this
module has no network client and never sends or cancels real orders. Existing
research journal checkpoints retain responsibility for durable persistence.
"""
import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from engine.portfolio.basket_reconciliation import (
    BasketCheckpoint,
    BasketReconciliationError,
    confirm_phase,
    freeze_checkpoint,
    record_cumulative_fill,
)
from engine.portfolio.basket_targets import LegId


@dataclass(frozen=True)
class BoundLegOrder:
    leg: LegId
    client_order_id: str
    side: Literal["BUY", "SELL"]
    quantity: Decimal


@dataclass(frozen=True)
class BoundBasketPhase:
    request_id: str
    fingerprint: str
    orders: tuple[BoundLegOrder, ...]
    executable: bool = False


@dataclass(frozen=True)
class AuthoritativeOrderEvidence:
    """Data from verified exchange query, NEVER a placement acknowledgement."""

    leg: LegId
    client_order_id: str
    venue_order_id: str
    execution_id: str
    filled_quantity: Decimal
    status: Literal[
        "FILLED", "PARTIALLY_FILLED", "CANCELED", "REJECTED",
        "EXPIRED", "UNKNOWN", "NEW",
    ]
    as_of_ns: int


def bind_phase_orders(checkpoint: BasketCheckpoint) -> BoundBasketPhase:
    """Produce deterministic stable identities; NO venue transport is available."""
    if (
        not isinstance(checkpoint, BasketCheckpoint)
        or checkpoint.plan.executable is not False
        or checkpoint.status != "awaiting_confirmation"
        or checkpoint.pending_phase is None
    ):
        raise BasketReconciliationError("只能为研究态待核对阶段生成绑定身份")
    orders = []
    for change in checkpoint.pending_phase.changes:
        if change.quantity_delta == 0:
            raise BasketReconciliationError("组合阶段不能生成零数量委托")
        payload = json.dumps(
            [checkpoint.fingerprint, checkpoint.request_id,
             change.leg.tenant_id, change.leg.account_id,
             change.leg.product, change.leg.symbol],
            separators=(",", ":"), ensure_ascii=True,
        )
        # Only an idempotency identity; never a certificate of exchange approval.
        client_id = "SA" + hashlib.sha256(payload.encode()).hexdigest()[:30]
        orders.append(BoundLegOrder(
            leg=change.leg, client_order_id=client_id,
            side="BUY" if change.quantity_delta > 0 else "SELL",
            quantity=abs(change.quantity_delta),
        ))
    if len({order.client_order_id for order in orders}) != len(orders):
        raise BasketReconciliationError("组合阶段委托身份冲突")
    return BoundBasketPhase(checkpoint.request_id, checkpoint.fingerprint, tuple(orders))


def reconcile_order_evidence(
    checkpoint: BasketCheckpoint,
    manifest: BoundBasketPhase,
    evidence: tuple[AuthoritativeOrderEvidence, ...],
    positions: dict[LegId, Decimal],
    *,
    positions_ts_ns: int,
    now_ns: int,
    max_age_ns: int,
) -> BasketCheckpoint:
    """Reconcile one authoritative snapshot per order, freeze on uncertainty.

    Caller MUST persist the returned journal via CAS before doing anything else.
    This cannot establish trust in the supplied account/exchange facts; a future
    verified venue adapter must authenticate, query, and bind them to the account.
    No retries, compensation orders or production authority are implemented.
    """
    if not isinstance(manifest, BoundBasketPhase) or manifest.executable is not False:
        raise BasketReconciliationError("必须使用不可执行的研究委托绑定")
    if not isinstance(checkpoint, BasketCheckpoint) or checkpoint.status != "awaiting_confirmation":
        raise BasketReconciliationError("组合阶段不在可核对状态")
    if (
        manifest.fingerprint != checkpoint.fingerprint
        or manifest.request_id != checkpoint.request_id
        or manifest != bind_phase_orders(checkpoint)
    ):
        raise BasketReconciliationError("组合阶段身份或审计指纹不一致")
    if (
        type(now_ns) is not int or now_ns <= 0
        or type(max_age_ns) is not int or max_age_ns <= 0
        or type(positions_ts_ns) is not int
        or not 0 <= now_ns - positions_ts_ns <= max_age_ns
    ):
        return freeze_checkpoint(checkpoint, "权威账户仓位时间无效或过期")

    expected = {order.leg: order for order in manifest.orders}
    if (
        not isinstance(evidence, tuple)
        or len(evidence) != len(expected)
        or not all(isinstance(item, AuthoritativeOrderEvidence) for item in evidence)
        or {item.leg for item in evidence} != set(expected)
        or not isinstance(positions, dict)
        or set(positions) != {leg for leg, _ in checkpoint.confirmed_positions}
    ):
        raise BasketReconciliationError("缺少完整的委托或权威账户仓位证据")
    if not all(isinstance(value, Decimal) and value.is_finite()
               for value in positions.values()):
        raise BasketReconciliationError("权威仓位数量格式无效")
    if len({item.client_order_id for item in evidence}) != len(evidence):
        raise BasketReconciliationError("委托客户端身份重复")
    # Exchange order and trade IDs may overlap across product namespaces.
    venue_ids = {
        (item.leg.product, item.leg.symbol, item.venue_order_id)
        for item in evidence if item.venue_order_id
    }
    if len(venue_ids) != len([item for item in evidence if item.venue_order_id]):
        raise BasketReconciliationError("同一产品合约交易所委托身份重复")

    state = checkpoint
    terminal = {}
    problem = ""
    for row in evidence:
        original = expected[row.leg]
        if row.client_order_id != original.client_order_id:
            raise BasketReconciliationError("交易所委托证据不属于已绑定腿")
        if (
            not isinstance(row.venue_order_id, str)
            or len(row.venue_order_id) > 64
            or not row.venue_order_id.isascii()
            or (row.venue_order_id and not row.venue_order_id.isalnum())
            or not isinstance(row.execution_id, str)
            or len(row.execution_id) > 64
            or not row.execution_id.isascii()
            or (row.execution_id and not row.execution_id.isalnum())
            or (row.filled_quantity > 0 and not (row.venue_order_id and row.execution_id))
            or not isinstance(row.filled_quantity, Decimal)
            or not row.filled_quantity.is_finite()
            or row.filled_quantity < 0
            or row.filled_quantity > original.quantity
            or row.status not in {
                "FILLED", "PARTIALLY_FILLED", "CANCELED", "REJECTED",
                "EXPIRED", "UNKNOWN", "NEW",
            }
        ):
            raise BasketReconciliationError("交易所订单状态或成交数据无效")
        if type(row.as_of_ns) is not int or not 0 <= now_ns - row.as_of_ns <= max_age_ns:
            problem = "交易所订单快照已过期或来自未来"
        if (row.status == "FILLED") != (row.filled_quantity == original.quantity):
            problem = "交易所完整成交状态与累计数量冲突"
        terminal[row.leg] = row.status
        # An execution-id and a matching source-bound order are needed even for
        # the last quantity. A static accepted acknowledgement is not a fill.
        if row.filled_quantity:
            sign = Decimal(1) if original.side == "BUY" else Decimal(-1)
            state = record_cumulative_fill(
                state,
                request_id=manifest.request_id,
                event_id="o" + hashlib.sha256(
                    (original.client_order_id + ":" + row.venue_order_id + ":"
                     + row.execution_id).encode()
                ).hexdigest()[:32],
                leg=row.leg,
                cumulative=sign * row.filled_quantity,
            )
        if row.status != "FILLED":
            problem = "组合仍有未完成、拒绝或状态不确定的交易所订单"
    if problem:
        return freeze_checkpoint(state, problem)
    try:
        return confirm_phase(
            state,
            request_id=manifest.request_id,
            terminal_statuses=terminal,
            observed_positions=positions,
        )
    except BasketReconciliationError as exc:
        return freeze_checkpoint(state, "权威订单与仓位无法完成对账：" + str(exc)[:350])
