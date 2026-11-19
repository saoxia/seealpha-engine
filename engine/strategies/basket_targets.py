"""Fail-closed, research-only cross-product target planning.

This module produces *intent*, not venue orders. Binance has no transaction
that atomically fills spot, USD-M perpetuals and options together.
"""
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal


class BasketPlanError(ValueError):
    """The portfolio lacks safe, complete evidence for a basket plan."""


Product = Literal["spot", "futures_um", "options"]


def _finite(value: Decimal, label: str, *, positive: bool = False) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise BasketPlanError(f"{label}必须为有限 Decimal")
    if positive and value <= 0:
        raise BasketPlanError(f"{label}必须大于零")
    return value


@dataclass(frozen=True, order=True)
class LegId:
    tenant_id: str
    account_id: str
    product: Product
    symbol: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.tenant_id, str)
            or not 1 <= len(self.tenant_id) <= 64
            or not self.tenant_id.strip()
            or not isinstance(self.account_id, str)
            or not 1 <= len(self.account_id) <= 40
            or not self.account_id.strip()
            or self.product not in {"spot", "futures_um", "options"}
            or not isinstance(self.symbol, str)
            or not 1 <= len(self.symbol) <= 64
            or not self.symbol.isascii()
            or not all(char.isupper() or char.isdigit() or char in "-_" for char in self.symbol)
        ):
            raise BasketPlanError("组合腿的账户、产品或合约身份无效")


@dataclass(frozen=True)
class MemberLegTarget:
    member_id: str
    leg: LegId
    signed_quantity: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.member_id, str) or not self.member_id.strip():
            raise BasketPlanError("成员标识无效")
        if not isinstance(self.leg, LegId):
            raise BasketPlanError("组合腿标识无效")
        _finite(self.signed_quantity, "目标数量")


@dataclass(frozen=True)
class LegEvidence:
    price: Decimal
    quantity_increment: Decimal
    multiplier: Decimal
    ts_ns: int

    def __post_init__(self) -> None:
        _finite(self.price, "成交价格", positive=True)
        _finite(self.quantity_increment, "合约数量步长", positive=True)
        _finite(self.multiplier, "合约乘数", positive=True)
        if type(self.ts_ns) is not int or self.ts_ns <= 0:
            raise BasketPlanError("行情时间戳无效")


@dataclass(frozen=True)
class LegChange:
    leg: LegId
    quantity_delta: Decimal


@dataclass(frozen=True)
class BasketPhase:
    name: str
    changes: tuple[LegChange, ...]


@dataclass(frozen=True)
class BasketPlan:
    """An auditable plan; no method on this object can submit an order."""

    targets: tuple[tuple[LegId, Decimal], ...]
    phases: tuple[BasketPhase, ...]
    gross_target_notional: Decimal
    executable: bool = False


def _option_contract(leg: LegId, underlying: str) -> tuple[str, str]:
    parts = leg.symbol.split("-")
    if (
        len(parts) != 4
        or parts[0] != underlying
        or len(parts[1]) != 6
        or not parts[1].isdigit()
        or not parts[2].isdigit()
        or parts[3] not in {"C", "P"}
    ):
        raise BasketPlanError("期权必须为同标的、有效到期标识的看涨/看跌合约")
    return parts[1], parts[3]


def plan_basket(
    member_targets: list[MemberLegTarget],
    positions: dict[LegId, Decimal],
    evidence: dict[LegId, LegEvidence],
    *,
    now_ns: int,
    max_age_ns: int,
    max_gross_notional: Decimal,
    policy: Literal["funding_carry", "option_protected"],
    max_leg_drift: Decimal = Decimal(0),
) -> BasketPlan:
    """Net identical instruments only; plan risk-reduction and hedge order.

    Strictly research-only. Callers must not translate phases into live orders.
    Missing positions, stale quotes, unsupported products and unsafe coverage
    fail closed. The budget is deliberately *gross*, not delta-netted.
    """
    if not member_targets or len(member_targets) > 64:
        raise BasketPlanError("组合目标不能为空，最多支持64条成员目标")
    if type(now_ns) is not int or now_ns <= 0 or type(max_age_ns) is not int or max_age_ns <= 0:
        raise BasketPlanError("时间窗口无效")
    _finite(max_gross_notional, "总名义风险预算", positive=True)
    _finite(max_leg_drift, "最大腿间偏差")
    if max_leg_drift < 0:
        raise BasketPlanError("最大腿间偏差不能为负")
    if policy not in {"funding_carry", "option_protected"}:
        raise BasketPlanError("未知跨产品组合策略")

    targets = defaultdict(lambda: Decimal(0))
    seen = set()
    for target in member_targets:
        identity = (target.member_id, target.leg)
        if identity in seen:
            raise BasketPlanError("同一成员对同一组合腿重复申报")
        seen.add(identity)
        targets[target.leg] += target.signed_quantity
    legs = set(targets)
    if len({(leg.tenant_id, leg.account_id) for leg in legs}) != 1:
        raise BasketPlanError("当前组合只允许同一租户、同一账户内的多产品腿")
    if not isinstance(positions, dict) or set(positions) != legs:
        raise BasketPlanError("必须提供全部组合腿的实际仓位，不允许省略或额外仓位")
    if not isinstance(evidence, dict) or not legs.issubset(evidence):
        raise BasketPlanError("缺少组合腿的合约与价格证据")

    for leg in legs:
        quote = evidence[leg]
        if not isinstance(quote, LegEvidence) or not 0 <= now_ns - quote.ts_ns <= max_age_ns:
            raise BasketPlanError("组合腿行情过期或来自未来")
        current = _finite(positions[leg], "实际仓位")
        desired = targets[leg]
        if desired % quote.quantity_increment or current % quote.quantity_increment:
            raise BasketPlanError("仓位数量不符合合约最小步长")
        if leg.product in {"spot", "options"} and (desired < 0 or current < 0):
            raise BasketPlanError("此阶段不支持借币卖空或卖出裸期权")

    futures = sorted(leg for leg in legs if leg.product == "futures_um")
    if len(futures) != 1:
        raise BasketPlanError("组合必须恰好包含一条 USD-M 永续腿")
    perp = futures[0]
    if policy == "funding_carry":
        spots = [leg for leg in legs if leg.product == "spot"]
        if len(legs) != 2 or len(spots) != 1 or spots[0].symbol != perp.symbol:
            raise BasketPlanError("资金费策略必须为同标的一条现货腿和一条永续腿")
        spot = spots[0]
        if targets[spot] < 0 or targets[perp] > 0 or targets[spot] + targets[perp] != 0:
            raise BasketPlanError("现货多头与永续空头必须等额对冲")
        if positions[spot] + positions[perp] > max_leg_drift or (
            positions[spot] + positions[perp] < -max_leg_drift
        ):
            raise BasketPlanError("现有资金费组合腿间偏差超过上限")
    else:
        options = sorted(leg for leg in legs if leg.product == "options")
        if len(legs) != 3 or len(options) != 2:
            raise BasketPlanError("期权保护组合必须为一条永续和两条保护腿")
        underlying = perp.symbol.removesuffix("USDT")
        names = [_option_contract(leg, underlying) for leg in options]
        if (len({expiry for expiry, _side in names}) != 1
                or {side for _expiry, side in names} != {"C", "P"}):
            raise BasketPlanError("保护期权必须有相同到期日且一看涨一看跌")
        for leg in options:
            if (
                targets[leg] * evidence[leg].multiplier < abs(targets[perp])
                or positions[leg] * evidence[leg].multiplier < abs(positions[perp])
            ):
                raise BasketPlanError("期权保护数量不足，禁止裸露永续方向仓位")

    gross = sum(
        (abs(quantity) * evidence[leg].price * evidence[leg].multiplier
         for leg, quantity in targets.items()), Decimal(0)
    )
    if gross > max_gross_notional:
        raise BasketPlanError("组合目标超过总名义风险预算")

    changes = {leg: targets[leg] - positions[leg] for leg in legs}
    phases = []
    if policy == "funding_carry":
        if any(changes.values()):
            phases.append(BasketPhase(
                "coordinated_spot_perpetual_required",
                tuple(LegChange(leg, changes[leg]) for leg in sorted(legs) if changes[leg]),
            ))
    else:
        down = []
        up = []
        future_current = positions[perp]
        future_target = targets[perp]
        if future_current and (
            future_current * future_target <= 0 or abs(future_target) < abs(future_current)
        ):
            reduced_to = Decimal(0) if future_current * future_target <= 0 else future_target
            down.append(LegChange(perp, reduced_to - future_current))
        if future_target and (
            future_current * future_target <= 0 or abs(future_target) > abs(future_current)
        ):
            opened_from = Decimal(0) if future_current * future_target <= 0 else future_current
            up.append(LegChange(perp, future_target - opened_from))
        acquire = [LegChange(leg, changes[leg]) for leg in options if changes[leg] > 0]
        release = [LegChange(leg, changes[leg]) for leg in options if changes[leg] < 0]
        for name, rows in (
            ("reduce_directional_risk", down),
            ("acquire_and_confirm_protection", acquire),
            ("increase_directional_risk_after_protection", up),
            ("release_protection_after_risk_reduction", release),
        ):
            if rows:
                phases.append(BasketPhase(name, tuple(rows)))

    return BasketPlan(
        targets=tuple(sorted(targets.items())),
        phases=tuple(phases),
        gross_target_notional=gross,
    )
