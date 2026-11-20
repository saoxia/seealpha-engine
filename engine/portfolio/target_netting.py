"""把多个成员的带符号目标合并成一个账户目标。

NautilusTrader 2.0.0rc7 的 Portfolio.net_position 只读已经成交的账户净持仓，
RiskEngine 只逐笔检查订单，OMS 的 NETTING 也只在成交之后合并持仓。
下单前把多空目标对冲掉，是这里补上的组合层，不是引擎已有能力。
"""

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal


@dataclass(frozen=True)
class MemberTarget:
    """一名成员希望账户为其保留的数量。正数做多，负数做空，零表示空仓。"""

    member_id: str
    signed_quantity: Decimal
    risk_exit: bool = False

    def __post_init__(self) -> None:
        quantity = self.signed_quantity
        if not isinstance(quantity, Decimal) or not quantity.is_finite():
            raise ValueError("目标数量必须是有限 Decimal")


def net_signed_quantity(targets: tuple[MemberTarget, ...] | list[MemberTarget]) -> Decimal:
    """同一合约上各成员目标的代数和。多空在这里抵消，而不是各发一笔订单。"""
    total = Decimal(0)
    for target in targets:
        total += target.signed_quantity
    return total


def execution_delta(net_target: Decimal, account_position: Decimal) -> Decimal:
    """账户还要成交的数量。正数买入，负数卖出。"""
    if not net_target.is_finite() or not account_position.is_finite():
        raise ValueError("净目标与账户持仓必须是有限数量")
    return net_target - account_position


def quantize_toward_zero(quantity: Decimal, increment: Decimal) -> Decimal:
    """按合约步长向零取整，避免净差额超过目标。"""
    if not increment.is_finite() or increment <= 0:
        raise ValueError("数量步长必须为正")
    if quantity == 0:
        return Decimal(0)
    steps = (abs(quantity) / increment).to_integral_value(rounding=ROUND_DOWN)
    sized = steps * increment
    return sized if quantity > 0 else -sized
