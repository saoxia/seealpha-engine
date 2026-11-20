"""The only value a strategy may publish to the engine.

Indicators and signals stay inside the strategy. The engine reads a target holding
and does not accept submit_order or close_all_positions from the strategy.
"""

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class TargetHolding:
    """Desired position for one symbol. Supply quantity or weight, not both."""

    symbol: str
    quantity: Decimal | None = None
    weight: Decimal | None = None

    def __post_init__(self) -> None:
        has_quantity = self.quantity is not None
        has_weight = self.weight is not None
        if has_quantity == has_weight:
            raise ValueError("目标持仓需要数量或权重其中之一")
        if has_quantity and (
            not isinstance(self.quantity, Decimal) or not self.quantity.is_finite()
        ):
            raise ValueError("目标数量必须是有限 Decimal")
        if has_weight and (
            not isinstance(self.weight, Decimal) or not self.weight.is_finite()
            or self.weight < -1 or self.weight > 1
        ):
            raise ValueError("目标权重必须是 -1 到 1 之间的有限 Decimal")


def signed_quantity(holding: TargetHolding, allocation: Decimal) -> Decimal:
    """Weight 1 is the full allocation; weight 0 is flat. Quantity is already signed."""
    if not isinstance(allocation, Decimal) or not allocation.is_finite() or allocation <= 0:
        raise ValueError("分配数量必须是大于零的有限 Decimal")
    if holding.quantity is not None:
        return holding.quantity
    return holding.weight * allocation
