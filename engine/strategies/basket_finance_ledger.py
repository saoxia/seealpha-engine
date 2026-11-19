"""Deterministic per-product USDT simulation ledger for research basket fills.

Explicit execution quotes and observable funding settlements are required.
This is an OFFLINE fill/cash accounting model, not a native exchange matcher,
authentication of downloaded data or a live order route.
"""
from dataclasses import dataclass
from decimal import Decimal

from engine.strategies.basket_targets import BasketPlanError, LegEvidence, LegId


def _number(value: Decimal, name: str, *, positive: bool = False) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise BasketPlanError(f"{name}必须为有限 Decimal")
    if positive and value <= 0:
        raise BasketPlanError(f"{name}必须为正数")
    return value


@dataclass(frozen=True)
class BasketContract:
    leg: LegId
    quantity_increment: Decimal
    multiplier: Decimal
    taker_fee_rate: Decimal
    initial_margin_ratio: Decimal = Decimal(0)

    def __post_init__(self) -> None:
        if not isinstance(self.leg, LegId):
            raise BasketPlanError("账本合约身份无效")
        _number(self.quantity_increment, "合约步长", positive=True)
        _number(self.multiplier, "合约乘数", positive=True)
        _number(self.taker_fee_rate, "手续费率")
        _number(self.initial_margin_ratio, "初始保证金比例")
        if (
            self.taker_fee_rate < 0 or self.taker_fee_rate > Decimal("0.1")
            or self.initial_margin_ratio < 0
            or self.initial_margin_ratio > 1
            or (self.leg.product == "futures_um") != (self.initial_margin_ratio > 0)
        ):
            raise BasketPlanError("合约费率或保证金证据无效")


@dataclass(frozen=True)
class BasketQuote:
    leg: LegId
    bid: Decimal
    ask: Decimal
    bid_size: Decimal
    ask_size: Decimal
    mark: Decimal
    ts_ns: int

    def __post_init__(self) -> None:
        if not isinstance(self.leg, LegId):
            raise BasketPlanError("组合报价缺少产品身份")
        for name in ("bid", "ask", "bid_size", "ask_size", "mark"):
            _number(getattr(self, name), name, positive=True)
        if self.bid > self.ask or type(self.ts_ns) is not int or self.ts_ns <= 0:
            raise BasketPlanError("组合历史买卖盘、标记价或时间错误")

    def evidence(self, contract: BasketContract) -> LegEvidence:
        if contract.leg != self.leg:
            raise BasketPlanError("期望合约与组合报价不一致")
        return LegEvidence(
            price=max(self.ask, self.mark), quantity_increment=contract.quantity_increment,
            multiplier=contract.multiplier, ts_ns=self.ts_ns,
        )


@dataclass(frozen=True)
class VerifiedFundingCash:
    leg: LegId
    event_id: str
    ts_ns: int
    funding_rate: Decimal
    cash_delta: Decimal
    source: str = "authenticated_venue_settlement"

    def __post_init__(self) -> None:
        if not isinstance(self.leg, LegId) or self.leg.product != "futures_um":
            raise BasketPlanError("资金费流水必须属于 UM 永续")
        if (
            not isinstance(self.event_id, str)
            or not 1 <= len(self.event_id) <= 128 or not self.event_id.isascii()
            or not all(char.isalnum() or char in "-_:." for char in self.event_id)
            or type(self.ts_ns) is not int or self.ts_ns <= 0
            or self.source != "authenticated_venue_settlement"
        ):
            raise BasketPlanError("资金费流水缺少可核验的原始身份")
        _number(self.funding_rate, "历史资金费率")
        _number(self.cash_delta, "实际资金费现金")


@dataclass(frozen=True)
class BasketFill:
    event_id: str
    leg: LegId
    ts_ns: int
    signed_quantity: Decimal
    price: Decimal
    fee: Decimal
    phase: str

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, str) or not self.event_id:
            raise BasketPlanError("模拟成交缺少审计事件号")
        if not isinstance(self.leg, LegId) or type(self.ts_ns) is not int or self.ts_ns <= 0:
            raise BasketPlanError("模拟成交的产品或时间无效")
        _number(self.signed_quantity, "模拟成交数量")
        _number(self.price, "模拟成交价格", positive=True)
        _number(self.fee, "模拟成交手续费")
        if self.signed_quantity == 0 or self.fee < 0 or not isinstance(self.phase, str):
            raise BasketPlanError("模拟成交数量、费用或阶段无效")


class BasketFinanceLedger:
    """Separate cash pools for Spot, UM margin, and Options premium/collateral.

    The ledger starts flat. Spot and option inventory are fully cash-funded,
    UM positions settle realized PnL to UM collateral, and equity marks open
    UM positions at a supplied mark price. Contract multipliers are explicit.
    """

    def __init__(
        self, contracts: tuple[BasketContract, ...],
        starting_cash: dict[str, Decimal],
    ) -> None:
        if not isinstance(contracts, tuple) or not 1 <= len(contracts) <= 64:
            raise BasketPlanError("通用研究必须提供 1~64 个有效合约")
        self.contracts = {contract.leg: contract for contract in contracts}
        if len(self.contracts) != len(contracts):
            raise BasketPlanError("通用研究合约重复")
        if len({(leg.tenant_id, leg.account_id) for leg in self.contracts}) != 1:
            raise BasketPlanError("研究资金账本不能跨账户净额")
        needed = {leg.product for leg in self.contracts}
        if not isinstance(starting_cash, dict) or set(starting_cash) != needed:
            raise BasketPlanError("缺少独立产品的初始 USDT 现金与保证金")
        for product, amount in starting_cash.items():
            _number(amount, f"{product}初始现金", positive=True)
        self.cash = dict(starting_cash)
        self.initial_cash = dict(starting_cash)
        self.positions = {leg: Decimal(0) for leg in self.contracts}
        self.average_prices = {leg: Decimal(0) for leg in self.contracts}
        self.realized = {leg: Decimal(0) for leg in self.contracts}
        self.fees = {leg: Decimal(0) for leg in self.contracts}
        self.funding = {leg: Decimal(0) for leg in self.contracts}
        self.event_ids: set[str] = set()
        self.events: list[dict] = []

    def _state(self):
        return (
            dict(self.cash), dict(self.positions), dict(self.average_prices),
            dict(self.realized), dict(self.fees), dict(self.funding),
            set(self.event_ids), list(self.events),
        )

    def _restore(self, state):
        (
            self.cash, self.positions, self.average_prices, self.realized,
            self.fees, self.funding, self.event_ids, self.events,
        ) = state

    def mark(
        self, quotes: dict[LegId, BasketQuote],
        *, max_age_ns: int, now_ns: int,
    ) -> dict:
        if not isinstance(quotes, dict) or set(quotes) != set(self.contracts):
            raise BasketPlanError("计算权益必须提供完整组合报价")
        if type(now_ns) is not int or type(max_age_ns) is not int or max_age_ns <= 0:
            raise BasketPlanError("权益时间窗口无效")
        unrealized = {}
        margin_required = Decimal(0)
        per_product = dict(self.cash)
        for leg, contract in self.contracts.items():
            quote = quotes[leg]
            if (
                not isinstance(quote, BasketQuote) or quote.leg != leg
                or not 0 <= now_ns - quote.ts_ns <= max_age_ns
            ):
                raise BasketPlanError("账户估值存在缺失或过期报价")
            position = self.positions[leg]
            unrealized[leg] = (
                position * contract.multiplier *
                (quote.mark - self.average_prices[leg])
            )
            if leg.product in {"spot", "options"}:
                per_product[leg.product] += position * quote.mark * contract.multiplier
            else:
                per_product[leg.product] += unrealized[leg]
                margin_required += (
                    abs(position) * quote.mark * contract.multiplier
                    * contract.initial_margin_ratio
                )
        if per_product.get("futures_um", Decimal(0)) < margin_required:
            raise BasketPlanError("UM 账户独立保证金不足")
        if any(value < 0 for value in self.cash.values()):
            raise BasketPlanError("独立产品现金余额不足，禁止跨产品隐式借贷")
        return {
            "ts_ns": now_ns,
            "cash": {key: format(value, "f") for key, value in sorted(self.cash.items())},
            "equity": {
                key: format(value, "f") for key, value in sorted(per_product.items())
            },
            "total_equity": format(sum(per_product.values(), Decimal(0)), "f"),
            "initial_margin_required": format(margin_required, "f"),
            "unrealized_by_leg": {
                self._leg_key(leg): format(value, "f")
                for leg, value in sorted(unrealized.items())
            },
            "realized_by_leg": {
                self._leg_key(leg): format(value, "f")
                for leg, value in sorted(self.realized.items())
            },
            "fees_by_leg": {
                self._leg_key(leg): format(value, "f")
                for leg, value in sorted(self.fees.items())
            },
            "funding_by_leg": {
                self._leg_key(leg): format(value, "f")
                for leg, value in sorted(self.funding.items())
            },
        }

    @staticmethod
    def _leg_key(leg: LegId) -> str:
        return f"{leg.tenant_id}/{leg.account_id}/{leg.product}/{leg.symbol}"

    def execute_fill(
        self, fill: BasketFill, quotes: dict[LegId, BasketQuote],
        *, max_age_ns: int,
    ) -> dict:
        if fill.event_id in self.event_ids:
            raise BasketPlanError("模拟成交身份重复")
        if fill.leg not in self.contracts:
            raise BasketPlanError("模拟成交指向未知腿")
        contract = self.contracts[fill.leg]
        if fill.signed_quantity % contract.quantity_increment:
            raise BasketPlanError("模拟成交未按原生合约步长执行")
        if fill.fee != abs(fill.signed_quantity) * fill.price * contract.multiplier * contract.taker_fee_rate:
            raise BasketPlanError("模拟手续费与合约费率不一致")
        before = self._state()
        leg, product, quantity = fill.leg, fill.leg.product, fill.signed_quantity
        old_position = self.positions[leg]
        new_position = old_position + quantity
        if product in {"spot", "options"} and new_position < 0:
            raise BasketPlanError("现货/期权研究不能生成无担保卖空")
        try:
            old_avg = self.average_prices[leg]
            if old_position == 0 or old_position * quantity > 0:
                basis = (
                    abs(old_position) * old_avg + abs(quantity) * fill.price
                ) / abs(new_position)
            elif abs(quantity) >= abs(old_position):
                basis = fill.price if new_position != 0 else Decimal(0)
            else:
                basis = old_avg
            closed = (
                min(abs(old_position), abs(quantity))
                if old_position * quantity < 0 else Decimal(0)
            )
            realized = (
                closed * (fill.price - old_avg) * contract.multiplier
                * (Decimal(1) if old_position > 0 else Decimal(-1))
            )
            if product in {"spot", "options"}:
                self.cash[product] -= quantity * fill.price * contract.multiplier
            else:
                self.cash[product] += realized
            self.cash[product] -= fill.fee
            self.positions[leg] = new_position
            self.average_prices[leg] = basis
            self.realized[leg] += realized
            self.fees[leg] += fill.fee
            self.event_ids.add(fill.event_id)
            self.events.append({
                "kind": "simulation_fill", "event_id": fill.event_id,
                "ts_ns": fill.ts_ns, "leg": self._leg_key(leg),
                "quantity": format(quantity, "f"), "price": format(fill.price, "f"),
                "fee": format(fill.fee, "f"), "phase": fill.phase,
                "executable": False,
            })
            return self.mark(quotes, now_ns=fill.ts_ns, max_age_ns=max_age_ns)
        except Exception:
            self._restore(before)
            raise

    def apply_funding(
        self, record: VerifiedFundingCash,
        quotes: dict[LegId, BasketQuote],
        *, max_age_ns: int,
    ) -> dict:
        if (
            not isinstance(record, VerifiedFundingCash)
            or record.leg not in self.contracts
            or record.event_id in self.event_ids
        ):
            raise BasketPlanError("资金费事件缺失、重复或不属于此组合")
        contract = self.contracts[record.leg]
        quote = quotes[record.leg]
        expected = (
            -self.positions[record.leg] * quote.mark * contract.multiplier
            * record.funding_rate
        )
        if record.cash_delta != expected:
            raise BasketPlanError("实际资金费记录与持仓、价格、费率不守恒")
        before = self._state()
        try:
            self.cash["futures_um"] += record.cash_delta
            self.funding[record.leg] += record.cash_delta
            self.event_ids.add(record.event_id)
            self.events.append({
                "kind": "verified_funding", "event_id": record.event_id,
                "ts_ns": record.ts_ns, "leg": self._leg_key(record.leg),
                "rate": format(record.funding_rate, "f"),
                "cash_delta": format(record.cash_delta, "f"),
                "source": record.source,
            })
            return self.mark(quotes, now_ns=record.ts_ns, max_age_ns=max_age_ns)
        except Exception:
            self._restore(before)
            raise
