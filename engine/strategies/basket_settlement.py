"""Strict, offline reconciliation of recorded USDT option-expiry cash flows.

Never estimate exercise cash from candle closes or assume a Binance order fill.
A separate authenticated adapter must supply the actual exercise/settlement
records, native contract specifications and a matching account cash snapshot.
"""
from dataclasses import dataclass
from decimal import Decimal

from engine.strategies.basket_targets import BasketPlanError, LegId


class BasketSettlementError(BasketPlanError):
    """Settlement evidence is missing, inconsistent, or out of scope."""


@dataclass(frozen=True)
class ExpiringOptionContract:
    leg: LegId
    expiry_ns: int
    strike: Decimal
    unit: Decimal
    quantity_increment: Decimal
    settle_asset: str = "USDT"


@dataclass(frozen=True)
class VerifiedOptionSettlement:
    leg: LegId
    settlement_id: str
    settled_at_ns: int
    settlement_index_price: Decimal
    settled_quantity: Decimal
    gross_credit: Decimal
    exercise_fee: Decimal
    net_credit: Decimal
    record_source: str = "authenticated_exchange_exercise_record"


@dataclass(frozen=True)
class BasketExpiryReconciliation:
    expiry_ns: int
    settlement_index_price: Decimal
    gross_option_credit: Decimal
    exercise_fees: Decimal
    net_option_cash_credit: Decimal
    account_cash_before: Decimal
    account_cash_after: Decimal
    event_ids: tuple[str, ...]
    verified: bool = True


def _money(value: Decimal, name: str, *, zero: bool = True) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise BasketSettlementError(f"{name}必须为有限 Decimal")
    if value < 0 or (not zero and value == 0):
        raise BasketSettlementError(f"{name}数值无效")
    return value


def reconcile_verified_option_expiry(
    contracts: tuple[ExpiringOptionContract, ...],
    positions_before: dict[LegId, Decimal],
    records: tuple[VerifiedOptionSettlement, ...],
    *,
    perpetual_position_at_expiry: Decimal,
    account_cash_before: Decimal,
    account_cash_after: Decimal,
    now_ns: int,
) -> BasketExpiryReconciliation:
    """Require complete exercise history AND reconciled cash before reporting.

    This is the final *cash accounting* check, not the source of settlement
    prices, fees, assignment or records. Callers MUST authenticate their
    exchange source and prove absence of unrelated account cash movements.
    """
    if not isinstance(contracts, tuple) or not 1 <= len(contracts) <= 16:
        raise BasketSettlementError("到期期权合约集合无效")
    if not isinstance(records, tuple) or len(records) != len(contracts):
        raise BasketSettlementError("没有完整的到期行权和结算记录")
    if type(now_ns) is not int or now_ns <= 0:
        raise BasketSettlementError("结算核对时间无效")
    if (
        not isinstance(perpetual_position_at_expiry, Decimal)
        or not perpetual_position_at_expiry.is_finite()
        or perpetual_position_at_expiry != 0
    ):
        raise BasketSettlementError("期权保护到期前必须先归零永续风险敞口")
    _money(account_cash_before, "期权账户结算前现金")
    _money(account_cash_after, "期权账户结算后现金")
    if not isinstance(positions_before, dict):
        raise BasketSettlementError("缺少完整期权到期前仓位")
    checked: dict[LegId, ExpiringOptionContract] = {}
    expiry = None
    owner = None
    underlying_expiry_code = None
    for contract in contracts:
        if not isinstance(contract, ExpiringOptionContract):
            raise BasketSettlementError("期权合约属性无效")
        leg = contract.leg
        if not isinstance(leg, LegId) or leg.product != "options":
            raise BasketSettlementError("到期核对仅允许期权产品")
        parts = leg.symbol.split("-")
        if len(parts) != 4 or parts[3] not in {"P", "C"}:
            raise BasketSettlementError("无效期权行权价和看涨看跌身份")
        try:
            strike_symbol = Decimal(parts[2])
        except (ValueError, ArithmeticError) as exc:
            raise BasketSettlementError("期权合约行权价无效") from exc
        if (
            leg in checked
            or type(contract.expiry_ns) is not int
            or contract.expiry_ns <= 0
            or contract.expiry_ns > now_ns
            or contract.settle_asset != "USDT"
            or strike_symbol != _money(contract.strike, "期权行权价", zero=False)
        ):
            raise BasketSettlementError("期权合约到期和结算币种证据无效")
        _money(contract.unit, "期权合约乘数", zero=False)
        _money(contract.quantity_increment, "期权数量步长", zero=False)
        if owner is None:
            owner = (leg.tenant_id, leg.account_id)
            expiry = contract.expiry_ns
            underlying_expiry_code = (parts[0], parts[1])
        if (
            owner != (leg.tenant_id, leg.account_id)
            or expiry != contract.expiry_ns
            or underlying_expiry_code != (parts[0], parts[1])
        ):
            raise BasketSettlementError("多个账户、标的或不同到期日不能合并核算")
        checked[leg] = contract
    if set(positions_before) != set(checked):
        raise BasketSettlementError("缺少或多出期权到期前仓位")
    for leg, quantity in positions_before.items():
        if _money(quantity, "期权结算前多头数量") % checked[leg].quantity_increment:
            raise BasketSettlementError("期权到期数量不满足合约最小步长")
    if not all(isinstance(row, VerifiedOptionSettlement) for row in records):
        raise BasketSettlementError("到期结算记录类型无效")
    if {row.leg for row in records} != set(checked):
        raise BasketSettlementError("到期结算记录包含错误产品或合约")
    identifiers = [row.settlement_id for row in records]
    if any(
        not isinstance(identifier, str)
        or not 1 <= len(identifier) <= 128
        or not identifier.isascii()
        or not all(char.isalnum() or char in "._:-" for char in identifier)
        for identifier in identifiers
    ) or len(identifiers) != len(set(identifiers)):
        raise BasketSettlementError("到期结算身份缺失或重复")
    gross = Decimal(0)
    fees = Decimal(0)
    credit = Decimal(0)
    common_index_price = None
    for row in records:
        contract = checked[row.leg]
        if (
            row.record_source != "authenticated_exchange_exercise_record"
            or type(row.settled_at_ns) is not int
            or not contract.expiry_ns <= row.settled_at_ns <= now_ns
            or row.settled_quantity != positions_before[row.leg]
        ):
            raise BasketSettlementError("结算来源、时间或数量未能核对")
        index_price = _money(row.settlement_index_price, "到期交割指数价格", zero=False)
        if common_index_price is None:
            common_index_price = index_price
        elif common_index_price != index_price:
            raise BasketSettlementError("同到期、同标的期权指数交割价矛盾")
        payoff = (
            max(Decimal(0), index_price - contract.strike)
            if row.leg.symbol.endswith("-C")
            else max(Decimal(0), contract.strike - index_price)
        ) * contract.unit * row.settled_quantity
        if _money(row.gross_credit, "实际行权总入账") != payoff:
            raise BasketSettlementError("交易所行权总额与合约面值不一致")
        fee = _money(row.exercise_fee, "实际行权费用")
        if fee > payoff:
            raise BasketSettlementError("行权费用不能高于总行权收入")
        if _money(row.net_credit, "实际结算净入账") != payoff - fee:
            raise BasketSettlementError("实际行权费用与净结算不一致")
        gross += payoff
        fees += fee
        credit += row.net_credit
    if account_cash_before + credit != account_cash_after:
        raise BasketSettlementError("期权账户现金前后余额无法与结算流水核对")
    return BasketExpiryReconciliation(
        expiry_ns=expiry,
        settlement_index_price=common_index_price,
        gross_option_credit=gross,
        exercise_fees=fees,
        net_option_cash_credit=credit,
        account_cash_before=account_cash_before,
        account_cash_after=account_cash_after,
        event_ids=tuple(sorted(identifiers)),
    )
