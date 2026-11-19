"""Development-only multi-symbol portfolio replay on ONE native BacktestEngine.

This intentionally reuses Nautilus' multi-venue accounts, bar replay, matching
and order events. SeeAlpha only aggregates authorized member targets and checks
whole-portfolio risk. It neither connects to a live execution client nor treats
orders across venues as atomic.
"""
from contextlib import ExitStack
from decimal import Decimal

from nautilus_trader.backtest import BacktestEngine
from nautilus_trader.model import AccountType, Bar, BarType, OmsType, OrderSide, TimeInForce
from nautilus_trader.trading import Strategy

from engine.runtime import EngineConflict
from engine.strategies.backtest_engine import backtest_engine_config
from engine.strategies.basket_research_session import BasketResearchSession
from engine.strategies.basket_targets import BasketPlanError, LegEvidence
from engine.strategies.carry_simulation import (
    _balance,
    _decimal,
    _money,
    _perp_unrealized,
    _position,
    _sim_bar,
    _sim_instrument,
)
from engine.strategies.issued_basket import issued_basket_members
from engine.strategies.strategy_config import StrategyConfig
from nautilus_support import instrument_fee_model

from nautilus_trader.model import CryptoPerpetual, CurrencyPair, Venue

_VENUES = {"spot": Venue("SIM_SPOT"), "futures_um": Venue("SIM_UM")}
_STEP_NS = {"1m": 60_000_000_000, "5m": 300_000_000_000}


class NativePortfolioCoordinator(Strategy):
    """Single native Strategy owns all account-level simulated orders."""

    def __init__(self, config):
        super().__init__(config)
        self.contracts = {}
        self.bars = {}
        self.pending = set()
        self.remaining = []
        self.target = None
        self.failure = None
        self.orders = []
        self.fills = []
        self._seen = set()
        self._seen_ns = None

    def bind(self, contracts, bars):
        self.contracts = dict(contracts)
        self.bars = dict(bars)

    def on_start(self):
        for bar_type in self.bars.values():
            self.subscribe_bars(bar_type)

    def plan(self, planned):
        target = dict(planned.targets)
        if self.pending or self.remaining:
            if self.target != target:
                raise EngineConflict("前一个组合阶段尚未完全成交，不允许更改组合目标。")
            return
        self.target = target
        self.remaining = list(planned.phases)

    def on_bar(self, bar):
        if bar.bar_type not in self.bars.values():
            return
        stamp = int(bar.ts_event)
        if stamp != self._seen_ns:
            self._seen_ns, self._seen = stamp, set()
        self._seen.add(bar.bar_type)
        if len(self._seen) == len(self.bars) and not self.pending:
            self._advance()

    def _advance(self):
        if self.failure or self.pending or not self.remaining:
            return
        phase = self.remaining.pop(0)
        for change in phase.changes:
            instrument = self.contracts[change.leg]
            delta = change.quantity_delta
            if not delta:
                continue
            order = self.order_factory.market(
                instrument_id=instrument.id,
                order_side=OrderSide.BUY if delta > 0 else OrderSide.SELL,
                quantity=instrument.make_qty(abs(delta)),
                time_in_force=TimeInForce.GTC,
                tags=["multi-asset-research", phase.name],
            )
            identifier = str(order.client_order_id)
            self.pending.add(identifier)
            self.orders.append({
                "client_order_id": identifier, "phase": phase.name,
                "product": change.leg.product, "symbol": change.leg.symbol,
                "side": "BUY" if delta > 0 else "SELL",
                "quantity": format(abs(delta), "f"),
            })
            self.submit_order(order)

    def on_order_filled(self, event):
        identifier = str(event.client_order_id)
        self.fills.append({
            "client_order_id": identifier, "instrument_id": str(event.instrument_id),
            "quantity": format(event.last_qty.as_decimal(), "f"),
            "price": format(event.last_px.as_decimal(), "f"),
            "commission": format(event.commission.as_decimal(), "f"),
            "currency": event.commission.currency.code,
        })
        order = self.cache.order(event.client_order_id)
        if order is not None and order.is_closed:
            self.pending.discard(identifier)
        if not self.pending:
            self._advance()

    def on_order_denied(self, event):
        self.failure = "模拟订单被拒绝：" + str(event.client_order_id)

    def on_order_rejected(self, event):
        self.failure = "模拟交易所拒单：" + str(event.client_order_id)

    def on_order_canceled(self, event):
        self.failure = "组合订单被取消：" + str(event.client_order_id)

    def on_order_expired(self, event):
        self.failure = "组合订单过期：" + str(event.client_order_id)


def _portfolio_equity(engine, contracts, closing):
    by_product = {}
    for product, venue in _VENUES.items():
        matching = [leg for leg in contracts if leg.product == product]
        if not matching:
            continue
        usdt = next(iter(contracts.values())).quote_currency
        amount = _balance(engine, venue, usdt)
        if product == "spot":
            amount += sum(
                (_balance(engine, venue, contracts[leg].base_currency) * closing[leg]
                 for leg in matching), Decimal(0),
            )
        else:
            amount += sum(
                (_perp_unrealized(engine, contracts[leg])
                 for leg in matching), Decimal(0),
            )
        by_product[product] = amount
    return by_product


def run_native_generic_portfolio(
    loads, frames, original_contracts, *,
    interval, starting_spot, starting_perpetual, max_gross_notional,
):
    """Replay authorized basket publishers over synchronized multi-market bars.

    frames is a bounded iterator of {LegId: native Bar}, validated by runtime.
    The first complete snapshot warms the strategy; subsequent target decisions
    use PREVIOUS closes (no current-bar look-ahead). Missing legs fail closed.
    Options remain on the separate quote-verified protected replay.
    """
    if not loads or len(loads) > 16 or not original_contracts or len(original_contracts) > 8:
        raise EngineConflict("通用回测最多允许16个策略成员和8条市场腿。")
    if interval not in _STEP_NS:
        raise EngineConflict("多标的回测周期无效。")
    legs = set(original_contracts)
    if len({(x.tenant_id, x.account_id) for x in legs}) != 1:
        raise EngineConflict("通用回测不得跨租户账户合并。")
    if any(leg.product not in _VENUES for leg in legs):
        raise EngineConflict("期权通用回测需要独立的完整历史报价及结算证据。")
    if any(
        str(load["tenant_id"]) != next(iter(legs)).tenant_id
        or str(load["account_id"]) != next(iter(legs)).account_id
        or load["grant_purpose"] != "development"
        for load in loads
    ):
        raise EngineConflict("组合成员缺少同账户开发授权。")

    cap = _decimal(max_gross_notional, "组合总毛名义预算", positive=True)
    instruments = {}
    bar_types = {}
    for leg, source in original_contracts.items():
        if leg.product == "spot" and not isinstance(source, CurrencyPair):
            raise EngineConflict("现货腿缺少真实原生合约。")
        if leg.product == "futures_um" and not isinstance(source, CryptoPerpetual):
            raise EngineConflict("永续腿缺少真实原生合约。")
        if source.quote_currency.code != "USDT":
            raise EngineConflict("目前通用组合仅允许 USDT 计价产品。")
        contract = _sim_instrument(source, _VENUES[leg.product])
        instruments[leg] = contract
        step = 1 if interval == "1m" else 5
        bar_types[leg] = BarType.from_str(f"{contract.id}-{step}-MINUTE-LAST-EXTERNAL")

    # A single engine owns all assets; two simulated venues retain independent
    # CASH/MARGIN balances. No per-leg engines and no summation of fake fills.
    engine = BacktestEngine(backtest_engine_config())
    resources = ExitStack()
    coordinator = NativePortfolioCoordinator(StrategyConfig(order_id_tag="MULTI"))
    coordinator.bind(instruments, bar_types)
    previous = None
    previous_stamp = None
    curve, targets, count = [], [], 0
    try:
        for product, venue in _VENUES.items():
            matching = [leg for leg in legs if leg.product == product]
            if not matching:
                continue
            money = _money(
                starting_spot if product == "spot" else starting_perpetual,
                "USDT",
            )
            engine.add_venue(
                venue, OmsType.NETTING,
                AccountType.CASH if product == "spot" else AccountType.MARGIN,
                [money], default_leverage=Decimal(1 if product == "spot" else 5),
                fee_model=instrument_fee_model(instruments[matching[0]]),
            )
        for contract in instruments.values():
            engine.add_instrument(contract)
        base = next((instruments[leg] for leg in sorted(legs) if leg.product == "futures_um"),
                    instruments[sorted(legs)[0]])
        base_bar = next(bar_types[leg] for leg in sorted(legs) if instruments[leg] == base)
        members = resources.enter_context(issued_basket_members(loads, base, base_bar))
        engine.add_strategy(coordinator)
        for frame in frames:
            if set(frame) != legs:
                raise EngineConflict("跨标的历史帧缺少市场腿。")
            stamps = {int(row.ts_event) for row in frame.values()}
            if len(stamps) != 1:
                raise EngineConflict("多标的行情时间未对齐。")
            stamp = stamps.pop()
            if previous_stamp is not None and stamp - previous_stamp != _STEP_NS[interval]:
                raise EngineConflict("多标的历史帧存在中间缺口。")
            for leg, row in frame.items():
                if row.bar_type.instrument_id != original_contracts[leg].id:
                    raise EngineConflict("组合历史帧合约身份不一致。")
                if str(row.bar_type) != str(
                    BarType.from_str(
                        f"{original_contracts[leg].id}-{1 if interval == '1m' else 5}-MINUTE-LAST-EXTERNAL"
                    )
                ):
                    raise EngineConflict("组合历史帧周期不匹配。")
            if previous is not None:
                positions = {leg: _position(engine, instruments[leg]) for leg in legs}
                evidence = {
                    leg: LegEvidence(
                        previous[leg].close.as_decimal(),
                        instruments[leg].size_increment.as_decimal(),
                        Decimal(str(instruments[leg].multiplier)) if hasattr(instruments[leg], "multiplier") else Decimal(1),
                        previous_stamp,
                    )
                    for leg in legs
                }
                try:
                    session = BasketResearchSession(
                        tenant_id=next(iter(legs)).tenant_id,
                        account_id=next(iter(legs)).account_id,
                        policy="generic", positions=positions, evidence=evidence,
                        now_ns=stamp, max_age_ns=_STEP_NS[interval] * 2,
                        max_gross_notional=cap,
                    )
                    snapshot = {
                        "now_ns": stamp,
                        "markets": {
                            f"{leg.product}:{leg.symbol}": {
                                "price": format(previous[leg].close.as_decimal(), "f"),
                                "ts_ns": previous_stamp,
                            }
                            for leg in sorted(legs)
                        },
                        "positions": {
                            f"{leg.product}:{leg.symbol}": format(positions[leg], "f")
                            for leg in sorted(legs)
                        },
                    }
                    for load in loads:
                        member_id = str(load["issuance_id"])
                        members[member_id].propose(session.member(member_id), snapshot)
                    planned = session.plan().plan
                except BasketPlanError as exc:
                    raise EngineConflict(str(exc)) from exc
                coordinator.plan(planned)
                if not targets or targets[-1]["targets"] != [
                    {"product": leg.product, "symbol": leg.symbol,
                     "quantity": format(qty, "f")}
                    for leg, qty in planned.targets
                ]:
                    targets.append({
                        "ts_ns": stamp, "targets": [
                            {"product": leg.product, "symbol": leg.symbol,
                             "quantity": format(qty, "f")}
                            for leg, qty in planned.targets
                        ],
                    })
            native_bars = [_sim_bar(frame[leg], instruments[leg]) for leg in sorted(legs)]
            engine.add_data(native_bars, sort=True)
            engine.run(streaming=True)
            engine.clear_data()
            if coordinator.failure:
                raise EngineConflict(coordinator.failure)
            closing = {leg: frame[leg].close.as_decimal() for leg in legs}
            equities = _portfolio_equity(engine, instruments, closing)
            curve.append({
                "ts_ns": stamp,
                "product_equity": {k: format(v, "f") for k, v in equities.items()},
                "total_equity": format(sum(equities.values(), Decimal(0)), "f"),
                "positions": [
                    {"product": leg.product, "symbol": leg.symbol,
                     "quantity": format(_position(engine, instruments[leg]), "f")}
                    for leg in sorted(legs)
                ],
            })
            previous, previous_stamp = frame, stamp
            count += 1
            if count > 20_000:
                raise EngineConflict("通用组合历史回测最多20000个同步时间点。")
        if count < 2:
            raise EngineConflict("通用组合至少需要两组完整历史K线。")
        engine.end()
        if coordinator.failure or coordinator.pending or coordinator.remaining:
            raise EngineConflict("组合模拟结束时仍有未确认的订单或阶段。")
        if coordinator.target is not None and any(
            _position(engine, instruments[leg]) != quantity
            for leg, quantity in coordinator.target.items()
        ):
            raise EngineConflict("组合目标与原生模拟最终仓位不一致。")
        return {
            "mode": "native_generic_development_only",
            "tenant_id": next(iter(legs)).tenant_id,
            "account_id": next(iter(legs)).account_id,
            "bar_count": count,
            "symbols": sorted({leg.symbol for leg in legs}),
            "venue_accounts": {p: "CASH" if p == "spot" else "MARGIN"
                               for p in _VENUES if any(leg.product == p for leg in legs)},
            "orders": coordinator.orders, "fills": coordinator.fills,
            "target_events": targets[-2000:],
            "equity_curve": curve[-2000:],
            "starting_balance": format(
                sum((_decimal(starting_spot if p == "spot" else starting_perpetual, "起始资产")
                     for p in _VENUES if any(leg.product == p for leg in legs)), Decimal(0)),
                "f",
            ),
            "ending_balance": curve[-1]["total_equity"],
            "funding_accounting": "NOT_INCLUDED_requires_verified_settlement_for_strategy_returns",
        }
    finally:
        resources.close()
        engine.dispose()
