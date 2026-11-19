"""Offline native Spot + USD-M funding-carry basket simulation.

Only already-issued development strategy parameters reach this runner. Two
synthetic *simulation* venues maintain isolated CASH and MARGIN accounts. The
matching, commissions, order status and funding settlements remain native
NautilusTrader behavior, never an approximation from the bar close.
"""
from decimal import Decimal, InvalidOperation

from nautilus_trader.backtest import BacktestEngine
from nautilus_trader.model import (
    AccountType, Bar, BarType, CryptoPerpetual, CurrencyPair, FundingRateUpdate,
    InstrumentId, Money, OmsType, OrderSide, TimeInForce, Venue,
)
from nautilus_trader.trading import Strategy

from engine.runtime import EngineConflict
from engine.strategies.backtest_engine import backtest_engine_config
from engine.strategies.basket_targets import (
    BasketPlanError, LegEvidence, LegId, MemberLegTarget, plan_basket,
)
from engine.strategies.strategy_config import StrategyConfig
from nautilus_support import instrument_fee_model

_SPOT_VENUE = Venue("SIM_SPOT")
_PERP_VENUE = Venue("SIM_UM")


def _money(value, currency):
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise EngineConflict("组合初始资金无效。") from exc
    if not amount.is_finite() or amount <= 0:
        raise EngineConflict("组合初始资金必须为正数。")
    return Money.from_str(f"{format(amount, 'f')} {currency}")


def _decimal(value, label, *, positive=False):
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise EngineConflict(f"{label}无效。") from exc
    if isinstance(value, bool) or not number.is_finite() or (positive and number <= 0):
        raise EngineConflict(f"{label}无效。")
    return number


def _sim_instrument(original, venue):
    """Clone a native contract's complete definition, changing only its venue ID."""
    if not isinstance(original, (CurrencyPair, CryptoPerpetual)):
        raise EngineConflict("组合回测合约类型无效。")
    content = type(original).to_dict(original)
    content["id"] = f"{original.id.symbol}.{venue}"
    try:
        instrument = type(original).from_dict(content)
    except (TypeError, ValueError) as exc:
        raise EngineConflict("交易合约无法转换为独立模拟场所。") from exc
    if instrument.id.venue != venue or instrument.quote_currency != original.quote_currency:
        raise EngineConflict("模拟合约身份或报价币种不匹配。")
    return instrument


def _sim_bar(original, instrument):
    kind = BarType.from_str(str(original.bar_type).replace(
        f"{original.bar_type.instrument_id}", str(instrument.id), 1,
    ))
    return Bar(
        kind, instrument.make_price(original.open.as_decimal()),
        instrument.make_price(original.high.as_decimal()),
        instrument.make_price(original.low.as_decimal()),
        instrument.make_price(original.close.as_decimal()),
        instrument.make_qty(original.volume.as_decimal()),
        original.ts_event, original.ts_init,
    )


def _sim_funding(original, instrument):
    if original.next_funding_ns != original.ts_event:
        raise EngineConflict("缺少经过验证的资金费结算时间，不可模拟到账。")
    return FundingRateUpdate(
        instrument.id, original.rate, original.ts_event, original.ts_init,
        next_funding_ns=original.next_funding_ns,
    )


def _position(engine, instrument):
    value = engine.portfolio.net_position(instrument.id)
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _balance(engine, venue, currency):
    balance = engine.portfolio.account(venue).balance_total(currency)
    return balance.as_decimal() if balance is not None else Decimal(0)


def _perp_unrealized(engine, instrument):
    pnls = engine.portfolio.unrealized_pnls(instrument.id.venue) or {}
    pnl = pnls.get(instrument.quote_currency)
    return pnl.as_decimal() if pnl is not None else Decimal(0)


class _NativeCarryCoordinator(Strategy):
    """One component owns both legs. Never submit from an issued demo package."""

    def __init__(self, spot, perp, spot_bar, perp_bar):
        super().__init__(StrategyConfig(order_id_tag="CARRY", close_positions_on_stop=False))
        self.spot = spot
        self.perp = perp
        self.spot_bar = spot_bar
        self.perp_bar = perp_bar
        self.desired = Decimal(0)
        self.current_bars = {}
        self.last_pair_ts = None
        self.pending = set()
        self.rejected = []
        self.fills = []
        self.orders = []

    def on_start(self):
        self.subscribe_bars(self.spot_bar)
        self.subscribe_bars(self.perp_bar)

    def on_bar(self, bar):
        if bar.bar_type not in (self.spot_bar, self.perp_bar):
            return
        self.current_bars[bar.bar_type] = bar
        first = self.current_bars.get(self.spot_bar)
        second = self.current_bars.get(self.perp_bar)
        if first is None or second is None or first.ts_event != second.ts_event:
            return
        if self.last_pair_ts == first.ts_event:
            return
        self.last_pair_ts = first.ts_event
        if self.rejected or self.pending:
            return
        changes = (
            (self.spot, self.desired - _position(self, self.spot)),
            (self.perp, -self.desired - _position(self, self.perp)),
        )
        for instrument, delta in changes:
            if not delta:
                continue
            increment = instrument.size_increment.as_decimal()
            if delta % increment:
                self.rejected.append("目标数量与合约步长不匹配")
                return
            order = self.order_factory.market(
                instrument_id=instrument.id,
                order_side=OrderSide.BUY if delta > 0 else OrderSide.SELL,
                quantity=instrument.make_qty(abs(delta)),
                time_in_force=TimeInForce.GTC,
                tags=["basket-carry-native-backtest"],
            )
            self.pending.add(str(order.client_order_id))
            self.orders.append({
                "venue": str(instrument.id.venue), "client_order_id": str(order.client_order_id),
                "side": "BUY" if delta > 0 else "SELL", "quantity": format(abs(delta), "f"),
            })
            self.submit_order(order)

    def on_order_filled(self, event):
        order = self.cache.order(event.client_order_id)
        if order is not None and order.is_closed:
            self.pending.discard(str(event.client_order_id))
        self.fills.append({
            "client_order_id": str(event.client_order_id),
            "instrument_id": str(event.instrument_id),
            "quantity": format(event.last_qty.as_decimal(), "f"),
            "price": format(event.last_px.as_decimal(), "f"),
            "commission": format(event.commission.as_decimal(), "f"),
            "currency": event.commission.currency.code,
        })

    def on_order_rejected(self, event):
        self.rejected.append(f"REJECTED {event.client_order_id}")

    def on_order_denied(self, event):
        self.rejected.append(f"DENIED {event.client_order_id}")

    def on_order_cancel_rejected(self, event):
        self.rejected.append(f"CANCEL_REJECTED {event.client_order_id}")


def _equity(engine, spot, perp, spot_price):
    spot_account = _balance(engine, spot.id.venue, spot.quote_currency)
    spot_base = _balance(engine, spot.id.venue, spot.base_currency)
    spot_value = spot_account + spot_base * spot_price
    derivatives = _balance(engine, perp.id.venue, perp.quote_currency)
    derivatives += _perp_unrealized(engine, perp)
    return spot_value, derivatives


def _target(loads, *, last_bar, last_rate, now_ns):
    symbol = loads[0]["parameters"]["symbol"]
    quantity = Decimal(0)
    member_targets = []
    leg_spot = LegId(str(loads[0]["tenant_id"]), str(loads[0]["account_id"]), "spot", symbol)
    leg_perp = LegId(str(loads[0]["tenant_id"]), str(loads[0]["account_id"]), "futures_um", symbol)
    if last_bar is not None and last_rate is not None:
        sprice, pprice, bar_ns = last_bar
        basis = abs((pprice / sprice - 1) * 10_000)
        for load in loads:
            params = load["parameters"]
            age = int(params["max_age_seconds"]) * 1_000_000_000
            if (
                0 <= now_ns - bar_ns <= age
                and 0 <= now_ns - last_rate.ts_event <= age
                and last_rate.rate >= _decimal(params["min_funding_rate"], "最低费率")
                and basis <= _decimal(params["max_basis_bps"], "最大基差")
            ):
                proposed = min(
                    _decimal(params["allocation"], "分配数量", positive=True),
                    _decimal(params["risk_limit"], "风险上限", positive=True),
                )
            else:
                proposed = Decimal(0)
            member_targets.extend((
                MemberLegTarget(str(load["issuance_id"]), leg_spot, proposed),
                MemberLegTarget(str(load["issuance_id"]), leg_perp, -proposed),
            ))
            quantity += proposed
    else:
        for load in loads:
            member_targets.extend((
                MemberLegTarget(str(load["issuance_id"]), leg_spot, Decimal(0)),
                MemberLegTarget(str(load["issuance_id"]), leg_perp, Decimal(0)),
            ))
    return quantity, member_targets, leg_spot, leg_perp


def run_native_carry(loads, events, spot_contract, perpetual_contract, *,
                     interval, starting_spot, starting_perpetual, max_gross_notional):
    """Simulate two native venues on one backtest clock from verified history.

    A failed/partial hedge aborts the *research result* instead of quietly
    continuing; this is not real-money compensation or atomic execution.
    """
    if not loads or len(loads) > 16:
        raise EngineConflict("组合回测要求 1 到 16 个已授权资金费策略。")
    if any(
        row["code_key"] != "demo_funding_carry"
        or row["grant_purpose"] != "development"
        or row["tenant_id"] != loads[0]["tenant_id"]
        or row["account_id"] != loads[0]["account_id"]
        or row["parameters"]["symbol"] != loads[0]["parameters"]["symbol"]
        for row in loads
    ):
        raise EngineConflict("组合回测只允许同账户下发的 Funding Carry 开发策略。")
    if interval not in {"1m", "5m"}:
        raise EngineConflict("组合回测只允许 1m/5m 历史。")
    interval_size = 1 if interval == "1m" else 5
    spot = _sim_instrument(spot_contract, _SPOT_VENUE)
    perp = _sim_instrument(perpetual_contract, _PERP_VENUE)
    if not isinstance(spot, CurrencyPair) or not isinstance(perp, CryptoPerpetual):
        raise EngineConflict("组合回测需要原生现货和 USD-M 永续合约。")
    if spot.base_currency != perp.base_currency or spot.quote_currency != perp.quote_currency:
        raise EngineConflict("现货与永续计价资产不匹配。")
    if spot.quote_currency.code != "USDT":
        raise EngineConflict("此阶段只支持 USDT 组合回测。")
    cap = _decimal(max_gross_notional, "总名义风险预算", positive=True)
    start_spot = _money(starting_spot, "USDT")
    start_perp = _money(starting_perpetual, "USDT")
    engine = BacktestEngine(backtest_engine_config())
    coordinator = None
    try:
        engine.add_venue(
            spot.id.venue, OmsType.NETTING, AccountType.CASH, [start_spot],
            fee_model=instrument_fee_model(spot), default_leverage=Decimal(1),
        )
        engine.add_venue(
            perp.id.venue, OmsType.NETTING, AccountType.MARGIN, [start_perp],
            fee_model=instrument_fee_model(perp), default_leverage=Decimal(5),
        )
        engine.add_instrument(spot)
        engine.add_instrument(perp)
        coordinator = _NativeCarryCoordinator(
            spot, perp,
            BarType.from_str(f"{spot.id}-{interval_size}-MINUTE-LAST-EXTERNAL"),
            BarType.from_str(f"{perp.id}-{interval_size}-MINUTE-LAST-EXTERNAL"),
        )
        engine.add_strategy(coordinator)
        previous_bar = None
        previous_rate = None
        curve = []
        funding = []
        active = 0
        for event in events:
            if event.kind == "funding":
                converted = _sim_funding(event.funding, perp)
                engine.add_data([converted])
                engine.run(streaming=True)
                engine.clear_data()
                previous_rate = event.funding
                funding.append({
                    "ts_ns": event.ts_ns, "rate": format(event.funding.rate, "f"),
                })
                if coordinator.rejected:
                    raise EngineConflict("组合订单被模拟场所拒绝。")
                if curve:
                    left, right = _equity(engine, spot, perp, previous_bar[0])
                    curve[-1]["spot_equity"] = format(left, "f")
                    curve[-1]["perpetual_equity"] = format(right, "f")
                    curve[-1]["total_equity"] = format(left + right, "f")
                continue
            if event.kind != "bars" or event.spot is None or event.perpetual is None:
                raise EngineConflict("组合历史包含无效记录。")
            sbar, pbar = _sim_bar(event.spot, spot), _sim_bar(event.perpetual, perp)
            desired, targets, leg_spot, leg_perp = _target(
                loads, last_bar=previous_bar, last_rate=previous_rate, now_ns=event.ts_ns,
            )
            spot_pos = _position(engine, spot) if active else Decimal(0)
            perp_pos = _position(engine, perp) if active else Decimal(0)
            spot_price = (previous_bar[0] if previous_bar else sbar.close.as_decimal())
            perp_price = (previous_bar[1] if previous_bar else pbar.close.as_decimal())
            quote_stamp = previous_bar[2] if previous_bar else event.ts_ns
            try:
                plan_basket(
                    targets, {leg_spot: spot_pos, leg_perp: perp_pos},
                    {
                        leg_spot: LegEvidence(
                            spot_price, spot.size_increment.as_decimal(), Decimal(1), quote_stamp,
                        ),
                        leg_perp: LegEvidence(
                            perp_price, perp.size_increment.as_decimal(), Decimal(1), quote_stamp,
                        ),
                    },
                    now_ns=event.ts_ns,
                    max_age_ns=max(300_000_000_000, event.ts_ns - quote_stamp),
                    max_gross_notional=cap,
                    policy="funding_carry",
                    max_leg_drift=min(
                        _decimal(x["parameters"]["max_leg_drift"], "腿间偏差")
                        for x in loads
                    ),
                )
            except BasketPlanError as exc:
                raise EngineConflict(str(exc)) from exc
            coordinator.desired = desired
            engine.add_data([sbar, pbar], sort=True)
            engine.run(streaming=True)
            engine.clear_data()
            if coordinator.rejected:
                raise EngineConflict("模拟交易所拒绝了一条组合腿；组合结果已中止。")
            spot_pos, perp_pos = _position(engine, spot), _position(engine, perp)
            drift = abs(spot_pos + perp_pos)
            max_drift = min(
                _decimal(x["parameters"]["max_leg_drift"], "腿间偏差") for x in loads
            )
            if drift > max_drift:
                raise EngineConflict("组合两腿发生未对冲成交；停止回测，不重试。")
            value_spot, value_perp = _equity(engine, spot, perp, sbar.close.as_decimal())
            curve.append({
                "ts_ns": event.ts_ns,
                "spot_equity": format(value_spot, "f"),
                "perpetual_equity": format(value_perp, "f"),
                "total_equity": format(value_spot + value_perp, "f"),
                "spot_quantity": format(spot_pos, "f"),
                "perpetual_quantity": format(perp_pos, "f"),
                "leg_drift": format(drift, "f"),
            })
            previous_bar = (sbar.close.as_decimal(), pbar.close.as_decimal(), event.ts_ns)
            active += 1
            if active > 20_000:
                raise EngineConflict("一次跨产品回测最多允许 20000 根对齐 K 线。")
        if not curve:
            raise EngineConflict("组合回测没有有效历史数据。")
        engine.end()
        if coordinator.rejected or coordinator.pending:
            raise EngineConflict("组合结束时仍有未确认或被拒绝订单。")
        return {
            "tenant_id": str(loads[0]["tenant_id"]),
            "account_id": str(loads[0]["account_id"]),
            "symbol": loads[0]["parameters"]["symbol"],
            "mode": "native_dual_venue_development_only",
            "bar_count": len(curve),
            "starting_balance": format(
                start_spot.as_decimal() + start_perp.as_decimal(), "f",
            ),
            "ending_balance": curve[-1]["total_equity"],
            "orders": coordinator.orders,
            "fills": coordinator.fills,
            "funding_observations": funding,
            "equity_curve": curve,
            "funding_accounting": "nautilus_native_settlement_from_recorded_observations",
            "venue_accounts": {"spot": "CASH", "futures_um": "MARGIN"},
        }
    finally:
        engine.dispose()
