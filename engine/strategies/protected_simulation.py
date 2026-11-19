"""Development-only native option protection replay using issued demo logic.

The original issued BollingerModel proposes a basket; every phase is an actual
NautilusTrader market order. Option fill events must confirm BOTH long wings
before any directional perpetual order can be submitted. This is offline
research, never an atomic live order promise or a live execution route.
"""

import tempfile
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from nautilus_trader.backtest import BacktestEngine
from nautilus_trader.model import (
    AccountType,
    BarType,
    CryptoOption,
    CryptoPerpetual,
    OmsType,
    OrderSide,
    QuoteTick,
    TimeInForce,
    Venue,
)
from nautilus_trader.trading import Strategy

from engine.runtime import EngineConflict
from engine.strategies.backtest_engine import backtest_engine_config
from engine.strategies.basket_reconciliation import BasketReconciliation
from engine.strategies.basket_targets import (
    BasketPlanError,
    LegEvidence,
    LegId,
    MemberLegTarget,
    plan_basket,
)
from engine.strategies.carry_simulation import (
    _balance,
    _decimal,
    _money,
    _perp_unrealized,
    _position,
    _sim_bar,
)
from engine.strategies.package import (
    PackageError,
    extract,
    load_entry,
    namespace_for,
    prepare_parameters,
    release_namespace,
)
from engine.strategies.strategy_config import StrategyConfig
from nautilus_support import instrument_fee_model

_PERP_VENUE = Venue("SIM_UM")
_OPTION_VENUE = Venue("SIM_OPT")


def _model_from_issued_load(load):
    """Use exact archived, grant-scoped Demo logic rather than a second signal copy."""
    archive = load.get("archive")
    if not isinstance(archive, bytes) or not archive:
        raise EngineConflict("已下发期权策略缺少原始归档。")
    with tempfile.TemporaryDirectory(prefix="option-research-") as directory:
        folder = Path(directory)
        namespace = namespace_for(load["tenant_id"], load["issuance_id"], uuid4().hex)
        try:
            meta = extract(archive, folder)
            if (
                meta.digest != load["package_hash"]
                or meta.name != load["code_key"]
                or meta.version != load["version"]
                or meta.entry_point != load["entry_point"]
            ):
                raise EngineConflict("期权回测归档与已授权下发记录不一致。")
            module, _strategy_class = load_entry(folder, meta.entry_point, namespace)
            params = prepare_parameters(module, load["parameters"])
            if params != load["parameters"]:
                raise EngineConflict("期权策略参数与已下发记录不一致。")
            model_class = getattr(module, "BollingerModel", None)
            if not isinstance(model_class, type):
                raise EngineConflict("已下发的期权 Demo 缺少 BollingerModel。")
            model = model_class(params["period"], params["deviation"])
            if not callable(getattr(model, "propose", None)) or not callable(
                getattr(model, "commit", None)
            ):
                raise EngineConflict("期权 Demo 不能提交完整的目标提议。")
            return model
        except PackageError as exc:
            raise EngineConflict(str(exc)) from exc
        finally:
            release_namespace(namespace)


def _sim_option(original):
    if not isinstance(original, CryptoOption):
        raise EngineConflict("期权模拟必须使用原生 CryptoOption。")
    payload = original.to_dict()
    payload["id"] = f"{original.id.symbol}.{_OPTION_VENUE}"
    option = CryptoOption.from_dict(payload)
    if option.id.venue != _OPTION_VENUE or option.quote_currency.code != "USDT":
        raise EngineConflict("期权合约无法建立独立的 USDT 模拟账户。")
    return option


def _sim_quote(row, instrument):
    return QuoteTick(
        instrument.id,
        instrument.make_price(row.bid_price.as_decimal()),
        instrument.make_price(row.ask_price.as_decimal()),
        instrument.make_qty(row.bid_size.as_decimal()),
        instrument.make_qty(row.ask_size.as_decimal()),
        row.ts_event,
        row.ts_init,
    )


class _NativeProtectedCoordinator(Strategy):
    """Order phase transitions are only triggered by actual native fill callbacks."""

    def __init__(self, config):
        super().__init__(config)
        self.instruments = {}
        self.ledger = {}
        self.outstanding = {}
        self.phases = []
        self.current_phase = None
        self.proposal = None
        self.evidence = None
        self.model = None
        self.params = {}
        self.perp = self.put = self.call = None
        self.bar_type = None
        self.max_gross_notional = Decimal(0)
        self.failed = []
        self.orders = []
        self.fills = []
        self.target_events = []
        self.pending_premium = Decimal(0)
        self.reconciler = None

    def bind(self, load, model, perp, put, call, interval, max_gross_notional):
        self.params = load["parameters"]
        self.model = model
        self.perp, self.put, self.call = perp, put, call
        minutes = 1 if interval == "1m" else 5
        self.bar_type = BarType.from_str(f"{perp.id}-{minutes}-MINUTE-LAST-EXTERNAL")
        self.max_gross_notional = max_gross_notional
        for product, instrument in (
            ("futures_um", perp), ("options", put), ("options", call),
        ):
            leg = LegId(
                str(load["tenant_id"]), str(load["account_id"]),
                product, str(instrument.raw_symbol).removesuffix("-PERP"),
            )
            self.instruments[leg] = instrument
            self.ledger[leg] = Decimal(0)
        self.member_id = str(load["issuance_id"])

    def on_start(self):
        self.subscribe_bars(self.bar_type)

    def _snapshot(self, bar):
        stamp = int(bar.ts_event)
        item = self.evidence

        def option_snapshot(instrument, quote):
            return {
                "symbol": str(instrument.raw_symbol), "ts_ns": int(quote.ts_event),
                "ask": str(quote.ask_price), "ask_size": str(quote.ask_size),
                "strike": str(instrument.strike_price),
                "multiplier": str(instrument.multiplier),
                "expiry_ns": int(instrument.expiration_ns),
            }

        return {
            "now_ns": stamp,
            "bar": {
                "ts_ns": stamp, "closed": True,
                "close": str(bar.close),
            },
            "put": option_snapshot(self.put, item.put_quote),
            "call": option_snapshot(self.call, item.call_quote),
        }

    def on_bar(self, bar):
        if bar.bar_type != self.bar_type:
            return
        if self.evidence is None or self.evidence.ts_ns != bar.ts_event:
            self.failed.append("保护组合的决策 K 线与证据时间不一致")
            return
        if self.outstanding or self.phases or self.current_phase:
            self.failed.append("上一组合仍有未完成或不确定状态")
            return
        if self.failed:
            return
        proposal = self.model.propose(self._snapshot(bar), self.params)
        action = proposal["action"]
        if action in {"flat", "warming_up"}:
            return
        if action not in {"enter_long", "enter_short", "exit"}:
            self.failed.append("未知期权保护策略动作")
            return

        targets = proposal["targets"]
        if not isinstance(targets, list) or len(targets) != 3:
            self.failed.append("期权组合目标不是完整的三个产品腿")
            return
        identities = {(leg.product, leg.symbol): leg for leg in self.instruments}
        desired = {}
        for row in targets:
            identity = (row["product"], row["symbol"])
            if identity not in identities or identities[identity] in desired:
                self.failed.append("组合目标含跨产品身份错误或重复腿")
                return
            desired[identities[identity]] = _decimal(row["quantity"], "组合目标数量")
        if len(desired) != 3:
            self.failed.append("组合目标有缺失的保护腿")
            return
        now = int(bar.ts_event)
        quote_price = {
            self.put.id: self.evidence.put_quote.ask_price.as_decimal(),
            self.call.id: self.evidence.call_quote.ask_price.as_decimal(),
            self.perp.id: bar.close.as_decimal(),
        }
        ts_map = {
            self.put.id: int(self.evidence.put_quote.ts_event),
            self.call.id: int(self.evidence.call_quote.ts_event),
            self.perp.id: now,
        }
        try:
            plan = plan_basket(
                [MemberLegTarget(self.member_id, leg, quantity)
                 for leg, quantity in desired.items()],
                dict(self.ledger),
                {leg: LegEvidence(
                    quote_price[instrument.id],
                    instrument.size_increment.as_decimal(),
                    instrument.multiplier.as_decimal()
                    if leg.product == "options" else Decimal(1),
                    ts_map[instrument.id],
                ) for leg, instrument in self.instruments.items()},
                now_ns=now,
                max_age_ns=int(self.params["max_age_seconds"]) * 1_000_000_000,
                max_gross_notional=self.max_gross_notional,
                policy="option_protected",
            )
        except BasketPlanError as exc:
            self.failed.append(str(exc))
            return
        self.proposal = (proposal, desired)
        self.target_events.append({
            "ts_ns": now, "action": action,
            "targets": [
                {"product": leg.product, "symbol": leg.symbol,
                 "quantity": format(quantity, "f")}
                for leg, quantity in sorted(desired.items())
            ],
        })
        self.reconciler = BasketReconciliation(
            plan, dict(self.ledger), run_id=f"{self.member_id}:{now}",
        )
        self.phases = list(plan.phases)
        self.pending_premium = Decimal(0)
        self._advance()

    def _advance(self):
        if self.failed or self.outstanding:
            return
        if self.current_phase is not None:
            self._verify_phase(self.current_phase)
            if not self.failed:
                try:
                    self.reconciler.confirm_phase({
                        leg: _position(self, instrument)
                        for leg, instrument in self.instruments.items()
                    })
                except BasketPlanError as exc:
                    self.failed.append(str(exc))
            self.current_phase = None
            if self.failed:
                return
        if not self.phases:
            if self.proposal is not None:
                plan, desired = self.proposal
                if any(self.ledger[leg] != quantity for leg, quantity in desired.items()):
                    self.failed.append("模拟成交未达成完整组合目标")
                    return
                self.model.commit(plan)
                self.proposal = None
            return
        phase = self.phases.pop(0)
        if self.reconciler.next_phase != phase:
            self.failed.append("组合协调阶段与已验证计划不一致")
            return
        self.current_phase = phase.name
        orders = []
        phase_order_ids = {}
        for change in phase.changes:
            instrument = self.instruments[change.leg]
            delta = change.quantity_delta
            order = self.order_factory.market(
                instrument_id=instrument.id,
                order_side=OrderSide.BUY if delta > 0 else OrderSide.SELL,
                quantity=instrument.make_qty(abs(delta)),
                time_in_force=TimeInForce.GTC,
                tags=["protected-basket-research", phase.name],
            )
            key = str(order.client_order_id)
            phase_order_ids[change.leg] = key
            self.outstanding[key] = {
                "leg": change.leg,
                "remaining": abs(delta),
                "sign": Decimal(1) if delta > 0 else Decimal(-1),
            }
            self.orders.append({
                "ts_ns": int(self.evidence.ts_ns),
                "phase": phase.name, "client_order_id": key,
                "instrument_id": str(instrument.id),
                "side": "BUY" if delta > 0 else "SELL",
                "quantity": format(abs(delta), "f"),
            })
            orders.append(order)
        try:
            self.reconciler.open_phase(
                phase_order_ids, observed_positions={
                    leg: _position(self, instrument)
                    for leg, instrument in self.instruments.items()
                },
                now_ns=int(self.evidence.ts_ns),
            )
        except BasketPlanError as exc:
            self.failed.append(str(exc))
            return
        # Register the entire concurrent phase before any native fill callback
        # can advance to the next phase.
        for order in orders:
            self.submit_order(order)

    def _verify_phase(self, phase_name):
        perpetual = next(leg for leg in self.ledger if leg.product == "futures_um")
        wings = [leg for leg in self.ledger if leg.product == "options"]
        exposure = abs(self.ledger[perpetual])
        if (
            phase_name in {
                "acquire_and_confirm_protection",
                "increase_directional_risk_after_protection",
            }
            and any(
                self.ledger[leg] * self.instruments[leg].multiplier.as_decimal() < exposure
                for leg in wings
            )
        ):
            self.failed.append("期权两翼未真实成交足额保护，禁止增加永续敞口")
        if phase_name == "acquire_and_confirm_protection":
            desired_perp = abs(self.proposal[1][perpetual])
            if any(
                self.ledger[leg] * self.instruments[leg].multiplier.as_decimal() < desired_perp
                for leg in wings
            ):
                self.failed.append("保护腿尚未达到目标覆盖率，拒绝建立永续方向风险")
            maximum = _decimal(self.params["max_total_premium"], "最大权利金", positive=True)
            if self.pending_premium > maximum:
                self.failed.append("实际买入保护腿的含费权利金超过预算")
        if phase_name == "release_protection_after_risk_reduction" and exposure:
            self.failed.append("永续敞口尚未消除，拒绝释放期权保护")

    def on_order_filled(self, event):
        key = str(event.client_order_id)
        entry = self.outstanding.get(key)
        if entry is None:
            self.failed.append("收到未知组合订单成交事件")
            return
        quantity = event.last_qty.as_decimal()
        if quantity <= 0 or quantity > entry["remaining"]:
            self.failed.append("组合订单成交数量不一致")
            return
        try:
            self.reconciler.record_fill(
                key, quantity, fill_id=str(event.id), now_ns=int(event.ts_event),
            )
        except BasketPlanError as exc:
            self.failed.append(str(exc))
            return
        entry["remaining"] -= quantity
        leg = entry["leg"]
        self.ledger[leg] += quantity * entry["sign"]
        premium = quantity * event.last_px.as_decimal()
        if leg.product == "options" and entry["sign"] > 0:
            premium *= self.instruments[leg].multiplier.as_decimal()
            self.pending_premium += premium
            if event.commission.currency.code == "USDT":
                self.pending_premium += event.commission.as_decimal()
        self.fills.append({
            "phase": self.current_phase,
            "client_order_id": key,
            "instrument_id": str(event.instrument_id),
            "quantity": format(quantity, "f"),
            "price": str(event.last_px),
            "commission": str(event.commission),
        })
        if entry["remaining"] == 0:
            self.outstanding.pop(key)
        if not self.outstanding:
            self._advance()

    def on_order_rejected(self, event):
        self.failed.append(f"组合订单拒绝 {event.client_order_id}")

    def on_order_denied(self, event):
        self.failed.append(f"组合订单未通过模拟风控 {event.client_order_id}")

    def on_order_canceled(self, event):
        self.failed.append(f"组合订单未完整成交即取消 {event.client_order_id}")

    def on_order_expired(self, event):
        self.failed.append(f"组合订单未完整成交即到期 {event.client_order_id}")


def _option_equity(engine, coordinator, put, call, quotes):
    currency = put.quote_currency
    cash = _balance(engine, put.id.venue, currency)
    marked = Decimal(0)
    for instrument, quote in ((put, quotes.put_quote), (call, quotes.call_quote)):
        leg = next(
            leg for leg, value in coordinator.instruments.items()
            if value.id == instrument.id
        )
        marked += (
            coordinator.ledger[leg]
            * instrument.multiplier.as_decimal()
            * quote.bid_price.as_decimal()
        )
    return cash + marked


def run_native_protected(load, evidence, contracts, *, interval,
                         starting_options_balance, starting_perpetual_balance,
                         max_gross_notional, model=None):
    """Native option BBO fills and hedged perpetual sequencing for development."""
    if (
        load.get("code_key") != "demo_option_hedged_bollinger"
        or load.get("grant_purpose") != "development"
        or not isinstance(load.get("parameters"), dict)
    ):
        raise EngineConflict("期权保护回测只接受已下发的开发 Demo。")
    params = load["parameters"]
    if (
        not params.get("put_symbol") or not params.get("call_symbol")
        or interval not in {"1m", "5m"}
    ):
        raise EngineConflict("期权保护回测缺少有效双翼或周期。")
    original_perp, original_put, original_call = contracts
    if not isinstance(original_perp, CryptoPerpetual):
        raise EngineConflict("缺少期权保护对应的原生永续合约。")
    if str(original_perp.raw_symbol).removesuffix("-PERP") != params["symbol"]:
        raise EngineConflict("期权保护基础合约不一致。")
    if (
        str(original_put.raw_symbol) != params["put_symbol"]
        or str(original_call.raw_symbol) != params["call_symbol"]
    ):
        raise EngineConflict("期权保护双翼与下发参数不一致。")
    if model is None:
        model = _model_from_issued_load(load)
        model_source = "issued_strategy_package"
    else:
        model_source = "explicit_offline_test_model"
    perpetual = CryptoPerpetual.from_dict({
        **original_perp.to_dict(),
        "id": f"{original_perp.id.symbol}.{_PERP_VENUE}",
    })
    put, call = _sim_option(original_put), _sim_option(original_call)
    if (
        put.quote_currency != perpetual.quote_currency
        or call.quote_currency != perpetual.quote_currency
    ):
        raise EngineConflict("永续与保护期权必须用相同的报价币种。")
    cash = _money(starting_options_balance, "USDT")
    margin = _money(starting_perpetual_balance, "USDT")
    limit = _decimal(max_gross_notional, "组合总名义风险", positive=True)
    engine = BacktestEngine(backtest_engine_config())
    coordinator = _NativeProtectedCoordinator(StrategyConfig(order_id_tag="PROTECT"))
    coordinator.bind(load, model, perpetual, put, call, interval, limit)
    try:
        engine.add_venue(
            perpetual.id.venue, OmsType.NETTING, AccountType.MARGIN, [margin],
            fee_model=instrument_fee_model(perpetual), default_leverage=Decimal(5),
        )
        engine.add_venue(
            put.id.venue, OmsType.NETTING, AccountType.CASH, [cash],
            fee_model=instrument_fee_model(put), default_leverage=Decimal(1),
        )
        for contract in (perpetual, put, call):
            engine.add_instrument(contract)
        engine.add_strategy(coordinator)
        curve = []
        last_quote_ts = {put.id: 0, call.id: 0}
        last_bar_ns = 0
        for row in evidence:
            if row.ts_ns <= last_bar_ns or len(curve) >= 1440:
                raise EngineConflict("期权保护回测时间重复或超过 24 小时上限。")
            last_bar_ns = row.ts_ns
            coordinator.evidence = row
            incoming = [_sim_bar(row.perpetual, perpetual)]
            for original, instrument in ((row.put_quote, put), (row.call_quote, call)):
                if int(original.ts_event) > last_quote_ts[instrument.id]:
                    incoming.append(_sim_quote(original, instrument))
                    last_quote_ts[instrument.id] = int(original.ts_event)
            engine.add_data(incoming, sort=True)
            engine.run(streaming=True)
            engine.clear_data()
            if coordinator.failed:
                raise EngineConflict("期权多腿模拟未能安全成交：" + coordinator.failed[0])
            if coordinator.outstanding or coordinator.current_phase or coordinator.phases:
                raise EngineConflict("期权多腿订单尚有部分成交或未确认风险。")
            if any(
                _position(engine, instrument) != coordinator.ledger[leg]
                for leg, instrument in coordinator.instruments.items()
            ):
                raise EngineConflict("模拟成交账本与实际原生仓位不一致。")
            exposure = abs(next(
                coordinator.ledger[leg] for leg in coordinator.ledger
                if leg.product == "futures_um"
            ))
            for leg, contract in coordinator.instruments.items():
                if leg.product == "options" and (
                    coordinator.ledger[leg] * contract.multiplier.as_decimal() < exposure
                ):
                    raise EngineConflict("模拟永续敞口未受到实际期权双翼覆盖。")
            option_value = _option_equity(engine, coordinator, put, call, row)
            perpetual_value = (
                _balance(engine, perpetual.id.venue, perpetual.quote_currency)
                + _perp_unrealized(engine, perpetual)
            )
            curve.append({
                "ts_ns": row.ts_ns,
                "options_equity": format(option_value, "f"),
                "perpetual_equity": format(perpetual_value, "f"),
                "total_equity": format(option_value + perpetual_value, "f"),
                "perpetual_quantity": format(exposure, "f"),
                "put_quantity": format(next(
                    coordinator.ledger[leg] for leg in coordinator.ledger
                    if leg.symbol == params["put_symbol"]
                ), "f"),
                "call_quantity": format(next(
                    coordinator.ledger[leg] for leg in coordinator.ledger
                    if leg.symbol == params["call_symbol"]
                ), "f"),
            })
        if not curve:
            raise EngineConflict("未提供有效期权保护历史输入。")
        engine.end()
        if coordinator.failed or coordinator.outstanding:
            raise EngineConflict("期权保护组合在回测结束时有未解决订单。")
        if coordinator.reconciler is not None and not coordinator.reconciler.complete:
            raise EngineConflict("期权保护组合的阶段成交尚未经独立仓位核对。")
        return {
            "tenant_id": str(load["tenant_id"]),
            "account_id": str(load["account_id"]),
            "issuance_id": str(load["issuance_id"]),
            "symbol": params["symbol"],
            "mode": "native_protected_options_development_only",
            "quote_source": "native_catalog_book_ticker",
            "model_source": model_source,
            "bar_count": len(curve),
            "starting_balance": format(cash.as_decimal() + margin.as_decimal(), "f"),
            "ending_balance": curve[-1]["total_equity"],
            "target_events": coordinator.target_events,
            "orders": coordinator.orders,
            "fills": coordinator.fills,
            "equity_curve": curve,
            "settlement_accounting": "not_in_window_expiry_requires_verified_settlement",
            "venue_accounts": {"options": "CASH", "futures_um": "MARGIN"},
        }
    finally:
        engine.dispose()
