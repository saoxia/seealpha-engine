"""多策略目标净额的唯一执行路径。

成员继续产生自己的目标或原有订单意图。协调器只在账户层比较
净目标与 Portfolio.net_position，并用一笔市价单交易差额。
原生 RiskEngine 仍检查这张订单；这里不复制名义金额或保证金规则。

保护性止损和应急退出不直接发到市场，否则会绕过净额、或在没有
成员持仓的 NETTING 账户上失去挂单。止损先记成条件指令，K 线触及
触发价后才把该成员目标缩小，并进入同一轮净额。应急市价单在当根
协调器回合生效。退出因此不会被丢掉：要么被对面目标接住而不再发单，
要么变成账户净差额的一部分成交。
"""

from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace

from nautilus_trader.model import BarType, InstrumentId, Money, OrderSide, PositionId, TimeInForce
from nautilus_trader.trading import Strategy

from engine.strategies.strategy_config import StrategyConfig
from engine.strategies.target_netting import (
    MemberTarget,
    execution_delta,
    net_signed_quantity,
    quantize_toward_zero,
)
from nautilus_support import instrument_fee_rates, make_strategy_price


class PortfolioCoordinatorConfig(StrategyConfig, frozen=True):
    """协调器只需要合约和输入 K 线，不携带任何成员的交易规则。"""

    instrument_id: InstrumentId
    bar_type: BarType
    # 与成员策略的默认值一致：回测停止时用一笔净额单平掉账户持仓。
    close_positions_on_stop: bool = True


@dataclass
class _MemberBook:
    strategy: object
    position: Decimal = Decimal(0)
    explicit: Decimal | None = None
    risk_exit: bool = False


class VirtualOrder:
    """尚未发到交易所的成员订单，供策略的改单和缓存读取。"""

    def __init__(self, source, strategy, armed_ts, position_id, risk_exit: bool) -> None:
        self._source = source
        self.strategy = strategy
        self.client_order_id = source.client_order_id
        self.side = source.side
        self.instrument_id = source.instrument_id
        self.quantity = source.quantity
        self.filled_qty = source.filled_qty
        self.filled_decimal = source.filled_qty.as_decimal()
        self.price = getattr(source, "price", None)
        self.trigger_price = getattr(source, "trigger_price", None)
        self.is_closed = False
        self.is_pending_cancel = False
        self.is_pending_update = False
        self.is_reduce_only = bool(getattr(source, "is_reduce_only", False))
        self.tags = list(source.tags or [])
        self.kind = type(source).__name__
        self.armed_ts = armed_ts
        self.position_id = position_id
        self.risk_exit = risk_exit
        self.stop_triggered = False

    def __getattr__(self, name):
        return getattr(self._source, name)

    @property
    def sign(self) -> Decimal:
        return Decimal(1) if self.side == OrderSide.BUY else Decimal(-1)

    @property
    def leaves(self) -> Decimal:
        return max(Decimal(0), self.quantity.as_decimal() - self.filled_decimal)


class PortfolioCoordinator(Strategy):
    """登记成员目标，净额后只提交账户差额。

    同一根 K 线的净额放在时间警报里，等全部成员的 on_bar 结束再算。
    停止回调按注册顺序发生；协调器最先注册，因此先用一笔单平掉账户净持仓。
    """

    def __init__(self, config: PortfolioCoordinatorConfig) -> None:
        super().__init__(config)
        self._instrument = None
        self._members: dict[str, _MemberBook] = {}
        self._orders: dict[str, VirtualOrder] = {}
        self._bar = None
        self._working_id = None
        self._cancel_sent = False
        self._denied_bar_ts = None
        self._reconciling = False
        self._again = False

    def on_start(self) -> None:
        self._instrument = self.cache.instrument(self.config.instrument_id)
        if self._instrument is None:
            self.log.error(f"找不到交易合约：{self.config.instrument_id}")
            self.stop()
            return
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar) -> None:
        if bar.bar_type != self.config.bar_type:
            return
        if self._bar is None or bar.ts_event != self._bar.ts_event:
            self._denied_bar_ts = None
        self._bar = bar
        # 同一时刻的定时器在全部策略的 on_bar 之后触发，因此成员 id 的先后不影响净额。
        self.clock.set_time_alert_ns(
            f"portfolio-net-{bar.ts_event}",
            bar.ts_event,
            self._reconcile_after_members,
        )

    def _reconcile_after_members(self, _event) -> None:
        self.reconcile()

    def on_stop(self) -> None:
        if not self.config.close_positions_on_stop or self._instrument is None:
            return
        if not self.portfolio.is_net_flat(self.config.instrument_id):
            self.close_all_positions(self.config.instrument_id)

    def register_member(self, strategy) -> None:
        self._members.setdefault(str(strategy.strategy_id), _MemberBook(strategy))

    def report_target(self, member_id: str, signed_quantity: Decimal, *, risk_exit: bool = False) -> None:
        """成员直接申报希望持有的带符号数量。订单网关会改走虚拟订单，不调用这里。"""
        if not isinstance(signed_quantity, Decimal) or not signed_quantity.is_finite():
            raise ValueError("目标数量必须是有限 Decimal")
        book = self._members.get(member_id)
        if book is None:
            book = _MemberBook(None)
            self._members[member_id] = book
        previous = book.explicit if book.explicit is not None else book.position
        if risk_exit and abs(signed_quantity) > abs(previous):
            raise ValueError("风险退出不能放大该成员敞口")
        book.explicit = signed_quantity
        book.risk_exit = risk_exit

    def net_quantity(self) -> Decimal:
        return net_signed_quantity(self._member_targets())

    def accept_order(self, strategy, order, position_id) -> None:
        key = str(order.client_order_id)
        current = self._orders.get(key)
        if current is not None and not current.is_closed:
            return
        position = position_id or PositionId(f"{strategy.strategy_id}-V")
        # 协调器正在处理本根 K 线时新挂出的条件单，要从下一根才允许触发。
        armed_ts = self._bar.ts_event if self._reconciling and self._bar is not None else None
        virtual = VirtualOrder(
            order,
            strategy,
            armed_ts=armed_ts,
            position_id=position,
            risk_exit=_is_risk_exit(order),
        )
        self._orders[key] = virtual
        self._members.setdefault(str(strategy.strategy_id), _MemberBook(strategy))
        if self._reconciling and virtual.kind == "MarketOrder":
            self._again = True

    def accept_modify(self, strategy, client_order_id, quantity, price, trigger_price) -> None:
        order = self._orders.get(str(client_order_id))
        if order is None or order.strategy is not strategy or order.is_closed:
            return
        if quantity is not None:
            order.quantity = quantity
        if price is not None:
            order.price = price
        if trigger_price is not None:
            order.trigger_price = trigger_price

    def accept_cancel(self, strategy, client_order_id) -> None:
        order = self._orders.get(str(client_order_id))
        if order is None or order.strategy is not strategy or order.is_closed:
            return
        order.is_closed = True
        self._orders.pop(str(client_order_id), None)
        handler = getattr(strategy, "on_order_canceled", None)
        if handler is not None:
            handler(SimpleNamespace(client_order_id=order.client_order_id))

    def cancel_member_orders(self, strategy, instrument_id, order_side) -> None:
        for order in list(self._orders.values()):
            if order.strategy is not strategy or order.is_closed:
                continue
            if instrument_id is not None and order.instrument_id != instrument_id:
                continue
            if order_side is not None and order.side != order_side:
                continue
            self.accept_cancel(strategy, order.client_order_id)

    def flatten_member(self, strategy, instrument_id) -> None:
        """把该成员目标打到零，并立刻重新净额。用于停机或显式全平。"""
        self.cancel_member_orders(strategy, instrument_id, None)
        book = self._members.setdefault(str(strategy.strategy_id), _MemberBook(strategy))
        book.explicit = Decimal(0)
        book.risk_exit = True
        if self.is_running():
            self.reconcile()

    def virtual_order(self, client_order_id):
        return self._orders.get(str(client_order_id))

    def reconcile(self) -> None:
        if self._reconciling:
            self._again = True
            return
        if not self.is_running() or self._instrument is None:
            return
        self._reconciling = True
        try:
            self._again = True
            while self._again:
                self._again = False
                self._reconcile_once()
        finally:
            self._reconciling = False

    def on_order_filled(self, event) -> None:
        if self._working_id is None or str(event.client_order_id) != str(self._working_id):
            return
        self._allocate_real_fill(event)
        self._working_id = None
        self._cancel_sent = False
        self.reconcile()

    def on_order_canceled(self, event) -> None:
        self._clear_working(event.client_order_id)

    def on_order_denied(self, event) -> None:
        if self._bar is not None:
            self._denied_bar_ts = self._bar.ts_event
        self._clear_working(event.client_order_id)
        self.log.error(f"净额订单被拒绝：{getattr(event, 'reason', event.client_order_id)}")

    def on_order_rejected(self, event) -> None:
        if self._bar is not None:
            self._denied_bar_ts = self._bar.ts_event
        self._clear_working(event.client_order_id)

    def _clear_working(self, client_order_id) -> None:
        if self._working_id is None or str(client_order_id) != str(self._working_id):
            return
        self._working_id = None
        self._cancel_sent = False
        self.reconcile()

    def _member_targets(self) -> list[MemberTarget]:
        actionable = {
            order.client_order_id: order
            for order in self._orders.values()
            if self._is_actionable(order)
        }
        targets = []
        for member_id, book in self._members.items():
            if book.explicit is not None:
                targets.append(MemberTarget(member_id, book.explicit, book.risk_exit))
                continue
            signed = book.position
            risk_exit = False
            for order in actionable.values():
                if order.strategy is None or str(order.strategy.strategy_id) != member_id:
                    continue
                signed += order.sign * order.leaves
                risk_exit = risk_exit or order.risk_exit
            targets.append(MemberTarget(member_id, signed, risk_exit))
        return targets

    def _reconcile_once(self) -> None:
        if self._bar is not None and self._denied_bar_ts == self._bar.ts_event:
            return
        self._arm_resting_orders()
        self._cross_actionable()
        targets = self._member_targets()
        net = net_signed_quantity(targets)
        account = self.portfolio.net_position(self.config.instrument_id)
        needed = quantize_toward_zero(
            execution_delta(net, account),
            self._instrument.size_increment.as_decimal(),
        )
        flight = self._in_flight()
        risk_exit = any(target.risk_exit for target in targets)
        if flight != 0:
            disagrees = needed == 0 or (needed > 0) != (flight > 0) or abs(flight) > abs(needed)
            if disagrees and risk_exit:
                self._cancel_working()
            return
        if needed != 0:
            self._submit_delta(needed)

    def _arm_resting_orders(self) -> None:
        # 本根新挂出的限价和止损要等下一根才允许触发，避免用决定它们的那根 K 线成交。
        if self._bar is None:
            return
        for order in self._orders.values():
            if order.is_closed or order.kind == "MarketOrder" or order.armed_ts is not None:
                continue
            order.armed_ts = self._bar.ts_event

    def _is_actionable(self, order: VirtualOrder) -> bool:
        if order.is_closed or order.leaves <= 0:
            return False
        if order.kind == "MarketOrder":
            return True
        bar = self._bar
        if bar is None or order.armed_ts is None or bar.ts_event <= order.armed_ts:
            return False
        low = bar.low.as_decimal()
        high = bar.high.as_decimal()
        buy = order.sign > 0
        trigger = order.trigger_price.as_decimal() if order.trigger_price is not None else None
        limit = order.price.as_decimal() if order.price is not None else None
        if order.kind == "LimitOrder":
            return limit is not None and (low <= limit if buy else high >= limit)
        if order.kind == "StopMarketOrder":
            return trigger is not None and (high >= trigger if buy else low <= trigger)
        if order.kind == "StopLimitOrder":
            if not order.stop_triggered:
                if trigger is None or not (high >= trigger if buy else low <= trigger):
                    return False
                order.stop_triggered = True
            return limit is None or (low <= limit if buy else high >= limit)
        return False

    def _cross_actionable(self) -> None:
        """多空同时可成交的数量在内部对冲，不再各发一笔市价单。"""
        if self._bar is None:
            return
        buys, sells = self._actionable_sides()
        cross = min(_leaves(buys), _leaves(sells))
        if cross <= 0:
            return
        price = self._bar.close.as_decimal()
        self._allocate(buys, cross, price, self._bar.ts_event)
        self._allocate(sells, cross, price, self._bar.ts_event)

    def _actionable_sides(self) -> tuple[list[VirtualOrder], list[VirtualOrder]]:
        orders = [order for order in self._orders.values() if self._is_actionable(order)]
        return (
            [order for order in orders if order.sign > 0],
            [order for order in orders if order.sign < 0],
        )

    def _allocate(self, orders: list[VirtualOrder], quantity: Decimal, price: Decimal, ts_event: int) -> None:
        remaining = quantity
        total = _leaves(orders)
        if total <= 0 or remaining <= 0:
            return
        increment = self._instrument.size_increment.as_decimal()
        for index, order in enumerate(orders):
            if remaining <= 0:
                return
            if index == len(orders) - 1:
                part = min(order.leaves, remaining)
            else:
                raw = quantity * order.leaves / total
                part = quantize_toward_zero(raw, increment)
                part = min(part, order.leaves, remaining)
            if part <= 0:
                continue
            remaining -= part
            self._fill_virtual(order, part, price, ts_event)

    def _fill_virtual(self, order: VirtualOrder, quantity: Decimal, price: Decimal, ts_event: int) -> None:
        order.filled_decimal += quantity
        order.filled_qty = self._instrument.make_qty(order.filled_decimal)
        if order.filled_decimal >= order.quantity.as_decimal() - self._instrument.size_increment.as_decimal() / 2:
            order.is_closed = True
        member_id = str(order.strategy.strategy_id)
        book = self._members.setdefault(member_id, _MemberBook(order.strategy))
        book.position += order.sign * quantity
        fill_price = make_strategy_price(self._instrument, price)
        _maker, taker = instrument_fee_rates(self._instrument)
        fee = (quantity * fill_price.as_decimal() * taker).quantize(Decimal("0.00000001"))
        commission = Money(fee, self._instrument.quote_currency)
        handler = getattr(order.strategy, "on_order_filled", None)
        if handler is not None:
            handler(
                SimpleNamespace(
                    client_order_id=order.client_order_id,
                    last_qty=self._instrument.make_qty(quantity),
                    last_px=fill_price,
                    commission=commission,
                    position_id=order.position_id,
                    ts_event=ts_event,
                ),
            )
        if order.is_closed:
            self._orders.pop(str(order.client_order_id), None)

    def _allocate_real_fill(self, event) -> None:
        quantity = event.last_qty.as_decimal()
        price = event.last_px.as_decimal()
        side = self.cache.order(event.client_order_id)
        buy = side is not None and side.side == OrderSide.BUY
        orders = [
            order
            for order in self._orders.values()
            if self._is_actionable(order) and (order.sign > 0) == buy
        ]
        if not orders:
            # 显式目标没有虚拟订单，账户成交本身就是净差额。
            return
        self._allocate(orders, quantity, price, event.ts_event)

    def _in_flight(self) -> Decimal:
        if self._working_id is None:
            return Decimal(0)
        order = self.cache.order(self._working_id)
        if order is None or order.is_closed:
            return Decimal(0)
        leaves = order.quantity.as_decimal() - order.filled_qty.as_decimal()
        sign = Decimal(1) if order.side == OrderSide.BUY else Decimal(-1)
        return sign * leaves

    def _cancel_working(self) -> None:
        if self._working_id is None or self._cancel_sent:
            return
        self._cancel_sent = True
        self.cancel_order(self._working_id)

    def _submit_delta(self, signed_quantity: Decimal) -> None:
        side = OrderSide.BUY if signed_quantity > 0 else OrderSide.SELL
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self._instrument.make_qty(abs(signed_quantity)),
            time_in_force=TimeInForce.GTC,
            tags=["portfolio-net-delta"],
        )
        self._working_id = order.client_order_id
        self._cancel_sent = False
        self.submit_order(order)


def _is_risk_exit(order) -> bool:
    text = " ".join(str(tag).lower() for tag in (order.tags or []))
    if any(token in text for token in ("emergency", "hard-stop", "hard_stop", "stop_loss", "stop-loss")):
        return True
    return bool(getattr(order, "is_reduce_only", False)) and type(order).__name__ in {
        "StopMarketOrder",
        "StopLimitOrder",
    }


def _leaves(orders: list[VirtualOrder]) -> Decimal:
    return sum((order.leaves for order in orders), Decimal(0))
