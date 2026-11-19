"""NautilusTrader Strategy callback bridge to generic basket research.

Each registered member subscribes to native BarType streams. On the last
real on_bar callback for one timestamp, the common gate asks a trusted
historical provider for a complete account/BBO snapshot. No direct orders
or cross-product exchange transaction are exposed to member strategies.
"""
from collections.abc import Callable
from dataclasses import dataclass

from nautilus_trader.model import Bar, BarType
from nautilus_trader.trading import Strategy

from engine.strategies.basket_generic_runtime import (
    BasketMarketBundle,
    GenericBasketRuntime,
)
from engine.strategies.basket_research_session import ResearchBasketMember
from engine.strategies.basket_targets import BasketPlanError


class BasketResearchStrategy(Strategy):
    """Extend a Nautilus Strategy to publish a complete member target per bar.

    Subclasses implement on_basket(bundle) and call publish_basket. The generic
    coordinator still decides the NET targets; one member cannot submit orders.
    """

    def __init__(self, config):
        super().__init__(config)
        self._basket_gate = None
        self._basket_member_id = None
        self._basket_bar_types = ()
        self._basket_publisher: ResearchBasketMember | None = None

    def bind_basket_gate(
        self, gate: "NativeBasketBarGate", member_id: str,
        bar_types: tuple[BarType, ...],
    ) -> None:
        if self._basket_gate is not None or not isinstance(gate, NativeBasketBarGate):
            raise BasketPlanError("Nautilus 成员研究网关只能绑定一次")
        gate.bind(member_id, self, bar_types)
        self._basket_gate = gate
        self._basket_member_id = member_id
        self._basket_bar_types = bar_types

    def on_start(self):
        if self._basket_gate is None:
            raise BasketPlanError("研究策略缺少授权的组合事件网关")
        for bar_type in self._basket_bar_types:
            self.subscribe_bars(bar_type)

    def on_bar(self, bar):
        if self._basket_gate is None:
            raise BasketPlanError("策略未绑定组合订阅")
        self._basket_gate.observe(self._basket_member_id, bar)

    def on_market_bundle(
        self, member: ResearchBasketMember, bundle: BasketMarketBundle,
    ) -> None:
        if self._basket_publisher is not None:
            raise BasketPlanError("同一成员不能重入另一个组合快照")
        self._basket_publisher = member
        try:
            self.on_basket(bundle)
        finally:
            self._basket_publisher = None

    def on_basket(self, bundle: BasketMarketBundle) -> None:
        raise NotImplementedError("成员 Strategy 应实现 on_basket")

    def publish_basket(self, *, targets: list[dict], request_id=None, constraints=None):
        if self._basket_publisher is None:
            raise BasketPlanError("只有已授权的统一研究快照内允许发布目标")
        self._basket_publisher.publish_basket(
            targets=targets, request_id=request_id, constraints=constraints,
        )

    def subscribe_market_bundle(self, *, symbol=None, products=(), data=(),
                                option_symbols=(), legs=()):
        if self._basket_publisher is None:
            raise BasketPlanError("策略订阅由可信网关完成，不接受自行扩大腿范围")
        # Subscription is already bound to the immutable registered leg set.
        if (
            symbol is not None or set(products) != {
                leg.product for leg in self._basket_gate.runtime_registration(self._basket_member_id)
            } or set(legs) != set(
                self._basket_gate.runtime_registration(self._basket_member_id)
            ) or set(data) != {"closed_bars", "quotes", "account_positions"}
        ):
            raise BasketPlanError("Nautilus 策略不得修改已授权市场订阅")
        # The registration is already complete in GenericBasketRuntime.


@dataclass(frozen=True)
class _BarIdentity:
    member_id: str
    bar_type: BarType


class NativeBasketBarGate:
    """Multi-member native on_bar barrier without implicit market forward-fill."""

    def __init__(
        self, runtime: GenericBasketRuntime,
        snapshot_provider: Callable,
    ) -> None:
        if not isinstance(runtime, GenericBasketRuntime) or not callable(snapshot_provider):
            raise BasketPlanError("原生组合网关缺少研究协调器或可信报价提供者")
        self.runtime = runtime
        self.snapshot_provider = snapshot_provider
        self.registered: dict[str, tuple[BasketResearchStrategy, tuple[BarType, ...]]] = {}
        self._seen = {}
        self._active_ns = 0
        self.decisions = []

    def runtime_registration(self, member_id):
        return next(
            registration.legs for registration in self.runtime.members
            if registration.member_id == member_id
        )

    def bind(
        self, member_id: str, strategy: BasketResearchStrategy,
        bar_types: tuple[BarType, ...],
    ) -> None:
        registrations = {
            reg.member_id: reg for reg in self.runtime.members
        }
        if (
            member_id not in registrations or registrations[member_id].model is not strategy
            or member_id in self.registered or self.decisions
            or not isinstance(bar_types, tuple)
            or not bar_types or len(bar_types) > 64
            or not all(isinstance(bar_type, BarType) for bar_type in bar_types)
            or len(set(str(item) for item in bar_types)) != len(bar_types)
        ):
            raise BasketPlanError("原生成员研究实例、市场订阅或生命周期无效")
        self.registered[member_id] = (strategy, bar_types)

    def observe(self, member_id: str, bar: Bar) -> None:
        if not isinstance(bar, Bar) or member_id not in self.registered:
            raise BasketPlanError("Nautilus 行情事件或成员身份无效")
        if len(self.registered) != len(self.runtime.members):
            raise BasketPlanError("所有原生策略必须完成订阅并注册")
        stamp = int(bar.ts_event)
        if stamp <= 0 or stamp <= self.runtime.last_ns:
            raise BasketPlanError("原生组合行情重复或乱序")
        if stamp != self._active_ns:
            if self._seen:
                raise BasketPlanError("前一时间戳的所有成员原生 Bar 尚未就绪")
            if stamp <= self._active_ns:
                raise BasketPlanError("原生市场时间倒退")
            self._active_ns = stamp
        _, allowed = self.registered[member_id]
        if str(bar.bar_type) not in {str(x) for x in allowed}:
            raise BasketPlanError("原生策略收到未声明的产品行情")
        key = _BarIdentity(member_id, bar.bar_type)
        if key in self._seen:
            raise BasketPlanError("原生多策略收到重复的 Bar")
        self._seen[key] = bar
        expected = {
            _BarIdentity(name, bar_type)
            for name, (_, bar_types) in self.registered.items()
            for bar_type in bar_types
        }
        if set(self._seen) == expected:
            current = dict(self._seen)
            self._seen.clear()
            positions, evidence = self.snapshot_provider(stamp, current)
            decision = self.runtime.decide(
                now_ns=stamp, positions=positions, evidence=evidence,
            )
            self.decisions.append(decision)
