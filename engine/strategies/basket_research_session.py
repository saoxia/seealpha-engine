"""Pure, fail-closed multi-member basket publication for DEVELOPMENT research.

This is a scope-bound adapter for the Demo `subscribe_market_bundle` and
`publish_basket` intent vocabulary, not a Nautilus Strategy mixin or an order
sender. A real data adapter must supply authoritative account/market evidence.
"""

import hashlib
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from engine.strategies.basket_reconciliation import (
    BasketCheckpoint,
    begin_phase,
    new_checkpoint,
)
from engine.strategies.basket_targets import (
    BasketPlan,
    BasketPlanError,
    LegEvidence,
    LegId,
    MemberLegTarget,
    plan_basket,
)

_MAX_MEMBERS = 16
_REQUIRED_DATA = {
    "generic": frozenset({"closed_bars", "account_positions"}),
    "funding_carry": frozenset({"closed_bars", "funding_rate", "account_positions"}),
    "option_protected": frozenset({
        "closed_bars", "option_quotes", "option_instruments", "positions",
    }),
}


@dataclass(frozen=True)
class BasketResearchDecision:
    """Immutable, non-executable decision and verified research audit metadata."""

    plan: BasketPlan
    member_count: int
    policy: str
    ts_ns: int

    @property
    def executable(self) -> bool:
        return False


class BasketResearchSession:
    """Collect all scoped member intents once per verified market snapshot.

    The session deliberately has no execution methods or venue credentials.
    Research workers must use a separate trusted source for fills and
    authoritative account snapshots before advancing the phase journal.
    """

    def __init__(
        self, *,
        tenant_id: str,
        account_id: str,
        policy: str,
        positions: dict[LegId, Decimal],
        evidence: dict[LegId, LegEvidence],
        now_ns: int,
        max_age_ns: int,
        max_gross_notional: Decimal,
        max_leg_drift: Decimal = Decimal(0),
    ) -> None:
        if policy not in _REQUIRED_DATA:
            raise BasketPlanError("未知研究组合策略")
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise BasketPlanError("研究租户标识无效")
        if not isinstance(account_id, str) or not account_id.strip():
            raise BasketPlanError("研究账户标识无效")
        if not isinstance(positions, dict) or not positions:
            raise BasketPlanError("缺少完整研究持仓")
        if not isinstance(evidence, dict) or set(evidence) != set(positions):
            raise BasketPlanError("组合研究行情必须完整且不得包含额外腿")
        for leg, quantity in positions.items():
            if not isinstance(leg, LegId) or (leg.tenant_id, leg.account_id) != (
                tenant_id, account_id,
            ):
                raise BasketPlanError("研究腿不属于指定租户和账户")
            if not isinstance(quantity, Decimal) or not quantity.is_finite():
                raise BasketPlanError("研究持仓必须是有限 Decimal")
            if not isinstance(evidence[leg], LegEvidence):
                raise BasketPlanError("缺少完整原生合约证据")
            if type(now_ns) is not int or type(max_age_ns) is not int or (
                now_ns <= 0 or max_age_ns <= 0
                or not 0 <= now_ns - evidence[leg].ts_ns <= max_age_ns
            ):
                raise BasketPlanError("研究行情过期、来自未来或时间窗口无效")
        self.tenant_id = tenant_id
        self.account_id = account_id
        self.policy = policy
        self.positions = dict(positions)
        self.evidence = dict(evidence)
        self.now_ns = now_ns
        self.max_age_ns = max_age_ns
        self.max_gross_notional = max_gross_notional
        self.max_leg_drift = max_leg_drift
        self._subscriptions: dict[str, frozenset[LegId]] = {}
        self._intents: dict[str, tuple[str, tuple[MemberLegTarget, ...]]] = {}
        self._decision: BasketResearchDecision | None = None

    def member(self, member_id: str) -> "ResearchBasketMember":
        """Bind an immutable strategy issuance identity; never infer user scope."""
        if (
            not isinstance(member_id, str)
            or not 1 <= len(member_id) <= 128
            or not member_id.isascii()
            or not all(char.isalnum() or char in "._-:" for char in member_id)
        ):
            raise BasketPlanError("成员发行标识无效")
        if self._decision is not None:
            raise BasketPlanError("研究快照已完成决策，禁止新增成员")
        if member_id not in self._subscriptions and len(self._subscriptions) >= _MAX_MEMBERS:
            raise BasketPlanError("研究策略成员超出上限")
        return ResearchBasketMember(self, member_id)

    def _subscribe(
        self, member_id: str, *,
        symbol: str | None = None,
        products: tuple[str, ...] = (),
        data: tuple[str, ...] = (),
        option_symbols: tuple[str, ...] = (),
        legs: tuple[dict, ...] = (),
    ) -> None:
        if self._decision is not None or member_id in self._intents:
            raise BasketPlanError("组合已经形成目标，不能更改历史订阅")
        if not isinstance(products, tuple) or not isinstance(data, tuple) or (
            not isinstance(option_symbols, tuple) or not isinstance(legs, tuple)
        ):
            raise BasketPlanError("历史订阅协议无效")
        if (
            not all(isinstance(item, str) for item in (*products, *data, *option_symbols))
            or len(products) != len(set(products))
            or len(data) != len(set(data))
            or set(data) != _REQUIRED_DATA[self.policy]
        ):
            raise BasketPlanError("订阅必须提供完整且无重复的策略证据")
        required_legs = set(self.positions)
        if self.policy == "generic":
            if symbol is not None or products or option_symbols or not legs:
                raise BasketPlanError("通用组合必须显式声明完整的产品合约腿")
            if any(
                not isinstance(row, dict)
                or set(row) != {"product", "symbol"}
                or not isinstance(row["product"], str)
                or not isinstance(row["symbol"], str)
                for row in legs
            ):
                raise BasketPlanError("通用组合腿结构无效")
            requested = tuple((row["product"], row["symbol"]) for row in legs)
            if (
                not requested or len(requested) != len(set(requested))
                or not set(requested).issubset(
                    {(leg.product, leg.symbol) for leg in required_legs}
                )
            ):
                raise BasketPlanError("通用组合订阅必须声明不重复的授权产品合约腿")
        else:
            if not isinstance(symbol, str) or legs:
                raise BasketPlanError("对冲策略订阅协议无效")
            expected_products = (
                {"spot", "futures_um"}
                if self.policy == "funding_carry" else {"futures_um", "options"}
            )
            if set(products) != expected_products:
                raise BasketPlanError("跨产品订阅类型与策略不匹配")
        if self.policy == "funding_carry":
            if option_symbols or any(leg.symbol != symbol for leg in required_legs):
                raise BasketPlanError("资金费订阅必须精确匹配 Spot/UM 标的")
        elif self.policy == "option_protected":
            futures = [leg for leg in required_legs if leg.product == "futures_um"]
            options = {leg.symbol for leg in required_legs if leg.product == "options"}
            if (
                len(futures) != 1 or futures[0].symbol != symbol
                or set(option_symbols) != options
                or len(option_symbols) != len(options)
            ):
                raise BasketPlanError("期权订阅必须绑定当前账户的完整永续和双翼")
        previous = self._subscriptions.get(member_id)
        if previous is not None and previous != frozenset(required_legs):
            raise BasketPlanError("相同成员不能重新绑定其他组合腿")
        if self.policy == "generic":
            declared = {leg for leg in required_legs
                        if (leg.product, leg.symbol) in requested}
            self._subscriptions[member_id] = frozenset(declared)
        else:
            self._subscriptions[member_id] = frozenset(required_legs)

    def _publish(
        self, member_id: str, *,
        targets: list[dict],
        request_id: str | None,
        constraints: dict | None = None,
    ) -> None:
        if request_id is None:
            # The archived Demo API does not provide an order key. Bind a
            # stable *research intent* identity to the member and snapshot.
            # This never becomes a client_order_id for a live venue.
            raw = f"{self.tenant_id}:{self.account_id}:{member_id}:{self.now_ns}"
            request_id = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        if self._decision is not None:
            raise BasketPlanError("同一市场快照已锁定，不允许修改目标")
        if member_id not in self._subscriptions:
            raise BasketPlanError("策略没有完成组合历史订阅")
        if (
            not isinstance(request_id, str) or not 1 <= len(request_id) <= 128
            or not request_id.isascii()
            or not all(char.isalnum() or char in "-_.:" for char in request_id)
        ):
            raise BasketPlanError("组合请求幂等标识无效")
        if not isinstance(targets, list) or len(targets) != len(self._subscriptions[member_id]):
            raise BasketPlanError("必须申报当前策略订阅的所有组合腿，包括零目标")
        if constraints is not None and not isinstance(constraints, dict):
            raise BasketPlanError("约束协议无效")
        # Demanded atomicity is an *intent*, not a guarantee. Reject unknown
        # relaxation flags rather than interpreting them as execution consent.
        allowed = (
            {"atomic_hedge", "max_leg_drift"} if self.policy == "funding_carry"
            else {"hedges_filled_before_risk", "atomic_reconciliation"}
            if self.policy == "option_protected" else set()
        )
        if constraints and (set(constraints) - allowed):
            raise BasketPlanError("未知多腿执行约束")
        if self.policy == "funding_carry" and constraints:
            if constraints.get("atomic_hedge") is not True:
                raise BasketPlanError("资金费发布必须声明完整对冲")
            try:
                drift = Decimal(str(constraints.get("max_leg_drift")))
            except (TypeError, InvalidOperation, ValueError) as exc:
                raise BasketPlanError("资金费腿间偏差约束无效") from exc
            if not drift.is_finite() or drift < 0 or drift > self.max_leg_drift:
                raise BasketPlanError("成员腿间偏差不得宽于研究风险限制")
        if self.policy == "option_protected" and constraints and (
            constraints.get("hedges_filled_before_risk") is not True
            or constraints.get("atomic_reconciliation") is not True
        ):
            raise BasketPlanError("必须保留保护腿优先及成交核对约束")
        lookup = {(leg.product, leg.symbol): leg for leg in self._subscriptions[member_id]}
        parsed = {}
        for row in targets:
            if not isinstance(row, dict) or set(row) != {"product", "symbol", "quantity"}:
                raise BasketPlanError("组合目标字段无效")
            leg = lookup.get((row["product"], row["symbol"]))
            if leg is None or leg in parsed:
                raise BasketPlanError("目标包含未知、跨账户或重复的组合腿")
            raw = row["quantity"]
            if isinstance(raw, bool) or not isinstance(raw, (Decimal, int, str)):
                raise BasketPlanError("数量必须为十进制数")
            try:
                quantity = Decimal(str(raw))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise BasketPlanError("组合目标数量格式无效") from exc
            if not quantity.is_finite():
                raise BasketPlanError("组合目标数量非有限 Decimal")
            parsed[leg] = quantity
        if set(parsed) != set(self._subscriptions[member_id]):
            raise BasketPlanError("成员组合目标腿不完整")
        intent = tuple(MemberLegTarget(member_id, leg, parsed[leg]) for leg in sorted(parsed))
        old = self._intents.get(member_id)
        if old is not None:
            if old == (request_id, intent):
                return  # canonical idempotent retry
            raise BasketPlanError("相同成员不能重放不同请求或改变已发布目标")
        if any(old_id == request_id for old_id, _ in self._intents.values()):
            raise BasketPlanError("组合请求标识不能跨成员重用")
        self._intents[member_id] = (request_id, intent)

    def plan(self) -> BasketResearchDecision:
        """Net ALL registered members only after all declared inputs are complete."""
        if self._decision is not None:
            return self._decision
        if not self._subscriptions or set(self._intents) != set(self._subscriptions):
            raise BasketPlanError("仍有成员未发布当前时间点的完整组合目标")
        targets = [
            item for member_id in sorted(self._intents)
            for item in self._intents[member_id][1]
        ]
        if {target.leg for target in targets} != set(self.positions):
            raise BasketPlanError("全部策略成员未覆盖授权组合的所有产品合约腿")
        plan = plan_basket(
            targets, self.positions, self.evidence,
            now_ns=self.now_ns, max_age_ns=self.max_age_ns,
            max_gross_notional=self.max_gross_notional,
            policy=self.policy, max_leg_drift=self.max_leg_drift,
        )
        self._decision = BasketResearchDecision(
            plan=plan, member_count=len(self._subscriptions),
            policy=self.policy, ts_ns=self.now_ns,
        )
        return self._decision

    def research_checkpoint(self, request_id: str) -> BasketCheckpoint:
        """Create journal intent, never transmit orders or manufacture fills."""
        checkpoint = new_checkpoint(self.plan().plan, self.positions)
        return begin_phase(checkpoint, request_id) if checkpoint.pending_phase else checkpoint


class ResearchBasketMember:
    """A member-scoped, non-Nautilus research adapter with the Demo API names."""

    def __init__(self, session: BasketResearchSession, member_id: str) -> None:
        self._session = session
        self.member_id = member_id

    def subscribe_market_bundle(
        self, *, symbol: str | None = None, products: tuple[str, ...] = (),
        data: tuple[str, ...] = (), option_symbols: tuple[str, ...] = (),
        legs: tuple[dict, ...] = (),
    ) -> None:
        self._session._subscribe(
            self.member_id, symbol=symbol, products=products, data=data,
            option_symbols=option_symbols, legs=legs,
        )

    def publish_basket(
        self, *, targets: list[dict], request_id: str | None = None,
        constraints: dict | None = None,
    ) -> None:
        self._session._publish(
            self.member_id, targets=targets, request_id=request_id,
            constraints=constraints,
        )
