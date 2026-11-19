"""Generic development basket member coordinator for Nautilus-shaped strategies.

An adapter invokes on_market_bundle only at a complete, known market snapshot.
This module never sends orders and does not claim that cross-venue fills are
atomic. Native Nautilus adapters can supply the same verified snapshots.
"""
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import Protocol

from engine.strategies.basket_research_session import (
    BasketResearchDecision,
    BasketResearchSession,
    ResearchBasketMember,
)
from engine.strategies.basket_targets import BasketPlanError, LegEvidence, LegId

_GENERIC_DATA = ("closed_bars", "quotes", "account_positions")


@dataclass(frozen=True)
class BasketMarketBundle:
    ts_ns: int
    positions: MappingProxyType
    evidence: MappingProxyType
    member_legs: tuple[LegId, ...]


class BasketModel(Protocol):
    def on_market_bundle(
        self, member: ResearchBasketMember, bundle: BasketMarketBundle,
    ) -> None: ...


@dataclass(frozen=True)
class BasketMemberRegistration:
    member_id: str
    legs: tuple[LegId, ...]
    model: BasketModel


class GenericBasketRuntime:
    """Multi-member same-clock barrier, validated scopes and target netting.

    Each member receives a read-only bundle and must publish EVERY subscribed
    leg, including explicitly zero targets. The decision is non-executable.
    """

    def __init__(
        self, *, tenant_id: str, account_id: str,
        members: tuple[BasketMemberRegistration, ...],
        max_age_ns: int, max_gross_notional: Decimal,
    ) -> None:
        if not isinstance(members, tuple) or not 1 <= len(members) <= 16:
            raise BasketPlanError("必须提供 1~16 个组合成员")
        if len({member.member_id for member in members}) != len(members):
            raise BasketPlanError("成员发行身份不得重复")
        if (
            type(max_age_ns) is not int or max_age_ns <= 0
            or not isinstance(max_gross_notional, Decimal)
            or not max_gross_notional.is_finite() or max_gross_notional <= 0
        ):
            raise BasketPlanError("研究组合市场时效或风险预算无效")
        union = set()
        for member in members:
            if (
                not isinstance(member, BasketMemberRegistration)
                or not isinstance(member.member_id, str)
                or not 1 <= len(member.member_id) <= 128
                or not isinstance(member.legs, tuple)
                or not 1 <= len(member.legs) <= 64
                or len(set(member.legs)) != len(member.legs)
                or not all(
                    isinstance(leg, LegId)
                    and (leg.tenant_id, leg.account_id) == (tenant_id, account_id)
                    for leg in member.legs
                )
                or not callable(getattr(member.model, "on_market_bundle", None))
            ):
                raise BasketPlanError("通用组合成员身份或绑定腿无效")
            union.update(member.legs)
        if not 1 <= len(union) <= 64:
            raise BasketPlanError("组合腿超出上限")
        self.tenant_id = tenant_id
        self.account_id = account_id
        self.members = tuple(sorted(members, key=lambda m: m.member_id))
        self.legs = tuple(sorted(union))
        self.max_age_ns = max_age_ns
        self.max_gross_notional = max_gross_notional
        self.last_ns = 0

    def decide(
        self, *, now_ns: int,
        positions: dict[LegId, Decimal],
        evidence: dict[LegId, LegEvidence],
    ) -> BasketResearchDecision:
        """A complete barrier, no partial member publication or stale reuse."""
        if type(now_ns) is not int or now_ns <= self.last_ns:
            raise BasketPlanError("通用组合市场时间必须严格递增")
        if not isinstance(positions, dict) or set(positions) != set(self.legs):
            raise BasketPlanError("缺少完整的组合实际模拟仓位")
        if not isinstance(evidence, dict) or set(evidence) != set(self.legs):
            raise BasketPlanError("组合行情集合必须覆盖所有策略成员的全部腿")
        session = BasketResearchSession(
            tenant_id=self.tenant_id, account_id=self.account_id,
            policy="generic_research", positions=positions, evidence=evidence,
            now_ns=now_ns, max_age_ns=self.max_age_ns,
            max_gross_notional=self.max_gross_notional,
        )
        for registration in self.members:
            member = session.member(registration.member_id)
            member.subscribe_market_bundle(
                products=tuple(sorted({leg.product for leg in registration.legs})),
                data=_GENERIC_DATA, legs=registration.legs,
                option_symbols=tuple(sorted(
                    leg.symbol for leg in registration.legs
                    if leg.product == "options"
                )),
            )
            # A strategy sees all synchronized market inputs but may only
            # publish on its own registered legs via its member token.
            view = BasketMarketBundle(
                ts_ns=now_ns, positions=MappingProxyType(dict(positions)),
                evidence=MappingProxyType(dict(evidence)),
                member_legs=registration.legs,
            )
            registration.model.on_market_bundle(member, view)
        result = session.plan()
        self.last_ns = now_ns
        return result
