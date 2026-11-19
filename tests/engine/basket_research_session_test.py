"""Offline research basket publication contracts; no venue or synthetic PnL."""
from decimal import Decimal

import pytest

from engine.strategies.basket_reconciliation import (
    BasketReconciliationError,
    begin_phase,
    checkpoint_to_record,
    confirm_phase,
    freeze_checkpoint,
    record_cumulative_fill,
    restore_checkpoint,
)
from engine.strategies.basket_research_session import BasketResearchSession
from engine.strategies.basket_targets import BasketPlanError, LegEvidence, LegId

_NOW = 1_000_000_000_000


def _carry(*, now=_NOW, max_age=100_000, positions=None):
    spot = LegId("development", "virtual", "spot", "ETHUSDT")
    perp = LegId("development", "virtual", "futures_um", "ETHUSDT")
    legs = (spot, perp)
    current = positions or {leg: Decimal(0) for leg in legs}
    quotes = {
        leg: LegEvidence(Decimal(2000), Decimal("0.1"), Decimal(1), now - 1000)
        for leg in legs
    }
    return BasketResearchSession(
        tenant_id="development", account_id="virtual", policy="funding_carry",
        positions=current, evidence=quotes,
        now_ns=now, max_age_ns=max_age,
        max_gross_notional=Decimal(5000),
        max_leg_drift=Decimal("0.1"),
    ), spot, perp


def _subscribe_carry(member):
    member.subscribe_market_bundle(
        symbol="ETHUSDT", products=("spot", "futures_um"),
        data=("closed_bars", "funding_rate", "account_positions"),
    )


def _publish_carry(member, quantity, *, request_id=None):
    member.publish_basket(
        targets=[
            {"product": "spot", "symbol": "ETHUSDT", "quantity": quantity},
            {"product": "futures_um", "symbol": "ETHUSDT",
             "quantity": -Decimal(str(quantity))},
        ],
        request_id=request_id,
        constraints={"atomic_hedge": True, "max_leg_drift": "0.1"},
    )


def test_two_members_net_only_identical_account_product_instrument():
    session, spot, perp = _carry()
    first, second = session.member("alpha"), session.member("beta")
    for member in (first, second):
        _subscribe_carry(member)
    _publish_carry(first, Decimal("0.3"), request_id="alpha-snapshot-1")
    _publish_carry(second, Decimal("0.2"), request_id="beta-snapshot-1")
    decision = session.plan()
    assert decision.member_count == 2
    assert decision.policy == "funding_carry" and not decision.executable
    assert not decision.plan.executable
    assert dict(decision.plan.targets) == {
        spot: Decimal("0.5"), perp: Decimal("-0.5"),
    }
    assert len(decision.plan.phases) == 1
    assert session.plan() == decision  # determinism
    with pytest.raises(BasketPlanError, match="锁定"):
        _publish_carry(first, Decimal("0.3"), request_id="retry-late")
    assert not hasattr(session, "submit_order") and not hasattr(first, "submit_order")


def test_missing_member_and_incomplete_evidence_fail_closed():
    session, _, _ = _carry()
    a, b = session.member("a"), session.member("b")
    _subscribe_carry(a)
    _subscribe_carry(b)
    _publish_carry(a, Decimal("0.1"))
    with pytest.raises(BasketPlanError, match="成员未发布"):
        session.plan()
    with pytest.raises(BasketPlanError, match="所有腿"):
        a.publish_basket(targets=[], request_id="bad")
    _publish_carry(b, Decimal("0.1"))
    assert session.plan().member_count == 2


def test_idempotent_research_publication_never_replaces_other_member():
    session, _, _ = _carry()
    first, second = session.member("one"), session.member("two")
    _subscribe_carry(first)
    _subscribe_carry(second)
    _publish_carry(first, Decimal("0.1"), request_id="shared")
    _publish_carry(first, Decimal("0.1"), request_id="shared")
    with pytest.raises(BasketPlanError, match="不同请求|改变"):
        _publish_carry(first, Decimal("0.2"), request_id="shared")
    with pytest.raises(BasketPlanError, match="跨成员"):
        _publish_carry(second, Decimal("0.1"), request_id="shared")
    _publish_carry(second, Decimal("0.1"), request_id="other")
    assert session.plan().member_count == 2


def test_wrong_leg_cross_account_unsafe_constraints_and_stale_quotes():
    session, _, _ = _carry()
    member = session.member("x")
    with pytest.raises(BasketPlanError, match="历史订阅"):
        _publish_carry(member, Decimal("0.1"))
    with pytest.raises(BasketPlanError, match="订阅类型"):
        member.subscribe_market_bundle(
            symbol="ETHUSDT", products=("spot",),
            data=("closed_bars", "funding_rate", "account_positions"),
        )
    _subscribe_carry(member)
    with pytest.raises(BasketPlanError, match="未知多腿"):
        member.publish_basket(
            targets=[
                {"product": "spot", "symbol": "ETHUSDT", "quantity": "0.1"},
                {"product": "futures_um", "symbol": "ETHUSDT", "quantity": "-0.1"},
            ], request_id="no", constraints={"skip_risk": True},
        )
    with pytest.raises(BasketPlanError, match="未知"):
        member.publish_basket(
            targets=[
                {"product": "spot", "symbol": "BTCUSDT", "quantity": "0.1"},
                {"product": "futures_um", "symbol": "ETHUSDT", "quantity": "-0.1"},
            ], request_id="wrong",
        )
    bad, spot, perp = _carry()
    with pytest.raises(BasketPlanError, match="不属于"):
        BasketResearchSession(
            tenant_id="another", account_id="virtual", policy="funding_carry",
            positions=bad.positions, evidence=bad.evidence,
            now_ns=_NOW, max_age_ns=10_000, max_gross_notional=Decimal(5000),
        )
    with pytest.raises(BasketPlanError, match="过期"):
        BasketResearchSession(
            tenant_id="development", account_id="virtual", policy="funding_carry",
            positions={spot: Decimal(0), perp: Decimal(0)},
            evidence={spot: bad.evidence[spot], perp: bad.evidence[perp]},
            now_ns=_NOW + 20_000, max_age_ns=1000,
            max_gross_notional=Decimal(5000),
        )


def test_option_two_members_require_complete_wings_and_coverage():
    perp = LegId("development", "virtual", "futures_um", "ETHUSDT")
    put = LegId("development", "virtual", "options", "ETH-261225-1800-P")
    call = LegId("development", "virtual", "options", "ETH-261225-2200-C")
    positions = {leg: Decimal(0) for leg in (perp, put, call)}
    evidence = {
        leg: LegEvidence(
            Decimal(2) if leg.product == "options" else Decimal(2000),
            Decimal("0.1"), Decimal(1), _NOW - 1000,
        ) for leg in positions
    }
    session = BasketResearchSession(
        tenant_id="development", account_id="virtual",
        policy="option_protected", positions=positions, evidence=evidence,
        now_ns=_NOW, max_age_ns=10_000, max_gross_notional=Decimal(2500),
    )
    for identifier in ("x", "y"):
        member = session.member(identifier)
        member.subscribe_market_bundle(
            symbol="ETHUSDT", products=("futures_um", "options"),
            option_symbols=(put.symbol, call.symbol),
            data=("closed_bars", "option_quotes", "option_instruments", "positions"),
        )
        member.publish_basket(
            targets=[
                {"product": "futures_um", "symbol": perp.symbol, "quantity": "0.2"},
                {"product": "options", "symbol": put.symbol, "quantity": "0.2"},
                {"product": "options", "symbol": call.symbol, "quantity": "0.2"},
            ],
            constraints={"hedges_filled_before_risk": True, "atomic_reconciliation": True},
        )
    plan = session.plan().plan
    assert dict(plan.targets) == {
        perp: Decimal("0.4"), put: Decimal("0.4"), call: Decimal("0.4"),
    }
    assert [phase.name for phase in plan.phases] == [
        "acquire_and_confirm_protection", "increase_directional_risk_after_protection",
    ]


def test_partial_fill_restart_keeps_phase_unresolved_and_freezable():
    session, spot, perp = _carry()
    publisher = session.member("carry")
    _subscribe_carry(publisher)
    _publish_carry(publisher, Decimal("0.3"))
    checkpoint = session.research_checkpoint("phase-1")
    assert checkpoint.status == "awaiting_confirmation"
    checkpoint = record_cumulative_fill(
        checkpoint, request_id="phase-1", event_id="fill-1",
        leg=spot, cumulative=Decimal("0.1"),
    )
    resumed = restore_checkpoint(session.plan().plan, checkpoint_to_record(checkpoint))
    assert resumed.status == "awaiting_confirmation"
    assert resumed.pending_phase is not None
    with pytest.raises(BasketReconciliationError, match="未完全成交"):
        confirm_phase(
            resumed, request_id="phase-1",
            terminal_statuses={spot: "FILLED", perp: "FILLED"},
            observed_positions={spot: Decimal("0.1"), perp: Decimal(0)},
        )
    frozen = freeze_checkpoint(resumed, "unknown venue state after restart")
    assert restore_checkpoint(session.plan().plan, checkpoint_to_record(frozen)) == frozen
    with pytest.raises(BasketReconciliationError, match="冻结"):
        begin_phase(frozen, "blind-resend")


def test_noop_plan_is_completed_and_never_sends_orders():
    session, _, _ = _carry()
    member = session.member("flat")
    _subscribe_carry(member)
    _publish_carry(member, Decimal(0))
    report = session.plan()
    assert report.plan.phases == ()
    assert session.research_checkpoint("unused").status == "completed"


def test_generic_session_supports_multiple_symbols_without_hedge_policy():
    eth_spot = LegId("development", "virtual", "spot", "ETHUSDT")
    btc_um = LegId("development", "virtual", "futures_um", "BTCUSDT")
    sol_um = LegId("development", "virtual", "futures_um", "SOLUSDT")
    positions = {leg: Decimal(0) for leg in (eth_spot, btc_um, sol_um)}
    evidence = {
        leg: LegEvidence(Decimal(100), Decimal("0.1"), Decimal(1), _NOW)
        for leg in positions
    }
    session = BasketResearchSession(
        tenant_id="development", account_id="virtual", policy="generic",
        positions=positions, evidence=evidence, now_ns=_NOW,
        max_age_ns=1000, max_gross_notional=Decimal(1000),
    )
    legs = tuple(
        {"product": leg.product, "symbol": leg.symbol}
        for leg in sorted(positions)
    )
    for member_id, btc in (("alpha", "-0.2"), ("beta", "0.1")):
        member = session.member(member_id)
        member.subscribe_market_bundle(
            legs=legs, data=("closed_bars", "account_positions"),
        )
        member.publish_basket(
            targets=[
                {"product": leg.product, "symbol": leg.symbol,
                 "quantity": btc if leg == btc_um else "0.2" if leg == eth_spot else "0"}
                for leg in sorted(positions)
            ],
        )
    result = session.plan()
    assert result.member_count == 2 and not result.executable
    assert dict(result.plan.targets) == {
        eth_spot: Decimal("0.4"), btc_um: Decimal("-0.1"), sol_um: Decimal(0),
    }
    assert result.plan.phases[0].name == "increase_new_exposure"
    assert session.research_checkpoint("multi-snapshot-1").status == "awaiting_confirmation"


def test_generic_session_rejects_missing_or_forged_market_leg():
    eth = LegId("development", "virtual", "spot", "ETHUSDT")
    btc = LegId("development", "virtual", "futures_um", "BTCUSDT")
    positions = {eth: Decimal(0), btc: Decimal(0)}
    session = BasketResearchSession(
        tenant_id="development", account_id="virtual", policy="generic",
        positions=positions,
        evidence={leg: LegEvidence(Decimal(10), Decimal("0.1"), Decimal(1), _NOW)
                  for leg in positions},
        now_ns=_NOW, max_age_ns=1000,
        max_gross_notional=Decimal(100),
    )
    member = session.member("one")
    with pytest.raises(BasketPlanError, match="结构"):
        member.subscribe_market_bundle(
            legs=({"product": ["spot"], "symbol": "ETHUSDT"},),
            data=("closed_bars", "account_positions"),
        )
    member.subscribe_market_bundle(
        legs=({"product": "spot", "symbol": "ETHUSDT"},
              {"product": "futures_um", "symbol": "BTCUSDT"}),
        data=("closed_bars", "account_positions"),
    )
    with pytest.raises(BasketPlanError, match="未知多腿"):
        member.publish_basket(
            targets=[
                {"product": "spot", "symbol": "ETHUSDT", "quantity": "0"},
                {"product": "futures_um", "symbol": "BTCUSDT", "quantity": "0"},
            ],
            constraints={"skip_risk": True},
        )


def test_generic_independent_members_can_publish_disjoint_instrument_subsets():
    eth = LegId("development", "virtual", "spot", "ETHUSDT")
    btc = LegId("development", "virtual", "futures_um", "BTCUSDT")
    positions = {eth: Decimal(0), btc: Decimal(0)}
    evidence = {
        leg: LegEvidence(Decimal(100), Decimal("0.1"), Decimal(1), _NOW)
        for leg in positions
    }
    session = BasketResearchSession(
        tenant_id="development", account_id="virtual", policy="generic",
        positions=positions, evidence=evidence,
        now_ns=_NOW, max_age_ns=1000, max_gross_notional=Decimal(1000),
    )
    first = session.member("eth-trend")
    first.subscribe_market_bundle(
        legs=({"product": "spot", "symbol": "ETHUSDT"},),
        data=("closed_bars", "account_positions"),
    )
    first.publish_basket(
        targets=[{"product": "spot", "symbol": "ETHUSDT", "quantity": "0.2"}],
    )
    with pytest.raises(BasketPlanError, match="未覆盖"):
        session.plan()
    second = session.member("btc-reversion")
    second.subscribe_market_bundle(
        legs=({"product": "futures_um", "symbol": "BTCUSDT"},),
        data=("closed_bars", "account_positions"),
    )
    second.publish_basket(
        targets=[{"product": "futures_um", "symbol": "BTCUSDT", "quantity": "-0.3"}],
    )
    assert dict(session.plan().plan.targets) == {
        eth: Decimal("0.2"), btc: Decimal("-0.3"),
    }
