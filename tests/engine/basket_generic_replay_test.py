"""Cross-product strategy composition, BBO replay and separated USDT PnL tests."""
from dataclasses import replace
from decimal import Decimal

import pytest

from engine.strategies.basket_finance_ledger import (
    BasketContract,
    BasketFinanceLedger,
    BasketQuote,
    VerifiedFundingCash,
)
from engine.strategies.basket_generic_replay import BasketReplayFrame, GenericBasketReplay
from engine.strategies.basket_generic_runtime import (
    BasketMemberRegistration,
    GenericBasketRuntime,
)
from engine.strategies.basket_settlement import VerifiedOptionSettlement
from engine.strategies.basket_targets import BasketPlanError, LegEvidence, LegId

_STEP = 60_000_000_000
_START = _STEP * 1000
_T1 = _START + _STEP
_T2 = _START + 2 * _STEP
_END = _START + 3 * _STEP


class StrategyModel:
    """Independent strategy member; only publishes the legs it subscribes."""

    def __init__(self, targets, *, closing_ts=None):
        self.targets = targets
        self.closing_ts = closing_ts
        self.seen = []

    def on_market_bundle(self, member, bundle):
        self.seen.append(bundle.ts_ns)
        assert set(bundle.positions) == set(bundle.evidence)
        with pytest.raises(TypeError):
            bundle.positions[next(iter(bundle.positions))] = Decimal(99)
        member.publish_basket(targets=[
            {
                "product": leg.product, "symbol": leg.symbol,
                "quantity": format(
                    Decimal(0) if self.closing_ts == bundle.ts_ns else quantity,
                    "f",
                ),
            }
            for leg, quantity in self.targets.items()
        ])


def _contract(leg, fee="0.001", *, expiry=None, strike=None):
    return BasketContract(
        leg, Decimal("0.1"), Decimal(1), Decimal(fee),
        initial_margin_ratio=Decimal("0.2") if leg.product == "futures_um"
        else Decimal(0),
        expiry_ns=expiry, strike=strike,
    )


def _quote(leg, stamp, bid, ask, mark, *, bid_size="10", ask_size="10"):
    return BasketQuote(
        leg, Decimal(str(bid)), Decimal(str(ask)),
        Decimal(bid_size), Decimal(ask_size), Decimal(str(mark)), stamp,
    )


def _carry(*, spot_ask_size="10", funding=True):
    spot = LegId("dev", "account", "spot", "ETHUSDT")
    um = LegId("dev", "account", "futures_um", "ETHUSDT")
    first = StrategyModel({spot: Decimal("0.2"), um: Decimal("-0.2")})
    second = StrategyModel({spot: Decimal("0.1"), um: Decimal("0.1")})
    runtime = GenericBasketRuntime(
        tenant_id="dev", account_id="account",
        members=(
            BasketMemberRegistration("alpha", (spot, um), first),
            BasketMemberRegistration("beta", (um, spot), second),
        ),
        max_age_ns=10_000_000_000, max_gross_notional=Decimal(500),
    )
    ledger = BasketFinanceLedger(
        (_contract(spot), _contract(um)),
        {"spot": Decimal(1000), "futures_um": Decimal(1000)},
    )
    replay = GenericBasketReplay(
        runtime, ledger, step_ns=_STEP, max_quote_age_ns=10_000_000_000,
        slippage_bps=Decimal(0),
        expected_funding={_T2: (um,)} if funding else None,
    )
    frame1 = BasketReplayFrame(_T1, (
        _quote(spot, _T1, 100, 101, 100, ask_size=spot_ask_size),
        _quote(um, _T1, 100, 101, 100),
    ))
    rate = VerifiedFundingCash(
        um, "funding-event-2", _T2, Decimal("0.001"), Decimal("0.011"),
    )
    frame2 = BasketReplayFrame(
        _T2, (_quote(spot, _T2, 110, 111, 110),
              _quote(um, _T2, 110, 111, 110)),
        (rate,) if funding else (),
    )
    return replay, (frame1, frame2), (spot, um), (first, second)


def test_two_independent_models_publish_same_clock_and_cash_ledger_reconciles():
    replay, frames, (spot, um), models = _carry()
    result = replay.run(frames, start_ns=_START, end_ns=_END)
    assert result["status"] == "completed"
    assert result["executable"] is False
    assert result["bar_count"] == 2
    assert result["member_count"] == 2
    assert result["funding_schedule_verified"] is True
    assert models[0].seen == [_T1, _T2]
    assert models[1].seen == [_T1, _T2]
    assert result["target_events"][0]["targets"] == [
        {"product": "futures_um", "symbol": "ETHUSDT", "quantity": "-0.1"},
        {"product": "spot", "symbol": "ETHUSDT", "quantity": "0.3"},
    ]
    assert len(result["fills"]) == 2
    assert result["ending_balance"] == "2001.6707"
    assert result["equity_curve"][-1]["total_pnl"] == "1.6707"
    assert result["equity_curve"][-1]["positions"][
        f"{spot.tenant_id}/{spot.account_id}/spot/ETHUSDT"
    ] == "0.3"
    assert result["equity_curve"][-1]["positions"][
        f"{um.tenant_id}/{um.account_id}/futures_um/ETHUSDT"
    ] == "-0.1"
    assert result["cashflow_events"][0]["cash_delta"] == "0.011"


def test_partial_bbo_fill_freezes_only_reconciled_simulated_quantities():
    replay, frames, (spot, _um), _models = _carry(spot_ask_size="0.15")
    result = replay.run(frames, start_ns=_START, end_ns=_END)
    assert result["status"] == "frozen"
    assert "部分成交" in result["freeze_reason"]
    assert result["bar_count"] == 1
    assert replay.ledger.positions[spot] == Decimal("0.1")
    assert len(result["fills"]) == 2
    assert result["funding_schedule_verified"] is False
    with pytest.raises(BasketPlanError, match="只允许启动一次"):
        replay.run(frames, start_ns=_START, end_ns=_END)


def test_missing_or_spoofed_funding_and_history_gap_are_not_extrapolated():
    replay, frames, (_spot, um), _models = _carry()
    missing_funding = (frames[0], replace(frames[1], funding=()))
    with pytest.raises(BasketPlanError, match="结算清单"):
        replay.run(missing_funding, start_ns=_START, end_ns=_END)
    replay, frames, _legs, _models = _carry()
    duplicated = (frames[0], replace(frames[1], ts_ns=_T1))
    with pytest.raises(BasketPlanError, match="时间线"):
        replay.run(duplicated, start_ns=_START, end_ns=_END)
    replay, frames, _legs, _models = _carry()
    bad_cash = VerifiedFundingCash(
        um, "funding-event-2", _T2, Decimal("0.001"), Decimal(99),
    )
    bad = (frames[0], replace(frames[1], funding=(bad_cash,)))
    result = replay.run(bad, start_ns=_START, end_ns=_END)
    assert result["status"] == "frozen"
    assert "资金费" in result["freeze_reason"]
    assert result["bar_count"] >= 1


def test_generic_members_must_publish_their_complete_registered_leg_set():
    spot = LegId("dev", "account", "spot", "ETHUSDT")
    um = LegId("dev", "account", "futures_um", "ETHUSDT")
    class IncompleteModel:
        def on_market_bundle(self, member, bundle):
            member.publish_basket(targets=[
                {"product": "spot", "symbol": "ETHUSDT", "quantity": "0.1"},
            ])
    runtime = GenericBasketRuntime(
        tenant_id="dev", account_id="account",
        members=(BasketMemberRegistration("bad", (spot, um), IncompleteModel()),),
        max_age_ns=100, max_gross_notional=Decimal(1000),
    )
    with pytest.raises(BasketPlanError, match="所有腿|数量|目标"):
        runtime.decide(
            now_ns=_T1,
            positions={spot: Decimal(0), um: Decimal(0)},
            evidence={
                spot: LegEvidence(Decimal(100), Decimal("0.1"), Decimal(1), _T1),
                um: LegEvidence(Decimal(100), Decimal("0.1"), Decimal(1), _T1),
            },
        )


def test_options_premium_exercise_and_cash_record_exactly_reconcile():
    put = LegId("dev", "account", "options", "ETH-261225-1800-P")
    call = LegId("dev", "account", "options", "ETH-261225-2200-C")
    model = StrategyModel(
        {put: Decimal("0.2"), call: Decimal("0.2")},
        closing_ts=_T2,
    )
    runtime = GenericBasketRuntime(
        tenant_id="dev", account_id="account",
        members=(BasketMemberRegistration("options-alpha", (put, call), model),),
        max_age_ns=1000, max_gross_notional=Decimal(1000),
    )
    ledger = BasketFinanceLedger(
        (_contract(put, fee="0", expiry=_T2, strike=Decimal(1800)),
         _contract(call, fee="0", expiry=_T2, strike=Decimal(2200))),
        {"options": Decimal(100)},
    )
    replay = GenericBasketReplay(
        runtime, ledger, step_ns=_STEP, max_quote_age_ns=1000,
        slippage_bps=Decimal(0), expected_funding={},
    )
    first = BasketReplayFrame(_T1, (
        _quote(put, _T1, "1.9", 2, 2),
        _quote(call, _T1, "0.9", 1, 1),
    ))
    settlements = (
        VerifiedOptionSettlement(
            put, "put-exercise", _T2, Decimal(1700), Decimal("0.2"),
            Decimal(20), Decimal(2), Decimal(18),
        ),
        VerifiedOptionSettlement(
            call, "call-exercise", _T2, Decimal(1700), Decimal("0.2"),
            Decimal(0), Decimal(0), Decimal(0),
        ),
    )
    last = BasketReplayFrame(
        _T2, (
            _quote(put, _T2, "0.01", "0.02", "0.02"),
            _quote(call, _T2, "0.01", "0.02", "0.02"),
        ),
        option_settlements=settlements,
        options_cash_after=Decimal("117.4"),
    )
    result = replay.run((first, last), start_ns=_START, end_ns=_END)
    assert result["status"] == "completed"
    assert result["verified_option_exercise_count"] == 2
    assert result["ending_balance"] == "117.4"
    assert result["equity_curve"][-1]["total_pnl"] == "17.4"
    assert len(result["cashflow_events"]) == 2


def test_absent_real_exercise_evidence_freezes_without_synthetic_payout():
    put = LegId("dev", "account", "options", "ETH-261225-1800-P")
    model = StrategyModel({put: Decimal("0.1")}, closing_ts=_T2)
    runtime = GenericBasketRuntime(
        tenant_id="dev", account_id="account",
        members=(BasketMemberRegistration("put", (put,), model),),
        max_age_ns=1000, max_gross_notional=Decimal(1000),
    )
    ledger = BasketFinanceLedger(
        (_contract(put, fee="0", expiry=_T2, strike=Decimal(1800)),),
        {"options": Decimal(100)},
    )
    replay = GenericBasketReplay(
        runtime, ledger, step_ns=_STEP, max_quote_age_ns=1000,
        slippage_bps=Decimal(0),
    )
    frames = (
        BasketReplayFrame(_T1, (_quote(put, _T1, 2, 2, 2),)),
        BasketReplayFrame(_T2, (_quote(put, _T2, 2, 2, 2),)),
    )
    result = replay.run(frames, start_ns=_START, end_ns=_END)
    assert result["status"] == "frozen"
    assert "期权到期" in result["freeze_reason"]
    assert result["ending_balance"] != "100"  # premium was paid, no invented expiry income
