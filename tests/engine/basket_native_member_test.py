"""Native Nautilus Strategy callbacks publish only through one scoped barrier."""
from decimal import Decimal

import pytest
from nautilus_trader.backtest import BacktestEngine
from nautilus_trader.model import (
    AccountType,
    Bar,
    BarType,
    Money,
    OmsType,
    Price,
    Quantity,
    Venue,
)

from core.market.instruments import bar_type_for
from engine.strategies.backtest_engine import backtest_engine_config
from engine.strategies.basket_generic_runtime import (
    BasketMemberRegistration,
    GenericBasketRuntime,
)
from engine.strategies.basket_native_member import (
    BasketResearchStrategy,
    NativeBasketBarGate,
)
from engine.strategies.basket_targets import (
    BasketPlanError,
    LegEvidence,
    LegId,
)
from engine.strategies.strategy_config import StrategyConfig
from nautilus_support import TestInstrumentProvider, instrument_fee_model

_TS = 60_000_000_000 * 1001


class NativeMember(BasketResearchStrategy):
    def __init__(self, *, identity=None):
        config = (
            StrategyConfig(order_id_tag="BASKET", strategy_id=identity)
            if identity is not None else StrategyConfig(order_id_tag="BASKET")
        )
        super().__init__(config)
        self.calls = []

    def on_basket(self, bundle):
        self.calls.append(bundle.ts_ns)
        self.publish_basket(targets=[
            {"product": leg.product, "symbol": leg.symbol, "quantity": "0.1"}
            for leg in bundle.member_legs
        ])


def _bar(bar_type, ts_ns):
    return Bar(
        bar_type, Price.from_str("100"), Price.from_str("100"),
        Price.from_str("100"), Price.from_str("100"),
        Quantity.from_str("1000"), ts_ns, ts_ns,
    )


def _gate():
    spot = LegId("tenant", "account", "spot", "ETHUSDT")
    perp = LegId("tenant", "account", "futures_um", "ETHUSDT")
    alpha, beta = NativeMember(), NativeMember()
    runtime = GenericBasketRuntime(
        tenant_id="tenant", account_id="account",
        members=(
            BasketMemberRegistration("alpha", (spot,), alpha),
            BasketMemberRegistration("beta", (perp,), beta),
        ),
        max_age_ns=1000, max_gross_notional=Decimal(1000),
    )
    seen = []
    def provider(ts, bars):
        seen.append((ts, len(bars)))
        return (
            {spot: Decimal(0), perp: Decimal(0)},
            {
                spot: LegEvidence(Decimal(100), Decimal("0.1"), Decimal(1), ts),
                perp: LegEvidence(Decimal(100), Decimal("0.1"), Decimal(1), ts),
            },
        )

    gate = NativeBasketBarGate(runtime, provider)
    spot_type = BarType.from_str(bar_type_for("ETHUSDT", "1m", "spot"))
    perp_type = BarType.from_str(bar_type_for("ETHUSDT", "1m", "futures_um"))
    alpha.bind_basket_gate(gate, "alpha", (spot_type,))
    beta.bind_basket_gate(gate, "beta", (perp_type,))
    return gate, alpha, beta, spot_type, perp_type, seen


def test_native_strategy_bar_barrier_waits_until_all_members_share_one_timestamp():
    gate, alpha, beta, spot_type, perp_type, observed = _gate()
    assert not gate.decisions
    alpha.on_bar(_bar(spot_type, _TS))
    assert not gate.decisions
    beta.on_bar(_bar(perp_type, _TS))
    assert observed == [(_TS, 2)]
    assert len(gate.decisions) == 1
    assert gate.decisions[0].member_count == 2
    assert dict(gate.decisions[0].plan.targets) == {
        LegId("tenant", "account", "spot", "ETHUSDT"): Decimal("0.1"),
        LegId("tenant", "account", "futures_um", "ETHUSDT"): Decimal("0.1"),
    }
    assert alpha.calls == [_TS] and beta.calls == [_TS]
    assert not gate.decisions[0].executable
    with pytest.raises(BasketPlanError, match="只有"):
        alpha.publish_basket(targets=[])


def test_native_strategy_rejects_duplicate_and_lookahead_bar_or_wrong_member():
    _unused, alpha, beta, spot_type, perp_type, _seen = _gate()
    alpha.on_bar(_bar(spot_type, _TS))
    with pytest.raises(BasketPlanError, match="重复"):
        alpha.on_bar(_bar(spot_type, _TS))
    _unused, alpha, beta, spot_type, perp_type, _seen = _gate()
    alpha.on_bar(_bar(spot_type, _TS))
    with pytest.raises(BasketPlanError, match="前一时间戳"):
        beta.on_bar(_bar(perp_type, _TS + 60_000_000_000))
    _unused, alpha, beta, spot_type, perp_type, _seen = _gate()
    with pytest.raises(BasketPlanError, match="未声明"):
        alpha.on_bar(_bar(perp_type, _TS))


def test_actual_nautilus_backtest_engine_dispatches_two_strategy_lifecycles():
    """Registered Nautilus strategies see the same native Bar exactly once."""
    perp = TestInstrumentProvider.ethusdt_perp_binance()
    leg = LegId("tenant", "account", "futures_um", "ETHUSDT")
    a, b = NativeMember(identity="BasketAlpha-1"), NativeMember(identity="BasketBeta-1")
    runtime = GenericBasketRuntime(
        tenant_id="tenant", account_id="account",
        members=(
            BasketMemberRegistration("member-a", (leg,), a),
            BasketMemberRegistration("member-b", (leg,), b),
        ),
        max_age_ns=1000, max_gross_notional=Decimal(1000),
    )
    def source(ts, _bars):
        return (
            {leg: Decimal(0)},
            {leg: LegEvidence(Decimal(100), Decimal("0.1"), Decimal(1), ts)},
        )

    gate = NativeBasketBarGate(runtime, source)
    bar_type = BarType.from_str(bar_type_for("ETHUSDT", "1m", "futures_um"))
    a.bind_basket_gate(gate, "member-a", (bar_type,))
    b.bind_basket_gate(gate, "member-b", (bar_type,))
    engine = BacktestEngine(backtest_engine_config())
    try:
        engine.add_venue(
            Venue("BINANCE"), OmsType.NETTING, AccountType.MARGIN,
            [Money.from_str("1000 USDT")],
            fee_model=instrument_fee_model(perp), default_leverage=Decimal(5),
        )
        engine.add_instrument(perp)
        engine.add_strategy(a)
        engine.add_strategy(b)
        engine.add_data([
            _bar(bar_type, _TS), _bar(bar_type, _TS + 60_000_000_000),
        ], sort=True)
        engine.run()
        engine.end()
        assert a.calls == [_TS, _TS + 60_000_000_000]
        assert b.calls == a.calls
        assert len(gate.decisions) == 2
        assert all(not decision.executable for decision in gate.decisions)
        assert dict(gate.decisions[-1].plan.targets)[leg] == Decimal("0.2")
    finally:
        engine.dispose()
