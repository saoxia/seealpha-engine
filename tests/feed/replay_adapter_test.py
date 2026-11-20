"""All legacy native historical K-line readers enter isolated Engine scopes."""

from decimal import Decimal
from types import SimpleNamespace

import pytest
from nautilus_trader.model import Bar, BarType

from core.market.instruments import bar_type_for
from engine.feed.market_data_service import EngineMarketDataService
from engine.feed.replay_adapter import (
    replay_bar_batches,
    replay_carry_events,
    replay_protected_evidence,
)
from engine.feed.subscription_hub import MarketDataHubError, MarketStream
from nautilus_support import TestInstrumentProvider

PERP = MarketStream("BINANCE", "futures_um", "ETHUSDT", "1m")
SPOT = MarketStream("BINANCE", "spot", "ETHUSDT", "1m")
PUT = MarketStream("BINANCE", "options", "ETH-241231-1000-P", "1m")
CALL = MarketStream("BINANCE", "options", "ETH-241231-1000-C", "1m")


def _bar(ts_ns):
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    return Bar(
        BarType.from_str(bar_type_for("ETHUSDT", "1m")),
        instrument.make_price(100),
        instrument.make_price(101),
        instrument.make_price(99),
        instrument.make_price(100),
        instrument.make_qty(1),
        ts_ns, ts_ns,
    )


def test_legacy_single_stream_ingresses_the_same_scoped_hub():
    service = EngineMarketDataService()
    original = [_bar(60_000_000_000), _bar(120_000_000_000)]
    with service.backtest("one", (PERP,)) as hub:
        assert list(replay_bar_batches(
            hub, "dev", "issued", PERP, [original],
        )) == [original]
        assert hub.requirements == {PERP: 1}
        with pytest.raises(MarketDataHubError, match="倒退"):
            list(replay_bar_batches(hub, "dev", "issued-again", PERP, [[original[0]]]))
    with service.backtest("another", (PERP,)) as hub:
        assert hub.requirements == {}


def test_legacy_carry_and_option_evidence_both_register_multileg_streams():
    service = EngineMarketDataService()
    bar = _bar(60_000_000_000)
    with service.backtest("carry", (SPOT, PERP)) as hub:
        event = SimpleNamespace(kind="bars", spot=bar, perpetual=bar)
        assert list(replay_carry_events(
            hub, "dev", "carry", SPOT, PERP, [event],
        )) == [event]
        assert hub.requirements == {SPOT: 1, PERP: 1}
    with service.backtest("option", (PERP, PUT, CALL)) as hub:
        evidence = SimpleNamespace(perpetual=bar, put=bar, call=bar)
        assert list(replay_protected_evidence(
            hub, "dev", "protected", (PERP, PUT, CALL), [evidence],
        )) == [evidence]
        assert len(hub.requirements) == 3
