"""Engine ownership and replay/live separation for strategy market data."""

from decimal import Decimal

import pytest

from engine.market_data.access.market_data_service import EngineMarketDataService
from engine.market_data.access.subscription_hub import ClosedBar, MarketDataHubError, MarketStream

BTC = MarketStream("BINANCE", "spot", "BTCUSDT", "1m")
ETH = MarketStream("BINANCE", "futures_um", "ETHUSDT", "1m")


def bar(ts_ns, close="100"):
    price = Decimal(close)
    return ClosedBar(ts_ns, price, price + 1, price - 1, price, Decimal(10))


def test_one_engine_deduplicates_live_consumers_and_supports_new_authorized_streams():
    service = EngineMarketDataService()
    service.configure_live((BTC,))
    assert service.subscribe_live("tenant", "trend", (BTC,), lookback_bars=10) == {BTC: 10}
    assert service.subscribe_live("tenant", "hedge", (BTC,), lookback_bars=30) == {BTC: 30}
    service.configure_live((ETH,))
    assert service.subscribe_live("tenant", "basket", (BTC, ETH)) == {BTC: 30, ETH: 1}
    assert service.accept_live_closed_bar(BTC, bar(60))
    assert service.accept_live_closed_bar(ETH, bar(60))
    snapshot = service.live_view(
        "tenant", "basket", now_ns=61,
        positions={BTC: Decimal(1), ETH: Decimal(-1)}, max_age_ns=60,
    )
    assert set(snapshot["markets"]) == {"spot:BTCUSDT", "futures_um:ETHUSDT"}
    assert service.unsubscribe_live("tenant", "hedge") == {BTC: 10, ETH: 1}
    service.unsubscribe_live("tenant", "basket")
    assert service.unsubscribe_live("tenant", "trend") == {}


def test_separate_engine_instances_do_not_share_live_streams_or_consumers():
    first, second = EngineMarketDataService(), EngineMarketDataService()
    first.configure_live((BTC,))
    first.subscribe_live("tenant", "strategy", (BTC,))
    first.accept_live_closed_bar(BTC, bar(60))
    with pytest.raises(MarketDataHubError, match="尚未"):
        second.subscribe_live("tenant", "strategy", (BTC,))
    second.configure_live((BTC,))
    second.subscribe_live("tenant", "strategy", (BTC,))
    with pytest.raises(MarketDataHubError, match="预热"):
        second.live_view(
            "tenant", "strategy", now_ns=61,
            positions={BTC: Decimal(0)}, max_age_ns=60,
        )


def test_backtest_sessions_share_within_run_but_never_leak_into_live():
    service = EngineMarketDataService()
    service.configure_live((BTC,))
    service.subscribe_live("prod", "trader", (BTC,))
    service.accept_live_closed_bar(BTC, bar(60, "110"))
    with service.backtest("run-A", (BTC,)) as replay:
        replay.register("dev", "model", (BTC,))
        replay.register("dev", "hedge", (BTC,))
        assert replay.requirements == {BTC: 1}
        replay.accept_closed_bar(BTC, bar(60, "95"))
        with service.backtest("run-B", (BTC,)) as other:
            other.register("dev", "model", (BTC,))
            with pytest.raises(MarketDataHubError, match="预热"):
                other.view(
                    "dev", "model", now_ns=61,
                    positions={BTC: Decimal(0)}, max_age_ns=60,
                )
            with pytest.raises(MarketDataHubError, match="正在使用"), service.backtest(
                "run-A", (BTC,),
            ):
                pass
        replay_view = replay.view(
            "dev", "model", now_ns=61,
            positions={BTC: Decimal(0)}, max_age_ns=60,
        )
        assert replay_view["markets"]["spot:BTCUSDT"]["price"] == "95"
        live_view = service.live_view(
            "prod", "trader", now_ns=61,
            positions={BTC: Decimal(1)}, max_age_ns=60,
        )
        assert live_view["markets"]["spot:BTCUSDT"]["price"] == "110"
    with service.backtest("run-A", (BTC,)) as new_run:
        new_run.register("dev", "model", (BTC,))
        with pytest.raises(MarketDataHubError, match="预热"):
            new_run.view(
                "dev", "model", now_ns=61,
                positions={BTC: Decimal(0)}, max_age_ns=60,
            )


def test_subscription_without_authorization_or_unsupported_stream_fails_closed():
    service = EngineMarketDataService()
    with pytest.raises(MarketDataHubError, match="尚未"):
        service.subscribe_live("dev", "a", (BTC,))
    service.configure_live((BTC,))
    with pytest.raises(MarketDataHubError, match="未获"):
        service.subscribe_live("dev", "a", (ETH,))
    with pytest.raises(MarketDataHubError, match="未授权"):
        service.live_view("dev", "a", now_ns=61, positions={}, max_age_ns=60)


def test_scope_cleanup_after_error():
    service = EngineMarketDataService()
    with pytest.raises(RuntimeError), service.backtest("run", (BTC,)):
        raise RuntimeError("simulation failed")
    with service.backtest("run", (BTC,)) as replay:
        assert replay.requirements == {}
    service.close()
    assert service.live_requirements == {}
