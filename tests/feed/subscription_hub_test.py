"""One shared market series, multiple independent registered strategy consumers."""

from decimal import Decimal

import pytest

from engine.feed.subscription_hub import (
    ClosedBar,
    MarketDataHubError,
    MarketStream,
    SharedMarketData,
)

BTC = MarketStream("BINANCE", "spot", "BTCUSDT", "1m")
ETH = MarketStream("BINANCE", "futures_um", "ETHUSDT", "1m")


def bar(ts_ns, close="100"):
    price = Decimal(close)
    return ClosedBar(
        ts_ns, price, price + 1, price - 1, price, Decimal(10),
    )


def test_deduplicates_upstream_market_and_tracks_max_lookback_and_release():
    hub = SharedMarketData((BTC, ETH))
    hub.register("alpha", "trend", (BTC, ETH), lookback_bars=20)
    hub.register("alpha", "hedge", (BTC,), lookback_bars=40)
    hub.register("beta", "model", (BTC,), lookback_bars=3)
    hub.register("alpha", "trend", (ETH, BTC), lookback_bars=20)
    assert hub.requirements == {BTC: 40, ETH: 20}
    hub.accept_closed_bar(BTC, bar(60))
    hub.accept_closed_bar(ETH, bar(60))
    a = hub.view(
        "alpha", "hedge", now_ns=61,
        positions={BTC: Decimal(1)}, max_age_ns=60,
    )
    b = hub.view(
        "beta", "model", now_ns=61,
        positions={BTC: Decimal(2)}, max_age_ns=60,
    )
    assert a["history"]["spot:BTCUSDT"] == b["history"]["spot:BTCUSDT"]
    assert a["positions"]["spot:BTCUSDT"] == "1"
    assert b["positions"]["spot:BTCUSDT"] == "2"
    a["history"]["spot:BTCUSDT"][0]["close"] = "999"
    assert hub.view(
        "beta", "model", now_ns=61,
        positions={BTC: Decimal(2)}, max_age_ns=60,
    )["history"]["spot:BTCUSDT"][0]["close"] == "100"
    hub.release("alpha", "hedge")
    assert hub.requirements[BTC] == 20
    hub.release("alpha", "trend")
    assert hub.requirements == {BTC: 3}
    hub.release("beta", "model")
    hub.release("beta", "model")
    assert hub.requirements == {}
    with pytest.raises(MarketDataHubError, match="未订阅"):
        hub.accept_closed_bar(BTC, bar(120))


def test_market_key_prevents_cross_venue_product_or_timeframe_aliasing():
    assert len({
        BTC,
        MarketStream("BINANCE", "futures_um", "BTCUSDT", "1m"),
        MarketStream("OTHER", "spot", "BTCUSDT", "1m"),
        MarketStream("BINANCE", "spot", "BTCUSDT", "5m"),
        MarketStream("BINANCE", "spot", "BTCUSDT", "event", "trade_ticks"),
    }) == 5
    hub = SharedMarketData((BTC,))
    with pytest.raises(MarketDataHubError, match="未获"):
        hub.register("tenant", "other", (ETH,), lookback_bars=1)
    with pytest.raises(MarketDataHubError, match="订阅"):
        hub.register("tenant", "bad", (BTC, BTC))
    with pytest.raises(MarketDataHubError, match="订阅"):
        hub.register("tenant", "bad", (BTC,), lookback_bars=True)
    hub.register("tenant", "ok", (BTC,))
    with pytest.raises(MarketDataHubError, match="更换"):
        hub.register("tenant", "ok", (BTC,), lookback_bars=2)
    with pytest.raises(MarketDataHubError, match="未授权"):
        hub.view("other", "ok", now_ns=100, positions={}, max_age_ns=60)


def test_closed_bar_delivery_is_idempotent_but_conflicts_fail_closed():
    hub = SharedMarketData((BTC,))
    hub.register("dev", "one", (BTC,))
    initial = bar(60)
    assert hub.accept_closed_bar(BTC, initial)
    assert not hub.accept_closed_bar(BTC, initial)
    for invalid in (bar(60, "101"), bar(59)):
        with pytest.raises(MarketDataHubError):
            hub.accept_closed_bar(BTC, invalid)
    with pytest.raises(MarketDataHubError, match="OHLCV"):
        bar(61, "0.5")
    assert hub.accept_closed_bar(BTC, bar(120))
    with pytest.raises(MarketDataHubError, match="过期"):
        hub.view("dev", "one", now_ns=200, positions={BTC: Decimal(0)}, max_age_ns=60)
    with pytest.raises(MarketDataHubError, match="未来"):
        hub.view("dev", "one", now_ns=120, positions={BTC: Decimal(0)}, max_age_ns=60)
    with pytest.raises(MarketDataHubError, match="证据"):
        hub.view("dev", "one", now_ns=121, positions={}, max_age_ns=60)


def test_joint_view_requires_same_closed_timestamp_and_only_declared_legs():
    hub = SharedMarketData((BTC, ETH))
    hub.register("dev", "pair", (BTC, ETH), lookback_bars=2)
    hub.register("dev", "single", (BTC,))
    hub.accept_closed_bar(BTC, bar(60))
    hub.accept_closed_bar(ETH, bar(60))
    hub.accept_closed_bar(BTC, bar(120))
    positions = {BTC: Decimal(1), ETH: Decimal(-1)}
    with pytest.raises(MarketDataHubError, match="未对齐"):
        hub.view("dev", "pair", now_ns=121, positions=positions, max_age_ns=120)
    view = hub.view("dev", "single", now_ns=121, positions=positions, max_age_ns=120)
    assert set(view["markets"]) == {"spot:BTCUSDT"}
    assert set(view["positions"]) == {"spot:BTCUSDT"}
    hub.accept_closed_bar(ETH, bar(120))
    joined = hub.view("dev", "pair", now_ns=121, positions=positions, max_age_ns=120)
    assert all(len(rows) <= 2 for rows in joined["history"].values())
    assert all(rows[-1]["ts_ns"] < joined["now_ns"] for rows in joined["history"].values())


def test_ring_buffer_retains_only_up_to_max_bars_and_numeric_quality():
    with pytest.raises(MarketDataHubError):
        ClosedBar(60, Decimal("NaN"), Decimal(2), Decimal(1), Decimal(1), Decimal(1))
    with pytest.raises(MarketDataHubError):
        ClosedBar(60, Decimal(2), Decimal(1), Decimal(1), Decimal(2), Decimal(1))
    hub = SharedMarketData((BTC,), history_limit=2)
    hub.register("dev", "one", (BTC,), lookback_bars=2)
    for i in range(1, 5):
        hub.accept_closed_bar(BTC, bar(i * 60))
    view = hub.view(
        "dev", "one", now_ns=241, positions={BTC: Decimal(1)}, max_age_ns=60,
    )
    assert [row["ts_ns"] for row in view["history"]["spot:BTCUSDT"]] == [180, 240]



def test_non_bar_data_type_has_distinct_requirement_but_no_unverified_adapter():
    ticks = MarketStream("BINANCE", "spot", "BTCUSDT", "event", "trade_ticks")
    hub = SharedMarketData((BTC, ticks))
    hub.register("dev", "research", (ticks,))
    assert hub.requirements == {ticks: 1}
    with pytest.raises(MarketDataHubError, match="非法"):
        hub.accept_closed_bar(ticks, bar(60))
    with pytest.raises(MarketDataHubError, match="非K线"):
        hub.view("dev", "research", now_ns=121, positions={}, max_age_ns=60)
    with pytest.raises(MarketDataHubError, match="周期"):
        MarketStream("BINANCE", "spot", "BTCUSDT", "event")
