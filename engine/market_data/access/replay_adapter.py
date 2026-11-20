"""Common, scoped market-data ingress for legacy native replay paths.

Replay retains each native event unchanged for existing Nautilus strategies.
All OHLCV observations first enter the same per-Engine hub used by the generic
portfolio replay. Funding and option quote evidence retain their specialized
typed loaders; the hub does not reinterpret those as close prices.
"""

from nautilus_trader.model import Bar

from engine.market_data.access.subscription_hub import ClosedBar, MarketDataHubError


def observe_native_bar(hub, stream, bar):
    """Validate and record one authoritative native OHLCV observation."""
    if not isinstance(bar, Bar):
        raise MarketDataHubError("历史行情不是原生闭合 K 线")
    hub.accept_closed_bar(
        stream,
        ClosedBar(
            int(bar.ts_event),
            bar.open.as_decimal(),
            bar.high.as_decimal(),
            bar.low.as_decimal(),
            bar.close.as_decimal(),
            bar.volume.as_decimal(),
        ),
    )


def replay_bar_batches(hub, tenant_id, consumer_id, stream, batches):
    """Check an issued one-instrument replay before native data delivery."""
    hub.register(str(tenant_id), consumer_id, (stream,))
    for batch in batches:
        for item in batch:
            if isinstance(item, Bar):
                observe_native_bar(hub, stream, item)
        yield batch


def replay_carry_events(hub, tenant_id, consumer_id, spot_stream, perp_stream, events):
    """Register both legs even when only one issued strategy consumes them."""
    hub.register(str(tenant_id), consumer_id, (spot_stream, perp_stream))
    for event in events:
        if event.kind == "bars":
            if event.spot is None or event.perpetual is None:
                raise MarketDataHubError("跨产品历史记录缺少一条市场腿")
            observe_native_bar(hub, spot_stream, event.spot)
            observe_native_bar(hub, perp_stream, event.perpetual)
        yield event


def replay_protected_evidence(hub, tenant_id, consumer_id, streams, evidence):
    """Track all three OHLCV sources, retaining original option BBO evidence."""
    if len(streams) != 3:
        raise MarketDataHubError("期权回测订阅必须包含永续和两个期权合约")
    hub.register(str(tenant_id), consumer_id, tuple(streams))
    for event in evidence:
        for stream, bar in zip(streams, (event.perpetual, event.put, event.call), strict=True):
            observe_native_bar(hub, stream, bar)
        yield event
