"""Engine-local strategy subscriptions: one live hub and isolated replay scopes.

Only trusted Engine callers may authorize streams and supply observations.
This service manages read-side consumers, not exchange sockets, data policies,
or execution permission. It must never be process-global or user-global.
"""

from contextlib import contextmanager
from threading import RLock

from engine.feed.subscription_hub import MarketDataHubError, SharedMarketData


class EngineMarketDataService:
    """A single Engine runtime's read-side market-data subscription registry.

    Live readers share one hub and unique upstream requirements. Each historical
    run receives an independent event-time history, so replay cannot leak into
    live trading or another backtest. A trusted feed adapter is responsible for
    reconciling ``live_requirements`` with the existing managed collectors.
    """

    def __init__(self):
        self._lock = RLock()
        self._live = None
        self._backtests = {}

    def configure_live(self, authorized_streams):
        """Only the Engine's policy/permission layer may extend the live catalog."""
        with self._lock:
            if self._live is None:
                self._live = SharedMarketData(authorized_streams)
            else:
                self._live.authorize_streams(authorized_streams)

    def subscribe_live(self, tenant_id, consumer_id, streams, *, lookback_bars=1):
        """Declare a live consumer; it does not bypass feed readiness checks."""
        with self._lock:
            if self._live is None:
                raise MarketDataHubError("实时市场数据尚未由 Engine 授权配置")
            self._live.register(
                tenant_id, consumer_id, streams, lookback_bars=lookback_bars,
            )
            return self._live.requirements

    def unsubscribe_live(self, tenant_id, consumer_id):
        with self._lock:
            if self._live is not None:
                self._live.release(tenant_id, consumer_id)
            return self.live_requirements

    @property
    def live_requirements(self):
        with self._lock:
            return {} if self._live is None else self._live.requirements

    def accept_live_closed_bar(self, stream, bar):
        """Trusted, readiness-checked feed observations only; never replay bars."""
        with self._lock:
            if self._live is None:
                raise MarketDataHubError("实时市场数据尚未由 Engine 授权配置")
            return self._live.accept_closed_bar(stream, bar)

    def live_view(self, tenant_id, consumer_id, *, now_ns, positions, max_age_ns):
        with self._lock:
            if self._live is None:
                raise MarketDataHubError("实时市场数据尚未由 Engine 授权配置")
            return self._live.view(
                tenant_id, consumer_id, now_ns=now_ns,
                positions=positions, max_age_ns=max_age_ns,
            )

    @contextmanager
    def backtest(self, run_id, authorized_streams):
        """One replay clock and cache per run, shared by its strategy members."""
        if not isinstance(run_id, str) or not run_id.strip():
            raise MarketDataHubError("历史行情回放编号无效")
        with self._lock:
            if run_id in self._backtests:
                raise MarketDataHubError("历史行情回放编号正在使用")
            hub = SharedMarketData(authorized_streams)
            self._backtests[run_id] = hub
        try:
            yield hub
        finally:
            with self._lock:
                if self._backtests.get(run_id) is hub:
                    del self._backtests[run_id]

    def close(self):
        """Remove Engine-owned caches; upstream collectors have their own owner."""
        with self._lock:
            self._live = None
            self._backtests.clear()
