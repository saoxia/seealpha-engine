"""Trusted Engine adapter: issued strategy reads from configured collectors.

The collector owns Binance WebSockets, reconnects and durable journals. This
adapter only consumes committed, complete derived bars. Public collector health
is *not* execution permission: data snapshots always deny new trading risk.
"""

import json
import sqlite3
import time
from decimal import Decimal
from threading import RLock

from engine.market_data.access.subscription_hub import (
    ClosedBar,
    MarketDataHubError,
    MarketStream,
)

_MAX_STATUS_BYTES = 131072
_SUPPORTED_PRODUCTS = frozenset({"spot", "futures_um"})
_SUPPORTED_INTERVALS = frozenset({
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h",
    "6h", "8h", "12h", "1d", "3d", "1w", "1M",
})


def _status_ready(path, now_ms, *, calculation=False):
    if not path.is_file() or path.stat().st_size > _MAX_STATUS_BYTES:
        return False
    state = json.loads(path.read_text())
    if not isinstance(state, dict) or state.get("state") != "READY":
        return False
    if type(state.get("valid_until_ms")) is not int or state["valid_until_ms"] < now_ms:
        return False
    if calculation:
        return state.get("backlog") == 0 and state.get("realtime_pending") == 0
    return state.get("data_ready") is True and state.get("scope") == (
        "managed_public_evidence_no_strategy_readiness"
    )


def _committed_bars(path, policy, stream, lookback):
    """Read a bounded verified tail from the collector's WAL, never write it."""
    if not path.is_file():
        raise MarketDataHubError("行情采集日志尚未创建")
    database = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=1)
    try:
        config = database.execute("SELECT symbol,base_level FROM config").fetchone()
        if config != (policy.symbol, policy.base_level):
            raise MarketDataHubError("采集日志市场身份与订阅配置不一致")
        rows = database.execute(
            "SELECT value FROM candles WHERE interval=? "
            "AND coalesce(json_extract(value,'$.retained'),1)=1 "
            "ORDER BY start_ms DESC LIMIT ?",
            (stream.interval, lookback),
        ).fetchall()
        values = [json.loads(row[0]) for row in reversed(rows)]
        from core.market.instruments import bar_window

        out = []
        previous_end = None
        for value in values:
            left, right = value["start_ms"], value["end_ms"]
            if (
                value.get("symbol") != stream.symbol
                or value.get("interval") != stream.interval
                or value.get("is_closed") is not True
                or value.get("is_complete") is not True
                or type(left) is not int
                or type(right) is not int
                or bar_window(stream.interval, left) != (left, right)
                or previous_end is not None and left != previous_end
            ):
                raise MarketDataHubError("历史预热包含缺口、未闭合或不完整K线")
            previous_end = right
            out.append(ClosedBar(
                (right - 1) * 1_000_000,
                Decimal(value["open"]), Decimal(value["high"]),
                Decimal(value["low"]), Decimal(value["close"]),
                Decimal(value["volume"]),
            ))
        return out
    finally:
        database.close()


def _package_symbols(parameters):
    """Only declared issued-package instruments can be added as data readers."""
    allowed = set()
    for name in ("symbol", "spot_symbol", "perpetual_symbol", "put_symbol", "call_symbol"):
        value = parameters.get(name)
        if isinstance(value, str):
            allowed.add(value)
    for field in ("symbols", "legs"):
        for item in parameters.get(field, ()) or ():
            symbol = item.get("symbol") if isinstance(item, dict) else item
            if isinstance(symbol, str):
                allowed.add(symbol)
    return allowed


class ManagedJournalBridge:
    """One Engine's read-only live ingress, never a collector controller."""

    def __init__(self, runtime, market_data):
        self.runtime = runtime
        self.manager = market_data.subscriptions
        self.service = runtime.market_subscriptions
        self.manager.active_readers = lambda: self.service.live_requirements
        self._lock = RLock()
        self._ready = set()
        self._latest = {}
        self._tails = {}
        self._warmed_bars = {}
        self._errors = {}

    @staticmethod
    def consumer_id(account_id, issuance_id):
        return f"{account_id}:{issuance_id}"

    def subscribe(self, tenant_id, account_id, issuance_id, streams, *, lookback_bars=1):
        """Activate an issued strategy's data needs; never activate its orders."""
        consumer = self.consumer_id(account_id, issuance_id)
        keys = tuple(streams)
        tenant = self.runtime.store.tenant(tenant_id)
        loads = self.runtime.store.strategy_loads(tenant_id, account_id)
        load = next((r for r in loads if str(r["issuance_id"]) == str(issuance_id)), None)
        if (tenant is None or tenant["status"] != "active" or load is None
                or load["grant_purpose"] != tenant["purpose"]):
            raise MarketDataHubError("策略未下发或账户/租户授权不匹配")
        if not keys or len(keys) > 8 or len(set(keys)) != len(keys):
            raise MarketDataHubError("策略行情需求必须是唯一的有效市场集合")
        allowed = _package_symbols(load["parameters"])
        if str(tenant_id) != str(self.manager.bound_tenant_id()):
            raise MarketDataHubError("策略订阅租户与 Engine 行情配置租户不匹配")
        with self._lock, self.manager.lock:
            policies = {
                (p.product, p.symbol): p for p in self.manager._read(refresh=True)
            }
            for stream in keys:
                if not isinstance(stream, MarketStream):
                    raise MarketDataHubError("策略市场流身份无效")
                policy = policies.get((stream.product, stream.symbol))
                if (
                    stream.source != "BINANCE"
                    or stream.product not in _SUPPORTED_PRODUCTS
                    or stream.kind != "closed_bars"
                    or stream.interval not in _SUPPORTED_INTERVALS
                    or policy is None or policy.base_level == "auto"
                    or not policy.realtime_enabled
                    or stream.interval not in policy.intervals
                    or stream.symbol not in allowed
                    or not self.runtime._has_subscription_intent(
                        tenant_id, stream.symbol, stream.product,
                    )
                ):
                    raise MarketDataHubError("策略行情尚未获准，或实时采集未在配置中启用")
            self.service.configure_live(keys)
            return self.service.subscribe_live(
                str(tenant_id), consumer, keys, lookback_bars=lookback_bars,
            )

    def unsubscribe(self, tenant_id, account_id, issuance_id):
        if str(tenant_id) != str(self.manager.bound_tenant_id()):
            raise MarketDataHubError("不能释放其他租户的行情订阅")
        with self._lock:
            return self.service.unsubscribe_live(
                str(tenant_id), self.consumer_id(account_id, issuance_id),
            )

    def _reset(self, stream, reason):
        self.service.invalidate_live_stream(stream)
        self._ready.discard(stream)
        self._latest.pop(stream, None)
        self._tails.pop(stream, None)
        self._warmed_bars.pop(stream, None)
        self._errors[stream] = reason

    def poll(self):
        """Pump committed collector bars into the Engine hub; no network requests."""
        with self._lock:
            needed = self.service.live_requirements
            for stream in set(self._ready) - set(needed):
                self._reset(stream, "unsubscribed")
            if not needed:
                return
            try:
                policies = {
                    (p.product, p.symbol): p for p in self.manager._read(refresh=True)
                }
            except (OSError, ValueError) as exc:
                for stream in needed:
                    self._reset(stream, str(exc))
                return
            now_ms = int(time.time() * 1000)
            for stream, lookback in needed.items():
                policy = policies.get((stream.product, stream.symbol))
                if policy is None or not policy.realtime_enabled:
                    self._reset(stream, "collector policy missing or realtime disabled")
                    continue
                root = self.manager.root(policy)
                try:
                    if not (
                        _status_ready(root / "status.json", now_ms)
                        and _status_ready(
                            root / "calculation" / "status.json", now_ms,
                            calculation=True,
                        )
                    ):
                        raise MarketDataHubError("采集器断线、尚未就绪或计算结果积压")
                    tail = _committed_bars(
                        root / "collection.sqlite", policy, stream, lookback,
                    )
                    if len(tail) < lookback:
                        raise MarketDataHubError("策略历史预热K线不足")
                    latest = self._latest.get(stream)
                    if latest is not None and lookback > self._warmed_bars.get(stream, 0):
                        # A new strategy may need a longer history than the
                        # previously cached one-bar feed; replay the entire tail.
                        self._reset(stream, "expanding warmup")
                        latest = None
                    delivered = {
                        bar.ts_ns: bar for bar in self._tails.get(stream, ())
                    }
                    if any(
                        bar.ts_ns in delivered and bar != delivered[bar.ts_ns]
                        for bar in tail
                    ):
                        raise MarketDataHubError("采集器修订了已交付K线，必须重新预热")
                    for bar in tail:
                        if latest is not None and bar.ts_ns > latest.ts_ns:
                            from core.market.instruments import bar_window

                            previous_end_ms = latest.ts_ns // 1_000_000 + 1
                            _, expected_end_ms = bar_window(
                                stream.interval, previous_end_ms,
                            )
                            if bar.ts_ns // 1_000_000 + 1 != expected_end_ms:
                                raise MarketDataHubError("采集器闭合K线存在缺口")
                        if latest is None or bar.ts_ns > latest.ts_ns:
                            self.service.accept_live_closed_bar(stream, bar)
                            latest = bar
                    self._latest[stream] = latest
                    self._tails[stream] = tuple(tail)
                    self._warmed_bars[stream] = lookback
                    self._ready.add(stream)
                    self._errors.pop(stream, None)
                except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
                    self._reset(stream, str(exc))

    def view(self, tenant_id, account_id, issuance_id, *, now_ns, positions, max_age_ns):
        """Read-only strategy snapshot: data readiness is NOT execution permission."""
        if str(tenant_id) != str(self.manager.bound_tenant_id()):
            raise MarketDataHubError("请求租户与 Engine 行情配置租户不匹配")
        tenant = self.runtime.store.tenant(tenant_id)
        load = self.runtime.store.strategy_load(tenant_id, account_id, issuance_id)
        if (
            tenant is None or tenant["status"] != "active" or load is None
            or load["grant_purpose"] != tenant["purpose"]
        ):
            raise MarketDataHubError("策略不再具有有效的行情访问授权")
        with self._lock:
            self.poll()
            consumer = self.consumer_id(account_id, issuance_id)
            required = self.service.live_consumer_streams(
                str(tenant_id), consumer,
            )
            if set(required) - self._ready:
                raise MarketDataHubError("策略有市场流尚未完成预热或正在降级")
            snapshot = self.service.live_view(
                str(tenant_id), consumer, now_ns=now_ns,
                positions=positions, max_age_ns=max_age_ns,
            )
            return {**snapshot, "allow_new_risk": False}

    def status(self):
        with self._lock:
            return {
                "requirements": [
                    {
                        "source": stream.source, "product": stream.product,
                        "symbol": stream.symbol, "interval": stream.interval,
                        "kind": stream.kind, "lookback_bars": count,
                        "data_ready": stream in self._ready,
                        "reason": self._errors.get(stream),
                    }
                    for stream, count in self.service.live_requirements.items()
                ],
                "allow_new_risk": False,
            }
