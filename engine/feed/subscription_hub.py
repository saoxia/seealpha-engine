"""Shared, in-process market data requirements and scoped closed-bar views.

This is a data-plane *abstraction*, not a connector or a new background service.
Only trusted Engine callers register already authorized strategy requirements.
Market sources have independent identities; subscriptions do not open sockets,
alter Feed readiness, or grant trade permissions.
"""

import re
from collections import deque
from dataclasses import dataclass
from decimal import Decimal


class MarketDataHubError(ValueError):
    """A requested stream, observation or consumer scope is not trustworthy."""


_SOURCE = re.compile(r"^[A-Z0-9_-]{2,40}$")
_PRODUCT = re.compile(r"^[a-z][a-z0-9_]{1,39}$")
_SYMBOL = re.compile(r"^[A-Z0-9_-]{2,64}$")
_INTERVAL = re.compile(r"^(?:[1-9][0-9]*[smhdwM]|event)$")
_KIND = re.compile(r"^[a-z][a-z0-9_]{1,39}$")
MAX_HISTORY_BARS = 256


@dataclass(frozen=True, order=True)
class MarketStream:
    """Canonical public market identity, independent of strategy/account.

    Data with different exchange, product, symbol or bar interval is never
    silently shared. A source adapter must explicitly supply this identity.
    """

    source: str
    product: str
    symbol: str
    interval: str
    kind: str = "closed_bars"

    def __post_init__(self):
        for value, pattern in (
            (self.source, _SOURCE),
            (self.product, _PRODUCT),
            (self.symbol, _SYMBOL),
            (self.interval, _INTERVAL),
            (self.kind, _KIND),
        ):
            if not isinstance(value, str) or pattern.fullmatch(value) is None:
                raise MarketDataHubError("市场流身份无效")
        if self.kind == "closed_bars" and self.interval == "event":
            raise MarketDataHubError("K线流必须声明完整周期")

    @property
    def label(self):
        return f"{self.product}:{self.symbol}"


@dataclass(frozen=True)
class ClosedBar:
    ts_ns: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    def __post_init__(self):
        if type(self.ts_ns) is not int or self.ts_ns <= 0:
            raise MarketDataHubError("闭合行情时间戳无效")
        for value in (self.open, self.high, self.low, self.close, self.volume):
            if not isinstance(value, Decimal) or not value.is_finite():
                raise MarketDataHubError("行情数值必须为有限 Decimal")
        if (
            min(self.open, self.high, self.low, self.close) <= 0
            or self.volume < 0
            or self.high < max(self.open, self.close)
            or self.low > min(self.open, self.close)
        ):
            raise MarketDataHubError("行情 OHLCV 不合法")

    def as_dict(self):
        return {
            "ts_ns": self.ts_ns,
            "open": format(self.open, "f"),
            "high": format(self.high, "f"),
            "low": format(self.low, "f"),
            "close": format(self.close, "f"),
            "volume": format(self.volume, "f"),
        }


class SharedMarketData:
    """One source series per stream; many scoped consumer subscriptions.

    A trusted owner passes the streams it already validated/authorized.
    Registration only declares data needs: upstream adapters can use
    `requirements` to open one real subscription per unique stream, with
    the largest requested lookback. No market sockets are opened here.

    Lifecycle is owned by the Engine runtime; this object is not thread-safe.
    """

    def __init__(self, available_streams, *, history_limit=MAX_HISTORY_BARS):
        keys = tuple(available_streams)
        if (
            not keys or not all(isinstance(key, MarketStream) for key in keys)
            or len(set(keys)) != len(keys)
            or type(history_limit) is not int
            or not 1 <= history_limit <= MAX_HISTORY_BARS
        ):
            raise MarketDataHubError("共享市场清单或历史上限无效")
        self._available = frozenset(keys)
        self._history_limit = history_limit
        self._subscribers = {}
        self._history = {}
        self._refs = {}

    def authorize_streams(self, streams):
        """Extend the live catalog after trusted Engine policy validation.

        Registering a strategy never authorizes new market sources. Historical
        scopes should instead start with an immutable run-specific catalog.
        """
        keys = tuple(streams)
        if (
            not keys or not all(isinstance(key, MarketStream) for key in keys)
            or len(keys) != len(set(keys))
        ):
            raise MarketDataHubError("市场授权清单无效")
        self._available = self._available.union(keys)

    def register(self, tenant_id, consumer_id, streams, *, lookback_bars=1):
        if (
            not isinstance(tenant_id, str) or not tenant_id.strip()
            or not isinstance(consumer_id, str) or not consumer_id.strip()
            or type(lookback_bars) is not int
            or not 1 <= lookback_bars <= self._history_limit
            or not isinstance(streams, tuple) or not streams
            or any(not isinstance(key, MarketStream) for key in streams)
            or len(set(streams)) != len(streams)
            or not set(streams).issubset(self._available)
            or len({key.label for key in streams}) != len(streams)
        ):
            raise MarketDataHubError("策略行情订阅无效或未获数据范围授权")
        consumer = (tenant_id, consumer_id)
        request = (frozenset(streams), lookback_bars)
        previous = self._subscribers.get(consumer)
        if previous is not None:
            if previous == request:
                return  # An idempotent retry does not add upstream references.
            raise MarketDataHubError("策略已注册，不允许静默更换订阅")
        self._subscribers[consumer] = request
        for key in request[0]:
            if key not in self._history:
                self._history[key] = deque(maxlen=self._history_limit)
                self._refs[key] = 0
            self._refs[key] += 1

    def release(self, tenant_id, consumer_id):
        request = self._subscribers.pop((tenant_id, consumer_id), None)
        if request is None:
            return
        for key in request[0]:
            self._refs[key] -= 1
            if self._refs[key] == 0:
                del self._refs[key]
                del self._history[key]

    @property
    def requirements(self):
        """Unique upstream keys and maximum requested warm-up length."""
        needed = {}
        for keys, lookback in self._subscribers.values():
            for key in keys:
                needed[key] = max(needed.get(key, 0), lookback)
        return dict(sorted(needed.items()))

    def accept_closed_bar(self, stream, bar):
        """Append only authoritative closed observations, never overwrite history.

        Duplicate *identical* delivery is idempotent. Conflicting or older data
        fails closed and is left to the real feed's repair/evidence subsystem.
        """
        if (
            stream not in self._history or not isinstance(bar, ClosedBar)
            or stream.kind != "closed_bars"
        ):
            raise MarketDataHubError("未订阅的市场或非法闭合行情")
        history = self._history[stream]
        if history and bar.ts_ns <= history[-1].ts_ns:
            if bar == history[-1]:
                return False
            raise MarketDataHubError("行情发生时间倒退或出现冲突")
        history.append(bar)
        return True

    def view(self, tenant_id, consumer_id, *, now_ns, positions, max_age_ns):
        """Copy only registered legs, with no current/future bar or lookahead.

        positions is trusted, current account evidence supplied by the Engine,
        *not* cached market state. A multi-leg view requires a common bar time.
        """
        request = self._subscribers.get((tenant_id, consumer_id))
        if request is None:
            raise MarketDataHubError("策略未授权订阅市场数据")
        if (
            type(now_ns) is not int or now_ns <= 0
            or type(max_age_ns) is not int or max_age_ns <= 0
            or not isinstance(positions, dict)
        ):
            raise MarketDataHubError("行情决策时间或持仓证据无效")
        keys, lookback = request
        if any(key.kind != "closed_bars" for key in keys):
            raise MarketDataHubError("非K线流尚无历史策略视图适配器")
        markets, history, holdings, timestamps = {}, {}, {}, set()
        for key in sorted(keys):
            rows = self._history.get(key)
            if not rows:
                raise MarketDataHubError("策略行情预热缺失")
            latest = rows[-1]
            if not 0 < now_ns - latest.ts_ns <= max_age_ns:
                raise MarketDataHubError("行情过期或包含未来数据")
            if key not in positions:
                raise MarketDataHubError("策略订阅的持仓证据不完整")
            quantity = positions[key]
            if not isinstance(quantity, Decimal) or not quantity.is_finite():
                raise MarketDataHubError("模拟持仓必须为有限 Decimal")
            timestamps.add(latest.ts_ns)
            markets[key.label] = {
                "price": format(latest.close, "f"), "ts_ns": latest.ts_ns,
            }
            history[key.label] = [row.as_dict() for row in list(rows)[-lookback:]]
            holdings[key.label] = format(quantity, "f")
        if len(timestamps) != 1:
            raise MarketDataHubError("跨市场快照时间戳未对齐")
        return {
            "now_ns": now_ns, "markets": markets,
            "positions": holdings, "history": history,
        }
