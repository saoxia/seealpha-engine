"""Binance options data-only Nautilus v2 client, using public extension hooks."""

import asyncio
import json
from collections import OrderedDict
from time import monotonic

from nautilus_trader.config import DataClientConfig
from nautilus_trader.live import (
    BarsResponse,
    InstrumentResponse,
    InstrumentsResponse,
    TradesResponse,
)
from nautilus_trader.live.clients import DataClientFactory, MarketDataClient
from nautilus_trader.model import BookType, Venue
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from core.market.instruments import bar_type_for, interval_delta, normalize_symbol

from . import options
from . import options_parsing as parsing
from .queries import QueryError

PUBLIC_WS = "wss://fstream.binance.com/public/stream"
MARKET_WS = "wss://fstream.binance.com/market/stream"
PERIODS = ("1m", "5m", "15m")


class BinanceOptionsDataClientConfig(DataClientConfig):
    symbols: list[str]

    def __init__(self, *, symbols=None, **kwargs):
        self.symbols = [normalize_symbol(s, "options") for s in symbols or []]
        if len(set(self.symbols)) != len(self.symbols) or len(self.symbols) > 32:
            raise ValueError("期权合约配置须唯一且不超过 32 个")


class BinanceOptionsDataClientFactory(DataClientFactory):
    @staticmethod
    def create(*, name, config, cache, clock):
        if not isinstance(config, BinanceOptionsDataClientConfig):
            raise TypeError("Expected BinanceOptionsDataClientConfig")
        return BinanceOptionsDataClient(name=name, config=config, cache=cache, clock=clock)


class BinanceOptionsDataClient(MarketDataClient):
    """No execution methods, credentials, storage, or automatic service startup.

    stream_status reports transport/receipt state separately from the node's
    startup is_connected flag. Disconnects remain explicit gap observations;
    reconnect does not claim that missed trades have been recovered.
    """

    def __init__(self, *, name, config, cache, clock):
        super().__init__(name=name, config=config, cache=cache, clock=clock, venue=Venue("BINANCE"))
        self.instruments = {}
        self.streams = {}
        self._recent = OrderedDict()
        self._closing = False

    @property
    def stream_status(self):
        now = monotonic()
        return {key: {k: v for k, v in state.items() if k not in {"task", "socket", "parser", "bar_evidence"}} |
                {"receipt_age_seconds": now - state["last_receipt"] if state["last_receipt"] else None,
                 "event_age_seconds": max(0, (self.clock.timestamp_ns() - state["last_event_ns"]) / 1e9)
                 if state["last_event_ns"] is not None else None}
                for key, state in self.streams.items()}

    async def _load(self):
        rows = await asyncio.to_thread(options.markets)
        loaded = {row["symbol"]: options.instrument(row) for row in rows}
        selected = self.config.symbols or list(loaded)
        if any(symbol not in loaded for symbol in selected):
            raise QueryError("配置的期权合约已到期或不在当前目录")
        for symbol in selected:
            self.instruments[symbol] = loaded[symbol]
            self._handle_instrument(loaded[symbol])
        return [loaded[symbol] for symbol in selected]

    async def _connect(self):
        self._closing = False
        await self._load()

    async def _disconnect(self):
        self._closing = True
        states = list(self.streams.values())
        for state in states:
            state["connected"] = False
            if state["task"] is not None and not state["task"].cancelling():
                state["task"].cancel()
        # All close handshakes run concurrently, bounded by each socket's timeout.
        await asyncio.gather(*(state["socket"].close() for state in states if state["socket"] is not None))
        await asyncio.gather(*(state["task"] for state in states if state["task"] is not None),
                             return_exceptions=True)
        self.streams.clear()
        self._recent.clear()

    def _instrument(self, instrument_id):
        symbol = str(instrument_id.symbol)
        if str(instrument_id.venue) != "BINANCE" or symbol not in self.instruments:
            raise ValueError("请先加载对应 Binance 期权合约")
        return self.instruments[symbol]

    async def _request_instruments(self, request):
        if request.venue is not None and request.venue != self.venue:
            raise ValueError("期权目录请求场所不匹配")
        data = await self._load()
        self._handle_response(InstrumentsResponse(self.client_id, self.venue, data, request.request_id,
                                                  self.clock.timestamp_ns(), params=request.params))

    async def _request_instrument(self, request):
        if getattr(request, "instrument_ids", None) is not None and len(request.instrument_ids) != 1:
            raise ValueError("期权合约查询暂不支持批量请求")
        await self._load()
        item = self._instrument(request.instrument_id)
        self._handle_response(InstrumentResponse(self.client_id, item.id, item, request.request_id,
                                                 self.clock.timestamp_ns(), params=request.params))

    async def _request_trades(self, request):
        if getattr(request, "instrument_ids", None) is not None and len(request.instrument_ids) != 1:
            raise ValueError("期权成交查询暂不支持批量请求")
        if request.start_ns is not None or request.end_ns is not None:
            raise ValueError("期权仅支持近期成交请求，不支持任意历史 Tick 时间查询")
        item = self._instrument(request.instrument_id)
        limit = 500 if request.limit is None else request.limit
        if not 1 <= limit <= 500:
            raise ValueError("期权近期成交数量须为 1 至 500")
        rows = await asyncio.to_thread(options.get, "trades", symbol=str(item.raw_symbol), limit=limit)
        if not isinstance(rows, list):
            raise TypeError("期权近期成交响应无效")
        result = {}
        for row in rows:
            tick = parsing.trade(item, row, ts_init=self.clock.timestamp_ns())
            key = str(tick.trade_id)
            if key in result and any(getattr(result[key], field) != getattr(tick, field)
                                     for field in ("price", "size", "aggressor_side", "ts_event")):
                raise ValueError("期权近期成交编号冲突")
            result[key] = tick
        data = sorted(result.values(), key=lambda tick: (tick.ts_event, int(str(tick.trade_id))))[-limit:]
        self._handle_response(TradesResponse(self.client_id, item.id, data, request.request_id,
                                             self.clock.timestamp_ns(), params=request.params))

    def _bar_level(self, bar_type):
        item = self._instrument(bar_type.instrument_id)
        for period in PERIODS:
            if str(bar_type) == bar_type_for(str(item.raw_symbol), period, "options"):
                return item, period
        raise ValueError("期权适配器仅支持外部 LAST 1m、5m、15m K 线")

    async def _request_bars(self, request):
        if getattr(request, "bar_types", None) is not None and len(request.bar_types) != 1:
            raise ValueError("期权 K 线查询暂不支持批量请求")
        item, period = self._bar_level(request.bar_type)
        if request.start_ns is None or request.end_ns is None or request.start_ns > request.end_ns:
            raise ValueError("期权历史 K 线须提供有界起止时间")
        first, last = request.start_ns // 1_000_000, request.end_ns // 1_000_000
        if last - first > 31 * 86400000:
            raise ValueError("单次期权历史 K 线请求最多 31 天")
        limit = request.limit or 50_000
        if not 1 <= limit <= 50_000:
            raise ValueError("期权 K 线请求数量超出限制")
        data = []
        duration = int(interval_delta(period).total_seconds() * 1000)
        start = first // duration * duration
        while start <= last and len(data) < limit:
            rows = await asyncio.to_thread(options.get, "klines", symbol=str(item.raw_symbol), interval=period,
                                           startTime=start, endTime=last, limit=min(1500, limit - len(data)))
            if not isinstance(rows, list):
                raise TypeError("期权 K 线响应无效")
            if not rows:
                break
            cursor = start
            for row in rows:
                # Native history is indexed by event time; streaming uses receipt time.
                value = parsing.bar(item, period, row)
                opened = value.ts_event // 1_000_000 + 1 - duration
                if opened < cursor or opened > last:
                    raise ValueError("期权 K 线分页时间倒退或超出请求")
                cursor = opened + duration
                if value.ts_event < self.clock.timestamp_ns() and request.start_ns <= value.ts_event <= request.end_ns:
                    data.append(value)
            if cursor <= start:
                raise ValueError("期权 K 线分页没有推进")
            start = cursor
        self._handle_response(BarsResponse(self.client_id, request.bar_type, data, request.request_id,
                                           self.clock.timestamp_ns(), request.start_ns, request.end_ns, request.params))

    async def _open(self, stream, market):
        socket = await connect(MARKET_WS if market else PUBLIC_WS, open_timeout=10, close_timeout=3,
                               ping_interval=20, ping_timeout=20, max_size=1024 * 1024, max_queue=32)
        try:
            await socket.send(json.dumps({"method": "SUBSCRIBE", "params": [stream], "id": 1}))
            buffered = []
            async with asyncio.timeout(10):
                while True:
                    packet = json.loads(await socket.recv())
                    if isinstance(packet, dict) and packet.get("id") == 1:
                        if packet.get("result", "missing") is not None or "code" in packet:
                            raise ValueError("Binance 期权订阅被拒绝")
                        return socket, buffered
                    if len(buffered) >= 64:
                        raise ValueError("期权订阅确认前缓冲已满")
                    buffered.append(packet)
        except BaseException:
            await socket.close()
            raise

    async def _subscribe_stream(self, stream, parser, *, market=False):
        if stream in self.streams:
            return
        if self._closing or len(self.streams) >= 32:
            raise ValueError("期权订阅已停止或超过 32 条流")
        state = {"connected": False, "last_receipt": None, "last_event_ns": None, "gap_count": 0,
                 "last_error": None, "last_sequence": None, "bar_evidence": None,
                 "socket": None, "task": None, "parser": parser}
        socket, buffered = await self._open(stream, market)
        state.update(socket=socket, connected=True)
        self.streams[stream] = state
        state["task"] = self.create_task(self._run_stream(stream, state, buffered, market), f"options:{stream}")

    def _publish(self, stream, state, packet):
        if isinstance(packet, dict) and "stream" in packet:
            if packet["stream"] != stream:
                raise ValueError("期权流身份不匹配")
            packet = packet["data"]
        if isinstance(packet, dict) and "id" in packet:
            raise ValueError("非预期的期权订阅确认")
        value = state["parser"](packet)
        state["last_receipt"] = monotonic()
        if value is None:
            return
        if value.ts_event > self.clock.timestamp_ns() + 5_000_000_000:
            raise ValueError("期权行情时间超前")
        if hasattr(value, "bar_type"):
            evidence = tuple(str(getattr(value, field)) for field in ("open", "high", "low", "close", "volume"))
            previous = state["bar_evidence"]
            if previous is not None and value.ts_event <= previous[0]:
                if value.ts_event == previous[0] and evidence != previous[1]:
                    raise ValueError("闭合期权 K 线重复值冲突")
                return
            state["bar_evidence"] = (value.ts_event, evidence)
        state["last_event_ns"] = value.ts_event
        if hasattr(value, "trade_id"):
            key = (str(value.instrument_id), str(value.trade_id))
            evidence = (str(value.price), str(value.size), str(value.aggressor_side), value.ts_event)
            if key in self._recent:
                if self._recent[key] != evidence:
                    raise ValueError("实时期权成交编号冲突")
                return
            self._recent[key] = evidence
            if len(self._recent) > 10_000:
                self._recent.popitem(last=False)
        if hasattr(value, "sequence"):
            if state["last_sequence"] is not None and value.sequence <= state["last_sequence"]:
                return
            state["last_sequence"] = value.sequence
        self._handle_data(value)

    async def _run_stream(self, stream, state, buffered, market):
        try:
            while not self._closing:
                try:
                    for packet in buffered:
                        self._publish(stream, state, packet)
                    buffered = []
                    async for message in state["socket"]:
                        self._publish(stream, state, json.loads(message))
                    raise ConnectionError("期权行情连接关闭")
                except (ConnectionClosed, ConnectionError, OSError, TimeoutError):
                    state.update(connected=False, gap_count=state["gap_count"] + 1,
                                 last_error="连接中断；成交缺口未补齐", last_sequence=None)
                    await state["socket"].close()
                    delay = 1
                    while not self._closing:
                        try:
                            await asyncio.sleep(delay)
                            socket, buffered = await self._open(stream, market)
                            state.update(socket=socket, connected=True)
                            break
                        except (ConnectionClosed, ConnectionError, OSError, TimeoutError):
                            delay = min(delay * 2, 30)
        except (ValueError, KeyError, TypeError):
            state.update(connected=False, last_error="行情结构或精度校验失败；订阅已停止")
            raise ValueError(state["last_error"]) from None
        finally:
            state["connected"] = False
            await state["socket"].close()

    async def _unsubscribe_stream(self, stream):
        state = self.streams.get(stream)
        if state is None:
            return
        state["task"].cancel()
        await state["socket"].close()
        await asyncio.gather(state["task"], return_exceptions=True)
        del self.streams[stream]

    async def _subscribe_trades(self, command):
        items = [self._instrument(instrument_id) for instrument_id in command.instrument_ids]
        for item in items:
            def parse(row, item=item):
                return parsing.trade_event(item, row, ts_init=self.clock.timestamp_ns())[0]

            await self._subscribe_stream(f"{str(item.raw_symbol).lower()}@optionTrade", parse)

    async def _unsubscribe_trades(self, command):
        for instrument_id in command.instrument_ids:
            item = self._instrument(instrument_id)
            await self._unsubscribe_stream(f"{str(item.raw_symbol).lower()}@optionTrade")

    async def _subscribe_quotes(self, command):
        items = [self._instrument(instrument_id) for instrument_id in command.instrument_ids]
        for item in items:
            await self._subscribe_stream(
                f"{str(item.raw_symbol).lower()}@bookTicker",
                lambda row, item=item: parsing.quote(item, row, self.clock.timestamp_ns()),
            )

    async def _unsubscribe_quotes(self, command):
        for instrument_id in command.instrument_ids:
            item = self._instrument(instrument_id)
            await self._unsubscribe_stream(f"{str(item.raw_symbol).lower()}@bookTicker")

    async def _subscribe_book_depth(self, command):
        levels = command.depth or 10
        if command.book_type != BookType.L2_MBP or levels not in {5, 10, 20}:
            raise ValueError("期权仅支持 L2 前 5/10/20 档完整快照")
        items = [self._instrument(instrument_id) for instrument_id in command.instrument_ids]
        for item in items:
            await self._subscribe_stream(
                f"{str(item.raw_symbol).lower()}@depth{levels}@100ms",
                lambda row, item=item: parsing.depth(item, row, levels, self.clock.timestamp_ns()),
            )

    async def _unsubscribe_book_depth(self, command):
        for instrument_id in command.instrument_ids:
            symbol = str(self._instrument(instrument_id).raw_symbol).lower()
            for stream in list(self.streams):
                if stream.startswith(symbol + "@depth"):
                    await self._unsubscribe_stream(stream)

    async def _subscribe_bars(self, command):
        members = [self._bar_level(bar_type) for bar_type in command.bar_types]
        for item, period in members:
            def parse(row, item=item, period=period):
                return parsing.bar_event(item, period, row, ts_init=self.clock.timestamp_ns())[0]

            await self._subscribe_stream(f"{str(item.raw_symbol).lower()}@kline_{period}", parse, market=True)

    async def _unsubscribe_bars(self, command):
        for bar_type in command.bar_types:
            item, period = self._bar_level(bar_type)
            await self._unsubscribe_stream(f"{str(item.raw_symbol).lower()}@kline_{period}")


async def market_events(symbol, intervals, *, trades=False, depth=False):
    """Public data-only native node stream for the managed collector.

    Native objects stay on this event loop. A bounded callback queue fails
    explicitly on overflow; transport state never claims missed Tick recovery.
    """
    from nautilus_trader.common import DataActor, Environment, LoggerConfig, LogLevel
    from nautilus_trader.live import LiveNode
    from nautilus_trader.model import BarType, InstrumentId, TraderId

    symbol = normalize_symbol(symbol, "options")
    if not intervals or any(level not in PERIODS for level in intervals):
        raise ValueError("期权采集周期无效")
    output = asyncio.Queue(maxsize=10000)
    clients = []
    overflow = False

    class Factory(BinanceOptionsDataClientFactory):
        @staticmethod
        def create(**kwargs):
            client = BinanceOptionsDataClientFactory.create(**kwargs)
            clients.append(client)
            return client

    class Observer(DataActor):
        def emit(self, value):
            nonlocal overflow
            try:
                output.put_nowait(value)
            except asyncio.QueueFull:
                overflow = True

        def on_start(self):
            client_id = clients[0].client_id
            identifier = InstrumentId.from_str(f"{symbol}.BINANCE")
            if trades:
                self.subscribe_trades(identifier, client_id=client_id)
            if depth:
                self.subscribe_book_depth(identifier, BookType.L2_MBP, depth=10, client_id=client_id)
            for level in intervals:
                self.subscribe_bars(BarType.from_str(bar_type_for(symbol, level, "options")), client_id=client_id)

        def on_trade(self, value):
            self.emit({"level": "tick", "row": {"symbol": symbol, "id": int(str(value.trade_id)),
                       "time": value.ts_event // 1000000, "price": str(value.price), "qty": str(value.size),
                       "side": 1 if str(value.aggressor_side) == "BUY" else -1}})

        def on_book_depth(self, value):
            self.emit({"level": "depth", "row": {"s": symbol, "T": value.ts_event // 1000000,
                       "u": value.sequence, "received_ns": value.ts_init,
                       "b": [[str(order.price), str(order.size)] for order in value.bids],
                       "a": [[str(order.price), str(order.size)] for order in value.asks]}})

        def on_bar(self, value):
            level = next(level for level in intervals if str(value.bar_type) == bar_type_for(symbol, level, "options"))
            right = (value.ts_event + 1) // 1000000
            self.emit({"level": level, "row": [right - int(interval_delta(level).total_seconds() * 1000),
                       *[str(getattr(value, field)) for field in ("open", "high", "low", "close", "volume")], right - 1]})

    builder = LiveNode.builder("options-collection", TraderId("OPTIONS-COLLECTOR"), Environment.LIVE)
    builder.with_logging(LoggerConfig(stdout_level=LogLevel.OFF))
    builder.with_delay_post_stop_secs(0).with_delay_shutdown_secs(0)
    builder.with_timeout_connection(30).with_timeout_disconnection_secs(3)
    builder.add_data_client("BINANCE_OPTIONS", Factory(), BinanceOptionsDataClientConfig(symbols=[symbol]))
    node = builder.build()
    node.add_actor(Observer())
    task = asyncio.create_task(node.run_async())
    last_status = 0
    try:
        while not task.done():
            if overflow:
                raise QueryError("期权采集缓冲已满；存在未恢复的 Tick 缺口")
            try:
                value = await asyncio.wait_for(output.get(), 1)
            except TimeoutError:
                value = None
            if value is not None:
                yield value
            states = clients[0].stream_status if clients else {}
            expected = len(intervals) + int(trades) + int(depth)
            connected = len(states) == expected and all(state["connected"] for state in states.values())
            failed = any(state["last_error"] and not state["connected"] for state in states.values())
            if monotonic() - last_status >= 1 or failed:
                yield {"level": "status", "connected": connected,
                       "gap_count": sum(state["gap_count"] for state in states.values())}
                last_status = monotonic()
            if failed:
                raise QueryError("期权实时流断线或校验失败，将恢复订阅；缺口保留")
        await task
        raise QueryError("期权行情节点已停止")
    finally:
        node.handle().stop()
        try:
            await asyncio.wait_for(asyncio.shield(task), 8)
        except (Exception, asyncio.CancelledError):  # noqa: BLE001 - Always dispose on owning loop.
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        node.dispose()


async def batch_market_events(instruments, *, tick_symbols=(), scope=None, depth=False):
    """Combined streams, at most 64 contracts per socket group, with live updates.

    Native parsing and contract units are shared with the DataClient. The optional
    scope is owned by the collector's event loop; selection replacement is atomic.
    All queues are bounded and apply backpressure instead of dropping evidence.
    """
    import time

    instruments = {str(item.raw_symbol): item for item in instruments}
    ticks = set(tick_symbols)
    if not instruments or not ticks <= instruments.keys() or scope is not None and len(instruments) > 64:
        raise ValueError("期权批量订阅范围无效")
    output = asyncio.Queue(maxsize=4096)
    channels, gaps = {}, {}

    def selection(members):
        if scope is None:
            return {symbol: instruments[symbol] for symbol in members}, ticks
        values, wanted_ticks = scope["selection"]
        values = {str(item.raw_symbol): item for item in values}
        if len(values) > 64 or not set(wanted_ticks) <= values.keys():
            raise ValueError("期权动态订阅范围无效")
        return values, set(wanted_ticks)

    async def receive(members, *, market):
        channel = "bars" if market else "trades"
        delay = 1
        while True:
            active = []
            try:
                items, wanted_ticks = selection(members)
                active = list(items) if market else [symbol for symbol in items if depth or symbol in wanted_ticks]
                if not active:
                    await asyncio.sleep(1)
                    continue
                async with connect(MARKET_WS if market else PUBLIC_WS, open_timeout=10, close_timeout=3,
                                   ping_interval=20, ping_timeout=20, max_size=1024 * 1024, max_queue=32) as socket:
                    streams, confirmed, pending, retired, latest = {}, set(), {}, set(), {}
                    request_id, last_status, last_scope = 0, 0, None
                    while True:
                        items, wanted_ticks = selection(members)
                        active = list(items) if market else [symbol for symbol in items if depth or symbol in wanted_ticks]
                        desired = {}
                        for symbol in active:
                            levels = PERIODS if market else (("tick",) if symbol in wanted_ticks else ()) + (("depth",) if depth else ())
                            for level in levels:
                                suffix = f"kline_{level}" if market else "optionTrade" if level == "tick" else "depth10@100ms"
                                desired[f"{symbol.lower()}@{suffix}"] = (symbol, level)
                        signature = tuple(sorted(desired))
                        if signature != last_scope:
                            added, removed = desired.keys() - streams.keys(), streams.keys() - desired.keys()
                            for method, names in (("SUBSCRIBE", added), ("UNSUBSCRIBE", removed)):
                                if names:
                                    request_id += 1
                                    await socket.send(json.dumps({"method": method, "params": sorted(names), "id": request_id}))
                                    pending[request_id] = (method, set(names), monotonic() + 10)
                                    latest.update({name: request_id for name in names})
                            retired.update(removed)
                            retired.difference_update(added)
                            confirmed.difference_update(removed)
                            streams, last_scope = desired, signature
                        if not streams:
                            break
                        if any(deadline < monotonic() for _method, _names, deadline in pending.values()):
                            raise TimeoutError("期权批量订阅确认超时")
                        try:
                            packet = json.loads(await asyncio.wait_for(socket.recv(), 1))
                        except TimeoutError:
                            packet = None
                        if packet is not None:
                            if not isinstance(packet, dict):
                                raise ValueError("期权批量响应格式无效")
                            if "id" in packet:
                                identity = packet["id"]
                                if identity not in pending or packet.get("result", "missing") is not None or "code" in packet:
                                    raise ValueError("期权批量订阅确认无效")
                                method, names, _deadline = pending.pop(identity)
                                if method == "SUBSCRIBE":
                                    confirmed.update(name for name in names & streams.keys() if latest.get(name) == identity)
                            else:
                                row = packet.get("data", packet)
                                if not isinstance(row, dict):
                                    raise ValueError("期权批量数据格式无效")
                                name = packet.get("stream")
                                if name is None:
                                    name = (f"{row['s'].lower()}@optionTrade" if row.get("e") == "trade" else
                                            f"{row['s'].lower()}@depth10@100ms" if row.get("e") == "depthUpdate" else
                                            f"{row['s'].lower()}@kline_{row['k']['i']}")
                                if name in retired:
                                    continue  # Already unsubscribed, with explicitly identified in-flight data.
                                if name not in streams:
                                    raise ValueError("期权批量流身份不匹配")
                                symbol, level = streams[name]
                                item = items[symbol]
                                if level == "depth":
                                    received = time.time_ns()
                                    value = parsing.depth(item, row, 10, received)
                                    payload = {**row, "received_ns": received}
                                else:
                                    value, payload = (parsing.bar_event(item, level, row) if market else parsing.trade_event(item, row))
                                if value is not None:
                                    if value.ts_event > time.time_ns() + 5_000_000_000 or value.ts_event > item.expiration_ns:
                                        raise ValueError("期权实时事件时间超前或超过到期日")
                                    await output.put({"level": level, "symbol": symbol, "row": payload})
                        if monotonic() - last_status >= 1:
                            for symbol in active:
                                expected = {name for name, (owner, _level) in streams.items() if owner == symbol}
                                await output.put({"level": "transport", "symbol": symbol, "channel": channel,
                                                  "connected": expected <= confirmed})
                            last_status, delay = monotonic(), 1
            except (ConnectionClosed, ConnectionError, OSError, TimeoutError, ValueError, KeyError, TypeError):
                for symbol in active:
                    await output.put({"level": "transport", "symbol": symbol, "channel": channel,
                                      "connected": False, "gap": True})
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    tasks = []
    symbols = sorted(instruments)
    for offset in range(0, len(symbols), 64):
        members = symbols[offset:offset + 64]
        tasks.append(asyncio.create_task(receive(members, market=True)))
        if depth or scope is not None or any(symbol in ticks for symbol in members):
            tasks.append(asyncio.create_task(receive(members, market=False)))
    try:
        while True:
            try:
                event = await asyncio.wait_for(output.get(), 1)
            except TimeoutError:
                for task in tasks:
                    if task.done():
                        await task
                        raise QueryError("期权批量接收意外停止")
                continue
            symbol = event["symbol"]
            if event["level"] == "transport":
                channel_state = channels.setdefault(symbol, {})
                channel_state[event["channel"]] = event["connected"]
                gaps[symbol] = gaps.get(symbol, 0) + int(event.get("gap", False))
                current_ticks = ticks if scope is None else selection(symbols)[1]
                connected = channel_state.get("bars", False) and (not depth and symbol not in current_ticks or channel_state.get("trades", False))
                yield {"level": "status", "symbol": symbol, "connected": connected, "gap_count": gaps[symbol]}
            else:
                yield event
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
