"""Data-only Binance adapter runtime; the node has no execution clients."""

import asyncio
from datetime import UTC, datetime
from time import monotonic

from nautilus_trader.adapters.binance import (
    BinanceDataClientConfig,
    BinanceDataClientFactory,
    BinanceEnvironment,
    BinanceInstrumentProviderConfig,
    BinanceProductType,
)
from nautilus_trader.common import DataActor, Environment, LoggerConfig, LogLevel, SocketState
from nautilus_trader.live import LiveNode
from nautilus_trader.model import BarType, InstrumentId, TraderId

from core.market.instruments import bar_type_for, instrument_id_for
from core.market.models import MINUTE, DataError, MinuteBar


def instrument_id(symbol):
    return InstrumentId.from_str(instrument_id_for(symbol))


def bar_type(symbol):
    return BarType.from_str(bar_type_for(symbol, "1m"))


def minute_bar(bar):
    symbol = str(bar.bar_type.instrument_id.symbol).removesuffix("-PERP")
    ts = bar.ts_event // 1_000_000
    end = ((ts + MINUTE - 1) // MINUTE) * MINUTE
    return MinuteBar(
        symbol,
        end - MINUTE,
        *(getattr(bar, k).as_decimal() for k in ("open", "high", "low", "close", "volume")),
    )


class MinuteActor(DataActor):
    def __init__(self, source):
        super().__init__()
        self.source = source
        self.last_heartbeat = {}
        self.partials = {}
        self.last_partial = {}

    def on_start(self):
        self.subscribe_socket_state()
        for symbol in self.source.symbols:
            self.subscribe_bars(bar_type(symbol))
            self.subscribe_trades(instrument_id(symbol))
        self.source.loop.call_soon_threadsafe(self.source.ready.set)

    def on_bar(self, bar):
        value = minute_bar(bar)
        self.source.deliver((value.symbol, bar.ts_init // 1_000_000, value))
        previous = self.partials.get(value.symbol)
        if previous and previous.start_ms <= value.start_ms:
            self.partials.pop(value.symbol, None)

    def on_trade(self, trade):
        symbol = str(trade.instrument_id.symbol).removesuffix("-PERP")
        if getattr(self.source, "on_partial", None) is not None:
            stamp = trade.ts_event // 1_000_000
            left = stamp // MINUTE * MINUTE
            previous = self.partials.get(symbol)
            price, size = trade.price.as_decimal(), trade.size.as_decimal()
            if previous is None or left > previous.start_ms:
                partial = MinuteBar(symbol, left, price, price, price, price, size)
            elif left == previous.start_ms:
                partial = MinuteBar(symbol, left, previous.open, max(previous.high, price),
                                    min(previous.low, price), price, previous.volume + size)
            else:
                return
            self.partials[symbol] = partial
            tick = monotonic()
            if tick - self.last_partial.get(symbol, 0) >= 0.25:
                self.last_partial[symbol] = tick
                self.source.loop.call_soon_threadsafe(self.source.on_partial, partial, stamp)
        now = monotonic()
        if now - self.last_heartbeat.get(symbol, 0) >= 1:
            self.last_heartbeat[symbol] = now
            self.source.deliver((symbol, trade.ts_event // 1_000_000, None))

    def on_socket_state(self, event):
        self.source.sockets[(str(event.client_id), event.endpoint)] = (
            event.state == SocketState.CONNECTED
        )
        self.source.connected = all(self.source.sockets.values())
        if not self.source.connected and self.source.on_reconnect is not None:
            self.source.loop.call_soon_threadsafe(self.source.on_reconnect)

    def on_historical_bars(self, bars):
        self.source.loop.call_soon_threadsafe(self.source.history_result, list(bars))


class NativeMarketData:
    def __init__(self, symbols, *, http_url=None, websocket_url=None, timeout=20):
        self.symbols = tuple(symbols)
        self.http_url, self.websocket_url = http_url, websocket_url
        self.timeout = timeout
        self.node = self.task = self.actor = None
        self.on_message = self.on_reconnect = self.on_partial = None
        self.connected = True
        self.sockets = {}
        self.pending = None

    async def start(self):
        if self.task is not None:
            if self.task.done():
                self.task.result()
                raise DataError("Native data node is no longer running")
            return
        self.loop = asyncio.get_running_loop()
        self.ready = asyncio.Event()
        self.history_lock = asyncio.Lock()
        config = BinanceDataClientConfig(
            product_type=BinanceProductType.USD_M,
            environment=BinanceEnvironment.LIVE,
            base_url_http=self.http_url,
            base_url_ws=self.websocket_url,
            instrument_provider=BinanceInstrumentProviderConfig(
                load_all=False,
                load_ids=[str(instrument_id(s)) for s in self.symbols],
                query_commission_rates=False,
            ),
            max_retries=4,
            retry_delay_initial_ms=1000,
            retry_delay_max_ms=30_000,
        )
        builder = LiveNode.builder("minute-data", TraderId("DATA-001"), Environment.LIVE)
        builder.with_delay_post_stop_secs(0).with_delay_shutdown_secs(0)
        builder.with_timeout_connection(int(self.timeout))
        builder.with_timeout_disconnection_secs(5)
        builder.with_logging(LoggerConfig(stdout_level=LogLevel.ERROR))
        builder.add_data_client("BINANCE", BinanceDataClientFactory(), config)
        self.node = builder.build()
        self.actor = MinuteActor(self)
        self.node.add_actor(self.actor)
        self.handle = self.node.handle()
        self.task = asyncio.create_task(self.node.run_async())
        waiter = asyncio.create_task(self.ready.wait())
        try:
            done, _ = await asyncio.wait(
                [waiter, self.task], timeout=self.timeout, return_when=asyncio.FIRST_COMPLETED
            )
            if self.task in done:
                await self.task
                raise DataError("Native data node stopped before becoming ready")
            if waiter not in done:
                raise TimeoutError("Native Binance data connection timed out")
        except BaseException:
            await self.close()
            raise
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)

    def deliver(self, item):
        if self.on_message is not None:
            self.loop.call_soon_threadsafe(self.on_message, item)

    def history_result(self, bars):
        if self.pending is not None and not self.pending.done():
            self.pending.set_result(bars)

    async def bars(self, symbol, start_ms, end_ms):
        await self.start()
        async with self.history_lock:
            future = self.loop.create_future()
            self.pending = future
            try:
                self.actor.request_bars(
                    bar_type(symbol),
                    datetime.fromtimestamp(start_ms / 1000, UTC),
                    datetime.fromtimestamp((end_ms - 1) / 1000, UTC),
                    limit=1000,
                    params={"update_catalog": False},
                )
                values = await asyncio.wait_for(future, self.timeout)
                result = [minute_bar(bar) for bar in values]
                return [bar for bar in result if start_ms <= bar.start_ms < end_ms]
            finally:
                self.pending = None

    @property
    def active(self):
        return self.connected and self.task is not None and not self.task.done()

    async def close(self):
        try:
            if self.node is not None:
                self.handle.stop()
            if self.task is not None:
                try:
                    await asyncio.wait_for(asyncio.shield(self.task), 15)
                except (TimeoutError, asyncio.CancelledError):
                    self.task.cancel()
                    await asyncio.gather(self.task, return_exceptions=True)
        finally:
            if self.node is not None:
                self.node.dispose()
            self.task = self.node = None
