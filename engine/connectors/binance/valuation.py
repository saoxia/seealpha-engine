"""Valuation evidence: existing read-only asset REST + native public mark prices.

Persistent execution nodes would share a listenKey with short-lived queries,
whose disposal deletes that key. This observer has only a data client. Account
state changes are detected every five seconds without a private stream/session.
"""

import asyncio
import logging
import queue
import threading
import time
from decimal import Decimal

from nautilus_trader.adapters.binance import (
    BinanceDataClientConfig,
    BinanceDataClientFactory,
    BinanceInstrumentProviderConfig,
    BinanceProductType,
)
from nautilus_trader.common import Environment, LoggerConfig, LogLevel, SocketState
from nautilus_trader.live import LiveNode
from nautilus_trader.model import InstrumentId, TraderId

from .assets import asset_account
from .queries import QueryError, SnapshotReady, _clear_exception_frames, environment


class ValuationObserver(SnapshotReady):
    def __init__(self):
        super().__init__()
        self.marks = {}
        self.subscribed = set()
        self.disconnected = False

    def on_start(self):
        self.subscribe_socket_state()
        super().on_start()

    def on_socket_state(self, event):
        if event.state == SocketState.DISCONNECTED:
            self.disconnected = True

    def on_mark_price(self, mark):
        key = str(mark.instrument_id)
        value = {"price": str(mark.value), "time": mark.ts_event // 1_000_000}
        old = self.marks.get(key)
        if old is None or value["time"] >= old["time"]:
            self.marks[key] = value


def observed_state(evidence, cache, observer):
    """Apply native instrument units; callbacks never perform network/database I/O."""
    positions = []
    for source in evidence["positions"]:
        instrument_id = InstrumentId.from_str(source["instrument"])
        instrument = cache.instrument(instrument_id)
        if instrument is None or instrument.is_inverse or str(instrument.settlement_currency) != source["currency"]:
            raise QueryError("资产估值仅支持已解析的 USD-M 线性合约。")
        key = source["instrument"]
        if key not in observer.subscribed:
            observer.subscribe_mark_prices(instrument_id)
            observer.subscribed.add(key)
        positions.append({**source, "multiplier": str(instrument.multiplier),
                          "quantity": str(Decimal(source["quantity"]) * Decimal(str(instrument.multiplier)))})
    active = {p["instrument"] for p in positions}
    for obsolete in observer.subscribed - active:
        observer.unsubscribe_mark_prices(InstrumentId.from_str(obsolete))
        observer.marks.pop(obsolete, None)
    observer.subscribed.intersection_update(active)
    return {"wallet": {currency: str(item["walletBalance"]) for currency, item in evidence["assets"].items()},
            "positions": sorted(positions, key=lambda p: p["id"]),
            "marks": {key: observer.marks[key] for key in active if key in observer.marks},
            "account_time": evidence["observed_at"] * 1000}


async def valuation_events(account, *, http_url=None, websocket_url=None, timeout=30):
    """Yield on state/price changes, plus 10s freshness heartbeats. No listenKey use.

    REST failures, disconnects and bounded queue overflow end the stream. The API
    records a gap and establishes a new baseline. Native objects remain on their
    owning thread; cancellation joins their disposal before returning.
    """
    output = queue.Queue(maxsize=256)
    stop = threading.Event()

    async def run():
        config = BinanceDataClientConfig(
            product_type=BinanceProductType.USD_M, environment=environment(account["environment"]),
            base_url_http=http_url, base_url_ws=websocket_url,
            instrument_provider=BinanceInstrumentProviderConfig(load_all=True, query_commission_rates=False),
            max_retries=1,
        )
        builder = LiveNode.builder(f"valuation-{account['id']}", TraderId(f"VALUE-{account['id']:03d}"), Environment.LIVE)
        builder.with_delay_post_stop_secs(0).with_delay_shutdown_secs(0)
        builder.with_timeout_connection(timeout).with_timeout_disconnection_secs(3)
        builder.with_logging(LoggerConfig(stdout_level=LogLevel.OFF))
        builder.add_data_client("BINANCE", BinanceDataClientFactory(), config)
        node = builder.build()
        observer = ValuationObserver()
        node.add_strategy(observer)
        cache, handle = node.cache, node.handle()
        task = asyncio.create_task(node.run_async())
        waiter = asyncio.create_task(observer.ready.wait())
        try:
            done, _ = await asyncio.wait([task, waiter], timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
            if waiter not in done or task in done:
                raise QueryError("资产公开行情节点启动失败。")
            previous, emitted, read_at = None, 0, 0
            evidence = None
            while not stop.is_set():
                if task.done() or observer.disconnected:
                    raise QueryError("资产公开行情节点已断线。")
                if evidence is None or time.monotonic() - read_at >= 5:
                    evidence = await asyncio.to_thread(asset_account, account, positions=True)
                    read_at = time.monotonic()
                state = observed_state(evidence, cache, observer)
                now = int(time.time() * 1000)
                # account_time remains evidence metadata, not a balance-change event.
                changed = previous is None or any(state[key] != previous[key] for key in ("wallet", "positions", "marks"))
                if changed or now - emitted >= 10_000:
                    output.put_nowait({**state, "time": now, "reason": "baseline" if previous is None else "update"})
                    previous, emitted = state, now
                await asyncio.sleep(0.25)
        finally:
            waiter.cancel()
            handle.stop()
            try:
                await asyncio.wait_for(asyncio.shield(task), 6)
            except (Exception, asyncio.CancelledError):  # noqa: BLE001 - Dispose on owning thread.
                task.cancel()
            await asyncio.gather(task, waiter, return_exceptions=True)
            if not task.cancelled() and task.exception() is not None:
                _clear_exception_frames(task.exception())
            node.dispose()

    def worker():
        try:
            asyncio.run(run())
        except Exception as error:  # noqa: BLE001 - No native objects or secret error text cross threads.
            trace = error.__traceback__
            while trace and trace.tb_next:
                trace = trace.tb_next
            logging.getLogger(__name__).warning("Valuation node failed: type=%s line=%s",
                                                type(error).__name__, trace.tb_lineno if trace else None)
            _clear_exception_frames(error)
        finally:
            stop.set()

    thread = threading.Thread(target=worker, name=f"valuation-{account['id']}", daemon=True)
    thread.start()
    try:
        while True:
            try:
                value = await asyncio.to_thread(output.get, True, 1)
            except queue.Empty:
                if stop.is_set():
                    raise QueryError("资产事件流中断，将重新对账；原记录保留。") from None
                continue
            yield value
    finally:
        stop.set()
        await asyncio.to_thread(thread.join)
