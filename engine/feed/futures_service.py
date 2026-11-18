"""Shared, public USD-M futures transport with per-contract durable journals.

Binance routes raw trades to /public and mark-price estimates to /market.
Each channel is multiplexed across up to 128 symbols per socket. Every contract
retains its independent writer,
clock, queue, recovery checkpoint and SQLite journal.
"""

import asyncio
import fcntl
import json
import logging
import time

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from core.market.instruments import INTERVALS, normalize_symbol
from engine.feed.pair_service import serve_pair
from engine.feed.service import write_status

# Binance permits 1,024 streams per connection. Reserve high-volume headroom:
# 128 trade streams on /public + 128 funding streams on /market per group.
LOGGER = logging.getLogger(__name__)

MAX_FUTURES_SYMBOLS_PER_SOCKET = 128
PUBLIC_SOCKET = "wss://fstream.binance.com/public/stream"
MARKET_SOCKET = "wss://fstream.binance.com/market/stream"


class FuturesCombinedSocket:
    """Subscribe/unsubscribe in place; failed connections notify every pair."""

    async def stream(self, scope, *, channel="trade"):
        if channel not in {"trade", "mark"}:
            raise ValueError("Unknown futures stream channel")
        endpoint = PUBLIC_SOCKET if channel == "trade" else MARKET_SOCKET
        suffix = "@trade" if channel == "trade" else "@markPrice@1s"
        delay = 1
        while True:
            known = set(scope["members"])
            try:
                if len(known) > MAX_FUTURES_SYMBOLS_PER_SOCKET:
                    raise ValueError("Too many USD-M symbols on one socket")
                async with connect(
                    endpoint, open_timeout=12, close_timeout=5,
                    ping_interval=20, ping_timeout=30, max_queue=4096, max_size=65536,
                ) as socket:
                    subscribed = set()
                    pending = {}
                    next_id = 0
                    last_control = float("-inf")
                    while True:
                        desired = set(scope["members"])
                        if len(desired) > MAX_FUTURES_SYMBOLS_PER_SOCKET:
                            raise ValueError("Too many USD-M symbols on one socket")
                        now = time.monotonic()
                        if now - last_control >= 1:
                            additions, removed = desired - subscribed, subscribed - desired
                            if additions:
                                next_id += 1
                                await socket.send(json.dumps({
                                    "method": "SUBSCRIBE",
                                    "params": [f"{symbol.lower()}{suffix}" for symbol in sorted(additions)],
                                    "id": str(next_id),
                                }))
                                pending[str(next_id)] = additions
                            if removed:
                                next_id += 1
                                await socket.send(json.dumps({
                                    "method": "UNSUBSCRIBE",
                                    "params": [f"{symbol.lower()}{suffix}" for symbol in sorted(removed)],
                                    "id": str(next_id),
                                }))
                            if additions or removed:
                                subscribed = desired
                                last_control = now
                        try:
                            message = json.loads(await asyncio.wait_for(socket.recv(), timeout=0.5))
                        except TimeoutError:
                            continue
                        if not isinstance(message, dict):
                            raise TypeError("Unexpected combined market message")
                        if "id" in message:
                            if message.get("result", object()) is not None:
                                raise ValueError("Binance rejected combined subscription")
                            for symbol in sorted(pending.pop(message["id"], ())):
                                if symbol in scope["members"]:
                                    yield {"symbol": symbol, "kind": "connected"}
                            continue
                        name, row = message.get("stream"), message.get("data")
                        if not isinstance(name, str) or not isinstance(row, dict):
                            raise TypeError("Unexpected combined futures payload")
                        symbol = row.get("s")
                        if symbol not in scope["members"]:
                            continue  # An in-flight event from an unsubscribed symbol.
                        if (channel == "trade" and name == f"{symbol.lower()}@trade"
                                and row.get("e") == "trade"):
                            event = {"kind": "trade", "row": {
                                "id": row["t"], "time": row["T"], "price": row["p"],
                                "qty": row["q"], "isBuyerMaker": row["m"],
                            }}
                        elif (channel == "mark" and name == f"{symbol.lower()}@markPrice@1s"
                              and row.get("e") == "markPriceUpdate"):
                            event = {"kind": "mark", "row": row}
                        else:
                            raise ValueError("Unexpected futures stream identity")
                        delay = 1
                        known.add(symbol)
                        yield {"symbol": symbol, **event}
            except (WebSocketException, OSError, TimeoutError, ConnectionError,
                    ValueError, KeyError, TypeError) as exc:
                for symbol in sorted(known | scope["members"]):
                    yield {"symbol": symbol, "kind": "disconnected",
                           "error": f"{type(exc).__name__}: {exc}"}
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)


class QueueSocket:
    """Feed one contract from the shared stream without changing its journal."""

    def __init__(self, queue):
        self.queue = queue

    async def stream(self, _symbol):
        while True:
            event = await self.queue.get()
            if event is None:
                return
            yield event


async def serve_futures_batch(manifest, managed_root, catalog, *, project, socket_factory=FuturesCombinedSocket):
    """One tenant-wide collector process; 128-symbol groups use separate sockets."""
    if not managed_root.is_dir():
        raise OSError("USD-M managed storage mount unavailable")
    jobs, groups, retry_at = {}, {}, {}
    next_group = 0

    async def run(policy, job):
        symbol = policy["symbol"]
        root = managed_root / symbol / policy["base_level"]
        access_path = project / "data/.locks/futures_um" / f"{symbol}.access.lock"
        access_path.parent.mkdir(parents=True, exist_ok=True)
        root.mkdir(parents=True, exist_ok=True)
        with access_path.open("a") as access:
            fcntl.flock(access, fcntl.LOCK_SH | fcntl.LOCK_NB)
            with (root / "writer.lock").open("a") as writer:
                fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
                try:
                    await serve_pair(
                        policy, root, catalog, calculate=False,
                        trade_socket=QueueSocket(job["trades"]),
                        funding_socket=QueueSocket(job["funding"]),
                    )
                except Exception as exc:
                    now = int(time.time() * 1000)
                    write_status(root, {
                        "state": "DEGRADED", "data_ready": False,
                        "reason": "shared_futures_collector_failed", "last_error": str(exc),
                        "checked_ms": now, "valid_until_ms": now,
                    })
                    raise

    async def receive(scope, channel):
        stream = socket_factory().stream(scope, channel=channel)
        try:
            async for event in stream:
                symbol = event["symbol"]
                job = jobs.get(symbol)
                if job is None or job["task"].done():
                    continue
                kind = event["kind"]
                if kind in {"connected", "disconnected"}:
                    queue = job["trades"] if channel == "trade" else job["funding"]
                    await queue.put({key: value for key, value in event.items() if key != "symbol"})
                elif kind == "trade" and channel == "trade":
                    await job["trades"].put({"kind": "trade", "row": event["row"]})
                elif kind == "mark" and channel == "mark":
                    await job["funding"].put({"kind": "mark", "row": event["row"]})
                else:
                    raise ValueError("Unknown combined futures event")
        finally:
            await stream.aclose()
            # Reconnect resets contract handover even if this group is recreated.
            for symbol in scope["members"]:
                job = jobs.get(symbol)
                if job is not None and not job["task"].done():
                    queue = job["trades"] if channel == "trade" else job["funding"]
                    try:
                        queue.put_nowait({"kind": "disconnected", "error": "group_restarted"})
                    except asyncio.QueueFull:
                        job["task"].cancel()  # Fail closed rather than silently drop data.

    async def stop(symbol):
        job = jobs[symbol]
        for group in groups.values():
            group["scope"]["members"].discard(symbol)
        # Stop accepting events first and let the pair drain its dispatched
        # trade/estimate queue before releasing its durable writer lock.
        deadline = time.monotonic() + 5
        while ((not job["trades"].empty() or not job["funding"].empty())
               and not job["task"].done() and time.monotonic() < deadline):
            await asyncio.sleep(0.05)
        if not job["trades"].empty() or not job["funding"].empty():
            LOGGER.error("Futures %s stopped with unprocessed buffered messages", symbol)
        await asyncio.sleep(0)  # Yield to the per-contract journal writer.
        job["task"].cancel()
        await asyncio.gather(job["task"], return_exceptions=True)
        del jobs[symbol]

    try:
        while True:
            try:
                if manifest.stat().st_size > 5 * 1024 * 1024:
                    raise ValueError("Oversized futures subscription manifest")
                document = json.loads(manifest.read_text())
                if document.get("version") != 1 or not isinstance(document.get("policies"), list):
                    raise ValueError("Invalid futures subscription manifest")
                desired = {}
                for policy in document["policies"]:
                    symbol = normalize_symbol(policy["symbol"], "futures_um")
                    if (policy["product"] != "futures_um" or policy["base_level"] not in {"tick", *INTERVALS}
                            or symbol in desired or type(policy.get("realtime_enabled")) is not bool):
                        raise ValueError("Invalid or duplicate USD-M funding/trade policy")
                    if policy["realtime_enabled"]:
                        desired[symbol] = {**policy, "symbol": symbol}
            except (OSError, ValueError, KeyError, TypeError) as exc:
                LOGGER.error("Invalid shared futures configuration: %s", exc)
                await asyncio.sleep(1)
                continue

            for symbol in list(jobs):
                if symbol not in desired or jobs[symbol]["policy"]["base_level"] != desired[symbol]["base_level"]:
                    await stop(symbol)
            for symbol in list(jobs):
                if jobs[symbol]["task"].done():
                    task = jobs[symbol]["task"]
                    if not task.cancelled() and task.exception() is not None:
                        LOGGER.error("Futures %s collector error: %s", symbol, task.exception())
                    await stop(symbol)
                    retry_at[symbol] = time.monotonic() + 30

            for symbol, policy in sorted(desired.items()):
                if symbol not in jobs and time.monotonic() >= retry_at.get(symbol, 0):
                    job = {"policy": policy, "trades": asyncio.Queue(maxsize=10000),
                           "funding": asyncio.Queue(maxsize=512)}
                    job["task"] = asyncio.create_task(run(policy, job))
                    jobs[symbol] = job

            active = set(jobs)
            assigned = set()
            for group_id, group in list(groups.items()):
                group["scope"]["members"].intersection_update(active)
                if not group["scope"]["members"] or any(task.done() for task in group["tasks"]):
                    for task in group["tasks"]:
                        task.cancel()
                    await asyncio.gather(*group["tasks"], return_exceptions=True)
                    if group["scope"]["members"]:
                        for symbol in group["scope"]["members"]:
                            job = jobs[symbol]
                            for queue in (job["trades"], job["funding"]):
                                await queue.put({"kind": "disconnected", "error": "group_restarted"})
                    del groups[group_id]
                else:
                    assigned.update(group["scope"]["members"])
            for symbol in sorted(active - assigned):
                group = next((g for g in groups.values()
                              if len(g["scope"]["members"]) < MAX_FUTURES_SYMBOLS_PER_SOCKET), None)
                if group is None:
                    group = {"scope": {"members": set()}, "tasks": None}
                    groups[next_group] = group
                    next_group += 1
                group["scope"]["members"].add(symbol)
            for group in groups.values():
                if group["tasks"] is None:
                    group["tasks"] = [
                        asyncio.create_task(receive(group["scope"], channel))
                        for channel in ("trade", "mark")
                    ]
            await asyncio.sleep(1)
    finally:
        for group in groups.values():
            for task in group["tasks"]:
                task.cancel()
        await asyncio.gather(
            *(task for g in groups.values() for task in g["tasks"]),
            return_exceptions=True,
        )
        for symbol in list(jobs):
            await stop(symbol)
