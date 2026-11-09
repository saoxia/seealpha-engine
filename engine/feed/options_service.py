"""Managed native option evidence; no strategy or execution client."""

import asyncio
import json
import os
import time

from engine.connectors.binance.options_client import market_events
from engine.connectors.binance.queries import QueryError

from .history import CatalogHistory
from .service import write_status


async def serve_options(policy, root, catalog, *, stream_factory=None, definition=None, repair_limiter=None):
    history = CatalogHistory(catalog)
    checkpoint = root / "checkpoint.json"
    saved = json.loads(checkpoint.read_text()) if checkpoint.exists() else {}
    repair_from = saved.get("repair_from_ms", saved.get("last_update_ms", int(time.time() * 1000) // 86400000 * 86400000))
    watermarks = saved.get("bar_watermarks", {})
    gap_ranges = saved.get("gaps", [])
    if saved:
        gap_ranges.append({"start_ms": saved["last_update_ms"], "end_ms": int(time.time() * 1000), "reason": "restart"})
    gaps = saved.get("gap_count", 0) + int(bool(saved))
    connected, last_update, error = False, saved.get("last_update_ms"), None
    expiry = None
    pending = []
    repair_task = None
    observed_gaps = 0
    repair_limiter = repair_limiter or asyncio.Semaphore(1)

    def publish(reason=None, *, stopped=False):
        now = int(time.time() * 1000)
        write_status(root, {"state": "STOPPED" if stopped else "DEGRADED" if error or not connected else "WARMING_UP" if repair_task is not None else "READY",
                     "data_ready": connected and not error and not stopped and repair_task is None, "checked_ms": now,
                     "valid_until_ms": now + 5000, "reason": reason or error or ("repair_pending" if repair_task is not None else "collecting_options"),
                     "symbols": {policy["symbol"]: {"last_websocket_ms": last_update, "last_update_ms": last_update}},
                     "backlog": len(pending), "last_error": error, "gap_count": gaps,
                     "historical_tick_complete": False, "allow_new_risk": False, "expiry_ms": expiry})

    def save_checkpoint():
        temporary = checkpoint.with_suffix(".json.tmp")
        with temporary.open("w") as target:
            json.dump({"last_update_ms": last_update, "repair_from_ms": repair_from,
                       "bar_watermarks": watermarks, "gap_count": gaps, "gaps": gap_ranges}, target)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, checkpoint)

    async def persist(**kwargs):
        # Cancellation must not release the inherited access lease while a writer remains active.
        if definition is not None:
            kwargs["definition"] = definition
        task = asyncio.create_task(asyncio.to_thread(history.options_evidence, policy["symbol"], **kwargs))
        try:
            return await asyncio.shield(task)
        finally:
            if not task.done():
                await task

    async def repair():
        async with repair_limiter:
            return await persist(repair_from=repair_from, recent=True)

    try:
        metadata = await persist(recover=True)
        expiry = metadata["expiry_ms"]
        while int(time.time() * 1000) < expiry:
            stream = (stream_factory or market_events)(policy["symbol"], ["1m", "5m", "15m"], trades=True, depth=True)
            repaired = False
            try:
                async for event in stream:
                    if event["level"] == "status":
                        connected = event["connected"]
                        discontinuity = event.get("discontinuity", False)
                        stream_gaps = event.get("gap_count", 0)
                        if discontinuity or stream_gaps > observed_gaps:
                            error = "期权实时流断线或订阅/数据校验失败"
                            gaps += max(1, stream_gaps - observed_gaps)
                            gap_ranges.append({"start_ms": last_update, "end_ms": int(time.time() * 1000), "reason": "disconnected"})
                            save_checkpoint()
                            repaired = False
                        observed_gaps = stream_gaps
                        if connected:
                            error = None
                        if not connected:
                            repaired = False
                            if repair_task is not None:
                                repair_task.cancel()
                                await asyncio.gather(repair_task, return_exceptions=True)
                                repair_task = None
                        if connected and not repaired and repair_task is None:
                            repair_task = asyncio.create_task(repair())
                        if repair_task is not None and repair_task.done():
                            result = repair_task.result()
                            repair_task = None
                            until = result.get("repaired_until_ms", int(time.time() * 1000))
                            for level, width in (("1m", 60000), ("5m", 300000), ("15m", 900000)):
                                watermarks[level] = max(watermarks.get(level, 0), until // width * width)
                            repair_from = min(watermarks.values())
                            repaired = connected
                            save_checkpoint()
                        if pending:
                            await persist(events=list(pending))
                            pending.clear()
                            last_update = int(time.time() * 1000)
                            repair_from = min(watermarks.values(), default=repair_from)
                            save_checkpoint()
                        publish()
                    else:
                        pending.append(event)
                        if event["level"] not in {"tick", "depth"}:
                            level = event["level"]
                            watermarks[level] = max(watermarks.get(level, 0), event["row"][6] + 1)
                        if len(pending) >= 1000:
                            await persist(events=list(pending))
                            pending.clear()
                            last_update = int(time.time() * 1000)
                            repair_from = min(watermarks.values(), default=repair_from)
                            save_checkpoint()
                    if int(time.time() * 1000) >= expiry:
                        break
                if pending:
                    await persist(events=list(pending))
                    pending.clear()
                    last_update = int(time.time() * 1000)
                    repair_from = min(watermarks.values(), default=repair_from)
                    save_checkpoint()
                if stream_factory is not None:
                    return
            except (OSError, ValueError, QueryError) as exc:
                connected, error = False, str(exc)
                if repair_task is not None:
                    repair_task.cancel()
                    await asyncio.gather(repair_task, return_exceptions=True)
                    repair_task = None
                gaps += 1
                gap_ranges.append({"start_ms": last_update, "end_ms": int(time.time() * 1000), "reason": "disconnected"})
                # Evidence already captured is retried before any new subscription.
                if pending:
                    await persist(events=list(pending))
                    pending.clear()
                publish("disconnected")
                save_checkpoint()
                await asyncio.sleep(5)
            finally:
                await stream.aclose()
            error = None
        publish("expired", stopped=True)
    finally:
        if repair_task is not None:
            repair_task.cancel()
            await asyncio.gather(repair_task, return_exceptions=True)
        if pending:
            await persist(events=list(pending))
            pending.clear()
            last_update = int(time.time() * 1000)
            repair_from = min(watermarks.values(), default=repair_from)
            save_checkpoint()
        if expiry is None or int(time.time() * 1000) < expiry:
            publish("service_stopped", stopped=True)


async def serve_batch(manifest, managed_root, catalog, *, project=None):
    """Reconcile contract workers and combined sockets inside one collector process."""
    import fcntl

    from nautilus_trader.model import CryptoOption

    from core.market.instruments import normalize_symbol
    from core.paths import PROJECT_ROOT
    from engine.connectors.binance import options
    from engine.connectors.binance.options_client import batch_market_events

    project = project or PROJECT_ROOT
    history = CatalogHistory(catalog)
    jobs, transports, definitions, retry_at = {}, {}, {}, {}
    repair_limiter = asyncio.Semaphore(2)
    fetch, fetched_at = None, float("-inf")

    async def run(policy, definition, queue):
        symbol = policy["symbol"]
        root = managed_root / symbol / policy["base_level"]
        access = project / ".local/run/api/data/options" / f"{symbol}.access.lock"
        access.parent.mkdir(parents=True, exist_ok=True)

        async def stream(_symbol, _levels, *, trades, depth):
            while True:
                event = await queue.get()
                if event is None:
                    return
                yield event

        with access.open("a") as lease:
            fcntl.flock(lease, fcntl.LOCK_SH | fcntl.LOCK_NB)
            if not managed_root.is_dir():
                raise OSError("行情存储不可用，不重建挂载目录")
            root.mkdir(parents=True, exist_ok=True)
            with (root / "writer.lock").open("a") as writer_lease:
                fcntl.flock(writer_lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                try:
                    await serve_options(policy, root, catalog, stream_factory=stream,
                                        definition=definition, repair_limiter=repair_limiter)
                    # Expiry may end the worker before it consumes its accepted tail.
                    # Stop dispatch first, then flush that tail under the same leases.
                    job = jobs.get(symbol)
                    if job is not None:
                        job["stopping"] = True
                    tail = []
                    for _ in range(2):
                        while not queue.empty():
                            event = queue.get_nowait()
                            if event is not None and event["level"] != "status":
                                tail.append(event)
                        await asyncio.sleep(0)
                    if tail:
                        pending = asyncio.create_task(asyncio.to_thread(history.options_evidence, symbol,
                                                                       events=tail, definition=definition))
                        try:
                            await asyncio.shield(pending)
                        finally:
                            if not pending.done():
                                await pending
                except (OSError, ValueError, QueryError) as exc:
                    if root.is_dir():
                        stamp = int(time.time() * 1000)
                        write_status(root, {"state": "DEGRADED", "data_ready": False, "reason": str(exc),
                                     "checked_ms": stamp, "valid_until_ms": stamp + 5000})
                    raise

    async def stop(job):
        job["stopping"] = True
        if not job["task"].done():
            marker = asyncio.create_task(job["queue"].put(None))
            try:
                await asyncio.wait((marker, job["task"]), return_when=asyncio.FIRST_COMPLETED)
            finally:
                marker.cancel()
                await asyncio.gather(marker, return_exceptions=True)

    async def receive(scope):
        items, ticks = scope["selection"]
        stream = batch_market_events(items, tick_symbols=ticks, scope=scope, depth=True)
        try:
            async for event in stream:
                job = jobs.get(event["symbol"])
                if job is not None:
                    while not job["task"].done() and not job.get("stopping", False):
                        try:
                            job["queue"].put_nowait(event)
                            break
                        except asyncio.QueueFull:
                            try:
                                await asyncio.wait_for(job["queue"].put(event), 1)
                                break
                            except TimeoutError:
                                pass
        finally:
            await stream.aclose()

    try:
        while True:
            now = time.monotonic()
            if fetch is None and now - fetched_at >= 300:
                fetch = asyncio.create_task(asyncio.to_thread(options.markets))
            if fetch is not None and fetch.done():
                try:
                    definitions.update({row["symbol"]: options.instrument(row).to_dict() for row in fetch.result()})
                    fetched_at = now
                except (OSError, ValueError, QueryError):
                    fetched_at = now - 270
                fetch = None
            try:
                if manifest.stat().st_size > 5 * 1024 * 1024:
                    raise ValueError("期权运行配置过大")
                value = json.loads(manifest.read_text())
                if value.get("version") != 1 or not isinstance(value.get("policies"), list):
                    raise ValueError("期权运行配置异常")
                desired = {}
                for policy in value["policies"]:
                    symbol = normalize_symbol(policy["symbol"], "options")
                    if policy["product"] != "options" or policy["base_level"] not in {"tick", "1m", "5m", "15m"}:
                        raise ValueError("期权运行范围异常")
                    if symbol in desired or type(policy.get("realtime_enabled")) is not bool:
                        raise ValueError("期权运行配置重复或开关无效")
                    if policy["realtime_enabled"]:
                        desired[symbol] = {**policy, "symbol": symbol}
            except (OSError, ValueError, KeyError, TypeError):
                await asyncio.sleep(1)
                continue
            removed = [symbol for symbol, job in jobs.items()
                       if symbol not in desired or job["policy"]["base_level"] != desired[symbol]["base_level"]]
            for symbol in removed:
                await stop(jobs[symbol])
            if removed:
                await asyncio.gather(*(jobs[symbol]["task"] for symbol in removed), return_exceptions=True)
                for symbol in removed:
                    del jobs[symbol]
            for symbol, policy in desired.items():
                if symbol in jobs and jobs[symbol]["task"].done():
                    task = jobs[symbol]["task"]
                    if task.cancelled() or task.exception() is not None:
                        del jobs[symbol]
                        retry_at[symbol] = now + 30
                if symbol not in jobs and now >= retry_at.get(symbol, 0):
                    definition = definitions.get(symbol)
                    if definition is None:
                        if fetch is not None:
                            continue
                        try:
                            definition = await asyncio.to_thread(history.options_definition, symbol)
                        except (OSError, ValueError, QueryError):
                            fetched_at = now - 300
                            retry_at[symbol] = now + 5
                            access = project / ".local/run/api/data/options" / f"{symbol}.access.lock"
                            access.parent.mkdir(parents=True, exist_ok=True)
                            try:
                                with access.open("a") as lease:
                                    fcntl.flock(lease, fcntl.LOCK_SH | fcntl.LOCK_NB)
                                    if not managed_root.is_dir():
                                        raise OSError("行情存储不可用")
                                    root = managed_root / symbol / policy["base_level"]
                                    root.mkdir(parents=True, exist_ok=True)
                                    stamp = int(time.time() * 1000)
                                    write_status(root, {"state": "DEGRADED", "data_ready": False, "reason": "waiting_definition",
                                                 "last_error": "等待官方或本地合约定义，尚未启动订阅", "checked_ms": stamp, "valid_until_ms": stamp + 5000})
                            except OSError:
                                pass
                            continue
                    queue = asyncio.Queue(maxsize=256)
                    jobs[symbol] = {"policy": policy, "definition": definition, "queue": queue,
                                    "task": asyncio.create_task(run(policy, definition, queue))}
            active = {symbol for symbol, job in jobs.items() if not job["task"].done()
                      and job["definition"]["expiration_ns"] > time.time_ns()}
            assigned = set()
            for identity, group in list(transports.items()):
                members = group["members"] & active
                if not members or group["task"].done():
                    group["task"].cancel()
                    await asyncio.gather(group["task"], return_exceptions=True)
                    if members:
                        for symbol in members:
                            try:
                                jobs[symbol]["queue"].put_nowait({"level": "status", "connected": False, "discontinuity": True})
                            except asyncio.QueueFull:
                                jobs[symbol]["task"].cancel()
                    del transports[identity]
                else:
                    group["members"] = members
                    assigned.update(members)
            for symbol in sorted(active - assigned):
                group = next((value for value in transports.values() if len(value["members"]) < 64), None)
                if group is None:
                    identity = max(transports, default=-1) + 1
                    group = {"members": set(), "scope": {"selection": ([], set())}, "task": None}
                    transports[identity] = group
                group["members"].add(symbol)
            for group in transports.values():
                members = sorted(group["members"])
                selection_key = tuple(members)
                if selection_key != group.get("selection_key"):
                    items = [CryptoOption.from_dict(jobs[symbol]["definition"]) for symbol in members]
                    ticks = set(members)
                    group["scope"]["selection"] = (items, ticks)
                    group["selection_key"] = selection_key
                if group["task"] is None:
                    group["task"] = asyncio.create_task(receive(group["scope"]))
            await asyncio.sleep(1)
    finally:
        for group in transports.values():
            group["task"].cancel()
        await asyncio.gather(*(group["task"] for group in transports.values()), return_exceptions=True)
        await asyncio.gather(*(stop(job) for job in jobs.values()))
        await asyncio.gather(*(job["task"] for job in jobs.values()), return_exceptions=True)
        if fetch is not None:
            await asyncio.gather(fetch, return_exceptions=True)
