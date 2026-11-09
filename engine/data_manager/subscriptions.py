"""Manage existing feed processes and their product/symbol settings, never market storage."""

import asyncio
import fcntl
import json
import os
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException

from .contracts import DataPolicy, option_pair


class Subscriptions:
    def __init__(self, manager):
        self.manager = manager
        self.project = manager.project
        self.lock = threading.RLock()
        self.processes = {}
        self.auxiliary = {}
        self.policies = None
        self.last_error = None
        self.lease = None
        self.closed = False
        self.deleting = set()

    def bound_tenant_id(self):
        return self.manager.tenant_id

    def _read(self, *, refresh=False):
        if self.policies is None or refresh:
            if self.manager.policy_repository is None:
                raise HTTPException(503, "行情服务配置需要已迁移的 ORM 数据库")
            try:
                values = self.manager.policy_repository.list(tenant_id=self.bound_tenant_id())
                policies = [DataPolicy.model_validate(item) for item in values]
                if len({(p.product, p.symbol) for p in policies}) != len(policies):
                    raise ValueError("Duplicate policy")
            except (ValueError, TypeError):
                raise HTTPException(503, "数据库行情配置异常；不会自动重置") from None
            self.policies = policies
        return list(self.policies)

    def _claim(self):
        if self.closed:
            raise HTTPException(503, "服务正在停止")
        if self.lease is not None:
            return
        path = self.project / ".local/run/api/subscriptions.lock"
        path.parent.mkdir(parents=True, exist_ok=True)
        lease = path.open("a")
        try:
            fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lease.close()
            raise HTTPException(409, "实时采集由另一个 API 进程管理，请在所属服务操作") from None
        self.lease = lease

    def root(self, policy):
        root = self.project / "data/market_data/binance_um_1m/managed" / policy.symbol
        return root if policy.base_level == "auto" else root / policy.base_level

    def journal_paths(self, policy):
        from core.market.instruments import INTERVALS
        root = self.project / "data/market_data/binance_um_1m/managed" / policy.symbol
        # A new minimum never hides older retained datasets. Known names only;
        # do not walk arbitrary directories or recreate an unavailable volume.
        paths = [self.root(policy) / "collection.sqlite", *(root / level / "collection.sqlite" for level in ("tick", *INTERVALS))]
        return list(dict.fromkeys(path for path in paths if path.is_file())) if policy.product == "futures_um" else []

    def _launch(self, command, log_name, access=None):
        """Ask the resident engine to fork feed/history. In-process mode keeps the same lock fds."""
        directory = self.project / "logs/api/data/feed"
        directory.mkdir(parents=True, exist_ok=True)
        log_path = directory / log_name
        engine = getattr(self.manager, "engine", None)
        if engine is not None and engine.remote:
            return engine.spawn_worker(
                command, log_path, tenant_id=self.bound_tenant_id(), access=access,
                hold_lease=self.lease is not None,
            )
        import contextlib

        from engine.supervisor import popen

        stack = contextlib.ExitStack()
        output = stack.enter_context(log_path.open("ab", buffering=0))
        fds = [self.lease.fileno()] if self.lease is not None else []
        if access is not None:
            symbol, product, shared = access
            lease = stack.enter_context(self.manager._lease(symbol, product, access=True, shared=shared))
            fds.append(lease.fileno())
        try:
            process = popen(
                command, cwd=self.project, stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                start_new_session=True, pass_fds=tuple(fds), env={**os.environ, "PYTHONUNBUFFERED": "1",
                     "ENGINE_DATA_JOBS": str(self.manager.directory)},
            )
        finally:
            stack.close()
        if engine is not None:
            engine.note_worker(command, tenant_id=self.bound_tenant_id())
        return process

    def _start(self, policy):
        if policy.product == "options":
            self._start_options()
            return
        key = (policy.product, policy.symbol)
        roles = (["collection"] if policy.realtime_enabled else [])
        if policy.base_level != "auto" and policy.product == "futures_um":
            if policy.realtime_enabled or policy.history_enabled:
                roles.append("calculation")
            if policy.history_enabled:
                roles.append("history")
        for role in roles:
            processes = self.processes if role == "collection" else self.auxiliary
            identity = key if role == "collection" else (*key, role)
            existing = processes.get(identity)
            if existing is not None and existing.poll() is None:
                continue
            command = ([sys.executable, "-m", "engine.pipeline.pair_data", "--service", "--root", str(self.root(policy)),
                        "--request", policy.model_dump_json()] if role == "history" else
                       [sys.executable, "-m", "engine.feed.managed", "--symbol", policy.symbol,
                        "--data-root", str(self.project / "data"), "--root", str(self.root(policy)),
                        "--catalog", str(self.project / "data/catalog"), "--intervals", "1m",
                        *(["--policy", policy.model_dump_json(), "--role", role] if policy.base_level != "auto" else policy.intervals)])
            processes[identity] = self._launch(
                command, f"{policy.symbol}-{role}.log", access=(policy.symbol, policy.product, True),
            )

    def restore(self):
        from .repository import StorageUnavailable
        with self.lock:
            try:
                previous = self.policies or []
                policies = self._read(refresh=True)
                if any(p.product == "options" for p in [*previous, *policies]):
                    self._claim()
                    self._publish_options()
                    self._start_options()
                if previous:
                    self._claim()
                    for old in previous:
                        if old.product == "options":
                            continue
                        key = (old.product, old.symbol)
                        new = next((p for p in policies if (p.product, p.symbol) == key), None)
                        if new != old:
                            same_collection = (new is not None and old.realtime_enabled and new.realtime_enabled
                                               and old.base_level == new.base_level and set(old.intervals) == set(new.intervals))
                            self._stop_pair(key, collection=not same_collection)
                if not any(p.realtime_enabled or p.history_enabled for p in policies):
                    self.last_error = None
                    return
                self.manager.require_storage(write=True)
                self._claim()
                errors = []
                for policy in policies:
                    if policy.product == "options":
                        continue
                    if policy.realtime_enabled or policy.history_enabled:
                        try:
                            self._start(policy)
                        except (HTTPException, OSError) as exc:
                            errors.append(f"{policy.symbol}: {exc}")
                self.last_error = "；".join(errors) or None
            except (HTTPException, StorageUnavailable, OSError) as exc:
                self.last_error = str(exc.detail if isinstance(exc, HTTPException) else exc)
                # Retain the previous configuration and running services on outages.

    async def sync_options(self, exchanges):
        """Discover contracts for persisted underlying scopes using the cached catalogue."""
        from .repository import StorageUnavailable
        try:
            policies = await asyncio.to_thread(self._read)
            if not any(p.product == "options" and "-" not in p.symbol for p in policies):
                return
            markets = await exchanges.catalog("production", product="options")
            await asyncio.to_thread(self._include_options, markets)
        except HTTPException as exc:
            self.last_error = str(exc.detail)
        except (StorageUnavailable, OSError, ValueError, RuntimeError, TimeoutError):
            self.last_error = "期权目录同步失败；保留原配置，稍后重试"

    def _include_options(self, markets):
        with self.lock:
            self._claim()
            policies = self._read(refresh=True)
            scopes = {p.symbol: p for p in policies if p.product == "options" and "-" not in p.symbol}
            if not scopes:
                return
            known = {(p.product, p.symbol) for p in policies} | self.deleting
            configured = {p.symbol: p for p in policies if p.product == "options"}
            additions, metadata = [], {}
            for symbol, market in {item["symbol"]: item for item in markets}.items():
                previous = configured.get(symbol)
                pair = market.get("underlying") or (previous.underlying if previous else None) or option_pair(symbol)
                scope = scopes.get(pair, scopes.get("ALL"))
                if scope is None or ("options", pair) in self.deleting:
                    continue
                if pair not in scopes:
                    scope = DataPolicy.model_validate({**scope.model_dump(), "symbol": pair})
                    scopes[pair] = scope
                    additions.append(scope)
                if ("options", symbol) not in known:
                    additions.append(DataPolicy.model_validate({**scope.model_dump(), "symbol": symbol, "underlying": pair}))
                metadata[symbol] = pair
            updated = [p.model_copy(update={"underlying": metadata[p.symbol]})
                       if p.product == "options" and p.symbol in metadata else p for p in policies]
            if additions or updated != policies:
                self._write([*updated, *additions])
                if any(p.realtime_enabled or p.history_enabled for p in additions):
                    self.manager.require_storage(write=True)
                    self._start(additions[0])
            if self.last_error == "期权目录同步失败；保留原配置，稍后重试":
                self.last_error = None

    def _stop_pair(self, key, *, collection=True, auxiliary=True):
        if key[0] == "options":
            self._start_options()
            return
        if collection:
            process = self.processes.pop(key, None)
            if process is not None:
                self.manager._stop(process)
        if auxiliary:
            for identity in [value for value in self.auxiliary if value[:2] == key]:
                self.manager._stop(self.auxiliary.pop(identity))

    def save(self, policy):
        # Shared access coexists with historical work; only deletion excludes it.
        # Keep lock ordering consistent with delete: subscription state, then access.
        with self.lock, self.manager._lease(policy.symbol, policy.product, access=True, shared=True):
            return self._save(policy)

    def resolve(self, policy, old=None):
        """Resolve input choices once to the UTC start stored in configuration."""
        if policy.product == "futures_um" and policy.base_level == "auto":
            policy = DataPolicy.model_validate({**policy.model_dump(), "base_level": "1m"})
        if policy.history_enabled and policy.product == "futures_um":
            start = policy.history_start
            if policy.history_mode == "all":
                from engine.connectors.binance.pair_data import PairREST
                try:
                    rows = asyncio.run(PairREST(symbols=(policy.symbol,)).get(
                        "klines", symbol=policy.symbol, interval="1m", startTime=0, limit=1))
                    if not rows:
                        raise ValueError("交易所没有可用历史")
                    start = datetime.fromtimestamp(int(rows[0][0]) / 1000, UTC)
                except (OSError, ValueError, RuntimeError, TimeoutError) as exc:
                    raise HTTPException(503, "无法确定交易所最早历史日期；配置未保存") from exc
            elif policy.history_mode == "duration":
                if old is not None and old.history_mode == "start" and old.history_start is not None:
                    start = old.history_start - timedelta(days=policy.history_days)
                else:
                    from engine.feed.pair_store import journal_inventory
                    from engine.pipeline.catalog import ParquetDataCatalog
                    from engine.pipeline.config import load_settings
                    from engine.pipeline.pair_data import history_bounds
                    settings = load_settings()
                    catalog = ParquetDataCatalog(settings.catalog_dir) if settings.catalog_dir.is_dir() else None
                    starts = [int(item["event_ranges_ns"][0][0])
                              for path in self.journal_paths(policy)
                              for item in journal_inventory(path, policy.symbol)
                              if item["level"] == policy.base_level and item["event_ranges_ns"]]
                    start, _ = history_bounds(policy.model_dump(mode="json"), catalog, datetime.now(UTC),
                                              retained_start_ns=min(starts, default=None))
            policy = policy.model_copy(update={
                "history_mode": "start", "history_start": start.replace(hour=0, minute=0, second=0, microsecond=0),
                "history_days": DataPolicy.model_fields["history_days"].default,
            })
        return policy

    def _save(self, policy, *, allow_later_start=False):
        if policy.realtime_enabled or policy.history_enabled:
            self.manager.require_storage(write=True)
        with self.lock:
            self._claim()
            policies = self._read(refresh=True)
            key = (policy.product, policy.symbol)
            if key in self.deleting or (policy.product == "options" and ("options", policy.pair) in self.deleting):
                raise HTTPException(409, "该交易对正在删除，请等待任务完成")
            old = next((item for item in policies if (item.product, item.symbol) == key), None)
            if old is not None and policy.product == "options" and "-" in policy.symbol:
                policy = policy.model_copy(update={"underlying": old.underlying or policy.underlying})
            policy = self.resolve(policy, old)
            if (policy.product == "futures_um" and not allow_later_start and old is not None and old.history_mode == "start" and old.history_start is not None
                    and policy.history_start is not None and policy.history_start > old.history_start):
                raise HTTPException(409, "开始日期调晚须先预览并确认删除旧数据")
            same_collection = (old is not None and old.realtime_enabled and policy.realtime_enabled
                               and old.base_level == policy.base_level
                               and set(old.intervals) == set(policy.intervals))
            if policy.product == "options" and "-" not in policy.symbol:
                # A user save applies the scope to existing contracts in one transaction.
                if any(product == "options" and (policy.symbol == "ALL" or symbol == policy.symbol
                       or any(p.symbol == symbol and p.pair == policy.symbol for p in policies))
                       for product, symbol in self.deleting):
                    raise HTTPException(409, "期权合约正在删除，完成后再调整范围配置")
                updated = [DataPolicy.model_validate({**policy.model_dump(), "symbol": item.symbol, "underlying": item.underlying})
                           if item.product == "options" and (policy.symbol == "ALL" or item.pair == policy.symbol)
                           else item for item in policies if (item.product, item.symbol) != key]
                self._write([*updated, policy])
                self._start(policy)
                return self._public(policy)
            policies = [item for item in policies if (item.product, item.symbol) != key] + [policy]
            self._write(policies)
            if not same_collection:
                self._stop_pair(key, auxiliary=False)
            if old != policy:
                self._stop_pair(key, collection=False)
            if policy.realtime_enabled or policy.history_enabled:
                try:
                    self._start(policy)
                    self.last_error = None
                except (OSError, HTTPException) as exc:
                    self.last_error = str(exc)
            return self._public(policy)

    def _write(self, policies):
        if self.manager.policy_repository is None:
            raise HTTPException(503, "行情服务配置需要已迁移的 ORM 数据库")
        self.manager.policy_repository.save(policies, tenant_id=self.bound_tenant_id())
        self.policies = list(policies)
        if any(p.product == "options" for p in policies) or (self.manager.directory / "options-runtime.json").exists():
            self._publish_options()

    def _publish_options(self):
        from engine.pipeline.convert_binance_bars import write_record
        path = self.manager.directory / "options-runtime.json"
        values = [p.model_dump(mode="json") for p in self.policies or [] if p.product == "options" and "-" in p.symbol]
        write_record(path, {"version": 1, "policies": values})
        return path

    def _start_options(self):
        """Two shared processes; no contract-sized process or inherited FD lists."""
        path = self._publish_options()
        root = self.project / "data/market_data/binance_um_1m/managed/.options"
        for role in ("collection", "history"):
            processes = self.processes if role == "collection" else self.auxiliary
            key = ("options", "ALL") if role == "collection" else ("options", "ALL", role)
            enabled = any(p.product == "options" and "-" in p.symbol and
                          (p.realtime_enabled if role == "collection" else p.history_enabled)
                          and (p.product, p.symbol) not in self.deleting for p in self.policies or [])
            existing = processes.get(key)
            if not enabled:
                if existing is not None:
                    self.manager._stop(processes.pop(key))
                continue
            if existing is not None and existing.poll() is None:
                continue
            self.manager.require_storage(write=True)
            command = ([sys.executable, "-m", "engine.feed.managed", "--options-config", str(path),
                        "--root", str(root), "--data-root", str(self.project / "data"),
                        "--catalog", str(self.project / "data/catalog")] if role == "collection" else
                       [sys.executable, "-m", "engine.pipeline.pair_data", "--service", "--options-config", str(path),
                        "--root", str(root)])
            processes[key] = self._launch(command, f"options-{role}.log")

    def pause_for_deletion(self, symbol, product):
        with self.lock:
            self._claim()
            policies = self._read(refresh=True)
            if product == "options" and "-" not in symbol:
                members = [p for p in policies if p.product == product and p.pair == symbol]
            else:
                members = [p for p in policies if (p.product, p.symbol) == (product, symbol)]
            if (product, symbol) in self.deleting or any((product, p.symbol) in self.deleting for p in members):
                raise HTTPException(409, "该交易对正在删除，请等待任务完成")
            if members:
                self._write([p.model_copy(update={"realtime_enabled": False, "history_enabled": False})
                             if p in members else p for p in policies])
                self._stop_pair((product, symbol))
                if product == "options":
                    # Both batch workers must finish writes for every selected
                    # contract before taking a stable deletion preview.
                    deadline = time.monotonic() + 5
                    for member in members:
                        if "-" not in member.symbol:
                            continue
                        while True:
                            try:
                                self.manager._lease(member.symbol, product, access=True).close()
                                break
                            except HTTPException:
                                if time.monotonic() >= deadline:
                                    raise HTTPException(409, "该期权正在停止采集或补齐，请稍后重新预览删除。") from None
                                time.sleep(0.05)

    def remove(self, symbol, product):
        with self.lock:
            self._claim()
            policies = self._read()
            group = product == "options" and "-" not in symbol
            if group:
                # Removing ALL discovery must retain discovery for other existing
                # underlyings, including legacy configurations not yet expanded.
                scope = next((p for p in policies if p.product == "options" and p.symbol == "ALL"), None)
                groups = {p.symbol for p in policies if p.product == "options" and "-" not in p.symbol}
                for policy in list(policies):
                    if policy.product == "options" and "-" in policy.symbol and policy.pair not in groups:
                        source = scope or policy
                        policies.append(DataPolicy.model_validate({**source.model_dump(), "symbol": policy.pair, "underlying": None}))
                        groups.add(policy.pair)
            self._write([p for p in policies if not (p.product == product and
                         (p.symbol == symbol or group and p.pair in {symbol, "ALL"}))])

    def _public(self, policy):
        if policy.product == "options" and "-" not in policy.symbol:
            return self._option_group(policy)
        state = {"state": "STOPPED", "data_ready": False, "reason": "not_started",
                 "bar_updates": [], "symbols": {}}
        path = self.root(policy) / "status.json"
        try:
            if path.is_file():
                if path.stat().st_size > 131072:
                    raise ValueError("Oversize status")
                value = json.loads(path.read_text())
                if (not isinstance(value, dict) or value.get("state") not in
                        {"STOPPED", "READY", "DEGRADED", "WARMING_UP"}
                        or type(value.get("valid_until_ms")) is not int
                        or type(value.get("data_ready", False)) is not bool):
                    raise ValueError("Invalid status")
                state = {key: value[key] for key in ("state", "data_ready", "reason", "checked_ms",
                         "valid_until_ms", "symbols", "job", "bar_updates", "last_error", "gap_count", "scope", "expiry_ms") if key in value}
                expired_option = (policy.product == "options" and value.get("reason") == "expired"
                                  and value.get("expiry_ms", float("inf")) <= time.time() * 1000)
                if int(time.time() * 1000) > value["valid_until_ms"] and not expired_option:
                    state.update(state="DEGRADED", data_ready=False, reason="status_expired", bar_updates=[])
        except (OSError, ValueError, TypeError):
            state.update(state="DEGRADED", data_ready=False, reason="invalid_status", bar_updates=[])
        process = self.processes.get((policy.product, "ALL" if policy.product == "options" else policy.symbol))
        if process is not None and process.poll() is not None and state.get("reason") != "expired":
            state.update(state="DEGRADED", data_ready=False, reason="process_exited", bar_updates=[])
        elif not policy.realtime_enabled:
            state.update(state="STOPPED", data_ready=False, reason="disabled", bar_updates=[])
        from .monitor import observe
        services = observe(self, policy, int(time.time() * 1000))
        state["bar_updates"] = services["calculation"].get("bar_updates", state.get("bar_updates", []))
        return {"policy": policy.model_dump(mode="json"), "runtime": state, "services": services}

    def _option_group(self, policy):
        from .monitor import observe

        now = int(time.time() * 1000)
        children = []
        for member in self._read():
            if member.product != "options" or "-" not in member.symbol or (policy.symbol != "ALL" and member.pair != policy.symbol):
                continue
            observations = observe(self, member, now)
            children.append({"policy": member.model_dump(mode="json"), "runtime": observations["collection"], "services": observations})
        services = {}
        for role in ("collection", "history", "calculation"):
            values = [child["services"][role] for child in children
                      if child["services"][role].get("reason") not in {"disabled", "expired"}]
            state = ("DEGRADED" if any(v["state"] == "DEGRADED" for v in values) else
                     "WARMING_UP" if any(v["state"] != "READY" for v in values) else "READY") if values else "STOPPED"
            services[role] = {"state": state, "healthy": bool(values) and all(v.get("healthy") for v in values),
                              "backlog": sum(v.get("backlog") or 0 for v in values),
                              "reason": next((v.get("reason") for v in values if not v.get("healthy")), None),
                              "last_error": next((v.get("last_error") for v in values if v.get("last_error")), None),
                              "valid_until_ms": min((v.get("valid_until_ms", now) for v in values), default=now + 5000)}
        live = [c["runtime"] for c in children if c["policy"]["realtime_enabled"] and c["runtime"].get("reason") != "expired"]
        ready = bool(live) and all(c.get("data_ready") and c.get("state") == "READY" for c in live)
        degraded = any(c.get("state") == "DEGRADED" or (c.get("valid_until_ms") or 0) < now for c in live)
        latest = max((s.get("last_update_ms") or s.get("last_websocket_ms") or 0
                      for c in live for s in c.get("symbols", {}).values()), default=0)
        runtime = {"state": "READY" if ready else "DEGRADED" if degraded else "WARMING_UP",
                   "data_ready": ready, "reason": "options_scope", "checked_ms": now,
                   "valid_until_ms": min((c.get("valid_until_ms") or now for c in live), default=now + 5000),
                   "symbols": {policy.symbol: {"last_update_ms": latest or None}}, "bar_updates": []}
        if not policy.realtime_enabled:
            runtime.update(state="STOPPED", data_ready=False, reason="disabled")
        return {"policy": policy.model_dump(mode="json"), "runtime": runtime, "services": services}

    def items(self, *, group_options=False):
        # Configurations are replaced as a list, never mutated in place. Status
        # readers must not wait behind a cloud configuration refresh transaction.
        policies = self._read()
        if group_options:
            groups = {p.symbol: p for p in policies if p.product == "options" and "-" not in p.symbol and p.symbol != "ALL"}
            for p in policies:
                if p.product == "options" and "-" in p.symbol:
                    pair = p.pair
                    groups.setdefault(pair, p.model_copy(update={"symbol": pair, "underlying": None}))
            policies = [p for p in policies if p.product != "options"] + list(groups.values())
        return [self._public(policy) for policy in policies]

    def close(self):
        with self.lock:
            self.closed = True
            for process in [*self.processes.values(), *self.auxiliary.values()]:
                self.manager._stop(process)
            self.processes.clear()
            self.auxiliary.clear()
            if self.lease:
                self.lease.close()
                self.lease = None
