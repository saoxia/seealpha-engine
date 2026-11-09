"""File-only service observations; observations never drive data recovery."""

import json
import time

from fastapi import HTTPException

from engine.pipeline.convert_binance_bars import write_record

ROLES = ("collection", "history", "calculation")


def observe(subscriptions, policy, now):
    root = subscriptions.root(policy)
    enabled = {"collection": policy.realtime_enabled, "history": policy.history_enabled,
               "calculation": policy.product == "futures_um" and policy.base_level != "auto" and (policy.realtime_enabled or policy.history_enabled)}
    result = {}
    for role in ROLES:
        item = {"state": "STOPPED", "healthy": False, "backlog": None, "reason": "disabled"}
        if enabled[role] and policy.symbol != "ALL":
            path = root / ("status.json" if role == "collection" else f"{role}/status.json")
            try:
                if path.stat().st_size > 131072:
                    raise ValueError("Oversize status")
                value = json.loads(path.read_text())
                if not isinstance(value, dict) or type(value.get("valid_until_ms")) is not int:
                    raise ValueError("Invalid status")
                item = {**value, "healthy": value.get("state") == "READY", "reason": value.get("reason") or value.get("last_error")}
                expired_option = (policy.product == "options" and value.get("reason") == "expired"
                                  and value.get("expiry_ms", float("inf")) <= now)
                if now > value["valid_until_ms"] and not expired_option:
                    item.update(state="DEGRADED", healthy=False, reason="status_expired")
            except (OSError, ValueError, TypeError):
                item.update(state="DEGRADED", reason="status_unavailable")
            processes = subscriptions.processes if role == "collection" else subscriptions.auxiliary
            symbol = "ALL" if policy.product == "options" else policy.symbol
            key = (policy.product, symbol) if role == "collection" else (policy.product, symbol, role)
            process = processes.get(key)
            if item.get("reason") == "expired" and item.get("expiry_ms", float("inf")) <= now:
                item.update(state="STOPPED", healthy=True)
            # A read-only API worker has no local handles for the owner's processes.
            elif (process is None and subscriptions.lease is not None) or (process is not None and process.poll() is not None):
                item.update(state="DEGRADED", healthy=False, reason="process_not_running")
        result[role] = item
    return result


class ServiceMonitor:
    def __init__(self, manager):
        self.manager = manager
        self.path = manager.project / ".local/state/api/data_services/status.json"

    def sample(self):
        now = int(time.time() * 1000)
        subscriptions = self.manager.subscriptions
        if subscriptions.lease is None:
            with subscriptions.lock:
                try:
                    subscriptions._claim()
                except HTTPException:
                    return self.read()  # Only the owner may publish shared observations.
        observations = subscriptions.items(group_options=True) if subscriptions.policies is not None else []
        items = [{"product": item["policy"]["product"], "symbol": item["policy"]["symbol"],
                  "services": item["services"]} for item in observations]
        config_error = subscriptions.last_error
        value = {"checked_ms": now, "valid_until_ms": now + 15000, "items": items,
                 "state": "DEGRADED" if config_error else "READY", "configuration_error": config_error}
        write_record(self.path, value)
        return value

    def read(self):
        try:
            if self.path.stat().st_size > 4 * 1024 * 1024:
                raise ValueError("Oversize monitor")
            value = json.loads(self.path.read_text())
            if int(time.time() * 1000) > value["valid_until_ms"]:
                value.update(state="DEGRADED", reason="monitor_expired")
                for item in value["items"]:
                    for state in item["services"].values():
                        state.update(healthy=False, reason="monitor_expired")
            return value
        except (OSError, ValueError, TypeError, KeyError):
            return {"state": "DEGRADED", "reason": "monitor_unavailable", "items": []}
