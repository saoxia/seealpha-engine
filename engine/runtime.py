"""Resident engine facts and the only place account, manual, and valuation nodes are built."""

import contextlib
import fcntl
import hashlib
import json
import os
import signal
import subprocess
import threading
import uuid
from pathlib import Path

from core.engine_contracts import EngineConflict
from engine.connectors.binance.queries import QueryError
from engine.store import Store
from engine.supervisor import popen

UNKNOWN = "请求已登记，交易结果待核实。请刷新委托，不要重复提交。"
UNCONFIRMED = "交易结果待核实，请刷新委托并按客户订单号核对，不要重复提交。"


def numeric_id(account_id):
    text = str(account_id)
    return int(text) if text.isdigit() else account_id


class EngineRuntime:
    """Long-running owner of credentials-in-memory, workers, and the SQLite book."""

    def __init__(self, root, project=None):
        self.root = Path(root)
        self.project = Path(project) if project is not None else Path(__file__).resolve().parents[1]
        self.root.mkdir(parents=True, exist_ok=True)
        self.store = Store(self.root / "book.sqlite")
        self.credentials = {}
        self.workers = {}
        self.worker_lock = threading.Lock()
        self.service_lease = None

    def close(self):
        with self.worker_lock:
            processes = [item["process"] for item in self.workers.values()]
        for process in processes:
            if process.poll() is None:
                with contextlib.suppress(ProcessLookupError, subprocess.TimeoutExpired):
                    os.killpg(process.pid, signal.SIGTERM)
                    process.wait(timeout=3)
        self.store.close()
        if self.service_lease is not None:
            self.service_lease.close()
            self.service_lease = None

    def ensure_tenant(self, tenant_id, purpose, status="active"):
        if purpose not in {"production", "development"} or status not in {"active", "archived"}:
            raise EngineConflict("租户类型或状态无效。")
        return self.store.ensure_tenant(tenant_id, purpose, status)

    def tenant_row(self, tenant_id):
        return self.store.tenant(tenant_id)

    def assert_runnable(self, tenant_id, action):
        row = self.store.tenant(tenant_id)
        if row is None or row["status"] != "active":
            raise EngineConflict("已归档的租户不能交易或回测。")
        if action == "live" and row["purpose"] != "production":
            raise EngineConflict("开发租户不能向交易所下单。")
        if action not in {"live", "backtest"}:
            raise EngineConflict("未知的租户操作。")
        return {"tenant_id": str(tenant_id), "action": action, "status": "runnable"}

    def register(self, tenant_id, account_id, environment, api_key, api_secret):
        self.assert_runnable(tenant_id, "live")
        key = (str(tenant_id), str(account_id), environment)
        self.credentials[key] = {"apiKey": api_key, "secret": api_secret}
        self.store.upsert_connection(tenant_id, account_id, environment, "registered")
        self.store.add_event(tenant_id, account_id, "connection", {
            "environment": environment, "status": "registered",
        })
        return {
            "tenant_id": str(tenant_id),
            "account_id": str(account_id),
            "environment": environment,
            "status": "registered",
        }

    def forget(self, tenant_id, account_id, environment):
        self.credentials.pop((str(tenant_id), str(account_id), environment), None)
        self.store.upsert_connection(tenant_id, account_id, environment, "closed")
        self.store.add_event(tenant_id, account_id, "connection", {
            "environment": environment, "status": "closed",
        })

    def health(self, tenant_id=None, account_id=None):
        with self.worker_lock:
            running = sum(item["process"].poll() is None for item in self.workers.values())
        body = {"status": "ok", "workers": running}
        if account_id is not None and tenant_id is not None:
            body["connections"] = [
                self.store.connection(tenant_id, account_id, environment)
                for environment in ("production", "sandbox")
            ]
            body["connections"] = [item for item in body["connections"] if item]
        return body

    def account_dict(self, tenant_id, account_id, environment, api_key=None, api_secret=None):
        creds = None
        if api_key is not None and api_secret is not None:
            creds = {"apiKey": api_key, "secret": api_secret}
            self.credentials[(str(tenant_id), str(account_id), environment)] = creds
        else:
            creds = self.credentials.get((str(tenant_id), str(account_id), environment))
        if creds is None:
            raise EngineConflict("账户连接未登记，业务端需先下发凭据。")
        return {
            "id": numeric_id(account_id),
            "tenant_id": str(tenant_id),
            "environment": environment,
            "credentials": creds,
        }

    def lock_path(self, tenant_id, account_id, environment):
        directory = self.root / str(tenant_id) / str(account_id) / environment
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        return directory / "account.lock"

    def _remember_order(self, tenant_id, account_id, environment, order, records):
        if order and order.get("client_order_id"):
            self.store.upsert_order(tenant_id, account_id, environment, order)
        for record in records or []:
            fill_id = record.get("trade_id") or record.get("id") or record.get("event_id")
            if fill_id:
                self.store.upsert_fill(tenant_id, account_id, environment, fill_id, record)
        if order or records:
            self.store.add_event(tenant_id, account_id, "execution", {
                "environment": environment,
                "client_order_id": None if not order else order.get("client_order_id"),
                "fill_count": len(records or []),
            })

    def _remember_positions(self, tenant_id, account_id, environment, positions):
        for position in positions or []:
            if position.get("id"):
                self.store.upsert_position(tenant_id, account_id, environment, position)

    def _tenant_of(self, account):
        tenant_id = account.get("tenant_id")
        if tenant_id is None:
            raise EngineConflict("引擎调用缺少租户。")
        return tenant_id

    async def account_snapshot(self, account):
        from engine.connectors.binance import queries

        tenant_id = self._tenant_of(account)
        snapshot = await queries.account_snapshot(account)
        self._remember_positions(tenant_id, account["id"], account["environment"], snapshot.positions)
        self.store.add_event(tenant_id, account["id"], "account_snapshot", {
            "environment": account["environment"], "positions": len(snapshot.positions),
        })
        return snapshot

    async def _manual_execute(self, account, symbol, command):
        from engine.connectors.binance import manual

        tenant_id = self._tenant_of(account)
        if command.get("action") != "snapshot":
            self.assert_runnable(tenant_id, "live")
        result = await manual.execute(account, symbol, command)
        self._remember_positions(tenant_id, account["id"], account["environment"], result.get("positions"))
        order = result.get("result")
        if isinstance(order, dict):
            self._remember_order(
                tenant_id, account["id"], account["environment"], order, result.get("records")
            )
        return result

    async def trading_state(self, account, symbol):
        return await self._manual_execute(account, symbol, {"action": "snapshot"})

    async def command(self, account, action, values):
        tenant_id = self._tenant_of(account)
        self.assert_runnable(tenant_id, "live")
        account_id, environment = account["id"], account["environment"]
        if action not in {"submit", "cancel"} or values["environment"] != environment:
            raise EngineConflict("交易操作或账户环境不匹配。")
        request_id = str(uuid.UUID(values["request_id"]))
        fingerprint = hashlib.sha256(json.dumps([action, values], sort_keys=True).encode()).hexdigest()
        client_order_id = (
            "SA" + uuid.UUID(request_id).hex if action == "submit" else values["client_order_id"]
        )
        path = self.lock_path(tenant_id, account_id, environment)
        with path.open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise EngineConflict("该账户有交易操作正在处理，请稍后核对委托。") from None
            existing = self.store.get_request(tenant_id, account_id, request_id)
            if existing:
                if existing["fingerprint"] != fingerprint:
                    raise EngineConflict("请求编号已用于另一笔操作。")
                return {**existing, "replay": True}
            response = {
                "request_id": request_id, "client_order_id": client_order_id,
                "status": "unknown", "order": None, "message": UNKNOWN,
            }
            body = {
                "fingerprint": fingerprint, "response": response, "symbol": values["symbol"],
                "account_revision": values["account_revision"], "environment": environment,
                "records": [], "ledger_synced": False,
            }
            self.store.save_request(tenant_id, account_id, request_id, body)
            try:
                result = await self._manual_execute(
                    account, values["symbol"], {**values, "action": action, "client_order_id": client_order_id},
                )
                if "validation_error" in result:
                    response.update(status="completed", message=result["validation_error"])
                    self.store.save_request(tenant_id, account_id, request_id, body)
                    return body
                order = result.get("result")
                records = result.pop("records", [])
                body["records"] = records
                if order is not None:
                    response.update(
                        status="completed", order=order,
                        message=("撤单被拒绝，当前订单状态：" if result.get("outcome") == "OrderCancelRejected"
                                 else "交易所订单状态：") + order["status"],
                    )
                self.store.save_request(tenant_id, account_id, request_id, body)
            except QueryError:
                response["message"] = UNCONFIRMED
                self.store.save_request(tenant_id, account_id, request_id, body)
            return body

    async def reconcile_request(self, account, request_id, account_revision):
        tenant_id = self._tenant_of(account)
        account_id, environment = account["id"], account["environment"]
        path = self.lock_path(tenant_id, account_id, environment)
        with path.open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise EngineConflict("该账户有交易操作正在处理，请稍后核对委托。") from None
            stored = self.store.get_request(tenant_id, account_id, request_id)
            if stored is None:
                return None
            if stored.get("account_revision") != account_revision or (
                stored.get("environment", environment) != environment
            ):
                raise EngineConflict("账户配置已变化，不能使用新凭据核对原委托。")
            if stored["response"]["status"] == "unknown":
                result = await self.trading_state(account, stored["symbol"])
                order = next(
                    (row for row in result["orders"]
                     if row["client_order_id"] == stored["response"]["client_order_id"]), None,
                )
                if order is not None:
                    stored["response"].update(
                        status="completed", order=order,
                        message="已在交易所核对到委托：" + order["status"],
                    )
                    self.store.save_request(tenant_id, account_id, request_id, stored)
            return stored

    def note_gap(self, tenant_id, account_id, scope, gap_key, gap):
        payload = {"scope": scope, "gap_key": gap_key, "tenant_id": str(tenant_id), **gap}
        self.store.upsert_gap(tenant_id, account_id, scope, gap_key, payload)
        self.store.add_event(tenant_id, account_id, "gap", payload)
        return payload

    def note_worker(self, tenant_id, account_id, command):
        role = "history" if "engine.pipeline.pair_data" in command else "collection"
        if "--role" in command:
            role = command[command.index("--role") + 1]
        product, symbol = "futures_um", None
        if "--symbol" in command:
            symbol = command[command.index("--symbol") + 1]
        elif "--request" in command:
            policy = json.loads(command[command.index("--request") + 1])
            symbol = policy.get("symbol")
            product = policy.get("product") or product
        elif "--options-config" in command:
            symbol, product = "ALL", "options"
        if not symbol:
            return None
        progress = {"command_module": command[2] if len(command) > 2 else None}
        self.store.upsert_subscription(tenant_id, account_id, product, symbol, role, "running", progress)
        self.store.add_event(tenant_id, account_id, "subscription", {
            "product": product, "symbol": symbol, "role": role, "state": "running",
        })
        return {
            "tenant_id": str(tenant_id), "account_id": str(account_id),
            "product": product, "symbol": symbol, "role": role, "state": "running",
        }

    def note_subscription(self, tenant_id, account_id, product, symbol, role, state, progress):
        self.store.upsert_subscription(tenant_id, account_id, product, symbol, role, state, progress)
        self.store.add_event(tenant_id, account_id, "subscription", {
            "product": product, "symbol": symbol, "role": role, "state": state,
        })
        return {
            "tenant_id": str(tenant_id), "account_id": str(account_id),
            "product": product, "symbol": symbol, "role": role, "state": state,
        }

    def _service_lease(self):
        if self.service_lease is None:
            path = self.project / ".local/run/engine/workers.lock"
            path.parent.mkdir(parents=True, exist_ok=True)
            lease = path.open("a")
            try:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                lease.close()
                raise EngineConflict("行情工作进程由另一个引擎实例管理。") from None
            self.service_lease = lease
        return self.service_lease

    def spawn_worker(self, command, log_path, *, tenant_id, access=None, hold_lease=False, log_mode="ab"):
        """Fork a feed or history worker and keep its lock fds until it exits."""
        log_path = Path(log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        output = log_path.open(log_mode, buffering=0)
        held = [output]
        fds = []
        try:
            if hold_lease:
                lease = self._service_lease()
                fds.append(lease.fileno())
            if access is not None:
                symbol, product, shared = access
                data = self.project / "data"
                if not data.is_dir():
                    raise EngineConflict("租户数据目录不可用，拒绝创建替代数据卷。")
                path = data / ".locks" / product / f"{symbol}.access.lock"
                path.parent.mkdir(parents=True, exist_ok=True)
                access_lease = path.open("a")
                flag = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
                try:
                    fcntl.flock(access_lease, flag | fcntl.LOCK_NB)
                except BlockingIOError:
                    access_lease.close()
                    raise EngineConflict("该交易对仍有采集或数据读写，请先停止后再删除。") from None
                held.append(access_lease)
                fds.append(access_lease.fileno())
            process = popen(
                command, cwd=self.project, stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                start_new_session=True, pass_fds=tuple(fds),
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
        except Exception:
            for item in held:
                item.close()
            raise
        worker_id = uuid.uuid4().hex
        record = {
            "process": process, "held": held, "command": command, "returncode": None,
            "tenant_id": str(tenant_id),
        }

        def watch():
            process.wait()
            record["returncode"] = process.returncode
            for item in held:
                item.close()

        threading.Thread(target=watch, daemon=True).start()
        with self.worker_lock:
            self.workers[worker_id] = record
        return {"id": worker_id, "pid": process.pid, "returncode": None}

    def worker(self, worker_id):
        with self.worker_lock:
            record = self.workers.get(worker_id)
        if record is None:
            raise EngineConflict("引擎没有该工作进程。")
        process = record["process"]
        returncode = process.poll()
        if returncode is None:
            returncode = record["returncode"]
        return {"id": worker_id, "pid": process.pid, "returncode": returncode}

    def stop_worker(self, worker_id):
        state = self.worker(worker_id)
        if state["returncode"] is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(state["pid"], signal.SIGTERM)
        return self.worker(worker_id)

    def receive_strategy(self, tenant_id, payload):
        from engine.strategies.loading import accept_load

        return accept_load(self, tenant_id, payload)

    def preview_strategy(self, tenant_id, account_id, bars):
        from engine.strategies.loading import preview_loads

        row = self.store.tenant(tenant_id)
        if row is None or row["status"] != "active":
            raise EngineConflict("已归档或不存在的租户不能预览策略。")
        loads = self.store.strategy_loads(tenant_id, account_id, include_archive=True)
        if not loads:
            raise EngineConflict("该租户账户没有已下发的策略。")
        return preview_loads(loads, bars)

    def run_backtest(
        self, tenant_id, account_id, start, end, interval, starting_balance, quote_currency,
    ):
        """Run this tenant's issued loads on catalog history and keep the latest result."""
        from decimal import Decimal, InvalidOperation

        from nautilus_trader.model import Money

        from engine.strategies.backtest import catalog_directory, parse_window, read_history
        from engine.strategies.loading import run_issued_history

        start_at, end_at, interval = parse_window(start, end, interval)
        self.assert_runnable(tenant_id, "backtest")
        row = self.store.tenant(tenant_id)
        if row["purpose"] != "development":
            raise EngineConflict("历史回测只在开发租户上运行。")
        loads = self.store.strategy_loads(tenant_id, account_id, include_archive=True)
        if not loads:
            raise EngineConflict("该租户账户没有已下发的策略。")
        if any(
            item["grant_purpose"] != "development" or str(item["tenant_id"]) != str(tenant_id)
            for item in loads
        ):
            raise EngineConflict("开发回测只能运行本租户已下发的开发授权。")
        symbols = {item["parameters"]["symbol"] for item in loads}
        if len(symbols) != 1:
            raise EngineConflict("一次回测只支持同一品种的已下发策略。")
        symbol = next(iter(symbols))
        if not self._has_subscription_intent(tenant_id, symbol):
            raise EngineConflict("开发租户尚未声明该品种的行情订阅。")
        try:
            amount = Decimal(str(starting_balance))
        except (InvalidOperation, ValueError) as exc:
            raise EngineConflict("初始资金无效。") from exc
        if not amount.is_finite() or amount <= 0:
            raise EngineConflict("初始资金无效。")
        if not isinstance(quote_currency, str) or quote_currency != "USDT":
            raise EngineConflict("虚拟账户报价币种与策略品种不一致。")
        try:
            starting = Money.from_str(f"{format(amount, 'f')} {quote_currency}")
        except (TypeError, ValueError) as exc:
            raise EngineConflict("初始资金与报价币种精度不匹配。") from exc
        if not (self.project / "data").is_dir():
            raise EngineConflict("租户历史数据目录不可用。")
        from engine.data_queries import read_lease

        # Delete/trim jobs must not unlink a Catalog dataset while a backtest
        # is loading its historical bars.
        with read_lease(self.project, symbol, "futures_um"):
            bars = read_history(catalog_directory(self.project), symbol, interval, start_at, end_at)
        if not bars:
            raise EngineConflict("指定区间没有可用的本地 K 线。")
        result = run_issued_history(loads, bars, starting)
        result.update(
            interval=interval,
            start=start_at.isoformat(),
            end=end_at.isoformat(),
            starting_balance=format(amount, "f"),
        )
        self.store.add_event(tenant_id, account_id, "backtest", result)
        return result

    def latest_backtest(self, tenant_id, account_id):
        if self.store.tenant(tenant_id) is None:
            raise EngineConflict("租户不存在。")
        event = self.store.latest_event(tenant_id, account_id, "backtest")
        if event is None:
            return None
        return event["body"]

    def _has_subscription_intent(self, tenant_id, symbol):
        wanted = str(symbol).upper()
        return any(
            str(row["symbol"]).upper() == wanted and row["role"] == "intent"
            for row in self.store.tenant_subscriptions(tenant_id)
        )

    def export_tenant(self, tenant_id):
        return self.store.export_tenant(tenant_id)

    def drop_tenant(self, tenant_id, *, remove=False):
        """Stop this tenant's workers and delete its runtime rows. Archive keeps a tombstone."""
        tenant_id = str(tenant_id)
        row = self.store.tenant(tenant_id)
        with self.worker_lock:
            worker_ids = [
                worker_id for worker_id, record in self.workers.items()
                if str(record.get("tenant_id")) == tenant_id
            ]
        for worker_id in worker_ids:
            with contextlib.suppress(EngineConflict, ProcessLookupError):
                self.stop_worker(worker_id)
            with self.worker_lock:
                self.workers.pop(worker_id, None)
        self.credentials = {
            key: value for key, value in self.credentials.items() if key[0] != tenant_id
        }
        if remove or row is None:
            self.store.delete_tenant(tenant_id)
            return {"tenant_id": tenant_id, "status": "deleted"}
        self.store.delete_runtime(tenant_id)
        self.store.ensure_tenant(tenant_id, row["purpose"], "archived")
        return {"tenant_id": tenant_id, "status": "archived"}

    def import_tenant(self, tenant_id, purpose, payload):
        return self.store.import_tenant(tenant_id, purpose, payload)


async def read_account_snapshot(account):
    """In-process entry used when the API is not pointed at a separate engine port."""
    from engine.connectors.binance import queries

    return await queries.account_snapshot(account)


async def valuation_events(account):
    """Mark stream. The LiveNode is built here, not in the business API."""
    from engine.connectors.binance.valuation import valuation_events as native

    async for event in native(account):
        yield event
