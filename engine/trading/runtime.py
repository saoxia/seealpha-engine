"""Resident engine facts and the only place account, manual, and valuation nodes are built."""

import asyncio
import contextlib
import fcntl
import hashlib
import json
import os
import signal
import subprocess
import threading
import time
import uuid
from pathlib import Path

from core.engine_contracts import EngineConflict
from engine.connectors.binance.queries import QueryError
from engine.infrastructure.supervisor import popen
from engine.market_data.access.market_data_service import EngineMarketDataService
from engine.trading.store import Store

UNKNOWN = "请求已登记，交易结果待核实。请刷新委托，不要重复提交。"
UNCONFIRMED = "交易结果待核实，请刷新委托并按客户订单号核对，不要重复提交。"


def numeric_id(account_id):
    text = str(account_id)
    return int(text) if text.isdigit() else account_id


class EngineRuntime:
    """Long-running owner of credentials-in-memory, workers, and the SQLite book."""

    def __init__(self, root, project=None):
        self.root = Path(root)
        self.project = Path(project) if project is not None else Path(__file__).resolve().parents[2]
        self.root.mkdir(parents=True, exist_ok=True)
        self.store = Store(self.root / "book.sqlite")
        self.credentials = {}
        self.market_subscriptions = EngineMarketDataService()
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
        self.market_subscriptions.close()
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
        result = await self._manual_execute(account, symbol, {"action": "snapshot"})
        if symbol.endswith("-PERP"):
            from engine.connectors.binance import manual_algo
            try:
                result["algo_orders"] = await asyncio.to_thread(manual_algo.open_orders, account, symbol)
                result["algo_checked_at"] = int(time.time() * 1000)
                result["algo_error"] = None
            except QueryError:
                # Missing permissions or transient errors cannot be interpreted as "no orders".
                result["algo_orders"] = None
                result["algo_checked_at"] = None
                result["algo_error"] = "无法读取交易所条件单；请到交易所核对，不应假定没有挂单。"
        return result

    async def command(self, account, action, values):
        tenant_id = self._tenant_of(account)
        self.assert_runnable(tenant_id, "live")
        account_id, environment = account["id"], account["environment"]
        if action not in {"submit", "cancel", "algo_submit", "algo_cancel", "leverage"} or values["environment"] != environment:
            raise EngineConflict("交易操作或账户环境不匹配。")
        request_id = str(uuid.UUID(values["request_id"]))
        fingerprint = hashlib.sha256(json.dumps([action, values], sort_keys=True).encode()).hexdigest()
        client_order_id = (
            "SA" + uuid.UUID(request_id).hex if action in {"submit", "algo_submit"} else
            values.get("client_order_id") or values.get("client_algo_id")
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
                "fingerprint": fingerprint, "response": response, "symbol": values["symbol"], "action": action,
                "target_leverage": values.get("leverage"), "client_algo_id": values.get("client_algo_id"),
                "account_revision": values["account_revision"], "environment": environment,
                "records": [], "ledger_synced": False,
            }
            self.store.save_request(tenant_id, account_id, request_id, body)
            try:
                if action in {"algo_submit", "algo_cancel", "leverage"}:
                    from engine.connectors.binance import manual_algo
                    result = await manual_algo.execute(account, action, values, client_order_id)
                else:
                    result = await self._manual_execute(
                        account, values["symbol"], {**values, "action": action, "client_order_id": client_order_id},
                    )
                if "validation_error" in result:
                    response.update(status="completed", message=result["validation_error"])
                    self.store.save_request(tenant_id, account_id, request_id, body)
                    return body
                if action in {"algo_submit", "algo_cancel", "leverage"}:
                    if result.get("algo_order"):
                        response.update(status="completed", algo_order=result["algo_order"],
                                        message=result["message"])
                    elif result.get("leverage"):
                        response.update(status="completed", leverage=result["leverage"],
                                        message=result["message"])
                    self.store.save_request(tenant_id, account_id, request_id, body)
                    return body
                order = result.get("result")
                records = result.pop("records", [])
                body["records"] = records
                if order is not None:
                    response.update(
                        status="completed", order=order,
                        message=("撤单被拒绝，当前订单状态：" if result.get("outcome") in {"OrderCancelRejected", "OrderCancelDenied"}
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
                action = stored.get("action")
                if action in {"algo_submit", "algo_cancel", "leverage"}:
                    from engine.connectors.binance import manual_algo
                    try:
                        if action == "leverage":
                            client = manual_algo.SignedFutures(account)
                            current = await asyncio.to_thread(manual_algo.current_leverage, client,
                                                              stored["symbol"].removesuffix("-PERP"))
                            if current == stored.get("target_leverage"):
                                stored["response"].update(status="completed", leverage=current,
                                                          message="已从交易所核对到目标杠杆。")
                        else:
                            client_id = (stored["response"]["client_order_id"] if action == "algo_submit"
                                         else stored.get("client_algo_id"))
                            order = await asyncio.to_thread(
                                manual_algo.query_algo, account, stored["symbol"], client_id)
                            if action == "algo_submit" or not order["open"]:
                                stored["response"].update(status="completed", algo_order=order,
                                                          message="已从交易所核对到条件单：" + order["status"])
                    except QueryError:
                        pass  # Absence, transport failures and unconfirmed writes remain unknown.
                    self.store.save_request(tenant_id, account_id, request_id, stored)
                    return stored
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
        role = "history" if "engine.market_data.repair.pair_data" in command else "collection"
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
        *, publish_event=True, product="futures_um", candidate=None,
    ):
        """Run an issued strategy or one normalized, Engine-owned research candidate."""
        from decimal import Decimal, InvalidOperation

        from nautilus_trader.model import Money

        from engine.backtesting.backtest import catalog_directory, parse_window
        from engine.strategies.loading import run_issued_history_stream

        if product not in {"futures_um", "spot"}:
            raise EngineConflict("回测不支持此交易产品。")
        start_at, end_at, interval = parse_window(start, end, interval)
        self.assert_runnable(tenant_id, "backtest")
        row = self.store.tenant(tenant_id)
        if row["purpose"] != "development":
            raise EngineConflict("历史回测只在开发租户上运行。")
        if candidate is None:
            loads = self.store.strategy_loads(tenant_id, account_id, include_archive=True)
            if not loads:
                raise EngineConflict("该租户账户没有已下发的策略。")
        elif candidate.get("mode") == "python":
            # This method runs inside the existing Backtest Worker, never the
            # API controller. The trusted Platform picked the destination
            # development Engine and immutable strategy package.
            import base64

            from engine.research.python_candidate import inspect_python_candidate

            package = inspect_python_candidate(
                candidate["archive"], package_hash=candidate["package_hash"],
                symbol=candidate["parameters"]["symbol"],
            )
            if any(
                package[name] != candidate[name]
                for name in ("name", "version", "entry_point", "parameters")
            ):
                raise EngineConflict("Python research package no longer matches task")
            loads = [{
                "tenant_id": str(tenant_id), "account_id": str(account_id),
                "issuance_id": "candidate_" + candidate["candidate_id"],
                "code_key": package["name"], "version": package["version"],
                "entry_point": package["entry_point"],
                "package_hash": package["package_hash"],
                "archive": base64.b64decode(candidate["archive"], validate=True),
                "parameters": package["parameters"],
                "grant_purpose": "development",
            }]
        else:
            # Existing restricted EMA/SMA recipe remains supported.
            from engine.research.candidate_template import candidate_load

            if set(candidate) != {"candidate_id", "package_hash", "recipe"}:
                raise EngineConflict("Candidate research identity invalid")
            loads = [candidate_load(
                candidate["recipe"], tenant_id=str(tenant_id), account_id=str(account_id),
                candidate_id=candidate["candidate_id"],
                package_hash=candidate["package_hash"],
            )]
        if any(
            item["grant_purpose"] != "development" or str(item["tenant_id"]) != str(tenant_id)
            for item in loads
        ):
            raise EngineConflict("开发回测只能运行本租户已下发的开发授权。")
        symbols = {item["parameters"]["symbol"] for item in loads}
        if len(symbols) != 1:
            raise EngineConflict("一次回测只支持同一品种的已下发策略。")
        symbol = next(iter(symbols))
        if not self._has_subscription_intent(tenant_id, symbol, product):
            raise EngineConflict("开发租户尚未声明该品种的行情订阅。")
        try:
            amount = Decimal(str(starting_balance))
        except (InvalidOperation, ValueError) as exc:
            raise EngineConflict("初始资金无效。") from exc
        if not amount.is_finite() or amount <= 0:
            raise EngineConflict("初始资金无效。")
        allowed_quotes = {"USDT"} if product == "futures_um" else {"USDT", "USDC", "FDUSD"}
        if not isinstance(quote_currency, str) or quote_currency not in allowed_quotes:
            raise EngineConflict("虚拟账户报价币种与策略品种不一致。")
        try:
            starting = Money.from_str(f"{format(amount, 'f')} {quote_currency}")
        except (TypeError, ValueError) as exc:
            raise EngineConflict("初始资金与报价币种精度不匹配。") from exc
        if not (self.project / "data").is_dir():
            raise EngineConflict("租户历史数据目录不可用。")
        from engine.api.data_queries import read_lease

        # Delete/trim jobs must not unlink a Catalog dataset while a backtest
        # is loading its historical bars.
        with read_lease(self.project, symbol, product):
            history = self.market_subscriptions.history(catalog_directory(self.project))
            instrument = None
            if product == "spot":
                from core.market.instruments import instrument_id_for

                values = history.instruments(instrument_ids=[instrument_id_for(symbol, "spot")])
                instrument = next((item for item in values if str(item.id) == instrument_id_for(symbol, "spot")), None)
                if instrument is None or instrument.quote_currency.code != quote_currency:
                    raise EngineConflict("现货品种定义不存在或计价资产与回测设置不一致。")
            batches = history.bar_batches(
                symbol, interval, start_at, end_at, product=product,
            )
            # Keep the lease for the entire iterator consumption and native replay.
            # A concurrent delete must not unlink Parquet files mid-backtest.
            from engine.market_data.access.replay_adapter import replay_bar_batches
            from engine.market_data.access.subscription_hub import MarketStream

            market = MarketStream("BINANCE", product, symbol, interval)
            with self.market_subscriptions.backtest(uuid.uuid4().hex, (market,)) as replay:
                result = run_issued_history_stream(
                    loads, replay_bar_batches(replay, tenant_id, "issued", market, batches),
                    starting, **({"instrument": instrument} if instrument else {}),
                )
        result.update(
            interval=interval,
            start=start_at.isoformat(),
            end=end_at.isoformat(),
            starting_balance=format(amount, "f"),
        )
        if product == "spot":
            result["product"] = "spot"
        if publish_event:
            self.store.add_event(tenant_id, account_id, "backtest", result)
        return result

    def iter_funding_carry_history(self, tenant_id, symbol, start, end, interval):
        """Research-only paired Catalog input; no matching, orders or settlement.

        Caller must fully consume/close this iterator before releasing dataset
        leases. No HTTP endpoint or production strategy runtime calls it.
        """
        from contextlib import ExitStack

        from core.market.instruments import normalize_symbol
        from engine.api.data_queries import read_lease
        from engine.backtesting.backtest import catalog_directory, parse_window
        from engine.portfolio.basket_history import iter_carry_history

        self.assert_runnable(tenant_id, "backtest")
        if self.store.tenant(tenant_id)["purpose"] != "development":
            raise EngineConflict("组合历史仅允许开发租户读取。")
        symbol = normalize_symbol(symbol, "futures_um")
        start_at, end_at, interval = parse_window(start, end, interval)
        if not all(
            self._has_subscription_intent(tenant_id, symbol, product)
            for product in ("futures_um", "spot")
        ):
            raise EngineConflict("开发租户必须同时声明现货和永续历史订阅。")
        if not (self.project / "data").is_dir():
            raise EngineConflict("租户历史数据目录不可用。")
        # Deterministic lock order avoids deadlocks with other multi-product readers.
        with ExitStack() as leases:
            for product in ("futures_um", "spot"):
                leases.enter_context(read_lease(self.project, symbol, product))
            directory = catalog_directory(self.project)
            history = self.market_subscriptions.history(directory)
            yield from iter_carry_history(
                directory, symbol, interval, start_at, end_at, reader=history,
            )

    def run_funding_carry_backtest(
        self, tenant_id, account_id, start, end, interval,
        starting_spot_balance, starting_perpetual_balance, max_gross_notional,
        *, publish_event=True, issuance_id=None,
    ):
        """Backtest an issued carry basket with separate native venue accounts.

        Research-only: NEVER reaches credentials or the live order routes.
        Signal callbacks come from verified archives, never duplicated rules.
        """
        from core.market.instruments import instrument_id_for
        from engine.api.data_queries import read_lease
        from engine.backtesting.backtest import catalog_directory, parse_window
        from engine.backtesting.carry_simulation import run_native_carry

        self.assert_runnable(tenant_id, "backtest")
        if self.store.tenant(tenant_id)["purpose"] != "development":
            raise EngineConflict("组合回测仅允许开发租户。")
        start_at, end_at, interval = parse_window(start, end, interval)
        loads = self.store.strategy_loads(
            tenant_id, account_id, include_archive=True,
        )
        if issuance_id is not None:
            if (
                not isinstance(issuance_id, str)
                or not 1 <= len(issuance_id) <= 64
            ):
                raise EngineConflict("指定的组合策略下发编号无效。")
            loads = [
                load for load in loads
                if str(load["issuance_id"]) == issuance_id
            ]
        if not loads or len(loads) > 16:
            raise EngineConflict("账户必须已下发 1 到 16 个组合开发策略。")
        if any(
            str(row["tenant_id"]) != str(tenant_id)
            or str(row["account_id"]) != str(account_id)
            or row["grant_purpose"] != "development"
            or (issuance_id is None and row["code_key"] != "demo_funding_carry")
            for row in loads
        ):
            raise EngineConflict("仅允许本租户开发授权的跨产品策略。")
        symbols = {row["parameters"]["symbol"] for row in loads}
        if len(symbols) != 1 or next(iter(symbols)) not in {"ETHUSDT", "BTCUSDT"}:
            raise EngineConflict("资金费策略品种不一致或不受支持。")
        symbol = next(iter(symbols))
        if not all(
            self._has_subscription_intent(tenant_id, symbol, product)
            for product in ("spot", "futures_um")
        ):
            raise EngineConflict("组合回测必须具有现货及永续独立历史订阅。")
        if not (self.project / "data").is_dir():
            raise EngineConflict("租户历史目录不可用。")
        from contextlib import ExitStack

        with ExitStack() as leases:
            for product in ("futures_um", "spot"):
                leases.enter_context(read_lease(self.project, symbol, product))
            catalog = self.market_subscriptions.history(catalog_directory(self.project))
            spot_id = instrument_id_for(symbol, "spot")
            perp_id = instrument_id_for(symbol, "futures_um")
            spot_rows = catalog.instruments(instrument_ids=[spot_id])
            perp_rows = catalog.instruments(instrument_ids=[perp_id])
            spot = next((x for x in spot_rows if str(x.id) == spot_id), None)
            perp = next((x for x in perp_rows if str(x.id) == perp_id), None)
            if spot is None or perp is None:
                raise EngineConflict("历史 Catalog 缺少现货或永续原生合约定义。")
            from engine.market_data.access.replay_adapter import replay_carry_events
            from engine.market_data.access.subscription_hub import MarketStream

            spot_stream = MarketStream("BINANCE", "spot", symbol, interval)
            perp_stream = MarketStream("BINANCE", "futures_um", symbol, interval)
            with self.market_subscriptions.backtest(
                uuid.uuid4().hex, (spot_stream, perp_stream),
            ) as replay:
                events = replay_carry_events(
                    replay, tenant_id, "carry", spot_stream, perp_stream,
                    self.iter_funding_carry_history(
                        tenant_id, symbol, start_at.isoformat(), end_at.isoformat(), interval,
                    ),
                )
                result = run_native_carry(
                    loads, events, spot, perp, interval=interval,
                    starting_spot=starting_spot_balance,
                    starting_perpetual=starting_perpetual_balance,
                    max_gross_notional=max_gross_notional,
                )
        result.update(
            interval=interval, start=start_at.isoformat(), end=end_at.isoformat(),
        )
        if publish_event:
            self.store.add_event(tenant_id, account_id, "carry_backtest", result)
        return result

    def run_generic_portfolio_backtest(
        self, tenant_id, account_id, issuance_ids, legs, start, end, interval,
        starting_spot_balance, starting_perpetual_balance, max_gross_notional,
    ):
        """One development-only Nautilus replay for several symbols/products.

        Every requested leg needs a real Catalog instrument, an explicit
        subscription and complete synchronized historical bars. Independent
        Engine-per-symbol replay is intentionally not used.
        """
        from contextlib import ExitStack
        from itertools import zip_longest

        from core.market.instruments import instrument_id_for, normalize_symbol
        from engine.api.data_queries import read_lease
        from engine.backtesting.backtest import _nanos, catalog_directory, parse_window
        from engine.backtesting.generic_simulation import run_native_generic_portfolio
        from engine.portfolio.basket_targets import LegId

        self.assert_runnable(tenant_id, "backtest")
        tenant = self.store.tenant(tenant_id)
        if tenant is None or tenant["purpose"] != "development":
            raise EngineConflict("通用多标的回测仅允许开发租户。")
        start_at, end_at, interval = parse_window(start, end, interval)
        if (
            not isinstance(issuance_ids, list) or not 1 <= len(issuance_ids) <= 16
            or any(not isinstance(v, str) or not 1 <= len(v) <= 64 for v in issuance_ids)
            or len(set(issuance_ids)) != len(issuance_ids)
        ):
            raise EngineConflict("策略下发列表无效或包含重复发行编号。")
        if not isinstance(legs, list) or not 1 <= len(legs) <= 8:
            raise EngineConflict("多标的回测需要1至8条市场腿。")
        identities = []
        for row in legs:
            if not isinstance(row, dict) or set(row) != {"product", "symbol"}:
                raise EngineConflict("组合市场腿结构无效。")
            product, symbol = row["product"], row["symbol"]
            if product not in {"spot", "futures_um"} or not isinstance(symbol, str):
                raise EngineConflict("通用回测仅支持现货和 USD-M 永续历史。")
            try:
                normalized = normalize_symbol(symbol, product)
            except (TypeError, ValueError) as exc:
                raise EngineConflict("组合交易合约代码无效。") from exc
            identities.append(LegId(str(tenant_id), str(account_id), product, normalized))
        if len(set(identities)) != len(identities):
            raise EngineConflict("同一产品和合约不能在一个组合中重复。")
        all_loads = self.store.strategy_loads(
            tenant_id, account_id, include_archive=True,
        )
        loads_by_id = {str(load["issuance_id"]): load for load in all_loads}
        if not set(issuance_ids).issubset(loads_by_id):
            raise EngineConflict("组合成员未在当前开发账户下发。")
        loads = [loads_by_id[identifier] for identifier in issuance_ids]
        if any(
            str(item["tenant_id"]) != str(tenant_id)
            or str(item["account_id"]) != str(account_id)
            or item["grant_purpose"] != "development"
            or item["parameters"].get("symbol") not in {leg.symbol for leg in identities}
            for item in loads
        ):
            raise EngineConflict("组合成员权限或策略主要标的不一致。")
        if any(
            not self._has_subscription_intent(tenant_id, leg.symbol, leg.product)
            for leg in identities
        ):
            raise EngineConflict("组合每条市场腿必须先声明历史行情订阅。")
        if not (self.project / "data").is_dir():
            raise EngineConflict("租户历史数据目录不可用。")

        # All Catalog leases stay held for the entire native replay.
        # Sorted acquisition prevents new deadlocks across overlapping baskets.
        with ExitStack() as resources:
            for leg in sorted(identities):
                resources.enter_context(read_lease(self.project, leg.symbol, leg.product))
            catalog_dir = catalog_directory(self.project)
            catalog = self.market_subscriptions.history(catalog_dir)
            instruments = {}
            streams = []
            for leg in identities:
                instrument_id = instrument_id_for(leg.symbol, leg.product)
                rows = catalog.instruments(instrument_ids=[instrument_id])
                instrument = next(
                    (value for value in rows if str(value.id) == instrument_id), None,
                )
                if instrument is None:
                    raise EngineConflict("组合历史缺少对应产品的原生合约定义。")
                instruments[leg] = instrument
                stream = (
                    bar for batch in catalog.bar_batches(
                        leg.symbol, interval, start_at, end_at, product=leg.product,
                    ) for bar in batch
                )
                streams.append(stream)
            step = 60_000_000_000 if interval == "1m" else 300_000_000_000
            start_ns, end_ns = _nanos(start_at), _nanos(end_at)
            sentinel = object()

            def aligned_frames():
                previous, count = None, 0
                for row in zip_longest(*streams, fillvalue=sentinel):
                    if any(item is sentinel for item in row):
                        raise EngineConflict("跨标的历史数据长度不一致，不能虚构K线。")
                    stamps = {int(item.ts_event) for item in row}
                    if len(stamps) != 1:
                        raise EngineConflict("跨标的历史数据未按同一时间戳闭合。")
                    stamp = stamps.pop()
                    if stamp != (start_ns + step if previous is None else previous + step):
                        raise EngineConflict("跨标的历史窗口存在缺失或重复的K线。")
                    previous = stamp
                    count += 1
                    yield dict(zip(identities, row))
                if count == 0 or previous != end_ns - step:
                    raise EngineConflict("组合历史窗口尾部缺失或没有可用行情。")

            report = run_native_generic_portfolio(
                loads, aligned_frames(), instruments, interval=interval,
                starting_spot=starting_spot_balance,
                starting_perpetual=starting_perpetual_balance,
                max_gross_notional=max_gross_notional,
                market_subscriptions=self.market_subscriptions,
            )
        report.update(start=start_at.isoformat(), end=end_at.isoformat(), interval=interval,
                      issuance_ids=issuance_ids)
        self.store.add_event(tenant_id, account_id, "generic_portfolio_backtest", report)
        return report

    def latest_generic_portfolio_backtest(self, tenant_id, account_id):
        if self.store.tenant(tenant_id) is None:
            raise EngineConflict("租户不存在。")
        event = self.store.latest_event(tenant_id, account_id, "generic_portfolio_backtest")
        return None if event is None else event["body"]

    def run_spot_perp_strategy_backtest(
        self, tenant_id, account_id, issuance_id, start, end, interval,
        starting_spot_balance, starting_perpetual_balance, max_gross_notional,
    ):
        """Replay ONE issued strategy across Spot/UM accounts (development only).

        Reuses the approved two-market history, native fills and research risk
        coordinator. This route never enables multi-venue live order sending.
        """
        result = self.run_funding_carry_backtest(
            tenant_id, account_id, start, end, interval,
            starting_spot_balance, starting_perpetual_balance,
            max_gross_notional, publish_event=False, issuance_id=issuance_id,
        )
        result["mode"] = "issued_spot_perp_development_only"
        result["issuance_id"] = issuance_id
        self.store.add_event(tenant_id, account_id, "issued_spot_perp_backtest", result)
        return result

    def latest_spot_perp_strategy_backtest(self, tenant_id, account_id):
        if self.store.tenant(tenant_id) is None:
            raise EngineConflict("租户不存在。")
        event = self.store.latest_event(
            tenant_id, account_id, "issued_spot_perp_backtest",
        )
        return None if event is None else event["body"]

    def latest_carry_backtest(self, tenant_id, account_id):
        if self.store.tenant(tenant_id) is None:
            raise EngineConflict("租户不存在。")
        event = self.store.latest_event(tenant_id, account_id, "carry_backtest")
        return None if event is None else event["body"]

    def run_protected_option_backtest(
        self, tenant_id, account_id, start, end, interval,
        starting_options_balance, starting_perpetual_balance, max_gross_notional,
        *, publish_event=True,
    ):
        """Run a grant-scoped pre-expiry protected option replay; never trade live."""
        from contextlib import ExitStack
        from datetime import timedelta
        from decimal import Decimal, InvalidOperation

        from nautilus_trader.model import CryptoOption, CryptoPerpetual

        from engine.api.data_queries import read_lease
        from engine.backtesting.backtest import catalog_directory, parse_window
        from engine.backtesting.option_history import (
            _instrument,
            read_option_protection_evidence,
        )
        from engine.backtesting.protected_simulation import run_native_protected

        self.assert_runnable(tenant_id, "backtest")
        tenant = self.store.tenant(tenant_id)
        if tenant is None or tenant["purpose"] != "development":
            raise EngineConflict("期权保护回测仅支持开发租户。")
        start_at, end_at, interval = parse_window(start, end, interval)
        if end_at - start_at > timedelta(days=1):
            raise EngineConflict("期权保护回测单次不能超过24小时。")
        loads = self.store.strategy_loads(tenant_id, account_id, include_archive=True)
        if len(loads) != 1:
            raise EngineConflict("期权保护回测需要单一已下发的 Demo 包。")
        load = loads[0]
        if (
            str(load["tenant_id"]) != str(tenant_id)
            or str(load["account_id"]) != str(account_id)
            or load["grant_purpose"] != "development"
            or load["code_key"] != "demo_option_hedged_bollinger"
        ):
            raise EngineConflict("期权回测策略未获得本账户的开发授权。")
        params = load["parameters"]
        symbol = params.get("symbol")
        put_name = params.get("put_symbol")
        call_name = params.get("call_symbol")
        if (
            symbol not in {"ETHUSDT", "BTCUSDT"}
            or not isinstance(put_name, str) or not isinstance(call_name, str)
            or not put_name or not call_name
        ):
            raise EngineConflict("期权保护 Demo 需要明确的基础合约及双翼合约。")
        if not all(
            self._has_subscription_intent(tenant_id, name, product)
            for product, name in (
                ("futures_um", symbol), ("options", put_name), ("options", call_name),
            )
        ):
            raise EngineConflict("开发账户尚未声明三条历史行情订阅。")
        try:
            contracts = Decimal(params["option_contracts"])
            age = int(params["max_age_seconds"]) * 1_000_000_000
            dte = int(params["min_dte_seconds"]) * 1_000_000_000
        except (TypeError, ValueError, KeyError, InvalidOperation) as exc:
            raise EngineConflict("期权保护参数无效。") from exc
        if not (contracts.is_finite() and contracts > 0 and 0 < age <= 300_000_000_000
                and 3_600_000_000_000 <= dte <= 2_592_000_000_000_000):
            raise EngineConflict("期权数量、到期剩余时间或报价时效无效。")
        if not (self.project / "data").is_dir():
            raise EngineConflict("历史数据目录不可用。")
        directory = catalog_directory(self.project)
        with ExitStack() as leases:
            for product, name in (
                ("futures_um", symbol), ("options", call_name), ("options", put_name),
            ):
                leases.enter_context(read_lease(self.project, name, product))
            catalog = self.market_subscriptions.history(directory)
            contracts_native = (
                _instrument(catalog, symbol, "futures_um", CryptoPerpetual),
                _instrument(catalog, put_name, "options", CryptoOption),
                _instrument(catalog, call_name, "options", CryptoOption),
            )
            if any(option.expiration_ns - int(end_at.timestamp() * 1_000_000_000) < dte
                   for option in contracts_native[1:]):
                raise EngineConflict("保护期权临近到期，无法通过研究回测风控。")
            evidence = read_option_protection_evidence(
                directory, symbol, put_name, call_name, start_at, end_at,
                interval, option_contracts=contracts, max_quote_age_ns=age,
                reader=catalog,
            )
            from engine.market_data.access.replay_adapter import replay_protected_evidence
            from engine.market_data.access.subscription_hub import MarketStream

            streams = (
                MarketStream("BINANCE", "futures_um", symbol, interval),
                MarketStream("BINANCE", "options", put_name, interval),
                MarketStream("BINANCE", "options", call_name, interval),
            )
            with self.market_subscriptions.backtest(uuid.uuid4().hex, streams) as replay:
                result = run_native_protected(
                    load, replay_protected_evidence(
                        replay, tenant_id, "protected", streams, evidence,
                    ), contracts_native, interval=interval,
                    starting_options_balance=starting_options_balance,
                    starting_perpetual_balance=starting_perpetual_balance,
                    max_gross_notional=max_gross_notional,
                )
        result.update(
            start=start_at.isoformat(), end=end_at.isoformat(), interval=interval,
        )
        if publish_event:
            self.store.add_event(tenant_id, account_id, "protected_option_backtest", result)
        return result

    def latest_protected_option_backtest(self, tenant_id, account_id):
        if self.store.tenant(tenant_id) is None:
            raise EngineConflict("租户不存在。")
        event = self.store.latest_event(tenant_id, account_id, "protected_option_backtest")
        return None if event is None else event["body"]

    def latest_backtest(self, tenant_id, account_id):
        if self.store.tenant(tenant_id) is None:
            raise EngineConflict("租户不存在。")
        event = self.store.latest_event(tenant_id, account_id, "backtest")
        if event is None:
            return None
        return event["body"]

    def _has_subscription_intent(self, tenant_id, symbol, product="futures_um"):
        wanted = str(symbol).upper()
        return any(
            str(row["symbol"]).upper() == wanted and row.get("product", "futures_um") == product
            and row["role"] == "intent"
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
