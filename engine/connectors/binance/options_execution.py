"""Nautilus v2 options execution: single-send writes and evidence-based recovery."""

import asyncio
import hashlib
from decimal import Decimal

from nautilus_trader.config import ExecutionClientConfig
from nautilus_trader.live.clients import ExecutionClient, ExecutionClientFactory
from nautilus_trader.model import (
    AccountBalance,
    AccountId,
    AccountType,
    ClientOrderId,
    Currency,
    ExecutionMassStatus,
    FillReport,
    InstrumentId,
    LiquiditySide,
    MarginBalance,
    Money,
    OmsType,
    OrderSide,
    OrderStatus,
    OrderStatusReport,
    OrderType,
    PositionSide,
    PositionStatusReport,
    Price,
    Quantity,
    TimeInForce,
    TradeId,
    Venue,
    VenueOrderId,
)

from core.market.instruments import instrument_id_for, normalize_symbol

from . import options
from .options_orders import BinanceOptionsOrderTransport, OptionsOrderError
from .options_parsing import number, stamp
from .queries import QueryError

STATES = {"NEW": OrderStatus.ACCEPTED, "ACCEPTED": OrderStatus.ACCEPTED,
          "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED, "FILLED": OrderStatus.FILLED,
          "CANCELED": OrderStatus.CANCELED, "CANCELLED": OrderStatus.CANCELED,
          "REJECTED": OrderStatus.REJECTED, "EXPIRED": OrderStatus.EXPIRED,
          "EXPIRED_IN_MATCH": OrderStatus.EXPIRED}


class BinanceOptionsExecutionClientConfig(ExecutionClientConfig):
    account_id: AccountId
    symbols: list[str]
    api_key: str
    api_secret: str

    def __init__(self, *, account_id, symbols, api_key, api_secret, **kwargs):
        self.account_id = account_id if isinstance(account_id, AccountId) else AccountId(account_id)
        if not str(self.account_id).startswith("BINANCE-OPTIONS-"):
            raise ValueError("期权执行须使用独立的 BINANCE-OPTIONS-* 账户标识")
        self.symbols = [normalize_symbol(s, "options") for s in symbols]
        if not 1 <= len(self.symbols) <= 32 or len(set(self.symbols)) != len(self.symbols):
            raise ValueError("期权执行须明确配置 1 至 32 个唯一合约")
        self.api_key, self.api_secret = api_key, api_secret

    def __repr__(self):
        return f"BinanceOptionsExecutionClientConfig(account_id={self.account_id!s}, credentials=<redacted>)"


class BinanceOptionsExecutionClientFactory(ExecutionClientFactory):
    @staticmethod
    def create(*, name, config, cache, clock, trader_id):
        if not isinstance(config, BinanceOptionsExecutionClientConfig):
            raise TypeError("Expected BinanceOptionsExecutionClientConfig")
        return BinanceOptionsExecutionClient(name=name, config=config, cache=cache, clock=clock, trader_id=trader_id)


class BinanceOptionsExecutionClient(ExecutionClient):
    def __init__(self, *, name, config, cache, clock, trader_id):
        super().__init__(name=name, config=config, cache=cache, clock=clock, trader_id=trader_id,
                         venue=Venue("BINANCE"), account_id=config.account_id,
                         account_type=AccountType.MARGIN, oms_type=OmsType.NETTING)
        self.transport = BinanceOptionsOrderTransport(config.api_key, config.api_secret)
        self.instruments = {}
        self._orders, self._wire_ids, self._venue_ids = {}, {}, {}
        self._sent, self._uncertain = set(), set()
        self._closing, self._ready = True, False
        self._poller = None
        self._last_sync, self._last_account, self._last_error = None, None, None

    @property
    def execution_status(self):
        age = (self.clock.timestamp_ns() - self._last_sync) / 1e9 if self._last_sync is not None else None
        account_age = (self.clock.timestamp_ns() - self._last_account) / 1e9 if self._last_account is not None else None
        return {"ready": self._ready and not self._closing and not self._uncertain and self._last_error is None
                and self._poller is not None and not self._poller.done() and age is not None and 0 <= age <= 15
                and account_age is not None and 0 <= account_age <= 15,
                "last_sync_age_seconds": age, "account_age_seconds": account_age, "last_error": self._last_error,
                "unresolved_orders": sorted(self._uncertain)}

    def _item(self, instrument_id):
        symbol = str(instrument_id.symbol)
        if str(instrument_id.venue) != "BINANCE" or symbol not in self.instruments:
            raise ValueError("期权执行合约不在本客户端配置范围")
        return self.instruments[symbol]

    def _wire(self, client_id):
        key = str(client_id)
        if key not in self._wire_ids:
            digest = hashlib.sha256(f"{self.account_id}:{key}".encode()).hexdigest()
            self._wire_ids[key] = "sa-" + digest[:32]
        return self._wire_ids[key]

    async def _call(self, method, endpoint, **params):
        return await self.transport.send(method, endpoint, **params)

    async def _connect(self):
        self._closing = False
        self._sync_lock = asyncio.Lock()
        try:
            rows = await asyncio.to_thread(options.markets)
            loaded = {row["symbol"]: options.instrument(row) for row in rows}
            if any(symbol not in loaded for symbol in self.config.symbols):
                raise ValueError("配置的期权合约不存在或已到期")
            self.instruments = {symbol: loaded[symbol] for symbol in self.config.symbols}
            for order in self.cache.orders(account_id=self.account_id):
                if str(order.instrument_id.symbol) in self.instruments:
                    self._orders[str(order.client_order_id)] = order
                    if order.status != OrderStatus.INITIALIZED:
                        self._sent.add(str(order.client_order_id))
                    self._wire(order.client_order_id)
                    if order.venue_order_id is not None:
                        self._venue_ids[str(order.client_order_id)] = str(order.venue_order_id)
            await self._account()
            self._poller = self.create_task(self._poll(), "options-order-reconciliation")
        except BaseException:
            await self._disconnect()
            raise

    async def _disconnect(self):
        self._closing, self._ready = True, False
        if self._poller is not None:
            if not self._poller.cancelling():
                self._poller.cancel()
            await asyncio.gather(self._poller, return_exceptions=True)
            self._poller = None

    async def _account(self):
        value = await self._call("GET", "marginAccount")
        if not isinstance(value, dict) or not isinstance(value.get("asset"), list) or not value["asset"]:
            raise ValueError("期权资产快照不完整")
        balances, margins, seen = [], [], set()
        for row in value["asset"]:
            currency = Currency.from_str(row["asset"])
            if currency in seen:
                raise ValueError("期权资产重复")
            seen.add(currency)
            total, free, initial, maintenance = [Decimal(row[key]) for key in ("marginBalance", "available", "initialMargin", "maintMargin")]
            if not all(v.is_finite() for v in (total, free, initial, maintenance)) or not 0 <= free <= total or min(initial, maintenance) < 0:
                raise ValueError("期权保证金快照不一致")
            balances.append(AccountBalance(Money(total, currency), Money(total - free, currency), Money(free, currency)))
            margins.append(MarginBalance(Money(initial, currency), Money(maintenance, currency)))
        self.generate_account_state(balances, margins, True, self.clock.timestamp_ns(),
                                    {"product": "options", "source": "eapi/v1/marginAccount", "asset": value["asset"],
                                     "balance_basis": "exchange_margin_balance"})
        self._last_account = self.clock.timestamp_ns()

    def _order_report(self, row, *, expected=None):
        item = self._item(InstrumentId.from_str(instrument_id_for(row["symbol"], "options")))
        wire = row["clientOrderId"]
        if not isinstance(wire, str) or not wire:
            raise ValueError("期权订单缺少客户端编号")
        native = next((key for key, value in self._wire_ids.items() if value == wire), wire)
        cid = ClientOrderId(native)
        venue_id = str(stamp(row["orderId"]))
        if native in self._venue_ids and self._venue_ids[native] != venue_id:
            raise ValueError("同一客户端订单号对应了不同交易所订单")
        post_only = row.get("postOnly", row["timeInForce"] == "GTX")
        tif = TimeInForce.GTC if row["timeInForce"] == "GTX" else TimeInForce.from_str(row["timeInForce"])
        if row["type"] != "LIMIT" or row["side"] not in {"BUY", "SELL"} or type(post_only) is not bool:
            raise ValueError("期权订单类型或参数无效")
        if expected is not None and (cid != expected.client_order_id or row["symbol"] != str(expected.instrument_id.symbol)
                or row["side"] != str(expected.side)
                or Decimal(row["quantity"]) != expected.quantity.as_decimal()
                or Decimal(row["price"]) != expected.price.as_decimal()
                or row.get("reduceOnly") is not expected.is_reduce_only or post_only != expected.is_post_only
                or tif != expected.time_in_force):
            raise ValueError("交易所期权订单与本地请求身份或参数冲突")
        self._wire_ids[native] = wire
        status = STATES[row["status"]]
        qty = Quantity.from_str(number(row["quantity"], item.size_precision))
        filled = Quantity.from_str(number(row["executedQty"], item.size_precision, zero=True))
        if filled > qty or status == OrderStatus.FILLED and filled != qty:
            raise ValueError("期权订单累计成交数量不一致")
        average = Decimal(row["avgPrice"]) if filled.as_decimal() else None
        if average is not None and (not average.is_finite() or average <= 0):
            raise ValueError("期权累计成交均价无效")
        created = stamp(row.get("createTime", row.get("createDate"))) * 1_000_000
        updated = stamp(row["updateTime"]) * 1_000_000
        if updated < created or type(row["reduceOnly"]) is not bool:
            raise ValueError("期权订单时间或 Reduce Only 字段无效")
        self._venue_ids[native] = venue_id
        return OrderStatusReport(self.account_id, item.id, VenueOrderId(venue_id),
                                 OrderSide.from_str(row["side"]), OrderType.LIMIT, tif, status,
                                 qty, filled, created, updated, self.clock.timestamp_ns(), client_order_id=cid,
                                 price=Price.from_str(number(row["price"], item.price_precision)),
                                 avg_px=average, post_only=post_only, reduce_only=row["reduceOnly"])

    async def _fills(self, item, start_ns, end_ns):
        params = {"symbol": str(item.raw_symbol), "startTime": start_ns // 1_000_000,
                  "endTime": end_ns // 1_000_000, "limit": 1000}
        result, previous = {}, -1
        for _ in range(20):
            rows = await self._call("GET", "userTrades", **params)
            if not isinstance(rows, list):
                raise TypeError("期权成交响应无效")
            for row in rows:
                identity = stamp(row["tradeId"])
                if row["symbol"] != str(item.raw_symbol) or row["quoteAsset"] != str(item.quote_currency):
                    raise ValueError("期权成交合约或手续费币种不一致")
                event = stamp(row["time"]) * 1_000_000
                if not params["startTime"] * 1_000_000 <= event <= params["endTime"] * 1_000_000:
                    raise ValueError("期权成交超出核对范围")
                if not start_ns <= event <= end_ns:
                    continue
                if identity in result and result[identity] != row:
                    raise ValueError("期权成交编号冲突")
                result[identity] = row
            if len(rows) < 1000:
                break
            cursor = max(stamp(row["tradeId"]) for row in rows)
            if cursor <= previous:
                raise ValueError("期权成交分页未推进")
            previous, params["fromId"] = cursor, cursor + 1
        else:
            raise ValueError("期权成交核对超过分页上限")
        reports = []
        for identity, row in sorted(result.items()):
            fee = Decimal(row["fee"])
            if not fee.is_finite():
                raise ValueError("期权手续费无效")
            reports.append(FillReport(self.account_id, item.id, VenueOrderId(str(stamp(row["orderId"]))),
                                      TradeId(str(identity)), OrderSide.from_str(row["side"]),
                                      Quantity.from_str(number(row["quantity"], item.size_precision)),
                                      Price.from_str(number(row["price"], item.price_precision)),
                                      Money(-fee, item.quote_currency), LiquiditySide.from_str(row["liquidity"]),
                                      stamp(row["time"]) * 1_000_000, self.clock.timestamp_ns()))
        return reports

    async def _observe(self, order):
        item = self._item(order.instrument_id)
        row = await self._call("GET", "order", symbol=str(item.raw_symbol), clientOrderId=self._wire(order.client_order_id))
        report = self._order_report(row, expected=order)
        fills = []
        if report.filled_qty.as_decimal():
            fills = [fill for fill in await self._fills(item, report.ts_accepted, self.clock.timestamp_ns())
                     if fill.venue_order_id == report.venue_order_id]
            if sum((fill.last_qty.as_decimal() for fill in fills), Decimal(0)) != report.filled_qty.as_decimal():
                raise ValueError("期权成交明细与累计数量尚未一致")
        self._handle_report(report, fills)
        self._uncertain.discard(str(order.client_order_id))
        return report

    async def _poll(self):
        while not self._closing:
            await asyncio.sleep(2)
            try:
                async with self._sync_lock:
                    await self._account()
                    if self._closing:
                        return
                    for key, order in list(self._orders.items()):
                        if self._closing:
                            return
                        cached = self.cache.order(order.client_order_id)
                        if cached is not None and cached.is_closed and key not in self._uncertain:
                            continue
                        await self._observe(order)
                    self._last_sync, self._last_error = self.clock.timestamp_ns(), None
            except (QueryError, ValueError, KeyError, TypeError, ArithmeticError):
                self._last_error = "期权账户或订单核对未完成；禁止新增风险"

    def _validate(self, order):
        item = self._item(order.instrument_id)
        now = self.clock.timestamp_ns()
        if order.trader_id != self.trader_id or order.account_id not in {None, self.account_id}:
            raise ValueError("期权委托账户归属不匹配")
        if self._closing or now >= item.expiration_ns:
            raise ValueError("期权执行已停止或合约已到期")
        if order.order_type != OrderType.LIMIT or order.time_in_force not in {TimeInForce.GTC, TimeInForce.IOC, TimeInForce.FOK}:
            raise ValueError("期权仅支持 LIMIT GTC/IOC/FOK 与 GTC Post Only")
        if order.is_post_only and order.time_in_force != TimeInForce.GTC:
            raise ValueError("Post Only 与 IOC/FOK 不兼容")
        if order.is_quote_quantity or order.display_qty is not None or order.contingency_type is not None:
            raise ValueError("期权不支持报价金额、冰山或关联订单")
        qty, price = order.quantity.as_decimal(), order.price.as_decimal()
        number(str(qty), item.size_precision)
        number(str(price), item.price_precision)
        if (qty % item.size_increment.as_decimal() or price % item.price_increment.as_decimal()
                or item.min_quantity is not None and qty < item.min_quantity.as_decimal()
                or item.max_quantity is not None and qty > item.max_quantity.as_decimal()
                or item.min_price is not None and price < item.min_price.as_decimal()
                or item.max_price is not None and price > item.max_price.as_decimal()):
            raise ValueError("期权委托未满足数量范围或最小变动单位")
        if not order.is_reduce_only:
            if not self.execution_status["ready"]:
                raise ValueError("期权账户或订单状态尚未核对，禁止新增风险")
            quote = self.cache.quote(item.id)
            if quote is None or not 0 <= now - quote.ts_event <= 15_000_000_000 or not 0 <= now - quote.ts_init <= 15_000_000_000:
                raise ValueError("期权报价尚未就绪或已过期，禁止新增风险")
            if min(quote.bid_price.as_decimal(), quote.ask_price.as_decimal(), quote.bid_size.as_decimal(), quote.ask_size.as_decimal()) <= 0:
                raise ValueError("期权盘口缺少有效双边报价，禁止新增风险")
        return item

    async def _submit_order(self, command):
        order, key = command.order, str(command.order.client_order_id)
        if key in self._sent:
            await self._observe(order)
            return
        try:
            if command.params:
                raise ValueError("期权委托不支持附加参数覆盖")
            item = self._validate(order)
        except (ValueError, KeyError, TypeError) as exc:
            self.generate_order_denied(order, str(exc))
            return
        wire = self._wire(order.client_order_id)
        try:
            existing = await self._call("GET", "order", symbol=str(item.raw_symbol), clientOrderId=wire)
        except OptionsOrderError as exc:
            if exc.code != -2013:
                self.generate_order_denied(order, "未能排除已有同编号订单，未发送新委托")
                return
        else:
            if self._closing:
                self.generate_order_denied(order, "执行客户端正在关闭，未发送新委托")
                return
            existing_owner = self.cache.client_order_id(VenueOrderId(str(stamp(existing["orderId"]))))
            if existing_owner is not None and existing_owner != order.client_order_id:
                self.generate_order_denied(order, "同一交易所订单已归属其他本地订单，拒绝重复提交")
                return
            self._orders[key] = order
            self._sent.add(key)
            self._uncertain.add(key)
            self.generate_order_submitted(order)
            await self._observe(order)
            return
        if self._closing:
            self.generate_order_denied(order, "执行客户端正在关闭，未发送新委托")
            return
        self._orders[key] = order
        self._sent.add(key)
        self._uncertain.add(key)
        self.generate_order_submitted(order)
        try:
            await self._call("POST", "order", symbol=str(item.raw_symbol), side=str(order.side), type="LIMIT",
                             quantity=str(order.quantity), price=str(order.price), timeInForce="GTX" if order.is_post_only else str(order.time_in_force),
                             postOnly=str(order.is_post_only).lower(),
                             reduceOnly=str(order.is_reduce_only).lower(), clientOrderId=wire, newOrderRespType="RESULT")
        except OptionsOrderError as exc:
            if self._closing:
                return
            try:
                await self._observe(order)
            except OptionsOrderError as lookup:
                if not exc.unknown and lookup.code == -2013:
                    self.generate_order_rejected(order, "交易所拒绝期权订单", self.clock.timestamp_ns(), False)
                    self._uncertain.discard(key)
            except (QueryError, ValueError, KeyError, TypeError):
                self._last_error = "期权订单返回证据冲突，状态待核实"
            return
        if self._closing:
            return
        # An HTTP acknowledgement alone is not authoritative lifecycle evidence.
        try:
            await self._observe(order)
        except (QueryError, ValueError, KeyError, TypeError):
            self._last_error = "期权订单已发送，状态待核实；不会重复提交"

    async def _cancel_order(self, command):
        order = self.cache.order(command.client_order_id)
        if (order is None or order.account_id != self.account_id or order.instrument_id != command.instrument_id
                or command.strategy_id != order.strategy_id
                or command.venue_order_id is not None and command.venue_order_id != order.venue_order_id):
            raise ValueError("无法确认期权撤单归属")
        self._item(order.instrument_id)
        self._orders[str(order.client_order_id)] = order
        self._uncertain.add(str(order.client_order_id))
        try:
            await self._call("DELETE", "order", symbol=str(order.instrument_id.symbol), clientOrderId=self._wire(order.client_order_id))
        except OptionsOrderError:
            pass
        if self._closing:
            return
        try:
            report = await self._observe(order)
            if report.is_open:
                self.generate_order_cancel_rejected(order, order.venue_order_id, "撤单后订单仍在场内", self.clock.timestamp_ns())
        except (QueryError, ValueError, KeyError, TypeError):
            self._last_error = "期权撤单结果待核实；未声明撤单成功"

    async def _cancel_all_orders(self, command):
        from types import SimpleNamespace
        orders = self.cache.orders_open(account_id=self.account_id, instrument_id=command.instrument_id,
                                         strategy_id=command.strategy_id, side=command.order_side)
        for order in orders:
            if self._closing:
                break
            if str(order.instrument_id.symbol) in self.instruments:
                await self._cancel_order(SimpleNamespace(client_order_id=order.client_order_id, instrument_id=order.instrument_id,
                                                        strategy_id=order.strategy_id, venue_order_id=order.venue_order_id))

    async def _modify_order(self, command):
        order = self.cache.order(command.client_order_id)
        if order is not None and order.account_id == self.account_id:
            self.generate_order_modify_rejected(order, order.venue_order_id, "期权不支持原地改单，请明确撤单后另下新单", self.clock.timestamp_ns())

    async def _register_external_order(self, client_order_id, venue_order_id, instrument_id, strategy_id, ts_init):
        order = self.cache.order(client_order_id)
        if order is not None and order.account_id == self.account_id and self._provides_bulk_position_coverage(instrument_id):
            self._orders[str(client_order_id)] = order
            self._sent.add(str(client_order_id))

    async def _submit_order_list(self, command):
        for order in command.order_list.orders:
            self.generate_order_denied(order, "期权适配器不支持关联或批量委托")

    async def _query_account(self, command):
        await self._account()

    async def _query_order(self, command):
        order = self.cache.order(command.client_order_id)
        if order is None or order.account_id != self.account_id:
            raise ValueError("无法确认期权订单归属")
        await self._observe(order)

    async def _generate_order_status_report(self, command):
        identity = command.instrument_id
        if identity is None and command.client_order_id is not None:
            cached = self.cache.order(command.client_order_id)
            identity = cached.instrument_id if cached is not None and cached.account_id == self.account_id else None
        if identity is None:
            raise ValueError("期权状态查询缺少可核实的合约身份")
        item = self._item(identity)
        if command.client_order_id is not None:
            params = {"clientOrderId": self._wire(command.client_order_id)}
        elif command.venue_order_id is not None:
            params = {"orderId": int(str(command.venue_order_id))}
        else:
            raise ValueError("期权状态查询须提供订单编号")
        row = await self._call("GET", "order", symbol=str(item.raw_symbol), **params)
        if (row["symbol"] != str(item.raw_symbol)
                or "clientOrderId" in params and row["clientOrderId"] != params["clientOrderId"]
                or "orderId" in params and str(row["orderId"]) != str(params["orderId"])):
            raise ValueError("期权状态响应与请求编号不一致")
        return self._order_report(row)

    def _provides_bulk_position_coverage(self, instrument_id):
        return str(instrument_id.venue) == "BINANCE" and str(instrument_id.symbol) in self.instruments

    async def _generate_position_status_reports(self, command):
        rows = await self._call("GET", "position")
        rows = self.transport._records(rows, "position")
        result, seen = [], set()
        for row in rows:
            if row["symbol"] not in self.instruments:
                continue
            item = self.instruments[row["symbol"]]
            if command.instrument_id is not None and command.instrument_id != item.id:
                continue
            qty = Decimal(row["quantity"])
            if item.id in seen:
                raise ValueError("期权净持仓返回重复方向，拒绝猜测合并")
            seen.add(item.id)
            result.append(PositionStatusReport(self.account_id, item.id,
                                                PositionSide.LONG if qty > 0 else PositionSide.SHORT if qty < 0 else PositionSide.FLAT,
                                                Quantity.from_str(number(str(abs(qty)), item.size_precision, zero=True)),
                                                stamp(row["time"]) * 1_000_000, self.clock.timestamp_ns(),
                                                avg_px_open=Decimal(row["entryPrice"])))
        for item in self.instruments.values():
            if item.id not in seen and (command.instrument_id is None or command.instrument_id == item.id):
                result.append(PositionStatusReport(self.account_id, item.id, PositionSide.FLAT,
                                                    Quantity.from_str("0"), self.clock.timestamp_ns(), self.clock.timestamp_ns()))
        return result

    async def _generate_order_status_reports(self, command):
        now = self.clock.timestamp_ns()
        first = command.start if command.start is not None else now - 5 * 86400 * 1_000_000_000
        last = command.end if command.end is not None else now
        if not now - 5 * 86400 * 1_000_000_000 <= first <= last <= now:
            raise ValueError("期权订单核对仅支持最近 5 天")
        result = {}
        for item in self.instruments.values():
            if command.instrument_id is not None and command.instrument_id != item.id:
                continue
            opened = await self._call("GET", "openOrders", symbol=str(item.raw_symbol), limit=1000)
            finished = [] if command.open_only else await self._call("GET", "historyOrders", symbol=str(item.raw_symbol),
                                                                    startTime=first // 1_000_000, endTime=last // 1_000_000, limit=1000)
            if not isinstance(opened, list) or not isinstance(finished, list) or len(opened) >= 1000 or len(finished) >= 1000:
                raise ValueError("期权订单核对结果可能截断，不能声明完整")
            for row in [*finished, *opened]:
                if row["symbol"] != str(item.raw_symbol):
                    raise ValueError("期权订单列表包含其他合约")
                report = self._order_report(row)
                if report.venue_order_id in result:
                    raise ValueError("期权核对期间订单状态变化，请重试核对")
                result[report.venue_order_id] = report
        return list(result.values())

    async def _generate_fill_reports(self, command):
        first = command.start if command.start is not None else self.clock.timestamp_ns() - 5 * 86400 * 1_000_000_000
        last = command.end if command.end is not None else self.clock.timestamp_ns()
        result = []
        for item in self.instruments.values():
            if command.instrument_id is None or command.instrument_id == item.id:
                result.extend(await self._fills(item, first, last))
        return [fill for fill in result if command.venue_order_id is None or fill.venue_order_id == command.venue_order_id]

    async def _generate_mass_status(self, lookback_mins):
        from types import SimpleNamespace
        now = self.clock.timestamp_ns()
        first = now - min(lookback_mins if lookback_mins is not None else 7200, 7200) * 60 * 1_000_000_000
        async with self._sync_lock:
            try:
                self._ready = False
                orders = await self._generate_order_status_reports(SimpleNamespace(start=first, end=now, open_only=False, instrument_id=None))
                fills = await self._generate_fill_reports(SimpleNamespace(start=first, end=now, instrument_id=None, venue_order_id=None))
                for report in orders:
                    observed = sum((fill.last_qty.as_decimal() for fill in fills if fill.venue_order_id == report.venue_order_id), Decimal(0))
                    if observed != report.filled_qty.as_decimal():
                        raise ValueError("启动核对的成交明细不完整；请扩大核对窗口或核实历史")
                positions = await self._generate_position_status_reports(SimpleNamespace(instrument_id=None))
                result = ExecutionMassStatus(self.client_id, self.account_id, self.venue, self.clock.timestamp_ns())
                result.add_order_reports(orders)
                result.add_fill_reports(fills)
                result.add_position_reports(positions)
                values = result.to_dict()
                values.update(lookback_start=first, reports_complete=True)
                for order in self._orders.values():
                    if not order.is_closed:
                        await self._observe(order)
                self._ready = True
                self._last_sync, self._last_error = self.clock.timestamp_ns(), None
                return ExecutionMassStatus.from_dict(values)
            except BaseException:
                self._last_error = "期权启动核对未完成，禁止新增风险"
                raise
