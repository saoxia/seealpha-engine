"""Bounded, signed, read-only Binance options account queries.

Credentials are supplied by the caller after existing server-side decryption.
This client never persists them or registers an execution client with a node.
"""

import asyncio
import hashlib
import hmac
import json
import threading
import time
from decimal import Decimal, InvalidOperation
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

from core.market.instruments import normalize_symbol

from . import options
from .queries import QueryError

READ_PARAMS = {"marginAccount": set(), "position": {"symbol"}, "openOrders": {"symbol", "limit"},
               "order": {"symbol", "orderId", "clientOrderId"},
               "historyOrders": {"symbol", "startTime", "endTime", "orderId", "limit"},
               "userTrades": {"symbol", "startTime", "endTime", "fromId", "limit"},
               "bill": {"currency", "startTime", "endTime", "recordId", "limit"},
               "exerciseRecord": {"symbol", "startTime", "endTime", "limit"}}


def signed_request(opener, key, secret, method, endpoint, params, server_ms, *, timeout):
    """Single signed send; callers own permissions and retry/outcome policy."""
    if not (method == "GET" and endpoint in {*READ_PARAMS, "userTrades"}
            or method in {"POST", "DELETE"} and endpoint == "order"):
        raise ValueError("不支持的期权端点或请求方法")
    query = urlencode({**params, "timestamp": server_ms, "recvWindow": 5000})
    signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    signed = query + "&signature=" + signature
    url = options.REST_URL + endpoint
    headers = {"X-MBX-APIKEY": key}
    data = None
    if method == "POST":
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        data = signed.encode()
    else:
        url += "?" + signed
    request = Request(url, data=data, headers=headers, method=method)
    with opener.open(request, timeout=timeout) as response:
        raw = response.read(8 * 1024 * 1024 + 1)
    if len(raw) > 8 * 1024 * 1024:
        raise ValueError("期权响应过大")
    return json.loads(raw, parse_float=Decimal)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class BinanceOptionsAccountClient:
    """GET-only HMAC account access. Responses retain exchange field names.

    Pages are explicit: a full limit page is not evidence of complete history.
    No balances are summed across currencies or presented as local estimates.
    """

    def __init__(self, api_key, api_secret, *, environment="production"):
        if environment != "production":
            raise ValueError("尚未验证 Binance 期权测试环境，拒绝发送账户凭据")
        if (not isinstance(api_key, str) or not isinstance(api_secret, str) or not api_key.strip()
                or not api_secret.strip() or api_key == api_secret or "\n" in api_key or "\r" in api_key):
            raise ValueError("期权账户凭据无效")
        self._key, self._secret = api_key, api_secret
        self._opener = build_opener(_NoRedirect())
        self._lock = threading.Lock()
        self._last_request = 0.0

    def __repr__(self):
        return "BinanceOptionsAccountClient(environment='production')"

    def _get(self, endpoint, **params):
        if endpoint not in READ_PARAMS or params.keys() - READ_PARAMS[endpoint]:
            raise ValueError("不支持的期权只读查询")
        with self._lock:
            for attempt in range(3):
                time.sleep(max(0, self._last_request + 0.3 - time.monotonic()))
                self._last_request = time.monotonic()
                try:
                    before = time.monotonic()
                    server = options.get("time")
                    if type(server["serverTime"]) is not int or time.monotonic() - before > 2:
                        raise ValueError("期权服务器校时无效或延迟过高")
                    value = signed_request(self._opener, self._key, self._secret, "GET", endpoint,
                                           params, server["serverTime"], timeout=15)
                    if isinstance(value, dict) and "code" in value:
                        raise ValueError("期权账户查询失败")
                    return value
                except HTTPError as exc:
                    if exc.code not in {429, 500, 502, 503, 504}:
                        break
                    try:
                        delay = min(max(float(exc.headers.get("Retry-After", "1")), 2**attempt), 30)
                    except (ValueError, TypeError):
                        delay = 2**attempt
                except (OSError, TimeoutError):
                    delay = 2**attempt
                except (ValueError, TypeError, KeyError, QueryError):
                    break
                if attempt < 2:
                    time.sleep(delay)
        raise QueryError("Binance 期权只读账户查询失败，请检查期权权限、凭据或网络。") from None

    @staticmethod
    def _records(value, kind, symbol=None):
        try:
            if not isinstance(value, list):
                raise TypeError
            seen = set()
            for row in value:
                code = normalize_symbol(row["symbol"], "options")
                if symbol is not None and code != symbol:
                    raise ValueError
                key = (code, row["side"]) if kind == "position" else (code, str(row["orderId"]))
                if key in seen:
                    raise ValueError
                seen.add(key)
                fields = ("quantity", "entryPrice", "markPrice", "unrealizedPNL") if kind == "position" else (
                    "quantity", "price", "executedQty")
                for field in fields:
                    if not isinstance(row[field], str) or not Decimal(row[field]).is_finite():
                        raise ValueError
                if kind == "position":
                    quantity = Decimal(row["quantity"])
                    if row["side"] not in {"LONG", "SHORT"} or quantity > 0 and row["side"] != "LONG" or quantity < 0 and row["side"] != "SHORT":
                        raise ValueError
                elif (row["side"] not in {"BUY", "SELL"} or not row["status"]
                      or Decimal(row["quantity"]) < 0 or not 0 <= Decimal(row["executedQty"]) <= Decimal(row["quantity"])):
                    raise ValueError
            return value
        except (KeyError, TypeError, ValueError, InvalidOperation):
            raise QueryError("Binance 期权账户数据不完整或不一致，拒绝返回不完整结果。") from None

    async def assets(self):
        value = await asyncio.to_thread(self._get, "marginAccount")
        try:
            rows = value["asset"]
            if not isinstance(rows, list) or not rows:
                raise ValueError
            seen = set()
            for row in rows:
                if not isinstance(row["asset"], str) or not row["asset"].isalnum() or row["asset"] in seen:
                    raise ValueError
                seen.add(row["asset"])
                for key in ("marginBalance", "equity", "available", "initialMargin", "maintMargin", "unrealizedPNL"):
                    if not isinstance(row[key], str) or not Decimal(row[key]).is_finite():
                        raise ValueError
            return value
        except (KeyError, TypeError, ValueError, InvalidOperation):
            raise QueryError("Binance 期权资产字段不完整，拒绝推算或补零。") from None

    async def positions(self, symbol=None):
        symbol = normalize_symbol(symbol, "options") if symbol is not None else None
        value = await asyncio.to_thread(self._get, "position", **({"symbol": symbol} if symbol else {}))
        return self._records(value, "position", symbol)

    async def open_orders(self, symbol=None, *, limit=1000):
        symbol = normalize_symbol(symbol, "options") if symbol is not None else None
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("订单查询数量须为 1 至 1000")
        value = await asyncio.to_thread(self._get, "openOrders", limit=limit, **({"symbol": symbol} if symbol else {}))
        return {"orders": self._records(value, "order", symbol), "limit": limit,
                "possibly_truncated": len(value) >= limit}

    async def order(self, symbol, *, order_id=None, client_order_id=None):
        symbol = normalize_symbol(symbol, "options")
        if (order_id is None) == (client_order_id is None):
            raise ValueError("须提供且只提供一个订单编号")
        if order_id is not None and (type(order_id) is not int or order_id < 0):
            raise ValueError("订单编号无效")
        if client_order_id is not None and (not isinstance(client_order_id, str) or not 1 <= len(client_order_id) <= 64):
            raise ValueError("客户端订单编号无效")
        params = {"orderId": order_id} if order_id is not None else {"clientOrderId": client_order_id}
        value = await asyncio.to_thread(self._get, "order", symbol=symbol, **params)
        result = self._records([value], "order", symbol)[0]
        if order_id is not None and str(result["orderId"]) != str(order_id) or client_order_id is not None and result.get("clientOrderId") != client_order_id:
            raise QueryError("Binance 期权订单编号不一致")
        return result

    async def order_history(self, symbol, start_ms, end_ms, *, order_id=None, limit=1000):
        symbol = normalize_symbol(symbol, "options")
        now = int(time.time() * 1000)
        if (type(start_ms) is not int or type(end_ms) is not int or not now - 5 * 86400000 <= start_ms <= end_ms <= now
                or type(limit) is not int or not 1 <= limit <= 1000):
            raise ValueError("期权已完成订单查询须在最近 5 天内，数量 1 至 1000")
        params = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": limit}
        if order_id is not None:
            if type(order_id) is not int or order_id < 0:
                raise ValueError("订单游标无效")
            params["orderId"] = order_id
        value = await asyncio.to_thread(self._get, "historyOrders", **params)
        return {"orders": self._records(value, "order", symbol), "limit": limit,
                "possibly_truncated": len(value) >= limit, "start_ms": start_ms, "end_ms": end_ms}

    async def history_pages(self, endpoint, start_ms, end_ms, *, symbol=None):
        """Validated leaf windows. A saturated millisecond never acknowledges coverage."""
        if (endpoint not in {"userTrades", "bill", "exerciseRecord"}
                or type(start_ms) is not int or type(end_ms) is not int
                or not int(time.time() * 1000) - 88 * 86400000 <= start_ms <= end_ms <= int(time.time() * 1000) + 60000):
            raise ValueError("期权历史查询范围或类型无效")
        if endpoint == "userTrades" and symbol is None:
            raise ValueError("期权成交查询需要合约")
        symbol = normalize_symbol(symbol, "options") if symbol is not None else None
        if endpoint == "bill" and symbol is not None:
            raise ValueError("期权资金流水按币种查询")
        windows = [(first, min(first + 7 * 86400000 - 1, end_ms))
                   for first in range(start_ms, end_ms + 1, 7 * 86400000)]
        pending, requests = list(reversed(windows)), 0
        while pending:
            first, last = pending.pop()
            params = {"startTime": first, "endTime": last, "limit": 1000}
            if endpoint == "bill":
                params["currency"] = "USDT"
            elif symbol is not None:
                params["symbol"] = symbol
            requests += 1
            if requests > 2048:
                raise QueryError("期权历史超过分页上限，未确认剩余区间。")
            rows = await asyncio.to_thread(self._get, endpoint, **params)
            try:
                if not isinstance(rows, list) or len(rows) > 1000:
                    raise ValueError
                seen = set()
                for row in rows:
                    identity = row["tradeId" if endpoint == "userTrades" else "id"]
                    if (isinstance(identity, bool) or not str(identity).isascii() or not str(identity).isdigit()
                            or str(identity) in seen):
                        raise ValueError
                    seen.add(str(identity))
                    stamp = row["time" if endpoint == "userTrades" else "createDate"]
                    if type(stamp) is not int or not first <= stamp <= last:
                        raise ValueError
                    code = row.get("symbol")
                    if (endpoint != "bill" or code) and (
                        normalize_symbol(code, "options") != code or symbol is not None and code != symbol
                    ):
                        raise ValueError
                    fields = ("price", "quantity", "fee") if endpoint == "userTrades" else (
                        ("quantity", "amount", "fee", "exercisePrice") if endpoint == "exerciseRecord" else ("amount",))
                    for field in fields:
                        if (not isinstance(row[field], str) or len(row[field]) > 100
                                or not Decimal(row[field]).is_finite()):
                            raise ValueError
                    currency = row["asset" if endpoint == "bill" else "quoteAsset"]
                    if not isinstance(currency, str) or not currency.isascii() or not currency.isalnum():
                        raise ValueError
                    if endpoint == "bill":
                        if currency != "USDT" or not isinstance(row["type"], str) or not 1 <= len(row["type"]) <= 64:
                            raise ValueError
                    elif endpoint == "userTrades":
                        if (row["side"] not in {"BUY", "SELL"} or Decimal(row["quantity"]) <= 0
                                or Decimal(row["price"]) <= 0 or not str(row["orderId"]).isascii()
                                or not str(row["orderId"]).isdigit() or not isinstance(row["type"], str)):
                            raise ValueError
                    elif row["positionSide"] not in {"LONG", "SHORT"} or Decimal(row["quantity"]) < 0:
                        raise ValueError
            except (KeyError, TypeError, ValueError, AttributeError, InvalidOperation):
                raise QueryError("期权历史字段、身份或查询范围不一致，未确认覆盖。") from None
            if len(rows) == 1000:
                if first == last:
                    raise QueryError("期权历史单毫秒满页，无法确认完整记录。")
                middle = (first + last) // 2
                pending.extend([(middle + 1, last), (first, middle)])
                continue
            yield {"start_ms": first, "end_ms": last, "rows": rows}
