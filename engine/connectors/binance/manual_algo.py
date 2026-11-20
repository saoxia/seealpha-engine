"""Exchange-hosted USD-M conditional orders and leverage. No browser-side triggers.

The pinned native Binance adapter does not expose the 2026 Algo REST endpoints.
This connector is strictly Engine-owned, using exact official endpoints and no retries.
"""
import asyncio
import hashlib
import hmac
import json
import re
import time
from decimal import Decimal
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

from engine.connectors.binance.manual import ManualValidationError, exact
from engine.connectors.binance.manual import execute as native_execute
from engine.connectors.binance.queries import QueryError

BASES = {"production": "https://fapi.binance.com", "sandbox": "https://testnet.binancefuture.com"}
CLOSED_STATUSES = {"CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FINISHED", "TRIGGERED", "FILLED"}
ACTIVE_STATUSES = {"NEW", "PENDING", "PARTIALLY_FILLED", "WORKING"}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class VenueRejected(ManualValidationError):
    """Definitive exchange rejection; may be saved as completed failure."""


class SignedFutures:
    def __init__(self, account, *, http_url=None):
        env = account["environment"]
        creds = account["credentials"]
        if env not in BASES or not creds.get("apiKey") or not creds.get("secret") or (
            creds["apiKey"] == creds["secret"] or "\r" in creds["apiKey"] or "\n" in creds["apiKey"]
        ):
            raise ManualValidationError("交易账户环境或凭据无效。")
        self.base = http_url if http_url is not None else BASES[env]
        self.key, self.secret = creds["apiKey"], creds["secret"]
        self.opener = build_opener(NoRedirect())
        self.offset = None

    def request(self, method, path, values=None):
        if path not in {
            "/fapi/v1/time", "/fapi/v1/algoOrder", "/fapi/v1/openAlgoOrders", "/fapi/v1/allAlgoOrders",
            "/fapi/v1/leverage", "/fapi/v1/leverageBracket", "/fapi/v1/symbolConfig",
        } or method not in {"GET", "POST", "DELETE"}:
            raise ValueError("Unapproved Binance endpoint")
        params = dict(values or {})
        if path != "/fapi/v1/time":
            if self.offset is None:
                self.server_time()
            params.update(timestamp=int(time.time() * 1000) + self.offset, recvWindow=5000)
            encoded = urlencode(params)
            signature = hmac.new(self.secret.encode(), encoded.encode(), hashlib.sha256).hexdigest()
            encoded += "&signature=" + signature
        else:
            encoded = ""
        body = encoded.encode() if method == "POST" else None
        url = self.base + path + ("?" + encoded if method != "POST" and encoded else "")
        request = Request(url, data=body, method=method,
                          headers={"X-MBX-APIKEY": self.key} if path != "/fapi/v1/time" else {})
        try:
            with self.opener.open(request, timeout=8) as response:
                data = response.read(512 * 1024 + 1)
        except HTTPError as exc:
            # A known 4xx response conclusively rejects the command; 408/429/5xx
            # can be ambiguous and are deliberately NOT retried.
            if 400 <= exc.code < 500 and exc.code not in {408, 429}:
                raise VenueRejected("交易所明确拒绝该操作，请核对账户权限、参数与仓位。") from None
            raise QueryError("交易所响应异常，操作结果待核实；不得重新发送。") from None
        except Exception:  # noqa: BLE001 - sanitize all transport failures without exposing signed requests.
            raise QueryError("交易所连接失败，操作结果待核实；不得重新发送。") from None
        if len(data) > 512 * 1024:
            raise QueryError("交易所响应过大，结果待核实。")
        try:
            return json.loads(data)
        except (TypeError, ValueError):
            raise QueryError("交易所响应无法验证，结果待核实。") from None

    def server_time(self):
        payload = self.request("GET", "/fapi/v1/time")
        if not isinstance(payload, dict) or type(payload.get("serverTime")) is not int:
            raise QueryError("交易所服务器时间不可用。")
        self.offset = payload["serverTime"] - int(time.time() * 1000)


def _symbol(symbol):
    if not re.fullmatch(r"[A-Z0-9]{2,30}(?:USDT|USDC)-PERP", symbol):
        raise ManualValidationError("只支持指定的 USD-M 永续合约。")
    return symbol.removesuffix("-PERP")


def algo_view(row):
    if not isinstance(row, dict):
        raise QueryError("条件单响应结构无效。")
    status = row.get("algoStatus")
    identity = str(row.get("clientAlgoId", ""))
    order_type = row.get("orderType")
    if (not re.fullmatch(r"[.A-Za-z0-9_:/-]{1,36}", identity)
            or status not in ACTIVE_STATUSES | CLOSED_STATUSES
            or order_type not in {"STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"}
            or row.get("side") not in {"BUY", "SELL"}
            or row.get("positionSide") not in {"BOTH", "LONG", "SHORT"}):
        raise QueryError("条件单响应字段无效。")
    return {
        "client_algo_id": identity,
        "algo_id": str(row["algoId"]) if row.get("algoId") is not None else None,
        "symbol": str(row["symbol"]) + "-PERP",
        "side": str(row["side"]).lower(),
        "position_side": row["positionSide"],
        "order_type": order_type,
        "quantity": str(row.get("quantity") or "0"),
        "price": str(row["price"]) if row.get("price") not in (None, "", "0") else None,
        "trigger_price": str(row["triggerPrice"]) if row.get("triggerPrice") not in (None, "", "0") else None,
        "activation_price": str(row["activatePrice"]) if row.get("activatePrice") not in (None, "", "0") else None,
        "callback_rate": str(row["callbackRate"]) if row.get("callbackRate") not in (None, "") else None,
        "working_type": row.get("workingType") or "CONTRACT_PRICE",
        "status": status.lower(),
        "open": status in ACTIVE_STATUSES,
        "timestamp": int(row.get("createTime") or row.get("updateTime") or 0),
    }


def open_orders(account, symbol, *, http_url=None):
    client = SignedFutures(account, http_url=http_url)
    rows = client.request("GET", "/fapi/v1/openAlgoOrders", {"symbol": _symbol(symbol)})
    if not isinstance(rows, list) or len(rows) > 250:
        raise QueryError("条件单列表响应无效或超过安全上限。")
    orders = [algo_view(row) for row in rows]
    if any(order["symbol"] != symbol for order in orders):
        raise QueryError("条件单账户或标的身份不匹配。")
    # Binance returns recent (up to seven days per query) algo history separately.
    # Historical coverage is best-effort and MUST NOT hide confirmed open orders.
    try:
        recent = client.request("GET", "/fapi/v1/allAlgoOrders",
                                {"symbol": _symbol(symbol), "limit": 100})
        if not isinstance(recent, list) or len(recent) > 100:
            raise QueryError("条件单历史响应超限。")
        history = [algo_view(row) for row in recent]
        if any(row["symbol"] != symbol for row in history):
            raise QueryError("条件单历史标的身份不匹配。")
        known = {row["client_algo_id"] for row in orders}
        orders.extend(row for row in history if row["client_algo_id"] not in known)
    except QueryError:
        pass  # Open orders are authoritative; never replace them with stale history.
    return sorted(orders, key=lambda row: row["timestamp"], reverse=True)[:250]


def query_algo(account, symbol, client_algo_id, *, http_url=None):
    client = SignedFutures(account, http_url=http_url)
    row = client.request("GET", "/fapi/v1/algoOrder", {"clientAlgoId": client_algo_id})
    order = algo_view(row)
    if order["symbol"] != symbol or order["client_algo_id"] != client_algo_id:
        raise QueryError("条件单查询身份不匹配。")
    return order


def current_leverage(client, symbol):
    configs = client.request("GET", "/fapi/v1/symbolConfig", {"symbol": symbol})
    if (not isinstance(configs, list) or len(configs) != 1
            or configs[0].get("symbol") != symbol or type(configs[0].get("leverage")) is not int
            or configs[0]["leverage"] < 1):
        raise QueryError("无法核对账户当前杠杆。")
    return configs[0]["leverage"]


def command_sync(account, action, values, client_id, *, http_url=None, snapshot=None):
    """Only explicit user-confirmed actions; all signed operations use no retry."""
    symbol = _symbol(values["symbol"])
    client = SignedFutures(account, http_url=http_url)
    if action == "algo_submit":
        if snapshot is None:
            # Native instrument/account reconciliation stays the source of truth.
            snapshot = asyncio.run(native_execute(account, values["symbol"], {"action": "snapshot"},
                                                  http_url=http_url))
        if "validation_error" in snapshot:
            raise ManualValidationError(snapshot["validation_error"])
        mode = snapshot["position_mode"]
        if values["expected_position_mode"] != mode:
            raise ManualValidationError("持仓模式已改变，请刷新后重新确认。")
        try:
            qty = exact(values["quantity"], snapshot["instrument"]["size_increment"],
                        snapshot["instrument"]["min_quantity"])
        except (ValueError, ArithmeticError):
            raise ManualValidationError("条件单数量或最小数量不符合交易所规则。") from None
        side = values["side"]
        closing_leg = "BOTH" if mode == "oneway" else ("LONG" if side == "sell" else "SHORT")
        available = Decimal(0)
        for pos in snapshot.get("positions", []):
            if pos.get("position_side") == closing_leg and (
                (side == "sell" and pos.get("side") == "long")
                or (side == "buy" and pos.get("side") == "short")
            ):
                available += Decimal(str(pos["quantity"]))
        if qty > available:
            raise ManualValidationError("仅允许风险减仓的条件单：仓位不足或方向不符。")
        step = snapshot["instrument"]["price_increment"]
        for field in ("trigger_price", "price", "activation_price"):
            if values.get(field) is not None:
                try:
                    exact(values[field], step)
                except (ValueError, ArithmeticError):
                    raise ManualValidationError("条件单价格不符合交易所精度及范围限制。") from None
        order_type = values["order_type"]
        payload = {
            "algoType": "CONDITIONAL", "symbol": symbol, "side": side.upper(),
            "type": order_type, "positionSide": closing_leg,
            "quantity": values["quantity"], "workingType": values.get("working_type", "MARK_PRICE"),
            "clientAlgoId": client_id,
        }
        if mode == "oneway":
            payload["reduceOnly"] = "true"
        if order_type == "TRAILING_STOP_MARKET":
            payload["callbackRate"] = values["callback_rate"]
            if values.get("activation_price"):
                payload["activatePrice"] = values["activation_price"]
        else:
            payload["triggerPrice"] = values["trigger_price"]
            if values.get("price"):
                payload.update(price=values["price"], timeInForce="GTC")
        if values.get("price_protect"):
            payload["priceProtect"] = "true"
        row = client.request("POST", "/fapi/v1/algoOrder", payload)
        order = algo_view(row)
        if order["client_algo_id"] != client_id or order["symbol"] != values["symbol"]:
            raise QueryError("交易所返回了不匹配的条件单，需人工核实。")
        return {"algo_order": order, "message": "条件单已由交易所受理；请继续核对状态。"}
    if action == "algo_cancel":
        existing = query_algo(account, values["symbol"], values["client_algo_id"], http_url=http_url)
        if not existing["open"]:
            raise ManualValidationError("条件单已非挂单状态，请刷新。")
        row = client.request("DELETE", "/fapi/v1/algoOrder", {"clientAlgoId": values["client_algo_id"]})
        if not isinstance(row, dict) or row.get("clientAlgoId") != values["client_algo_id"]:
            raise QueryError("撤销回执无法确认，请查询交易所条件单状态。")
        return {"algo_order": {**existing, "status": "canceled", "open": False},
                "message": "条件单撤销已被交易所受理。"}
    if action == "leverage":
        requested = values["leverage"]
        brackets = client.request("GET", "/fapi/v1/leverageBracket", {"symbol": symbol})
        if (not isinstance(brackets, list) or len(brackets) != 1
                or brackets[0].get("symbol") != symbol or not brackets[0].get("brackets")):
            raise QueryError("无法核对交易所杠杆档位。")
        maximum = brackets[0]["brackets"][0].get("initialLeverage")
        if type(maximum) is not int or requested > maximum:
            raise ManualValidationError("杠杆超过交易所对当前合约/账户允许的档位。")
        current_leverage(client, symbol)
        result = client.request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": requested})
        if not isinstance(result, dict) or result.get("symbol") != symbol or result.get("leverage") != requested:
            raise QueryError("杠杆修改回执不一致，请核对交易所设置。")
        checked = current_leverage(client, symbol)
        if checked != requested:
            raise QueryError("杠杆修改后的账户读回结果不一致，请核对。")
        return {"leverage": checked, "message": "杠杆调整已由交易所确认。"}
    raise ManualValidationError("不支持的手动交易操作。")


async def execute(account, action, values, client_id, **kwargs):
    def run():
        try:
            return command_sync(account, action, values, client_id, **kwargs)
        except ManualValidationError as exc:
            return {"validation_error": str(exc)}
    return await asyncio.to_thread(run)
