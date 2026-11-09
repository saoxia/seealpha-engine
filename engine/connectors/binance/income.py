"""USD-M wallet income evidence via fixed, read-only signed GET endpoints."""

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

from .queries import QueryError

DAY_MS = 86_400_000
PAGE_SIZE = 1000
REQUEST_INTERVAL = 1.0  # Each income read costs 30 IP weight; share pacing across accounts.
_request_lock = threading.Lock()
_last_request = 0.0
BASE_URLS = {
    "production": "https://fapi.binance.com/fapi/v1/",
    "sandbox": "https://testnet.binancefuture.com/fapi/v1/",
}


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _json(opener, request):
    with opener.open(request, timeout=15) as response:
        raw = response.read(4 * 1024 * 1024 + 1)
    if len(raw) > 4 * 1024 * 1024:
        raise ValueError("Oversize income response")
    return json.loads(raw)


def _income_page(account, opener, start_ms, end_ms):
    global _last_request
    base = BASE_URLS[account["environment"]]
    credentials = account["credentials"]
    with _request_lock:
        for attempt in range(3):
            time.sleep(max(0, _last_request + REQUEST_INTERVAL - time.monotonic()))
            _last_request = time.monotonic()
            try:
                server = _json(opener, Request(base + "time", method="GET"))
                if type(server.get("serverTime")) is not int or server["serverTime"] <= 0:
                    raise ValueError("Invalid server time")
                params = urlencode({"startTime": start_ms, "endTime": end_ms,
                                    "limit": PAGE_SIZE, "timestamp": server["serverTime"],
                                    "recvWindow": 5000})
                signature = hmac.new(credentials["secret"].encode(), params.encode(),
                                     hashlib.sha256).hexdigest()
                request = Request(base + "income?" + params + "&signature=" + signature,
                                  headers={"X-MBX-APIKEY": credentials["apiKey"]}, method="GET")
                result = _json(opener, request)
                if not isinstance(result, list) or len(result) > PAGE_SIZE:
                    raise ValueError("Invalid income page")
                return result
            except HTTPError as exc:
                if exc.code not in {429, 500, 502, 503, 504}:
                    break
                try:
                    delay = float(exc.headers.get("Retry-After", "1"))
                    if not 0 <= delay <= 30:
                        break
                    delay = max(delay, 2**attempt)
                except (ValueError, TypeError):
                    break
            except (OSError, TimeoutError):
                delay = 2**attempt
            except (ValueError, TypeError, KeyError):
                break
            if attempt < 2:
                time.sleep(delay)
    raise QueryError("Binance 资金流水读取失败，请检查合约读取权限、IP 限制、凭据、校时和网络。") from None


def income_observation(row, account, start_ms, end_ms):
    """Preserve currency, signed amount, venue type and exact transaction identity."""
    try:
        if not isinstance(row, dict):
            raise TypeError
        stamp, transaction = row["time"], row["tranId"]
        if (type(stamp) is not int or not start_ms <= stamp <= end_ms
                or type(transaction) not in (int, str) or not str(transaction).isascii()
                or not str(transaction).isdigit() or len(str(transaction)) > 80):
            raise ValueError
        for field, maximum in (("incomeType", 80), ("asset", 30), ("income", 100)):
            if not isinstance(row[field], str) or not 0 < len(row[field]) <= maximum:
                raise ValueError
        if not row["asset"].isascii() or not row["asset"].isalnum():
            raise ValueError
        if not Decimal(row["income"]).is_finite():
            raise ValueError
        symbol, info, trade_id = row.get("symbol", ""), row.get("info", ""), row.get("tradeId", "")
        if (not isinstance(symbol, str) or len(symbol) > 100 or not isinstance(info, str)
                or len(info) > 2000 or not isinstance(trade_id, str) or len(trade_id) > 200):
            raise ValueError
        return {
            "event_id": f"binance-income:{row['incomeType']}:{transaction}",
            **{key: account[key] for key in ("account_name", "exchange", "environment")},
            "account_id": account["id"], "symbol": symbol, "event_type": "cashflow",
            "timestamp": stamp, "evidence": "venue", "amount": row["income"],
            "asset": row["asset"], "income_type": row["incomeType"],
            "transaction_id": str(transaction), "trade_id": trade_id or None,
            "info": info, "status": "confirmed",
        }
    except (KeyError, TypeError, ValueError, InvalidOperation):
        raise QueryError("Binance 资金流水字段缺失、数值无效或超出请求区间，未确认覆盖。") from None


async def income_history(account, start_ms, end_ms):
    """Yield fully read inclusive windows; split full pages without skipping equal timestamps.

    No undocumented page/fromId parameter or assumption about response ordering is used.
    Saturation at a single millisecond is an explicit gap, never an empty success.
    """
    if (type(start_ms) is not int or type(end_ms) is not int or not 0 < start_ms <= end_ms
            or end_ms - start_ms > 87 * DAY_MS or end_ms > int(time.time() * 1000) + 60_000):
        raise ValueError("资金流水查询区间无效或超过 87 天")
    if account["environment"] not in BASE_URLS or account["exchange"] != "binance":
        raise ValueError("不支持的资金流水账户或环境")
    credentials = account["credentials"]
    if (not all(isinstance(credentials.get(key), str) and credentials[key]
                for key in ("apiKey", "secret"))
            or credentials["apiKey"] == credentials["secret"]
            or any(char in credentials["apiKey"] for char in "\r\n")):
        raise QueryError("资金流水账户凭据无效。")
    opener = build_opener(_NoRedirect())
    cursor = start_ms
    while cursor <= end_ms:
        last = min(end_ms, cursor + 7 * DAY_MS - 1)
        pending = [(cursor, last)]
        while pending:
            first, stop = pending.pop()
            rows = await asyncio.to_thread(_income_page, account, opener, first, stop)
            records = [income_observation(row, account, first, stop) for row in rows]
            identities = [(row["income_type"], row["transaction_id"]) for row in records]
            if len(set(identities)) != len(identities):
                raise QueryError("Binance 资金流水页内出现重复身份，未确认覆盖。")
            if len(rows) == PAGE_SIZE:
                if first == stop:
                    raise QueryError("同一毫秒资金流水达到接口上限，无法确认完整覆盖。")
                middle = (first + stop) // 2
                pending.extend([(middle + 1, stop), (first, middle)])
                continue
            yield {"start_ms": first, "end_ms": stop, "records": records}
        cursor = last + 1
