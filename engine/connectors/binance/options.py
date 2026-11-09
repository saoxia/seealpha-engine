"""Binance options public metadata and bounded market-data reads.

The installed native Binance adapter does not load options. These fixed public
endpoints supplement it without adding any account or execution capabilities.
"""

import json
import threading
import time
from decimal import Decimal, InvalidOperation
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from nautilus_trader.model import (
    CryptoOption,
    Currency,
    InstrumentId,
    OptionKind,
    Price,
    Quantity,
    Symbol,
    Venue,
)

from core.market.instruments import normalize_symbol

from .queries import QueryError

REST_URL = "https://eapi.binance.com/eapi/v1/"
_gate = threading.Lock()
_last_request = 0.0


def get(endpoint, **params):
    if endpoint not in {"exchangeInfo", "trades", "klines", "time"}:
        raise ValueError("仅支持期权公开行情端点")
    global _last_request
    with _gate:
        for attempt in range(3):
            time.sleep(max(0, _last_request + 0.3 - time.monotonic()))
            _last_request = time.monotonic()
            try:
                request = Request(REST_URL + endpoint + ("?" + urlencode(params) if params else ""),
                                  headers={"User-Agent": "SeeAlpha-options-data/1"})
                with urlopen(request, timeout=20) as response:
                    if not response.url.startswith(REST_URL):
                        raise ValueError("期权行情发生非预期重定向")
                    raw = response.read(16 * 1024 * 1024 + 1)
                if len(raw) > 16 * 1024 * 1024:
                    raise ValueError("期权行情响应过大")
                value = json.loads(raw)
                if isinstance(value, dict) and "code" in value:
                    raise ValueError("期权行情返回错误")
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
            except (ValueError, TypeError):
                break
            if attempt < 2:
                time.sleep(delay)
    raise QueryError("Binance 期权公开行情读取失败，请检查网络或稍后重试。")


def decimal_text(value, *, allow_zero=False):
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        raise ValueError("期权合约数值无效") from None
    if not number.is_finite() or number < 0 or not allow_zero and number == 0:
        raise ValueError("期权合约数值须为正数")
    return format(number, "f")


def markets():
    value = get("exchangeInfo")
    try:
        if not isinstance(value, dict) or type(value["serverTime"]) is not int:
            raise ValueError
        underlyings = {item["underlying"]: item for item in value["optionContracts"]}
        result, seen = [], set()
        for item in value["optionSymbols"]:
            if (item.get("status", "TRADING") != "TRADING" or item["expiryDate"] <= value["serverTime"]):
                continue
            symbol = normalize_symbol(item["symbol"], "options")
            underlying = underlyings[item["underlying"]]
            quote, settlement = item["quoteAsset"], underlying["settleAsset"]
            if quote not in {"USDT", "USDC"} or settlement != quote:
                continue
            filters = {entry["filterType"]: entry for entry in item["filters"]}
            parts = symbol.split("-")
            side = {"C": "CALL", "P": "PUT"}[parts[-1]]
            if (symbol in seen or item["side"] != side or Decimal(parts[2]) != Decimal(item["strikePrice"])
                    or type(item["expiryDate"]) is not int
                    or time.strftime("%y%m%d", time.gmtime(item["expiryDate"] / 1000)) != parts[1]):
                raise ValueError
            seen.add(symbol)
            pp, sp = item["priceScale"], item["quantityScale"]
            if type(pp) is not int or type(sp) is not int or not 0 <= pp <= 16 or not 0 <= sp <= 16:
                raise ValueError
            result.append({"symbol": symbol, "base": underlying["baseAsset"], "quote": quote,
                           "settlement": settlement, "underlying": item["underlying"],
                           "expiry": item["expiryDate"], "strike": decimal_text(item["strikePrice"]),
                           "right": side, "multiplier": decimal_text(item["unit"]),
                           "price_precision": pp, "size_precision": sp,
                           "price_increment": decimal_text(filters["PRICE_FILTER"]["tickSize"]),
                           "min_price": decimal_text(filters["PRICE_FILTER"]["minPrice"], allow_zero=True)
                           if "minPrice" in filters["PRICE_FILTER"] else None,
                           "max_price": decimal_text(filters["PRICE_FILTER"]["maxPrice"])
                           if "maxPrice" in filters["PRICE_FILTER"] else None,
                           "size_increment": decimal_text(filters["LOT_SIZE"]["stepSize"]),
                           "min_quantity": decimal_text(item["minQty"]),
                           "max_quantity": decimal_text(item["maxQty"]),
                           "margin_init": decimal_text(item["initialMargin"], allow_zero=True),
                           "margin_maint": decimal_text(item["maintenanceMargin"], allow_zero=True),
                           "underlying_type": item.get("underlyingType", "CRYPTO"),
                           "contract_type": item.get("contractType", "CRYPTO_OPTIONS"),
                           "fetched_ms": value["serverTime"]})
        return sorted(result, key=lambda row: (row["underlying"], row["expiry"], Decimal(row["strike"]), row["right"]))
    except (KeyError, TypeError, ValueError, InvalidOperation):
        raise QueryError("Binance 期权合约定义不完整或不一致，拒绝发布。") from None


def instrument(item):
    symbol = normalize_symbol(item["symbol"], "options")
    return CryptoOption(
        instrument_id=InstrumentId(Symbol(symbol), Venue("BINANCE")), raw_symbol=Symbol(symbol),
        underlying=Currency.from_str(item["base"]), quote_currency=Currency.from_str(item["quote"]),
        settlement_currency=Currency.from_str(item["settlement"]), is_inverse=False,
        option_kind=OptionKind.CALL if item["right"] == "CALL" else OptionKind.PUT,
        strike_price=Price.from_str(item["strike"]), activation_ns=0,
        expiration_ns=item["expiry"] * 1_000_000, price_precision=item["price_precision"],
        size_precision=item["size_precision"], price_increment=Price.from_str(item["price_increment"]),
        size_increment=Quantity.from_str(item["size_increment"]),
        min_price=Price.from_str(item["min_price"]) if item.get("min_price") is not None else None,
        max_price=Price.from_str(item["max_price"]) if item.get("max_price") is not None else None,
        multiplier=Quantity.from_str(item["multiplier"]),
        min_quantity=Quantity.from_str(item["min_quantity"]), max_quantity=Quantity.from_str(item["max_quantity"]),
        margin_init=Decimal(item["margin_init"]), margin_maint=Decimal(item["margin_maint"]),
        ts_event=item["fetched_ms"] * 1_000_000, ts_init=item["fetched_ms"] * 1_000_000,
        info={"binance_options": item, "activation_time_known": False},
    )
