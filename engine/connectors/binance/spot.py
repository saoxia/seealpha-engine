"""Read-only Binance Spot public market-data queries.

No credentials or order endpoints. Values retain the venue's string precision;
recent trades are not advertised as complete historical ticks.
"""

from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal, InvalidOperation

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from core.market.instruments import INTERVALS, normalize_symbol
from core.market.models import DataError

from engine.connectors.binance.transport import PublicREST


class SpotREST(PublicREST):
    base_url = "https://data-api.binance.vision/api/v3/"
    allowed_endpoints = frozenset({"exchangeInfo", "klines", "trades", "ticker/24hr", "time"})

    def __init__(self, *, timeout=12, attempts=4, minimum_interval=0.3, environment="production"):
        if environment not in {"production", "sandbox"}:
            raise ValueError("Invalid Spot environment")
        self.base_url = ("https://testnet.binance.vision/api/v3/" if environment == "sandbox"
                         else "https://data-api.binance.vision/api/v3/")
        super().__init__(
            symbols=(), timeout=timeout, attempts=attempts,
            minimum_interval=minimum_interval, use_native=False,
        )


async def markets(*, rest=None, environment="production"):
    client = rest or SpotREST(environment=environment)
    response = await client.get("exchangeInfo")
    if not isinstance(response, dict) or not isinstance(response.get("symbols"), list):
        raise DataError("Invalid Binance Spot exchangeInfo response")
    result = []
    for row in response["symbols"]:
        if not isinstance(row, dict) or row.get("status") != "TRADING":
            continue
        if row.get("isSpotTradingAllowed") is False:
            continue
        permissions = row.get("permissions")
        if isinstance(permissions, list) and permissions and "SPOT" not in permissions:
            continue
        symbol, base, quote = row.get("symbol"), row.get("baseAsset"), row.get("quoteAsset")
        if not all(isinstance(value, str) and value.isalnum() for value in (symbol, base, quote)):
            continue
        if normalize_symbol(symbol, "spot") != (base + quote).upper():
            continue
        result.append({
            "symbol": f"{base.upper()}/{quote.upper()}",
            "market_type": "spot", "base": base.upper(), "quote": quote.upper(),
            "expiry": None,
        })
    return sorted(result, key=lambda item: item["symbol"])


def _positive_number(value, *, zero=False):
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise DataError("Invalid Spot numeric evidence") from exc
    if not amount.is_finite() or amount < 0 or (not zero and amount == 0):
        raise DataError("Invalid Spot numeric evidence")
    return format(amount, "f")


async def candles(symbol, interval, start_ms, end_ms, *, limit=1000, rest=None, environment="production"):
    symbol = normalize_symbol(symbol, "spot")
    if interval not in INTERVALS or not 0 <= start_ms < end_ms or not 1 <= limit <= 1000:
        raise ValueError("Invalid Spot candle request")
    client = rest or SpotREST(environment=environment)
    rows = await client.get(
        "klines", symbol=symbol, interval=interval,
        startTime=start_ms, endTime=end_ms - 1, limit=limit,
    )
    if not isinstance(rows, list) or len(rows) > limit:
        raise DataError("Invalid Binance Spot candle page")
    values = []
    now_ms = int(time.time() * 1000)
    previous = -1
    for row in rows:
        if not isinstance(row, list) or len(row) < 7:
            raise DataError("Invalid Spot candle row")
        try:
            opening, closing = int(row[0]), int(row[6])
        except (TypeError, ValueError, OverflowError) as exc:
            raise DataError("Invalid Spot candle timestamps") from exc
        if not start_ms <= opening < end_ms or closing < opening or opening <= previous:
            raise DataError("Non-monotonic or out-of-range Spot candles")
        o, h, l, c = (_positive_number(row[i]) for i in (1, 2, 3, 4))
        v = _positive_number(row[5], zero=True)
        if Decimal(l) > min(map(Decimal, (o, h, l, c))) or Decimal(h) < max(map(Decimal, (o, h, l, c))):
            raise DataError("Invalid Spot OHLC range")
        values.append({
            "start_ms": opening, "end_ms": closing + 1,
            "open": o, "high": h, "low": l, "close": c, "volume": v,
            "is_closed": closing < now_ms,
        })
        previous = opening
    next_start_ms = values[-1]["end_ms"] if len(values) == limit and values[-1]["end_ms"] < end_ms else None
    return {
        "product": "spot", "symbol": symbol, "interval": interval,
        "candles": values, "next_start_ms": next_start_ms,
        "source": "binance_spot_public_rest",
    }


async def recent_trades(symbol, *, limit=500, rest=None, environment="production"):
    symbol = normalize_symbol(symbol, "spot")
    if not 1 <= limit <= 1000:
        raise ValueError("Invalid Spot trade limit")
    client = rest or SpotREST(environment=environment)
    rows = await client.get("trades", symbol=symbol, limit=limit)
    if not isinstance(rows, list) or len(rows) > limit:
        raise DataError("Invalid Binance Spot recent trades")
    values = []
    previous = -1
    for row in rows:
        if not isinstance(row, dict):
            raise DataError("Invalid Spot trade row")
        try:
            identity, timestamp = int(row["id"]), int(row["time"])
        except (KeyError, ValueError, TypeError, OverflowError) as exc:
            raise DataError("Invalid Spot trade identity") from exc
        if identity <= previous or timestamp < 0 or type(row.get("isBuyerMaker")) is not bool:
            raise DataError("Non-monotonic or invalid Spot trade")
        values.append({
            "id": identity, "time": timestamp,
            "price": _positive_number(row.get("price")),
            "qty": _positive_number(row.get("qty")),
            "isBuyerMaker": row["isBuyerMaker"],
        })
        previous = identity
    return {"product": "spot", "symbol": symbol, "trades": values, "complete_history": False,
            "source": "binance_spot_public_rest"}


async def market_tickers(*, rest=None, environment="production"):
    """Binance Spot public 24h snapshots; funding and open interest do not apply."""
    client = rest or SpotREST(attempts=2, environment=environment)
    rows = await client.get("ticker/24hr")
    if not isinstance(rows, list):
        raise DataError("Invalid Binance Spot ticker response")
    result = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("symbol"), str):
            raise DataError("Invalid Spot ticker identity")
        symbol = normalize_symbol(row["symbol"], "spot")
        if symbol in result:
            raise DataError("Duplicate Spot ticker")
        result[symbol] = {
            "last_price": _positive_number(row.get("lastPrice"), zero=True),
            "change_percent": _signed_number(row.get("priceChangePercent")),
            "quote_volume": _positive_number(row.get("quoteVolume"), zero=True),
            "funding_rate": None,
        }
    return {"checked_at": int(time.time() * 1000), "rows": result}


def _signed_number(value):
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise DataError("Invalid Spot ticker number") from exc
    if not number.is_finite():
        raise DataError("Invalid Spot ticker number")
    return format(number, "f")


class SpotTradeSocket:
    """Venue raw Spot trades, never aggregated trades or synthetic tick IDs."""

    async def stream(self, symbol):
        symbol = normalize_symbol(symbol, "spot")
        delay = 1
        while True:
            try:
                async with connect(
                    f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade",
                    open_timeout=12, close_timeout=5, ping_interval=20,
                    ping_timeout=30, max_queue=1024, max_size=65536,
                ) as socket:
                    yield {"kind": "connected"}
                    async for message in socket:
                        row = json.loads(message)
                        if (not isinstance(row, dict) or row.get("e") != "trade"
                                or row.get("s") != symbol):
                            raise ValueError("Unexpected Binance Spot trade identity")
                        trade = {
                            "id": row["t"], "time": row["T"], "price": row["p"],
                            "qty": row["q"], "isBuyerMaker": row["m"],
                        }
                        yield {"kind": "trade", "row": trade}
                        delay = 1
                    raise ConnectionError("Spot trade WebSocket closed")
            except (WebSocketException, OSError, TimeoutError, ConnectionError,
                    ValueError, KeyError, TypeError) as exc:
                yield {"kind": "disconnected", "error": f"{type(exc).__name__}: {exc}"}
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
