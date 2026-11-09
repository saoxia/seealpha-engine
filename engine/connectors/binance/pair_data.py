"""Public evidence unavailable through the pinned native trade/minute facade.

Raw trades must never be substituted with the native USD-M aggTrade stream.
Endpoints and hosts are fixed; no credentials or trading operations are accepted.
"""

import asyncio
import json

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from core.market.instruments import normalize_symbol

from .transport import PublicREST


class PairREST(PublicREST):
    # The pinned native facade cannot deliver raw (non-aggregated) futures ticks,
    # forming candles or calendar request windows. Preserve their venue evidence.
    allowed_endpoints = frozenset({"time", "klines", "trades"})


class PairTradeSocket:
    """Raw USD-M trade evidence unavailable from the pinned aggTrade adapter."""

    async def stream(self, symbol):
        symbol = normalize_symbol(symbol)
        delay = 1
        while True:
            try:
                async with connect(f"wss://fstream.binance.com/ws/{symbol.lower()}@trade",
                                   open_timeout=12, close_timeout=5, ping_interval=20,
                                   ping_timeout=30, max_queue=1024, max_size=65536) as socket:
                    yield {"kind": "connected"}
                    async for message in socket:
                        row = json.loads(message)
                        if row.get("e") != "trade" or row.get("s") != symbol:
                            raise ValueError("Unexpected raw trade stream payload")
                        yield {"kind": "trade", "row": {"id": row["t"], "time": row["T"],
                               "price": row["p"], "qty": row["q"], "isBuyerMaker": row["m"]}}
                        delay = 1
                    raise ConnectionError("Raw trade stream closed")
            except (WebSocketException, OSError, TimeoutError, ConnectionError,
                    ValueError, KeyError, TypeError) as exc:
                yield {"kind": "disconnected", "error": f"{type(exc).__name__}: {exc}"}
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
