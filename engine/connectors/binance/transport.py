"""Public Binance transport facades over the native data-only adapter."""

import asyncio
import json
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from core.market.models import DataError

REST_URL = "https://fapi.binance.com/fapi/v1/"


class PublicREST:
    base_url = REST_URL
    allowed_endpoints = frozenset({"time"})

    def __init__(
        self, *, symbols=("BTCUSDT", "ETHUSDT"), timeout=12, attempts=4, minimum_interval=0.3
    ):
        from .native import NativeMarketData

        self.source = NativeMarketData(symbols, timeout=timeout)
        self.timeout, self.attempts = timeout, attempts
        self.minimum_interval = minimum_interval
        self.lock = asyncio.Lock()
        self.last_request = 0.0

    def _request(self, endpoint, params):
        request = Request(
            self.base_url + endpoint + "?" + urlencode(params),
            headers={"User-Agent": "Nautilus-unified-minute-feed/1"},
        )
        with urlopen(request, timeout=self.timeout) as response:
            if not response.url.startswith(self.base_url):
                raise DataError("Unexpected REST redirect")
            raw = response.read(4 * 1024 * 1024 + 1)
        if len(raw) > 4 * 1024 * 1024:
            raise DataError("Oversize REST response")
        result = json.loads(raw)
        if isinstance(result, dict) and "code" in result:
            raise DataError("Binance public API error: " + str(result))
        return result

    async def get(self, endpoint, **params):
        if endpoint not in self.allowed_endpoints:
            raise ValueError("Unsupported public REST endpoint")
        async with self.lock:
            for attempt in range(self.attempts):
                await asyncio.sleep(
                    max(0, self.last_request + self.minimum_interval - time.monotonic())
                )
                self.last_request = time.monotonic()
                try:
                    return await asyncio.to_thread(self._request, endpoint, params)
                except HTTPError as exc:
                    if exc.code not in {418, 429, 500, 502, 503, 504}:
                        raise
                    delay = max(min(2**attempt, 30), float(exc.headers.get("Retry-After", "1")))
                except (OSError, TimeoutError):
                    delay = min(2**attempt, 30)
                if attempt + 1 == self.attempts:
                    raise DataError("Public REST retries exhausted")
                await asyncio.sleep(delay)

    async def bars(self, symbol, start_ms, end_ms):
        return await self.source.bars(symbol, start_ms, end_ms)


class KlineSocket:
    """Subscription facade over a shared native data-only Binance node."""

    def __init__(self, symbols, on_message, on_reconnect, *, source=None):
        from .native import NativeMarketData

        self.source = source or NativeMarketData(symbols)
        self.source.on_message, self.source.on_reconnect = on_message, on_reconnect

    async def connect(self):
        await self.source.start()

    @property
    def active(self):
        return self.source.active

    async def close(self):
        await self.source.close()
