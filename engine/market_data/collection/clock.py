"""Venue-independent clock estimate and freshness checks."""

import time

from core.market.models import DataError


class ServerClock:
    def __init__(self):
        self.base_ms = 0
        self.monotonic_at = 0.0
        self.uncertainty_ms = 0
        self.valid = False

    async def sync(self, rest):
        self.valid = False
        start = time.monotonic()
        result = await rest.get("time")
        end = time.monotonic()
        elapsed = int((end - start) * 1000)
        server = int(result["serverTime"])
        if elapsed > 2000 or server <= 0:
            raise DataError("Server clock roundtrip exceeds 2s")
        if self.base_ms and abs(server + elapsed // 2 - self.now()) > 5000:
            raise DataError("Server clock shifted by more than 5s")
        self.base_ms = server + elapsed // 2
        self.uncertainty_ms = elapsed // 2 + 1
        self.monotonic_at = end
        self.valid = True

    def now(self):
        if not self.base_ms:
            raise DataError("Clock not synchronized")
        return self.base_ms + int((time.monotonic() - self.monotonic_at) * 1000)

    def confirmed(self):
        return self.now() - self.uncertainty_ms

    @property
    def healthy(self):
        return self.valid and bool(self.base_ms) and time.monotonic() - self.monotonic_at < 120
