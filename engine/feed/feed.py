"""Fail-closed data gate and ordered, replay-aware delivery to consumers."""

from .models import MINUTE, DataError, Delivery, State


class Feed:
    def __init__(
        self,
        store,
        *,
        warmup_minutes=135 * 1440,
        stale_ms=15_000,
        close_grace_ms=2000,
        on_delivery=None,
    ):
        if warmup_minutes < 1 or stale_ms < 1000 or not 0 <= close_grace_ms < MINUTE:
            raise ValueError("Invalid feed limits")
        self.store = store
        self.warmup_minutes = warmup_minutes
        self.stale_ms = stale_ms
        self.close_grace_ms = close_grace_ms
        self.on_delivery = on_delivery
        self.state = State.WARMING_UP
        self.reason = "startup_replay_required"
        self.connected = False
        self.clock_ok = False
        self.last_seen = {}
        self.recovery_required = True
        self.consumer_failed = False
        self.delivered = {}
        self.live_candidates = set()
        self.epoch = 0

    def degrade(self, reason):
        self.epoch += 1
        self.state, self.reason = State.DEGRADED, reason
        self.recovery_required = True
        self.live_candidates.clear()

    def transport_lost(self, reason="websocket_disconnected"):
        self.connected = False
        self.last_seen.clear()
        self.degrade(reason)

    def observe(self, symbol, event_ms, now_ms, bar=None):
        if (
            symbol not in self.store.symbols
            or not now_ms - self.stale_ms <= event_ms <= now_ms + 2000
        ):
            self.degrade("stale_or_future_websocket_event")
            raise DataError(self.reason)
        self.last_seen[symbol] = now_ms
        if bar is None:
            return
        previous = self.store.next_ms(symbol)
        was_ready = self.state == State.READY and not self.recovery_required
        if bar.start_ms > previous:
            self.degrade("minute_gap")
            self.store.incident(
                "gap", {"symbol": symbol, "from_ms": previous, "to_ms": bar.start_ms}, now_ms
            )
        try:
            self.store.put([bar], "websocket", now_ms)
        except DataError:
            self.degrade("invalid_or_conflicting_bar")
            raise
        if was_ready and not self.recovery_required and bar.start_ms == previous:
            self.live_candidates.add((symbol, bar.start_ms))

    def health_reason(self, now_ms):
        if self.consumer_failed:
            return "consumer_failed_restart_required"
        if self.store.conflicted:
            return "conflicting_data_manual_review_required"
        if not self.clock_ok:
            return "clock_unverified"
        if not self.connected:
            return "websocket_disconnected"
        if any(
            not 0 <= now_ms - self.last_seen.get(s, -self.stale_ms - 1) <= self.stale_ms
            for s in self.store.symbols
        ):
            return "websocket_stale"
        target = (now_ms - self.close_grace_ms) // MINUTE * MINUTE
        for symbol in self.store.symbols:
            if self.store.next_ms(symbol) < target:
                return "missing_closed_minutes"
            if self.store.next_ms(symbol) - self.store.start_ms < self.warmup_minutes * MINUTE:
                return "insufficient_warmup"
        return None

    def check(self, now_ms):
        reason = self.health_reason(now_ms)
        if reason:
            if self.state == State.READY:
                self.degrade(reason)
            else:
                self.reason = reason
            return False
        return self.state == State.READY and not self.recovery_required

    def recovered(self, now_ms, epoch):
        """Only the repair coordinator may reopen the gate after overlap verification."""
        if epoch != self.epoch or self.health_reason(now_ms):
            return False
        self.live_candidates.clear()  # No recovery/startup bar is ever a live decision.
        self._deliver(now_ms, live=False)
        self.recovery_required = False
        self.state, self.reason = State.READY, "continuous_closed_minutes"
        return True

    def flush(self, now_ms):
        if self.check(now_ms):
            self._deliver(now_ms, live=True)

    def _deliver(self, now_ms, *, live):
        for symbol in self.store.symbols:
            end = min(self.store.next_ms(symbol), now_ms // MINUTE * MINUTE)
            start = self.delivered.get(
                symbol, max(self.store.start_ms, end - self.warmup_minutes * MINUTE)
            )
            for bar in self.store.read(symbol, start, end):
                eligible = (
                    live
                    and (symbol, bar.start_ms) in self.live_candidates
                    and 0 <= now_ms - bar.end_ms <= self.stale_ms
                )
                if self.on_delivery:
                    try:
                        self.on_delivery(Delivery(bar, eligible, "live" if eligible else "replay"))
                    except Exception:
                        self.consumer_failed = True
                        self.degrade("consumer_failed_restart_required")
                        raise
                self.delivered[symbol] = bar.end_ms
                self.live_candidates.discard((symbol, bar.start_ms))

    def status(self, now_ms):
        ready = self.check(now_ms)
        return {
            "schema": 1,
            "state": self.state,
            "reason": self.reason,
            "data_ready": ready,
            "checked_ms": now_ms,
            "valid_until_ms": now_ms + min(self.stale_ms, 3000),
            "warmup_minutes": self.warmup_minutes,
            "epoch": self.epoch,
            "symbols": {
                s: {"next_ms": self.store.next_ms(s), "last_websocket_ms": self.last_seen.get(s)}
                for s in self.store.symbols
            },
        }
