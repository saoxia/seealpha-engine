"""Capture Nautilus execution evidence and project events.

The observer submits no orders. A read node snapshots public cached events after
reconciliation. An execution runtime must explicitly feed its callbacks into the
queue and persist from its worker (never block native callbacks).
"""

import hashlib
import json
import queue
from decimal import Decimal

from nautilus_trader.model import AccountId
from nautilus_trader.trading import Strategy

from core.option_records import normalize_option_record

__all__ = ["normalize_option_record"]


def text(value):
    return str(value) if value is not None else None


def enum_text(value):
    return str(value).split(".")[-1].lower() if value is not None else ""


def instrument_symbol(value):
    # Match Nautilus instrument identity exactly in both event and report ingestion.
    return str(value).removesuffix(".BINANCE")


def normalize_report(report, account):
    kind = type(report).__name__
    if kind not in ("FillReport", "OrderStatusReport"):
        return None
    fill = kind == "FillReport"
    status = enum_text(getattr(report, "order_status", None))
    event_type = (
        "fill"
        if fill
        else (
            "cancel"
            if status == "canceled"
            else "expire"
            if status == "expired"
            else "reject"
            if status == "rejected"
            else "place"
        )
    )
    commission = getattr(report, "commission", None)
    amount = text(getattr(report, "last_qty" if fill else "quantity", None))
    price = text(getattr(report, "last_px" if fill else "price", None))
    record = {
        "account_id": account["id"],
        "account_name": account["account_name"],
        "exchange": account["exchange"],
        "environment": account["environment"],
        "symbol": instrument_symbol(report.instrument_id),
        "event_type": event_type,
        "timestamp": int(getattr(report, "ts_event" if fill else "ts_last")) // 1_000_000,
        "evidence": "venue",
        "order_id": text(report.venue_order_id),
        "client_order_id": text(getattr(report, "client_order_id", None)),
        "trade_id": text(getattr(report, "trade_id", None)),
        "strategy_id": None,
        "side": enum_text(report.order_side),
        "order_type": enum_text(getattr(report, "order_type", None)),
        "amount": amount,
        "price": price,
        "cost": str(Decimal(amount) * Decimal(price))
        if amount is not None and price is not None
        else None,
        "filled": amount if fill else text(getattr(report, "filled_qty", None)),
        "fee_cost": str(commission.as_decimal()) if commission is not None else None,
        "fee_currency": str(commission.currency) if commission is not None else "",
        "status": "filled" if fill else status,
    }
    # Identical retries deduplicate; changed venue evidence is retained as a new observation.
    record["event_id"] = (
        "venue:" + hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()
    )
    return record


def normalize_event(event, order, account):
    names = {
        "OrderSubmitted": "place",
        "OrderAccepted": "place",
        "OrderUpdated": "amend",
        "OrderCanceled": "cancel",
        "OrderFilled": "fill",
        "OrderRejected": "reject",
        "OrderDenied": "reject",
        "OrderExpired": "expire",
    }
    event_type = names.get(type(event).__name__)
    if event_type is None or order is None:
        return None
    strategy = text(order.strategy_id)
    tags = [str(t).upper() for t in (getattr(order, "tags", None) or [])]
    synthetic = "RECONCILIATION" in tags
    if event_type == "fill" and not str(getattr(event, "trade_id", "")).isascii():
        synthetic = True
    if event_type == "fill" and not str(getattr(event, "trade_id", "")).isdigit():
        synthetic = True
    # A strategy can claim venue orders; ownership alone is not proof of origination.
    project_origin = strategy != "EXTERNAL" and (
        type(event).__name__ in {"OrderSubmitted", "OrderDenied"}
        or any(
            type(item).__name__ == "OrderSubmitted" and not getattr(item, "reconciliation", False)
            for item in order.events()
        )
    )
    if synthetic:
        event_type = "correction"
    record = {
        "event_id": str(event.event_id),
        "account_id": account["id"],
        "account_name": account["account_name"],
        "exchange": account["exchange"],
        "environment": account["environment"],
        "symbol": instrument_symbol(order.instrument_id),
        "event_type": event_type,
        "timestamp": int(event.ts_event) // 1_000_000,
        "evidence": "reconciliation" if synthetic else "project" if project_origin else "venue",
        "order_id": text(getattr(event, "venue_order_id", None) or order.venue_order_id),
        "client_order_id": str(order.client_order_id),
        "trade_id": text(getattr(event, "trade_id", None)) if not synthetic else None,
        "strategy_id": strategy if project_origin and not synthetic else None,
        "side": enum_text(order.side),
        "order_type": enum_text(order.order_type),
        "amount": text(
            getattr(event, "last_qty", None) or getattr(event, "quantity", None) or order.quantity
        ),
        "price": text(
            getattr(event, "last_px", None)
            or getattr(event, "price", None)
            or getattr(order, "price", None)
        ),
        "status": {
            "OrderSubmitted": "submitted",
            "OrderAccepted": "accepted",
            "OrderUpdated": "updated",
            "OrderCanceled": "canceled",
            "OrderFilled": "filled",
            "OrderRejected": "rejected",
            "OrderDenied": "denied",
            "OrderExpired": "expired",
        }[type(event).__name__],
    }
    commission = getattr(event, "commission", None)
    if commission is not None:
        record.update(fee_cost=str(commission.as_decimal()), fee_currency=str(commission.currency))
    return record


class LedgerObserver(Strategy):
    """Explicit queue bridge usable by future strategy nodes as well as read-only sync."""

    def __init__(self, account):
        super().__init__()
        self.account = dict(account)
        self.observations = queue.SimpleQueue()
        self.capture_errors = 0

    def capture_report(self, report):
        try:
            if str(report.account_id) != f"BINANCE-{self.account['id']}":
                return
            record = normalize_report(report, self.account)
            if record:
                self.observations.put(record)
        except Exception:  # noqa: BLE001 - No exception or raw signed payload escapes native callbacks.
            self.capture_errors += 1

    def capture_event(self, event):
        try:
            client_id = getattr(event, "client_order_id", None)
            order = self.cache.order(client_id) if client_id is not None else None
            if order is None or (
                getattr(order, "account_id", None) is not None
                and str(order.account_id) != f"BINANCE-{self.account['id']}"
            ):
                return
            record = normalize_event(event, order, self.account)
            if record:
                self.observations.put(record)
        except Exception:  # noqa: BLE001
            self.capture_errors += 1

    def capture_cached(self):
        for order in self.cache.orders(account_id=AccountId(f"BINANCE-{self.account['id']}")):
            for record in cached_order_observations(order, self.account):
                self.observations.put(record)

    def drain(self):
        result = []
        while not self.observations.empty():
            result.append(self.observations.get_nowait())
        if self.capture_errors:
            raise ValueError("原生操作事件解析不完整，未标记同步完成。")
        return result


async def operation_history(account, *, lookback_minutes=1440, **options):
    from .queries import account_snapshot

    records = []
    await account_snapshot(
        account,
        operation_records=records,
        reconciliation_lookback_minutes=lookback_minutes,
        **options,
    )
    return records


def cached_order_observations(order, account):
    """Python 2.0rc7 exposes native cache events, but not typed raw-report subscriptions.

    Only numeric Binance trade IDs are treated as real fills. Generated fills in
    this disposable read node are excluded; they do not describe project activity.
    This is a bounded cache projection, not a complete raw-report audit feed.
    """
    synthetic = "RECONCILIATION" in [str(t).upper() for t in (getattr(order, "tags", None) or [])]
    if synthetic:
        return []
    base = {
        "account_id": account["id"],
        "account_name": account["account_name"],
        "exchange": account["exchange"],
        "environment": account["environment"],
        "symbol": instrument_symbol(order.instrument_id),
        "order_id": text(order.venue_order_id),
        "client_order_id": str(order.client_order_id),
        "side": enum_text(order.side),
        "order_type": enum_text(order.order_type),
        "strategy_id": None,
        "timestamp": int(order.ts_last) // 1_000_000,
    }
    result = []
    status = enum_text(order.status)
    if order.venue_order_id is not None and not synthetic:
        state = {
            **base,
            "event_type": (
                "cancel"
                if status == "canceled"
                else "expire"
                if status == "expired"
                else "reject"
                if status == "rejected"
                else "place"
            ),
            "evidence": "venue",
            "amount": str(order.quantity),
            "price": text(getattr(order, "price", None)),
            "filled": str(order.filled_qty),
            "status": status,
        }
        state["event_id"] = (
            "cache-status:" + hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()
        )
        result.append(state)
    for event in order.events():
        if type(event).__name__ != "OrderFilled":
            continue
        real = not synthetic and str(event.trade_id).isdigit()
        if not real:
            continue
        record = {
            **base,
            "event_id": "cache-fill:" + str(event.event_id),
            "event_type": "fill" if real else "correction",
            "evidence": "venue" if real else "reconciliation",
            "timestamp": int(event.ts_event) // 1_000_000,
            "trade_id": str(event.trade_id) if real else None,
            "amount": str(event.last_qty),
            "price": str(event.last_px),
            "cost": str(Decimal(str(event.last_qty)) * Decimal(str(event.last_px))),
            "fee_cost": str(event.commission.as_decimal()),
            "fee_currency": str(event.commission.currency),
            "status": "filled" if real else "reconciled",
        }
        # Native event UUIDs change when a read node is rebuilt. Venue identity is stable.
        record["event_id"] = (
            "cache-fill:"
            + hashlib.sha256(
                json.dumps(
                    {k: v for k, v in record.items() if k != "event_id"}, sort_keys=True
                ).encode()
            ).hexdigest()
        )
        result.append(record)
    return result
