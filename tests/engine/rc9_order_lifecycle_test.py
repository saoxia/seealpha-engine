"""RC9 order lifecycle guards; synthetic events only, no exchange connectivity."""

from decimal import Decimal
from types import MethodType, SimpleNamespace

from nautilus_trader.model import OrderSide, Price, Quantity

from engine.connectors.binance import manual, operations
from engine.portfolio.portfolio_coordinator import PortfolioCoordinator


def test_partial_fill_keeps_working_order_and_inflight_quantity():
    order = SimpleNamespace(
        quantity=Quantity.from_str("10"), filled_qty=Quantity.from_str("4"),
        side=OrderSide.BUY, is_closed=False,
    )
    seen = []
    fake = SimpleNamespace(
        _working_id="net-order", _working_filled=Decimal(0),
        _working_requested=Decimal(10),
        cache=SimpleNamespace(order=lambda _: order),
        _allocate_real_fill=lambda event: seen.append("allocated"),
        reconcile=lambda: seen.append("reconciled"),
        _clear_working=lambda _: seen.append("cleared"),
    )
    event = SimpleNamespace(
        client_order_id="net-order", last_qty=Quantity.from_str("4"),
        last_px=Price.from_str("100.0"),
    )
    PortfolioCoordinator.on_order_filled(fake, event)
    assert seen == ["allocated", "reconciled"]
    assert PortfolioCoordinator._in_flight(fake) == Decimal(6)
    assert fake._working_filled == Decimal(4)
    order.filled_qty = Quantity.from_str("10")
    order.is_closed = True
    event.last_qty = Quantity.from_str("6")
    PortfolioCoordinator.on_order_filled(fake, event)
    assert seen[-2:] == ["allocated", "cleared"]


def test_unregistered_working_order_is_not_treated_as_zero_exposure():
    fake = SimpleNamespace(
        _working_id="net-order", _working_filled=Decimal(0),
        _working_requested=Decimal(-2),
        cache=SimpleNamespace(order=lambda _: None),
    )
    assert PortfolioCoordinator._in_flight(fake) == Decimal(-2)


def test_cancel_denial_preserves_working_id_and_suppresses_same_bar_retries():
    messages = []
    fake = SimpleNamespace(
        _working_id="net-order", _bar=SimpleNamespace(ts_event=12),
        _denied_bar_ts=None, _cancel_sent=True,
        log=SimpleNamespace(error=messages.append),
    )
    fake._on_cancel_failed = MethodType(PortfolioCoordinator._on_cancel_failed, fake)
    event = SimpleNamespace(client_order_id="net-order", reason="LOCAL_NOT_SENT")
    PortfolioCoordinator.on_order_cancel_denied(fake, event)
    assert fake._working_id == "net-order"
    assert fake._denied_bar_ts == 12
    assert not fake._cancel_sent and messages
    fake._cancel_sent = True
    PortfolioCoordinator.on_order_cancel_rejected(fake, event)
    assert fake._working_id == "net-order"
    assert not fake._cancel_sent


def test_manual_command_finishes_on_rc9_local_cancel_denial(monkeypatch):
    monkeypatch.setattr(manual, "normalize_event", lambda *_: None)
    outcomes = []
    current = SimpleNamespace(is_closed=False)
    fake = SimpleNamespace(
        cache=SimpleNamespace(order=lambda _: current),
        account={}, records=[], result="net-order", command={"action": "cancel"},
        finish=lambda: outcomes.append("finished"), outcome=None,
    )
    event = type("OrderCancelDenied", (), {"client_order_id": "net-order"})()
    manual.ManualCommand.capture_event(fake, event)
    assert fake.outcome == "OrderCancelDenied"
    assert outcomes == ["finished"]


def test_ledger_retains_rc9_denial_reason_and_does_not_claim_cancel_success():
    account = {"id": 1, "account_name": "test", "exchange": "binance",
               "environment": "sandbox"}
    order = SimpleNamespace(
        strategy_id="STRATEGY", tags=["MANUAL"], instrument_id="ETHUSDT-PERP.BINANCE",
        client_order_id="net-order", venue_order_id=None,
        side=OrderSide.BUY, order_type="LIMIT",
        quantity=Quantity.from_str("1"), price=Price.from_str("100.00"),
        events=list,
    )
    event = type("OrderCancelDenied", (), {
        "client_order_id": "net-order", "event_id": "event-1",
        "ts_event": 1_000_000_000, "reason": "NOT_SENT",
        "venue_order_id": None,
    })()
    record = operations.normalize_event(event, order, account)
    assert record["event_type"] == "reject"
    assert record["status"] == "cancel_denied"
    assert record["reason"] == "NOT_SENT"
