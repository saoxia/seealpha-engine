"""Opt-in partials accumulate exact Decimal values without altering the closed stream."""

from types import SimpleNamespace

from nautilus_trader.model import AggressorSide, InstrumentId, Price, Quantity, TradeId, TradeTick

from core.market.models import MINUTE
from engine.connectors.binance.native import MinuteActor


def test_native_partial_delivery_is_opt_in_throttled_and_ignores_late_trades(monkeypatch):
    clock = [10.0]
    monkeypatch.setattr("engine.connectors.binance.native.monotonic", lambda: clock[0])
    partials, delivered = [], []
    source = SimpleNamespace(on_partial=lambda *args: partials.append(args), deliver=delivered.append,
                             loop=SimpleNamespace(call_soon_threadsafe=lambda callback, *args: callback(*args)))
    actor = MinuteActor(source)

    def trade(number, stamp, price="10.00"):
        return TradeTick(InstrumentId.from_str("ETHUSDT-PERP.BINANCE"), Price.from_str(price),
                         Quantity.from_str("0.025"), AggressorSide.BUY, TradeId(str(number)),
                         stamp * 1_000_000, stamp * 1_000_000)

    actor.on_trade(trade(1, MINUTE + 100))
    assert len(partials) == 1 and str(partials[-1][0].volume) == "0.025"
    clock[0] += 0.05
    actor.on_trade(trade(2, MINUTE + 200, "11.00"))
    assert len(partials) == 1
    clock[0] += 0.5
    actor.on_trade(trade(3, MINUTE + 300, "9.00"))
    assert str(partials[-1][0].volume) == "0.075"
    assert str(partials[-1][0].open) == "10.00" and str(partials[-1][0].close) == "9.00"
    assert str(partials[-1][0].high) == "11.00"
    assert all(value[2] is None for value in delivered)
    actor.on_trade(trade(4, 0))
    assert actor.partials["ETHUSDT"].start_ms == MINUTE
    source.on_partial = None
    clock[0] += 1
    actor.on_trade(trade(5, 2 * MINUTE))
    assert len(partials) == 2 and delivered[-1][1] == 2 * MINUTE
