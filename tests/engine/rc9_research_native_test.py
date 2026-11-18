"""Opt-in RC9 native research helpers: no live APIs or production mutations."""

from decimal import Decimal
from types import SimpleNamespace

import pytest
from nautilus_trader.model import InstrumentId, OptionGreeks

from engine.strategies.research_native import greeks_exposures, protective_bracket


def test_native_greeks_are_contract_adjusted_and_grouped_per_underlying():
    iid = InstrumentId.from_str("BTC-261225-85000-C.BINANCE")
    observation = OptionGreeks(
        instrument_id=iid, delta=0.5, gamma=0.02, vega=1.5, theta=-0.25,
        ts_event=100_000, ts_init=100_000,
    )
    result = greeks_exposures(
        [(iid, "BTC", Decimal(2), Decimal("0.1"))], [observation],
        now_ns=110_000, max_age_ns=20_000,
    )
    assert result["BTC"]["delta"] == Decimal("0.10")
    assert result["BTC"]["gamma"] == Decimal("0.004")
    with pytest.raises(ValueError, match="Stale"):
        greeks_exposures(
            [(iid, "BTC", Decimal(2), Decimal("0.1"))], [observation],
            now_ns=200_000, max_age_ns=20_000,
        )


def test_bracket_helper_requires_both_native_exits_reduce_only():
    orders = [SimpleNamespace(is_reduce_only=False),
              SimpleNamespace(is_reduce_only=True),
              SimpleNamespace(is_reduce_only=True)]
    calls = []
    fake = SimpleNamespace(order_factory=SimpleNamespace(
        bracket=lambda **kwargs: (calls.append(kwargs), orders)[1],
    ))
    assert protective_bracket(
        fake, instrument_id="ETHUSDT-PERP.BINANCE", order_side="BUY",
        quantity="1", tp_price="2200.00", sl_trigger_price="1800.00",
    ) is orders
    orders[-1].is_reduce_only = False
    with pytest.raises(ValueError, match="reduce-only"):
        protective_bracket(
            fake, instrument_id="ETHUSDT-PERP.BINANCE", order_side="BUY",
            quantity="1", tp_price="2200.00", sl_trigger_price="1800.00",
        )


def test_depth_batches_keep_global_order_and_bounded_memory(monkeypatch):
    from engine.strategies import research_native as native

    class Depth:
        def __init__(self, instrument_id, ts_event):
            self.instrument_id, self.ts_event = instrument_id, ts_event

    monkeypatch.setattr(native, "OrderBookDepth", Depth)
    identifier = InstrumentId.from_str("BTC-261225-85000-C.BINANCE")
    data = [Depth(identifier, stamp) for stamp in range(1, 6)]
    assert [len(batch) for batch in native.iter_validated_depth_batches(iter(data), identifier, batch_size=2)] == [2, 2, 1]
    assert native.validated_depths(iter(data), identifier) == data
    with pytest.raises(ValueError, match="repeated or regressing"):
        list(native.iter_validated_depth_batches(iter([data[0], data[1], data[1]]), identifier, batch_size=2))
    with pytest.raises(ValueError, match="No L2 snapshots"):
        list(native.iter_validated_depth_batches(iter(()), identifier))
    with pytest.raises(ValueError, match="batch size"):
        list(native.iter_validated_depth_batches(iter(data), identifier, batch_size=0))
    with pytest.raises(ValueError, match="instrument"):
        list(native.iter_validated_depth_batches([Depth("WRONG", 10)], identifier))


def test_depth_backtest_runs_streaming_and_disposes_on_failure(monkeypatch):
    from engine.strategies import research_native as native

    class Depth:
        def __init__(self, instrument_id, ts_event):
            self.instrument_id, self.ts_event = instrument_id, ts_event

    monkeypatch.setattr(native, "OrderBookDepth", Depth)
    identifier = InstrumentId.from_str("BTC-261225-85000-C.BINANCE")
    instrument = SimpleNamespace(id=identifier)
    produced, engines = [], []

    def source(times):
        for ts in times:
            produced.append(ts)
            yield Depth(identifier, ts)

    class Engine:
        def __init__(self, _config):
            self.trace = []
            self.disposed = False
            engines.append(self)

        def add_venue(self, *args, **kwargs):
            assert kwargs["book_type"] == native.BookType.L2_MBP

        def add_instrument(self, *_args):
            pass

        def add_strategies(self, *_args):
            pass

        def add_data(self, batch):
            self.trace.append(("batch", tuple(item.ts_event for item in batch)))
            # No read-ahead: each native run sees at most one new bounded batch.
            assert len(produced) <= 2 * len([x for x in self.trace if x[0] == "batch"])

        def run(self, *, streaming):
            assert streaming is True
            self.trace.append("run")

        def clear_data(self):
            self.trace.append("clear")

        def end(self):
            self.trace.append("end")

        def get_result(self):
            return {"ok": True}

        def dispose(self):
            self.disposed = True

    monkeypatch.setattr(native, "BacktestEngine", Engine)
    assert native.run_depth_backtest(instrument, source(range(1, 6)), "100 USDT", batch_size=2) == {"ok": True}
    assert engines[0].trace == [
        ("batch", (1, 2)), "run", "clear",
        ("batch", (3, 4)), "run", "clear",
        ("batch", (5,)), "run", "clear", "end",
    ]
    assert engines[0].disposed

    produced.clear()
    with pytest.raises(ValueError, match="repeated or regressing"):
        native.run_depth_backtest(instrument, source([1, 2, 2]), "100 USDT", batch_size=2)
    assert engines[1].disposed
    assert "end" not in engines[1].trace
    before = len(engines)
    with pytest.raises(ValueError, match="No L2 snapshots"):
        native.run_depth_backtest(instrument, iter(()), "100 USDT")
    assert len(engines) == before


def test_funding_join_is_ordered_one_pass_and_bounded(monkeypatch):
    from engine.strategies import research_native as native

    class Bar:
        def __init__(self, ts_event):
            self.ts_event = ts_event

    monkeypatch.setattr(native, "Bar", Bar)
    identifier = InstrumentId.from_str("ETHUSDT-PERP.BINANCE")
    observed = []

    def rates():
        for index in range(10_025):
            observed.append(index)
            yield SimpleNamespace(
                instrument_id=identifier, rate=Decimal("0.0001"), ts_event=index,
            )

    batches = native.join_funding_updates([[Bar(10), Bar(20)]], rates(), identifier)
    first = next(batches)
    assert len(first) == 23  # 0..20 funding observations plus two Bars
    assert len(observed) < 100
    remaining = list(batches)
    assert max(len(batch) for batch in remaining) <= 10_000
    assert len([event for batch in [first, *remaining] for event in batch]) == 10_027
    with pytest.raises(ValueError, match="repeated or out of order"):
        list(native.join_funding_updates(
            [], iter([
                SimpleNamespace(instrument_id=identifier, rate=Decimal(0), ts_event=2),
                SimpleNamespace(instrument_id=identifier, rate=Decimal(0), ts_event=1),
            ]), identifier,
        ))
