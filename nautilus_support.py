"""Type aliases for this project's Binance instrument and order surfaces."""

from nautilus_trader.model import (
    CryptoPerpetual,
    CurrencyPair,
    LimitOrder,
    MarketOrder,
    StopLimitOrder,
    StopMarketOrder,
)

Instrument = CryptoPerpetual | CurrencyPair
Order = LimitOrder | MarketOrder | StopLimitOrder | StopMarketOrder

from nautilus_trader.model import Currency

USD = Currency.from_str("USD")
USDT = Currency.from_str("USDT")
BTC = Currency.from_str("BTC")
ETH = Currency.from_str("ETH")

from decimal import Decimal

from nautilus_trader.model import InstrumentId, Money, Price, Quantity, Symbol, Venue
from nautilus_trader.testkit.providers import TestInstrumentProvider as NativeInstrumentProvider


class TestInstrumentProvider(NativeInstrumentProvider):
    """Local reference definitions retained for deterministic cross-version tests."""
    __test__ = False

    @staticmethod
    def ethusdt_perp_binance():
        return CryptoPerpetual(
            instrument_id=InstrumentId(Symbol("ETHUSDT-PERP"), Venue("BINANCE")),
            raw_symbol=Symbol("ETHUSDT"), base_currency=ETH, quote_currency=USDT,
            settlement_currency=USDT, is_inverse=False, price_precision=2, size_precision=3,
            price_increment=Price.from_str("0.01"), size_increment=Quantity.from_str("0.001"),
            max_quantity=Quantity.from_str("10000.000"), min_quantity=Quantity.from_str("0.001"),
            min_notional=Money(10.00, USDT), max_price=Price.from_str("152588.43"),
            min_price=Price.from_str("29.91"), margin_init=Decimal("1.00"),
            margin_maint=Decimal("0.35"),
            info={"project_fee_rates": {"maker": "0.0002", "taker": "0.0004"}},
            ts_event=1646199312128000000,
            ts_init=1646199342953849862,
        )


def result_summary(result):
    """Serialize native BacktestResult without assuming it is a Python dataclass."""
    from dataclasses import asdict, is_dataclass
    if is_dataclass(result):
        return asdict(result)
    return {
        name: getattr(result, name)
        for name in type(result).__dict__
        if isinstance(getattr(type(result), name), property)
        or type(getattr(type(result), name)).__name__ == "getset_descriptor"
    }


def instrument_fee_rates(instrument):
    rates = (instrument.info or {}).get("project_fee_rates", {})
    return Decimal(rates.get("maker", "0.0002")), Decimal(rates.get("taker", "0.0004"))


def instrument_fee_model(instrument):
    from nautilus_trader.execution import MakerTakerFeeModel
    maker, taker = instrument_fee_rates(instrument)
    return MakerTakerFeeModel(maker, taker)


def strategy_cache(strategy):
    """A native component has no cache until registered with an engine."""
    from nautilus_trader.trading import Strategy
    if isinstance(strategy, Strategy) and strategy.trader_id is None:
        return None
    return strategy.cache


def make_strategy_price(instrument, value):
    """Keep v1's float-to-price rounding at the strategy/order boundary.

    v2 uses decimal half-even rounding; v1 rounded the binary float to the
    instrument precision. Explicit rounding preserves existing order levels.
    """
    return instrument.make_price(round(float(value), instrument.price_precision))
