"""Exact public EAPI payload conversion shared by offline and live clients."""

from decimal import Decimal, InvalidOperation

from nautilus_trader.model import (
    AggressorSide,
    Bar,
    BarType,
    BookOrder,
    OrderBookDepth,
    OrderSide,
    Price,
    Quantity,
    QuoteTick,
    RecordFlag,
    TradeId,
    TradeTick,
)

from core.market.instruments import bar_type_for, interval_delta


def number(value, precision, *, zero=False):
    if not isinstance(value, (str, Decimal)) and type(value) is not int:
        raise ValueError("期权价格数量必须为精确十进制字符串或整数")
    try:
        value = Decimal(str(value))
        exact = value.quantize(Decimal(1).scaleb(-precision))
    except (InvalidOperation, ValueError):
        raise ValueError("期权行情数值或精度无效") from None
    if not value.is_finite() or value < 0 or not zero and value == 0 or exact != value:
        raise ValueError("期权行情数值超出合约精度或有效范围")
    return format(exact, f".{precision}f")


def stamp(value):
    if type(value) is not int or value < 0:
        raise ValueError("期权时间或编号无效")
    return value


def trade(instrument, row, *, ts_init=None):
    symbol = str(instrument.raw_symbol)
    if row.get("symbol", symbol) != symbol:
        raise ValueError("期权成交合约不一致")
    identity = stamp(row.get("tradeId", row.get("id")))
    event = stamp(row["time"]) * 1_000_000
    if type(row["side"]) is not int or row["side"] not in {-1, 1}:
        raise ValueError("期权成交方向无效")
    return TradeTick(instrument.id, Price.from_str(number(row["price"], instrument.price_precision)),
                     Quantity.from_str(number(row["qty"], instrument.size_precision)),
                     AggressorSide.BUY if row["side"] == 1 else AggressorSide.SELL,
                     TradeId(str(identity)), event, event if ts_init is None else ts_init)


def bar(instrument, level, row, *, ts_init=None):
    values = ([row["openTime"], row["open"], row["high"], row["low"], row["close"],
               row["volume"], row["closeTime"]] if isinstance(row, dict) else row)
    if len(values) < 7:
        raise ValueError("期权 K 线字段不完整")
    start, end = stamp(values[0]), stamp(values[6])
    duration = int(interval_delta(level).total_seconds() * 1000)
    if start % duration or end != start + duration - 1:
        raise ValueError("期权 K 线时间未对齐")
    o, h, low, c = [number(v, instrument.price_precision, zero=True) for v in values[1:5]]
    if Decimal(h) < max(map(Decimal, (o, low, c))) or Decimal(low) > min(map(Decimal, (o, h, c))):
        raise ValueError("期权 OHLC 数值不一致")
    volume = number(values[5], instrument.size_precision, zero=True)
    event = (start + duration) * 1_000_000 - 1
    return Bar(BarType.from_str(bar_type_for(str(instrument.raw_symbol), level, "options")),
               *[Price.from_str(v) for v in (o, h, low, c)], Quantity.from_str(volume),
               event, event if ts_init is None else ts_init)


def quote(instrument, row, ts_init):
    if row["s"] != str(instrument.raw_symbol):
        raise ValueError("期权报价合约不一致")
    bid, ask = [Price.from_str(number(row[k], instrument.price_precision, zero=True)) for k in ("b", "a")]
    bs, az = [Quantity.from_str(number(row[k], instrument.size_precision, zero=True)) for k in ("B", "A")]
    if bs.as_decimal() > 0 and az.as_decimal() > 0 and bid > ask:
        raise ValueError("期权盘口买卖价交叉")
    return QuoteTick(instrument.id, bid, ask, bs, az, stamp(row["T"]) * 1_000_000, ts_init)


def depth(instrument, row, levels, ts_init):
    if row["s"] != str(instrument.raw_symbol):
        raise ValueError("期权盘口合约不一致")
    sides = []
    for key, side in (("b", OrderSide.BUY), ("a", OrderSide.SELL)):
        if len(row[key]) > levels:
            raise ValueError("期权盘口超出订阅深度")
        orders = [BookOrder(side, Price.from_str(number(p, instrument.price_precision)),
                            Quantity.from_str(number(q, instrument.size_precision)), 0) for p, q in row[key]]
        prices = [item.price.as_decimal() for item in orders]
        if len(set(prices)) != len(prices) or prices != sorted(prices, reverse=key == "b"):
            raise ValueError("期权盘口顺序无效")
        sides.append(orders)
    bids, asks = sides
    if bids and asks and bids[0].price > asks[0].price:
        raise ValueError("期权盘口买卖价交叉")
    return OrderBookDepth(instrument.id, bids, asks, [0] * len(bids), [0] * len(asks),
                          RecordFlag.F_SNAPSHOT.value | RecordFlag.F_LAST.value,
                          stamp(row["u"]), stamp(row["T"]) * 1_000_000, ts_init)


def trade_event(instrument, row, *, ts_init=None):
    """Normalize the public optionTrade stream once for native and managed clients."""
    if row["e"] != "trade" or row["s"] != str(instrument.raw_symbol) or row["X"] not in {"MARKET", "BLOCK"}:
        raise ValueError("期权成交类型无效")
    if type(row["m"]) is not bool:
        raise ValueError("期权成交主动方无效")
    payload = {"symbol": row["s"], "time": row["T"], "id": row["t"],
               "price": row["p"], "qty": row["q"], "side": -1 if row["m"] else 1}
    return trade(instrument, payload, ts_init=ts_init), payload


def bar_event(instrument, period, row, *, ts_init=None):
    k = row["k"]
    if row["s"] != str(instrument.raw_symbol) or k["s"] != row["s"] or k["i"] != period or type(k["x"]) is not bool:
        raise ValueError("期权 K 线流身份无效")
    payload = [k["t"], k["o"], k["h"], k["l"], k["c"], k["v"], k["T"]]
    value = bar(instrument, period, payload, ts_init=ts_init)
    return (value if k["x"] else None), payload
