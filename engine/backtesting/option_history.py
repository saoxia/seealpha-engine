"""Fail-closed historical option protection evidence for future native replay.

The current Engine does not claim that option OHLC closes are executable asks.
This loader requires actual stored QuoteTicks (with bid/ask sizes), complete
underlying/wing K-line coverage, verified native contracts and unexpired wings.
It produces input evidence only, never orders or protection guarantees.
"""

from dataclasses import dataclass
from decimal import Decimal

from nautilus_trader.model import Bar, CryptoOption, CryptoPerpetual, OptionKind, QuoteTick

from core.market.instruments import bar_type_for, instrument_id_for, normalize_symbol
from engine.market_data.access.historical_data import HistoricalMarketData, _nanos
from engine.trading.runtime import EngineConflict


@dataclass(frozen=True)
class OptionProtectionEvidence:
    ts_ns: int
    perpetual: Bar
    put: Bar
    call: Bar
    put_quote: QuoteTick
    call_quote: QuoteTick


def _instrument(catalog, symbol, product, kind):
    identity = instrument_id_for(symbol, product)
    matches = catalog.instruments(instrument_ids=[identity])
    found = next((row for row in matches if str(row.id) == identity), None)
    if not isinstance(found, kind):
        raise EngineConflict("缺少匹配的原生永续或期权合约定义。")
    return found


def _quote_evidence(catalog, option, start_ns, end_ns):
    rows = catalog.quote_ticks(
        instrument_ids=[str(option.id)], start=start_ns, end=end_ns - 1,
    ) or []
    if not rows or len(rows) > 250_000:
        raise EngineConflict("期权历史买卖盘报价缺失或记录超出研究上限。")
    previous = None
    for row in rows:
        if not isinstance(row, QuoteTick) or row.instrument_id != option.id:
            raise EngineConflict("期权历史报价的合约身份不一致。")
        stamp = int(row.ts_event)
        if (
            not start_ns <= stamp < end_ns
            or (previous is not None and stamp <= previous)
            or row.ask_price.as_decimal() <= 0
            or row.bid_price.as_decimal() < 0
            or row.bid_price > row.ask_price
            or row.ask_size.as_decimal() <= 0
        ):
            raise EngineConflict("期权历史报价无效、乱序或缺乏卖盘深度。")
        previous = stamp
    return rows


def _at_or_before(quotes, cursor, decision_ns):
    last = None
    while cursor < len(quotes) and int(quotes[cursor].ts_event) <= decision_ns:
        last = quotes[cursor]
        cursor += 1
    return last, cursor


def read_option_protection_evidence(
    catalog_dir, perpetual_symbol, put_symbol, call_symbol, start, end, interval,
    *, option_contracts: Decimal, max_quote_age_ns: int, reader=None,
):
    """Return bounded, fully aligned native market evidence (not simulated fills).

    To avoid symbol/expiry lookahead, this requires actual stored native contract
    definitions and observed quotes, not inferred premiums from option bars.
    """
    symbol = normalize_symbol(perpetual_symbol, "futures_um")
    put_name = normalize_symbol(put_symbol, "options")
    call_name = normalize_symbol(call_symbol, "options")
    if not put_name.endswith("-P") or not call_name.endswith("-C"):
        raise EngineConflict("保护组合必须分别持有看跌和看涨期权。")
    if (
        not isinstance(option_contracts, Decimal) or not option_contracts.is_finite()
        or option_contracts <= 0
        or type(max_quote_age_ns) is not int or not 0 < max_quote_age_ns <= 300_000_000_000
    ):
        raise EngineConflict("期权保护数量或报价时效限制无效。")
    step = {"1m": 60_000_000_000, "5m": 300_000_000_000}.get(interval)
    start_ns, end_ns = _nanos(start), _nanos(end)
    if step is None or end_ns <= start_ns or end_ns - start_ns > 86_400_000_000_000:
        raise EngineConflict("期权保护研究仅支持 1m/5m、最多 24 小时的窗口。")
    if start_ns % step or end_ns % step:
        raise EngineConflict("期权回测窗口必须按 K 线周期对齐。")

    catalog = reader if reader is not None else HistoricalMarketData(catalog_dir)
    perp = _instrument(catalog, symbol, "futures_um", CryptoPerpetual)
    put = _instrument(catalog, put_name, "options", CryptoOption)
    call = _instrument(catalog, call_name, "options", CryptoOption)
    if (
        put.underlying != perp.base_currency or call.underlying != perp.base_currency
        or put.quote_currency != perp.quote_currency
        or call.quote_currency != perp.quote_currency
        or put.expiration_ns != call.expiration_ns
        or put.expiration_ns <= end_ns
        or put.option_kind != OptionKind.PUT
        or call.option_kind != OptionKind.CALL
        or put.multiplier.as_decimal() <= 0
        or call.multiplier.as_decimal() <= 0
    ):
        raise EngineConflict("期权标的、币种、方向、到期或乘数不满足保护条件。")
    for option in (put, call):
        if option_contracts % option.size_increment.as_decimal():
            raise EngineConflict("期权保护数量不符合最小合约步长。")

    streams = (
        catalog.bars(symbol, interval, start, end, product="futures_um"),
        catalog.bars(put_name, interval, start, end, product="options"),
        catalog.bars(call_name, interval, start, end, product="options"),
    )
    perp_bars, put_bars, call_bars = streams
    expected_count = (end_ns - start_ns) // step - 1
    if (
        len(perp_bars) != expected_count
        or len(put_bars) != expected_count
        or len(call_bars) != expected_count
    ):
        raise EngineConflict("期权与永续的历史 K 线覆盖不完整，不允许前向填充。")
    for i, (bar, put_bar, call_bar) in enumerate(zip(*streams, strict=True)):
        ts = start_ns + (i + 1) * step
        if (
            int(bar.ts_event) != ts
            or int(put_bar.ts_event) != ts - 1
            or int(call_bar.ts_event) != ts - 1
            or str(bar.bar_type) != bar_type_for(symbol, interval)
            or str(put_bar.bar_type) != bar_type_for(put_name, interval, "options")
            or str(call_bar.bar_type) != bar_type_for(call_name, interval, "options")
        ):
            raise EngineConflict("期权与永续 K 线没有对应同一个收盘时间。")

    # Query timestamps once, then perform a linear as-of join. Do not borrow
    # future quotes or use closed-bar OHLC as a substitute for the ask.
    put_quotes = _quote_evidence(catalog, put, start_ns, end_ns)
    call_quotes = _quote_evidence(catalog, call, start_ns, end_ns)
    pi = ci = 0
    last_put = last_call = None
    result = []
    for bar, put_bar, call_bar in zip(*streams, strict=True):
        stamp = int(bar.ts_event)
        newer, pi = _at_or_before(put_quotes, pi, stamp)
        if newer is not None:
            last_put = newer
        newer, ci = _at_or_before(call_quotes, ci, stamp)
        if newer is not None:
            last_call = newer
        if (
            last_put is None or last_call is None
            or stamp - last_put.ts_event > max_quote_age_ns
            or stamp - last_call.ts_event > max_quote_age_ns
            or last_put.ask_size.as_decimal() < option_contracts
            or last_call.ask_size.as_decimal() < option_contracts
        ):
            raise EngineConflict("期权保护历史卖盘报价缺失、过期或不足额。")
        result.append(OptionProtectionEvidence(
            stamp, bar, put_bar, call_bar, last_put, last_call,
        ))
    if not result:
        raise EngineConflict("该区间没有可以验证的期权保护历史证据。")
    return result
