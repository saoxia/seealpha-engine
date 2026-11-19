"""Strict native Parquet QuoteTick -> generic basket replay frames.

The reference clock comes from real closed Spot/UM Nautilus Bars, not an
invented daily schedule. Every product requires verified native instruments
and BBO quotes, with bounded explicit as-of age (no candle-close execution).
Venue cash settlements must be supplied separately as immutable records.
"""
from decimal import Decimal

from nautilus_trader.model import (
    Bar,
    CryptoOption,
    CryptoPerpetual,
    CurrencyPair,
    QuoteTick,
)

from core.market.instruments import (
    bar_type_for,
    instrument_id_for,
    normalize_symbol,
)
from engine.pipeline.catalog import ParquetDataCatalog
from engine.runtime import EngineConflict
from engine.strategies.backtest import _nanos, parse_window, read_history
from engine.strategies.basket_finance_ledger import (
    BasketContract,
    BasketQuote,
)
from engine.strategies.basket_generic_replay import BasketReplayFrame
from engine.strategies.basket_targets import BasketPlanError, LegId

_PRODUCTS = {
    "spot": CurrencyPair,
    "futures_um": CryptoPerpetual,
    "options": CryptoOption,
}


def read_generic_quote_history(
    catalog_dir, *,
    legs: tuple[LegId, ...], clock_leg: LegId,
    start: str, end: str, interval: str,
    max_quote_age_ns: int,
) -> tuple[tuple[BasketContract, ...], tuple[BasketReplayFrame, ...]]:
    """Read complete source BBO from catalog and align on a native Bar clock.

    Return synthetic research *valuation* marks from quote midpoints; these
    are NOT exchange mark-price or actual executable close reports. Fees are
    extracted from native contract taker rates. Explicit funding and exercise
    records must be added by the calling development controller; absence of
    such records is never interpreted as proof that no settlement occurred.
    """
    if not isinstance(legs, tuple) or not 1 <= len(legs) <= 64:
        raise BasketPlanError("通用原生历史必须提供组合腿")
    if (
        len(set(legs)) != len(legs) or not all(isinstance(leg, LegId) for leg in legs)
        or len({(leg.tenant_id, leg.account_id) for leg in legs}) != 1
    ):
        raise BasketPlanError("原生组合腿的作用域或合约身份不一致")
    if clock_leg not in legs or clock_leg.product not in {"spot", "futures_um"}:
        raise BasketPlanError("历史决策时钟必须属于已授权现货或永续腿")
    if type(max_quote_age_ns) is not int or not 0 < max_quote_age_ns <= 300_000_000_000:
        raise BasketPlanError("原生 BBO 有效窗口无效")
    start_at, end_at, interval = parse_window(start, end, interval)
    step = {"1m": 60_000_000_000, "5m": 300_000_000_000}.get(interval)
    start_ns, end_ns = _nanos(start_at), _nanos(end_at)
    if (
        step is None or start_ns % step or end_ns % step
        or end_ns - start_ns > 86_400_000_000_000
        or (end_ns - start_ns) // step < 2
    ):
        raise BasketPlanError("通用组合研究最多24小时，并必须按1m/5m真实Bar时钟对齐")
    bars = read_history(
        catalog_dir, clock_leg.symbol, interval, start, end,
        product=clock_leg.product,
    )
    expected_ts = list(range(start_ns + step, end_ns, step))
    if (
        len(bars) != len(expected_ts)
        or any(
            not isinstance(row, Bar)
            or int(row.ts_event) != stamp
            or str(row.bar_type) != bar_type_for(
                clock_leg.symbol, interval, clock_leg.product,
            )
            for row, stamp in zip(bars, expected_ts, strict=True)
        )
    ):
        raise BasketPlanError("参考品种的原生 Bar 存在缺口、重复或时间错位")
    catalog = ParquetDataCatalog(catalog_dir)
    contracts = []
    streams: dict[LegId, list[QuoteTick]] = {}
    for leg in sorted(legs):
        symbol = normalize_symbol(leg.symbol, leg.product)
        if symbol != leg.symbol:
            raise BasketPlanError("组合合约必须使用规范化 Binance 产品代码")
        instrument_id = instrument_id_for(symbol, leg.product)
        matches = catalog.instruments(instrument_ids=[instrument_id]) or []
        if len(matches) != 1 or not isinstance(matches[0], _PRODUCTS[leg.product]):
            raise BasketPlanError("缺少精确匹配产品的原生 Binance 合约")
        instrument = matches[0]
        if (
            str(instrument.id) != instrument_id
            or instrument.quote_currency.code != "USDT"
        ):
            raise BasketPlanError("组合仅支持来源明确的 USDT 产品合约")
        try:
            unit = (
                instrument.multiplier.as_decimal()
                if leg.product == "options" else Decimal(1)
            )
            step_size = instrument.size_increment.as_decimal()
            taker_fee = Decimal(str(instrument.taker_fee))
            expiry = int(instrument.expiration_ns) if leg.product == "options" else None
            strike = (
                instrument.strike_price.as_decimal()
                if leg.product == "options" else None
            )
            contract = BasketContract(
                leg, step_size, unit, taker_fee,
                Decimal("0.2") if leg.product == "futures_um" else Decimal(0),
                expiry_ns=expiry, strike=strike,
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise BasketPlanError("原生合约费率、乘数或期限无法验证") from exc
        # Fetch strictly bounded history including the last valid quote
        # before start. The as-of price is never extrapolated beyond age cap.
        ticks = catalog.quote_ticks(
            instrument_ids=[instrument_id],
            start=max(0, start_ns - max_quote_age_ns),
            end=end_ns - 1,
        ) or []
        if not ticks or len(ticks) > 250_000:
            raise BasketPlanError("缺少完整的跨产品原生 BBO 报价")
        last = 0
        for row in ticks:
            if (
                not isinstance(row, QuoteTick)
                or str(row.instrument_id) != instrument_id
                or int(row.ts_event) <= last
                or row.bid_price.as_decimal() <= 0
                or row.ask_price.as_decimal() < row.bid_price.as_decimal()
                or row.bid_size.as_decimal() <= 0
                or row.ask_size.as_decimal() <= 0
            ):
                raise BasketPlanError("原生 BBO 存在身份、时间或价差错误")
            last = int(row.ts_event)
        contracts.append(contract)
        streams[leg] = ticks

    cursor = {leg: 0 for leg in streams}
    last_rows = {leg: None for leg in streams}
    frames = []
    for ts_ns in expected_ts:
        quotes = []
        for leg in sorted(streams):
            rows = streams[leg]
            offset = cursor[leg]
            while offset < len(rows) and int(rows[offset].ts_event) <= ts_ns:
                last_rows[leg] = rows[offset]
                offset += 1
            cursor[leg] = offset
            source = last_rows[leg]
            if source is None or ts_ns - int(source.ts_event) > max_quote_age_ns:
                raise BasketPlanError("缺少指定时间线的某条 Spot/UM/Options BBO")
            bid = source.bid_price.as_decimal()
            ask = source.ask_price.as_decimal()
            quotes.append(BasketQuote(
                leg, bid, ask,
                source.bid_size.as_decimal(), source.ask_size.as_decimal(),
                (bid + ask) / Decimal(2), int(source.ts_event),
            ))
        frames.append(BasketReplayFrame(ts_ns, tuple(quotes)))
    return tuple(contracts), tuple(frames)
