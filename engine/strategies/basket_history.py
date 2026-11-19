"""Strict, read-only synchronization of Spot, USD-M and observed funding history.

This is a bounded input stream for a *future* basket backtest, not a matching
engine or a funding cash-flow calculation. Missing data never becomes a price.
"""

from dataclasses import dataclass
from itertools import zip_longest
from typing import Literal

from nautilus_trader.model import Bar, BarType, FundingRateUpdate, InstrumentId

from core.market.instruments import bar_type_for, instrument_id_for
from engine.runtime import EngineConflict
from engine.strategies.backtest import _nanos, read_history_batches


@dataclass(frozen=True)
class CarryHistoryEvent:
    ts_ns: int
    kind: Literal["bars", "funding"]
    spot: Bar | None = None
    perpetual: Bar | None = None
    funding: FundingRateUpdate | None = None


def align_carry_bars(spot_batches, perpetual_batches, *, symbol, interval,
                     start_ns=None, end_ns=None):
    """Align two bounded, already ordered native Bar streams without forward fills.

    Both products must have the same *closed-bar event timestamps*. Missing bars,
    duplicate bars or any interior time gap fail closed, rather than shifting
    market evidence across products.
    """
    expected_spot = BarType.from_str(bar_type_for(symbol, interval, "spot"))
    expected_perp = BarType.from_str(bar_type_for(symbol, interval, "futures_um"))
    step = {"1m": 60_000_000_000, "5m": 300_000_000_000}.get(interval)
    if step is None:
        raise EngineConflict("组合历史仅支持 1m 或 5m。")
    missing = object()
    spot = (bar for batch in spot_batches for bar in batch)
    perpetual = (bar for batch in perpetual_batches for bar in batch)
    last = None
    count = 0
    for first, second in zip_longest(spot, perpetual, fillvalue=missing):
        if first is missing or second is missing:
            raise EngineConflict("现货与永续历史 K 线数量或时间覆盖不一致。")
        if (
            not isinstance(first, Bar)
            or not isinstance(second, Bar)
            or first.bar_type != expected_spot
            or second.bar_type != expected_perp
        ):
            raise EngineConflict("跨产品历史 K 线的品种或周期不一致。")
        stamp = int(first.ts_event)
        if stamp != int(second.ts_event):
            raise EngineConflict("现货与永续历史时间戳不一致；禁止插值或前向填充。")
        if last is not None and stamp - last != step:
            raise EngineConflict("组合历史存在重复、倒退或缺失 K 线。")
        last = stamp
        count += 1
        yield CarryHistoryEvent(stamp, "bars", first, second)
    if count == 0:
        raise EngineConflict("该时间区间没有可用的现货与永续历史数据。")


def iter_carry_history(catalog_dir, symbol, interval, start, end, *, require_funding=True):
    """Read Catalog records in timestamp order, retaining funding as events.

    A historical funding-rate observation is not an executed exchange cashflow.
    Each funding record is emitted at its own event timestamp, not assigned to
    the preceding/following bar. The caller must establish settlement evidence,
    the position at settlement, and funding PnL in a separate backtest stage.
    """
    from engine.pipeline.catalog import ParquetDataCatalog

    start_ns, end_ns = _nanos(start), _nanos(end)
    if start_ns >= end_ns or interval not in {"1m", "5m"}:
        raise EngineConflict("组合历史时间窗口无效。")
    catalog = ParquetDataCatalog(catalog_dir)
    perp_id = InstrumentId.from_str(instrument_id_for(symbol, "futures_um"))
    # The native query end is inclusive. Our public history contract is [start, end).
    observations = catalog.funding_rate_updates(
        [str(perp_id)], start=start_ns, end=end_ns - 1,
    ) or []
    if len(observations) > 10_000:
        raise EngineConflict("该区间的资金费观测记录超出研究上限。")
    funding = []
    previous = None
    for item in observations:
        if not isinstance(item, FundingRateUpdate) or item.instrument_id != perp_id:
            raise EngineConflict("历史资金费数据的合约身份不一致。")
        ts = int(item.ts_event)
        if (
            not start_ns <= ts < end_ns
            or (previous is not None and ts <= previous)
            or not item.rate.is_finite()
        ):
            raise EngineConflict("资金费历史包含无效、重复或乱序的观测。")
        previous = ts
        funding.append(item)
    if require_funding and not funding:
        raise EngineConflict("该区间缺少真实历史资金费观测，不能假定资金费为零。")

    spot_batches = read_history_batches(
        catalog_dir, symbol, interval, start, end, product="spot",
    )
    perp_batches = read_history_batches(
        catalog_dir, symbol, interval, start, end, product="futures_um",
    )
    bars = align_carry_bars(
        spot_batches, perp_batches, symbol=symbol, interval=interval,
        start_ns=start_ns, end_ns=end_ns,
    )
    cursor = iter(funding)
    next_rate = next(cursor, None)
    for bar in bars:
        while next_rate is not None and next_rate.ts_event < bar.ts_ns:
            yield CarryHistoryEvent(int(next_rate.ts_event), "funding", funding=next_rate)
            next_rate = next(cursor, None)
        yield bar
        while next_rate is not None and next_rate.ts_event == bar.ts_ns:
            yield CarryHistoryEvent(int(next_rate.ts_event), "funding", funding=next_rate)
            next_rate = next(cursor, None)
    while next_rate is not None:
        yield CarryHistoryEvent(int(next_rate.ts_event), "funding", funding=next_rate)
        next_rate = next(cursor, None)
