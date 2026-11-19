"""One-call generic development basket replay over native Catalog evidence.

This API accepts caller-provisioned strategy models and explicit settlement
evidence. It must run only inside a trusted isolated development worker.
Never mount an arbitrary user script or expose it over an Engine admin route.
"""
from decimal import Decimal

from engine.strategies.basket_finance_ledger import (
    BasketFinanceLedger,
    VerifiedFundingCash,
)
from engine.strategies.basket_generic_history import read_generic_quote_history
from engine.strategies.basket_generic_replay import (
    BasketReplayFrame,
    GenericBasketReplay,
)
from engine.strategies.basket_generic_runtime import (
    BasketMemberRegistration,
    GenericBasketRuntime,
)
from engine.strategies.basket_journal_store import BasketJournalStore
from engine.strategies.basket_targets import BasketPlanError, LegId


def run_generic_catalog_research(
    catalog_dir, *,
    tenant_id: str,
    account_id: str,
    members: tuple[BasketMemberRegistration, ...],
    clock_leg: LegId,
    start: str,
    end: str,
    interval: str,
    starting_cash: dict[str, Decimal],
    max_quote_age_ns: int,
    fee_rates: dict[LegId, Decimal],
    um_margin_ratios: dict[LegId, Decimal],
    max_gross_notional: Decimal,
    slippage_bps: Decimal = Decimal(0),
    expected_funding: dict[int, tuple[LegId, ...]] | None = None,
    funding_cash: tuple[VerifiedFundingCash, ...] = (),
    option_settlements: dict[int, tuple] | None = None,
    options_cash_after: dict[int, Decimal] | None = None,
    journal_store: BasketJournalStore | None = None,
) -> dict:
    """Read real source BBO and run bounded, model-independent simulation.

    The quote midpoint is only a research mark proxy. `expected_funding=None`
    explicitly means we cannot attest that all funding events are present.
    """
    from engine.strategies.backtest import _nanos, parse_window

    if not isinstance(members, tuple) or not members:
        raise BasketPlanError("通用回测缺少已授权策略成员")
    runtime = GenericBasketRuntime(
        tenant_id=tenant_id, account_id=account_id, members=members,
        max_age_ns=max_quote_age_ns,
        max_gross_notional=max_gross_notional,
    )
    if clock_leg not in runtime.legs:
        raise BasketPlanError("参考行情时钟不在策略组合里")
    if (
        not isinstance(funding_cash, tuple)
        or not isinstance(option_settlements, (type(None), dict))
        or not isinstance(options_cash_after, (type(None), dict))
    ):
        raise BasketPlanError("组合真实结算来源类型无效")
    contracts, native_frames = read_generic_quote_history(
        catalog_dir, legs=runtime.legs, clock_leg=clock_leg,
        start=start, end=end, interval=interval,
        max_quote_age_ns=max_quote_age_ns,
        fee_rates=fee_rates,
        um_margin_ratios=um_margin_ratios,
    )
    funding: dict[int, list[VerifiedFundingCash]] = {}
    for item in funding_cash:
        if not isinstance(item, VerifiedFundingCash):
            raise BasketPlanError("历史资金费流水必须具有确定的身份和来源")
        funding.setdefault(item.ts_ns, []).append(item)
    option_settlements = option_settlements or {}
    options_cash_after = options_cash_after or {}
    times = {frame.ts_ns for frame in native_frames}
    if (
        not set(funding).issubset(times)
        or not set(option_settlements).issubset(times)
        or not set(options_cash_after).issubset(times)
    ):
        raise BasketPlanError("历史结算事件不得落在真实回测时钟窗口之外")
    frames = tuple(
        BasketReplayFrame(
            ts_ns=frame.ts_ns, quotes=frame.quotes,
            funding=tuple(funding.get(frame.ts_ns, ())),
            option_settlements=option_settlements.get(frame.ts_ns, ()),
            options_cash_after=options_cash_after.get(frame.ts_ns),
        ) for frame in native_frames
    )
    ledger = BasketFinanceLedger(contracts, starting_cash)
    replay = GenericBasketReplay(
        runtime, ledger,
        step_ns={"1m": 60_000_000_000, "5m": 300_000_000_000}[interval],
        max_quote_age_ns=max_quote_age_ns,
        slippage_bps=slippage_bps,
        expected_funding=expected_funding,
        journal_store=journal_store,
    )
    start_at, end_at, _ = parse_window(start, end, interval)
    result = replay.run(
        frames, start_ns=_nanos(start_at), end_ns=_nanos(end_at),
    )
    result["source"] = "native_parquet_bars_contracts_and_bbo"
    result["valuation_basis"] = "bid_ask_midpoint_proxy_not_exchange_mark"
    result["native_matching"] = False
    result["production_authorized"] = False
    return result
