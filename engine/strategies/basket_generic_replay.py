"""Development-only synchronized multi-product research replay with audited cash.

Real BBO depth, contract multipliers and funding/expiry records must be supplied
by a trusted historical adapter. The deterministic depth/slippage matcher here
is NOT NautilusTrader venue matching, order transport, or live trading.
"""
import hashlib
from dataclasses import dataclass
from decimal import Decimal

from engine.strategies.basket_finance_ledger import (
    BasketFill,
    BasketFinanceLedger,
    BasketQuote,
    VerifiedFundingCash,
    _number,
)
from engine.strategies.basket_generic_runtime import GenericBasketRuntime
from engine.strategies.basket_journal_store import BasketJournalStore
from engine.strategies.basket_reconciliation import (
    begin_phase,
    checkpoint_to_record,
    confirm_phase,
    freeze_checkpoint,
    new_checkpoint,
    record_cumulative_fill,
)
from engine.strategies.basket_targets import BasketPlanError, LegId


@dataclass(frozen=True)
class BasketReplayFrame:
    ts_ns: int
    quotes: tuple[BasketQuote, ...]
    funding: tuple[VerifiedFundingCash, ...] = ()
    option_settlements: tuple = ()
    options_cash_after: Decimal | None = None


class GenericBasketReplay:
    """One joint clock and netted member target per product/account/instrument.

    A blocked risk gate freezes the research run; no auto reissue, synthetic
    hedge, fake fill or hidden transfer between Spot/UM/Options is allowed.
    """

    def __init__(
        self, runtime: GenericBasketRuntime, ledger: BasketFinanceLedger, *,
        step_ns: int, max_quote_age_ns: int, slippage_bps: Decimal,
        expected_funding: dict[int, tuple[LegId, ...]] | None = None,
        journal_store: BasketJournalStore | None = None,
    ) -> None:
        if set(runtime.legs) != set(ledger.contracts):
            raise BasketPlanError("成员订阅与资金账本的完整合约集合不一致")
        if type(step_ns) is not int or step_ns <= 0:
            raise BasketPlanError("通用研究的统一时间步长无效")
        if type(max_quote_age_ns) is not int or max_quote_age_ns <= 0:
            raise BasketPlanError("历史 BBO 有效窗口无效")
        _number(slippage_bps, "研究滑点")
        if slippage_bps < 0 or slippage_bps > Decimal(1000):
            raise BasketPlanError("模拟滑点不得为负或超过10%")
        if expected_funding is not None and (
            not isinstance(expected_funding, dict)
            or any(
                type(ts) is not int or ts <= 0
                or not isinstance(legs, tuple)
                or len(legs) != len(set(legs))
                or any(
                    leg not in ledger.contracts or leg.product != "futures_um"
                    for leg in legs
                )
                for ts, legs in expected_funding.items()
            )
        ):
            raise BasketPlanError("历史资金费结算计划和产品身份无效")
        if journal_store is not None and not isinstance(journal_store, BasketJournalStore):
            raise BasketPlanError("通用模拟阶段日志只接受私有 CAS 研究账本")
        self.journal_store = journal_store
        self._journal_identity = ""
        self._journal_revision = None
        self.checkpoint = None
        self.phase_audits: list[dict] = []
        self.runtime = runtime
        self.ledger = ledger
        self.step_ns = step_ns
        self.max_quote_age_ns = max_quote_age_ns
        self.slippage_bps = slippage_bps
        self.expected_funding = expected_funding
        self.curve: list[dict] = []
        self.target_events: list[dict] = []
        self.status = "ready"
        self.freeze_reason: str | None = None
        self.last_ns = 0
        self.expiry_events = 0

    def _freeze(self, reason: str) -> None:
        self.status = "frozen"
        self.freeze_reason = reason

    def _persist_checkpoint(self) -> None:
        if self.journal_store is not None:
            saved = self.journal_store.save(
                self._journal_identity, self.checkpoint,
                expected_revision=self._journal_revision,
            )
            self._journal_revision = saved.revision

    def _execute_phase(self, phase, quotes: dict[LegId, BasketQuote], ts_ns: int) -> bool:
        request_id = f"sim-{ts_ns}-{self.checkpoint.phase_index}"
        self.checkpoint = begin_phase(self.checkpoint, request_id)
        self._persist_checkpoint()
        depth_used: dict[tuple[LegId, str], Decimal] = {}
        for index, change in enumerate(phase.changes):
            leg, delta = change.leg, change.quantity_delta
            contract, quote = self.ledger.contracts[leg], quotes[leg]
            if contract.expiry_ns is not None and ts_ns >= contract.expiry_ns:
                self._freeze("已到期期权不得接受新的模拟订单")
                return False
            side = "BUY" if delta > 0 else "SELL"
            step = contract.quantity_increment
            depth = quote.ask_size if delta > 0 else quote.bid_size
            remaining = max(Decimal(0), depth - depth_used.get((leg, side), Decimal(0)))
            fill_size = min(abs(delta), remaining)
            fill_size -= fill_size % step
            if not fill_size:
                self._freeze("组合腿缺少可执行深度，冻结未决订单")
                return False
            price = (quote.ask if delta > 0 else quote.bid) * (
                Decimal(1) + self.slippage_bps / Decimal(10000)
                if delta > 0 else Decimal(1) - self.slippage_bps / Decimal(10000)
            )
            if price <= 0:
                self._freeze("模拟滑点造成无效成交价格")
                return False
            traded = fill_size if delta > 0 else -fill_size
            fee = (
                fill_size * price * contract.multiplier * contract.taker_fee_rate
            )
            identity = f"sim:{ts_ns}:{phase.name}:{index}:{leg.product}:{leg.symbol}"
            fill = BasketFill(identity, leg, ts_ns, traded, price, fee, phase.name)
            try:
                self.ledger.execute_fill(
                    fill, quotes, max_age_ns=self.max_quote_age_ns,
                )
            except BasketPlanError as exc:
                self._freeze("模拟成交及资金风控拒绝：" + str(exc))
                return False
            self.checkpoint = record_cumulative_fill(
                self.checkpoint, request_id=request_id, event_id=identity,
                leg=leg, cumulative=traded,
            )
            self._persist_checkpoint()
            depth_used[(leg, side)] = (
                depth_used.get((leg, side), Decimal(0)) + fill_size
            )
            if fill_size != abs(delta):
                self._freeze("组合发生可核算的部分成交，禁止盲目补齐剩余腿")
                return False
            gross = sum((
                abs(quantity) * quotes[item].mark * entry.multiplier
                for item, entry in self.ledger.contracts.items()
                for quantity in (self.ledger.positions[item],)
            ), Decimal(0))
            if gross > self.runtime.max_gross_notional:
                self._freeze("模拟成交后实际毛名义风险超过预算")
                return False
        self.checkpoint = confirm_phase(
            self.checkpoint, request_id=request_id,
            terminal_statuses={row.leg: "FILLED" for row in phase.changes},
            observed_positions=dict(self.ledger.positions),
        )
        self._persist_checkpoint()
        return True

    def _snapshot(self, quotes: dict[LegId, BasketQuote], stamp: int) -> dict:
        report = self.ledger.mark(
            quotes, now_ns=stamp, max_age_ns=self.max_quote_age_ns,
        )
        total = Decimal(report["total_equity"])
        pnl = sum((
            self.ledger.realized[leg]
            + Decimal(report["unrealized_by_leg"][self.ledger._leg_key(leg)])
            - self.ledger.fees[leg] + self.ledger.funding[leg]
            for leg in self.ledger.contracts
        ), Decimal(0))
        starting = sum(self.ledger.initial_cash.values(), Decimal(0))
        if total != starting + pnl:
            raise BasketPlanError("组合现金与各腿收益归因不守恒")
        report["total_pnl"] = format(pnl, "f")
        report["positions"] = {
            self.ledger._leg_key(leg): format(quantity, "f")
            for leg, quantity in sorted(self.ledger.positions.items())
        }
        return report

    def run(
        self, frames: tuple[BasketReplayFrame, ...], *,
        start_ns: int, end_ns: int,
    ) -> dict:
        if self.status != "ready" or self.curve or self.last_ns:
            raise BasketPlanError("同一组合研究回放只允许启动一次")
        if (
            not isinstance(frames, tuple) or not frames or len(frames) > 20_000
            or type(start_ns) is not int or type(end_ns) is not int
            or start_ns <= 0 or end_ns <= start_ns
            or start_ns % self.step_ns or end_ns % self.step_ns
        ):
            raise BasketPlanError("组合历史回测时间窗口或事件数量无效")
        if len(frames) != (end_ns - start_ns) // self.step_ns - 1:
            raise BasketPlanError("组合历史时间线缺少或多出收盘事件")
        seen_funding: set[tuple[int, LegId]] = set()
        for index, frame in enumerate(frames):
            ts_ns = start_ns + (index + 1) * self.step_ns
            if not isinstance(frame, BasketReplayFrame) or frame.ts_ns != ts_ns:
                raise BasketPlanError("组合历史时间线存在缺口、重复或乱序")
            if (
                not isinstance(frame.quotes, tuple)
                or len(frame.quotes) != len(self.ledger.contracts)
                or len({item.leg for item in frame.quotes}) != len(frame.quotes)
            ):
                raise BasketPlanError("跨产品历史买卖盘集合不完整")
            quotes = {quote.leg: quote for quote in frame.quotes}
            if set(quotes) != set(self.ledger.contracts):
                raise BasketPlanError("策略订阅产品缺少真实同一时刻的历史报价")
            for quote in quotes.values():
                if (
                    not isinstance(quote, BasketQuote)
                    or not 0 <= ts_ns - quote.ts_ns <= self.max_quote_age_ns
                ):
                    raise BasketPlanError("历史报价来自未来、过期或存在空档")
            funding_by_leg = {}
            if not isinstance(frame.funding, tuple):
                raise BasketPlanError("真实资金费事件集合类型无效")
            for row in frame.funding:
                if (
                    not isinstance(row, VerifiedFundingCash)
                    or row.ts_ns != ts_ns
                    or row.leg in funding_by_leg
                ):
                    raise BasketPlanError("历史资金费事件与统一时间线不一致")
                funding_by_leg[row.leg] = row
            required = (
                set(self.expected_funding.get(ts_ns, ()))
                if self.expected_funding is not None else set(funding_by_leg)
            )
            if set(funding_by_leg) != required:
                raise BasketPlanError("历史资金费结算清单与实际流水不一致")
            try:
                for record in frame.funding:
                    self.ledger.apply_funding(
                        record, quotes, max_age_ns=self.max_quote_age_ns,
                    )
                    seen_funding.add((ts_ns, record.leg))
                expiring = {
                    leg for leg, contract in self.ledger.contracts.items()
                    if contract.expiry_ns == ts_ns
                }
                if frame.option_settlements:
                    if not expiring or frame.options_cash_after is None:
                        raise BasketPlanError("没有匹配期权到期日及权威现金快照")
                    self.ledger.settle_verified_options(
                        frame.option_settlements, quotes,
                        account_cash_after=frame.options_cash_after,
                        now_ns=ts_ns, max_age_ns=self.max_quote_age_ns,
                    )
                    self.expiry_events += len(frame.option_settlements)
                elif (
                    frame.options_cash_after is not None
                    or any(self.ledger.positions[leg] for leg in expiring)
                ):
                    raise BasketPlanError("期权到期日缺少权威行权与现金流水")
                evidence = {
                    leg: quote.evidence(self.ledger.contracts[leg])
                    for leg, quote in quotes.items()
                }
                decision = self.runtime.decide(
                    now_ns=ts_ns, positions=dict(self.ledger.positions),
                    evidence=evidence,
                )
                self.checkpoint = new_checkpoint(
                    decision.plan, dict(self.ledger.positions),
                )
                raw_id = (
                    f"{self.runtime.tenant_id}:{self.runtime.account_id}:{ts_ns}"
                ).encode("utf-8")
                self._journal_identity = (
                    "research-" + hashlib.sha256(raw_id).hexdigest()[:32]
                )
                self._journal_revision = None
                if self.journal_store is not None:
                    # Re-entering an old phase without replaying simulated cash
                    # and venue identities would silently double-count fills.
                    prior = self.journal_store.load(
                        self._journal_identity, decision.plan,
                    )
                    if prior is not None:
                        raise BasketPlanError(
                            "组合研究阶段已有持久历史；重启前必须核对账户与成交"
                        )
                self._persist_checkpoint()
                self.target_events.append({
                    "ts_ns": ts_ns, "member_count": decision.member_count,
                    "targets": [
                        {
                            "product": leg.product, "symbol": leg.symbol,
                            "quantity": format(quantity, "f"),
                        } for leg, quantity in decision.plan.targets
                    ],
                    "executable": False,
                })
                for phase in decision.plan.phases:
                    if not self._execute_phase(phase, quotes, ts_ns):
                        break
                self.phase_audits.append(checkpoint_to_record(self.checkpoint))
                self.curve.append(self._snapshot(quotes, ts_ns))
                self.last_ns = ts_ns
            except BasketPlanError as exc:
                self._freeze("组合风险或历史财务证据未通过：" + str(exc))
                # A frozen result never implies a synthetic fill or continued
                # exposure reduction. Retain the already-confirmed prior state.
                try:
                    self.curve.append(self._snapshot(quotes, ts_ns))
                except BasketPlanError:
                    pass
                self.last_ns = ts_ns
            if self.status == "frozen":
                if self.checkpoint is not None and self.checkpoint.status != "completed":
                    self.checkpoint = freeze_checkpoint(
                        self.checkpoint,
                        (self.freeze_reason or "组合研究拒绝继续")[:512],
                    )
                    self._persist_checkpoint()
                    self.phase_audits.append(checkpoint_to_record(self.checkpoint))
                break
        if self.expected_funding is not None:
            expected = {
                (ts, leg) for ts, legs in self.expected_funding.items()
                for leg in legs
            }
            if expected != seen_funding:
                self._freeze("全窗口资金费结算计划存在缺失、越界或未证实的事件")
        if self.status != "frozen":
            self.status = "completed"
        last = self.curve[-1] if self.curve else None
        return {
            "mode": "generic_basket_offline_research_only",
            "status": self.status,
            "freeze_reason": self.freeze_reason,
            "executable": False,
            "bar_count": len(self.curve),
            "member_count": len(self.runtime.members),
            "starting_balance": format(
                sum(self.ledger.initial_cash.values(), Decimal(0)), "f",
            ),
            "ending_balance": last["total_equity"] if last else None,
            "funding_schedule_verified": self.expected_funding is not None
            and self.status == "completed",
            "verified_option_exercise_count": self.expiry_events,
            "orders": [],
            "fills": [
                event for event in self.ledger.events
                if event["kind"] == "simulation_fill"
            ],
            "cashflow_events": [
                event for event in self.ledger.events
                if event["kind"] != "simulation_fill"
            ],
            "target_events": self.target_events,
            "phase_audits": self.phase_audits,
            "equity_curve": self.curve,
            "execution": "offline_depth_limited_simulated_bbo",
            "financial_model": "per_product_usdt_cash_margin_fee_funding_and_expiry",
        }
