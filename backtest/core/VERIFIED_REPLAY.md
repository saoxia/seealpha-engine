# Verified offline execution replay (opt-in, not Nautilus parity)

Module: seealpha_backtest::timeline::verified

This is an additive research-only engine. Existing timeline::replay and matching::match_bar still use **hypothetical OHLC fills** and MUST NOT be used as execution-liquidity evidence. No runtime, HTTP, data-source, or strategy-host integration is changed by this PR.

## Integration contract

1. Register Contract definitions with stable instrument IDs, a single quote currency, exact multiplier, taker fee, initial/maintenance margin ratios, and (for cash options) an explicit underlying, right, strike, and expiry. The caller must validate definitions against native Nautilus/instrument metadata; this module does not invent them.
2. Supply Event::Quote { bid, ask, bid_size, ask_size } from strictly historical, contract-specific QuoteTicks. Event::Trade only triggers stops; Event::Mark marks holdings but creates no fills. Source deduplication and interval coverage must occur upstream. Events are deterministically sorted by (timestamp, event type, instrument, source order); duplicate timestamps for the same instrument/event kind are rejected.
3. Supply Event::Funding **only when a settlement has been independently verified**, with actual settlement time, signed rate, mark price and nonempty unique evidence ID. FundingRateUpdate observations and absence of events do not prove settlement or completeness. There is no 8-hour cadence.
4. Supply Event::OptionSettlement only with a verified contract-specific cash-settlement record at the registered expiry, actual underlying settlement price and nonempty unique evidence ID. No expiry event means no exercise/assignment; remaining positions at or beyond expiry fail closed. Physical exercise/assignment is unsupported.
5. Call replay(&contracts, &events, Policy { starting_cash, max_mark_age_ms, taker_slippage_bps }, &mut strategy). Implement Strategy::on_start, on_quote, on_trade, on_fill, on_funding, on_option_settlement, on_stop as needed; return Command::Submit/Cancel/Replace.
6. Inspect Report::{fills,settlements,rejected,pending,cash,equity,positions,funding,commissions,curve}. A filled quote consumes bid/ask size once, shared FIFO by insertion order; there are no inferred maker fills, no L2 walk and no positive execution claims when zero quoted liquidity. All callback orders wait for a *strictly later timestamp* (except on_start, which occurs before the first event).
7. The report always sets historical_coverage_certified=false. The caller must separately document feed continuity, clock accuracy, missing settlement events, quote age, account provenance, order-ack uncertainty and fee-tier evidence. This crate never sets data READY and never submits exchange orders.

Execution model: taker market orders and marketable taker limits use the recorded ask for buys or bid for sells, plus a configurable **adverse** bps adjustment. A limit is never exceeded for the sake of slippage. Resting limits cannot become maker fills from an OHLC touch or public print. Stop-market/stop-limit instructions require a subsequent trade to cross the stop, and then a still-later executable quote. Unexecuted residual quantity remains pending until cancelled/replaced or simulation ends. Commission is charged on actual fill notional. All cash products are long-only and fully paid. Perpetual cash moves only for realized PnL, fees and evidenced funding, **not** notional; option cash moves on premium purchase/disposal and verified cash settlement. Cash plus unrealized perpetual PnL, excluding spot and option marks, is the conservative margin collateral base.

## NautilusTrader 2.0.0rc9 comparison

The legacy Python system constructs native Nautilus BacktestEngine venues:
- carry_simulation.py uses distinct Spot CASH and USD-M MARGIN accounts, real native fill callbacks and FundingRateUpdate that explicitly marks its next_funding_ns equal to ts_event for settled-event evidence;
- generic_simulation.py coordinates multiple strategy legs with previous-bar signals and refuses incomplete synchronized frames;
- protected_simulation.py and option_history.py insist on historical option QuoteTick ask prices/sizes, native option identities, and fill-before-risk phase ordering.

Nautilus offers book-type-specific matching (L1 vs L2/L3), probabilistic fill/slippage models, liquidity consumption choices, OMS netting/hedging, venue balances and native exchange/order events. The verified Rust kernel intentionally implements a smaller conservative subset. It is not bit-for-bit or order-by-order equivalent to Nautilus 2.0.0rc9, even when some fixture balances agree. True Python/Rust differential fixtures must run both systems with the **same normalized historical events, instrument precisions, fee tier and venue matching configuration** before parity is claimed.

## Remaining gaps

- No L2/L3 market-depth queue, passive maker priority, maker rebates, variable fee tiers, exchange trading filters, order-in-flight latency, IOC/FOK/time-in-force, trailing stops, bracket/OCO, simulated exchange rejections or matching of bar-only historical windows.
- No liquidation, insurance/ADL, isolated margin, borrow, short spot, physical option exercise/assignment, fractional settlement delivery, multi-currency FX, or separate venue cash/margin accounts.
- No automatic feed/catolog coverage certification, settlement oracle, data READY, native Nautilus callbacks or Python strategy package bridge.
- Existing legacy bar-close run_backtest and hypothetical matching helpers are unchanged and remain non-parity paths. Runtime/HTTP must consciously opt into the verified module; there is no automatic switch.

## Offline test commands and status

Run from the repository root, on a machine with the pinned Rust toolchain and dependencies installed:

    cargo test -p seealpha-backtest --locked
    cargo fmt --all --check
    cargo clippy --workspace --all-targets --locked -- -D warnings
    cargo test --workspace --locked
    python3 -m unittest discover -s tests -t . -p '*_test.py' -v

The Rust fixtures cover partial fills, shared displayed liquidity, limit and stop order timing, cancellation and replacement, slippage limits, fees, explicit funding, cash-settled options, expiry without proof, cross-product equity, realized PnL, margin rejection/breach, duplicate evidence and stale marks. They are **not yet executed** in this PR's authoring environment: Cargo is not installed and the repository's GitHub Actions workflows remain paused. Do not interpret the presence of test source as a passing test result.
