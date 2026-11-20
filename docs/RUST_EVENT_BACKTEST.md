# Native Rust event backtest (experimental)

The `seealpha_backtest::timeline` API provides a deterministic, offline replay
kernel. This module is additive and does **not** replace the existing HTTP
backtest task or the legacy NautilusTrader/Python strategy runner.

## Implemented

- Multi-instrument timeline sorted by event timestamp, event type, instrument,
  and source order. Duplicate per-instrument timestamps are rejected.
- Strategy callbacks `on_start`, `on_bar`, `on_fill`, `on_funding`,
  `on_stop`. Callback orders execute no earlier than the next matching bar.
- Market and limit matching on OHLC, with a documented gap-through rule.
- Explicit per-instrument fee rates and product multipliers.
- Cash spot, fully paid long options (marked at quoted option price), and
  linear perpetual positions with average entry, realized PnL, unrealized PnL,
  initial and maintenance margin requirements.
- Funding cashflows only from supplied historical events, with no invented
  settlement cadence.
- Cross-product account equity, rejected order IDs, open orders and curve.

## Known gaps — not full production parity

- No order-book trades, bar volume liquidity caps, partial fills, cancel/replace,
  stop orders, slippage models or configurable fill priority.
- No automatic liquidation, insurance/ADL, hedge-mode long/short legs,
  isolated margin, multi-currency FX conversion or borrowing.
- No options expiry, exercise, assignment, contract-specific settlement.
- No cash deposits, withdrawals, dividends, dynamic fee tiers or borrow interest.
- No native NautilusTrader callback compatibility or Python strategy migration;
  the existing Rust HTTP job remains on its previous execution path.
- The Rust code has not passed the full workspace checks; CI workflows are
  intentionally paused. This is an implementation step, **not acceptance**.

## Deterministic event policy

A submitted order is queued until a subsequent bar for its instrument. The
matching price is the new bar's open for market orders, or the matching limit/
gap-open price for limit orders. At a timestamp, bars precede funding and ties
use instrument ID. Fees are assessed only on actual accepted simulated fills.
The account rejects orders that would create negative cash for fully-paid
products or violate available initial margin. A maintenance-margin breach
returns an error rather than silently fabricating a liquidation fill.

The scope must be completed, integrated into HTTP and native strategy loading,
and validated against independent fixtures before the legacy engine can be
considered replaced.
