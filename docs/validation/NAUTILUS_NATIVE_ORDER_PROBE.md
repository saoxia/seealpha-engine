# Nautilus native Binance order probe (2026-11-23)

## Purpose

This is a minimal, operator-controlled order entry and event-observation
program for Binance Spot and USD-M. It uses the pinned NautilusTrader
v2.0.0rc9 (Rust 0.68.0) `LiveNode`, `OrderApi`, `Strategy::submit_order`,
`Strategy::cancel_order`, native RiskEngine, ExecutionEngine,
BinanceExecutionClient and order/position event handling.

It **does not** implement a second exchange sender or a second OMS.

There are **no SeeAlpha-imposed position-size caps, forced sandbox, one-shot
request IDs, manual arm flag, or automatic cancellation**. The operator picks
`environment: "live"` or `"testnet"` and the actual order quantity.
`cancel_on_accept` defaults to false and is an optional way to test
native cancellation. Without `--validate-only`, running the CLI attempts
to submit the specified order when the native node starts.

Basic protocol checks remain: positive quantity/price, known product,
position mode and side, and no `reduce_only` for Binance Spot. These are
request validation and exchange contract requirements, not SeeAlpha risk limits.

**Important scope distinction:** This independent CLI is not connected to
SeeAlpha's main Engine Runtime, HTTP Gateway, Ledger, or production orchestration.
It does not persist native order IDs across processes, certify actual fills,
provide cross-restart exactly-once submission, or expose a general external
cancel/modify API. Native recovery and integration acceptance remain pending.

## Example order

This is an illustrative USD-M short closing limit order at 1000 USDT,
provided that the exchange account really operates in one-way mode and
the position exists. Select the environment appropriate to your account.

```json
{
  "environment": "testnet",
  "product": "futures_um",
  "position_mode": "one_way",
  "symbol": "ETHUSDT",
  "account_id": "BINANCE-UM-001",
  "side": "buy",
  "kind": "limit",
  "quantity": "2",
  "price": "1000",
  "reduce_only": true,
  "cancel_on_accept": false
}
```

- To use a normal market order, choose `kind: "market"` and omit `price`.
- For a venue-side take-profit market trigger, choose
  `kind: "take_profit_market"`, where `price` is a **trigger**, not
  an execution-price guarantee.
- On USD-M in **one-way** mode, Nautilus sends `reduceOnly`.
  In **hedge** mode, its Binance adapter maps a reduce-only BUY to
  `positionSide=SHORT` (and SELL to `LONG`); do not send a mismatched
  position mode.
- To test cancellation, set `cancel_on_accept: true`. The order might fill
  before a cancel response; receipt of a cancel command is not confirmation.
- Spot supports ordinary limit and market orders, but not `reduce_only`.

## Build and run

```sh
cargo check --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml --all-targets
cargo test --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml --lib order_probe::

# Only validate the local order contract; no connection and no order.
cargo run --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml \
  --bin native_order_probe -- ./order.json --validate-only

# Order is sent to the environment configured in the JSON.
# Use these names for a Testnet credential:
export SEEALPHA_BINANCE_TESTNET_KEY='...'
export SEEALPHA_BINANCE_TESTNET_SECRET='...'

# Or for a Live credential:
# export SEEALPHA_BINANCE_API_KEY='...'
# export SEEALPHA_BINANCE_API_SECRET='...'
cargo run --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml \
  --bin native_order_probe -- ./order.json
```

The node keeps running after submission to receive execution events and
process native reconciliation. Stop it explicitly and gracefully. It does not
silently cancel open orders on exit or guarantee that stopping the program
cancels orders at Binance.

The fixed `TraderId` and strategy ID are currently appropriate only for a
single isolated CLI process; do not use overlapping processes with the same
venue account and identity, and do not assume a repeated invocation is
idempotent. Nautilus order IDs are generated anew per process. Production
integration must control persistent identities, account ownership and
reconciliation before routing external target-position instructions.

## Validation matrix

| Behavior | Evidence | Status |
| --- | --- | --- |
| Native MARKET / LIMIT create, submit | Source wiring | Implemented; compile check pending |
| Native cancel on acceptance (optional) | Source wiring | Implemented; exchange round trip pending |
| Submitted, Accepted, Rejected, Filled, Canceled | Nautilus event callbacks | Implemented; exchange round trip pending |
| USD-M limit with reduce-only | rc9 Binance native adapter contract | Static source inspection only |
| USD-M hedge-mode close-side mapping | rc9 position side conversion | Static source inspection only |
| Conditional take-profit market | `MARKET_IF_TOUCHED` / Binance algo API | Static source inspection only |
| Live/Testnet selected by config | Native factory configs | Source wired; not connected here |
| Partial fill and restart reconciliation | Independent native/exchange evidence | Not verified |
| Integration into SeeAlpha Engine / Ledger | End-to-end contract and evidence | Not implemented |

References:
- [NautilusTrader v2.0.0rc9 Binance support](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/docs/integrations/binance.md)
- [Nautilus Rust strategy order API](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/crates/trading/src/strategy/api.rs)
- [Nautilus Binance hedge-mode mapping](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/crates/adapters/binance/src/futures/conversions.rs)
