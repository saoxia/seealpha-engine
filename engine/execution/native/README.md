# SeeAlpha Execution — Nautilus native adapter

This is an independent execution crate under `engine/execution/native/`.
It is intentionally outside the root Rust workspace (`[workspace]` in
the local manifest). This change does **not** modify `engine/pipeline/`,
the existing market-data Nautilus source, or the main Engine Runtime.

## Ownership

- SeeAlpha Execution: validate an explicit order request and select the
  Nautilus native order API; observe native events.
- NautilusTrader v2.0.0rc9 (Rust 0.68.0): OrderFactory, Strategy order
  commands, RiskEngine, ExecutionEngine, Binance execution adapter, cache,
  OMS and reconciliation.
- Pipeline: market data and Catalog only. The existing legacy native
  Binance code in Pipeline is unchanged by this PR; its longer-term
  ownership cleanup is a separate architectural migration.

This first step creates a **manual standalone order probe**, not yet a
Runtime/Gateway order endpoint. It does not replicate Binance REST calls or
the Nautilus OMS. It also does not implement SeeAlpha Ledger settlement or
persistent cross-process order-intent handling.

## Input

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

Allowed `environment`: `testnet`, `live`. Allowed `kind`:
`market`, `limit`, `take_profit_market`; for `market`, omit
`price`. For `take_profit_market`, `price` is the conditional
trigger price, not a guaranteed execution price. `cancel_on_accept`
defaults to false; when true it sends Nautilus's native cancel command
after acceptance for manual testing. Cancellation is not guaranteed
until a venue cancellation event confirms it.

The input is deliberately **not** subject to a SeeAlpha quantity cap,
testnet requirement, artificial one-shot constraint or mandatory
post-acceptance cancel. Exchange protocol validation remains: for example,
Binance Spot does not accept `reduce_only`, quantities must be positive,
and account position mode must match Binance.

USD-M Hedge Mode closes the short leg using a BUY with
`reduce_only=true` at the Nautilus API level. The rc9 Binance adapter
maps this into `positionSide=SHORT` rather than sending an invalid
Binance `reduceOnly` parameter with a hedge-mode position side.

## Build and run

From the repository root:

```sh
cargo check --manifest-path engine/execution/native/Cargo.toml --all-targets
cargo test --manifest-path engine/execution/native/Cargo.toml

# Only validates the JSON; does not connect or submit:
cargo run --manifest-path engine/execution/native/Cargo.toml \
  --bin native_order_probe -- ./order.json --validate-only

# Choose exchange environment in order.json, then supply matching credentials:
export SEEALPHA_BINANCE_TESTNET_KEY='...'
export SEEALPHA_BINANCE_TESTNET_SECRET='...'
# For live: SEEALPHA_BINANCE_API_KEY / SEEALPHA_BINANCE_API_SECRET

# Submits the order described in JSON to the selected exchange environment:
cargo run --manifest-path engine/execution/native/Cargo.toml \
  --bin native_order_probe -- ./order.json
```

Do not commit credentials. The probe keeps the native node running
after submission so order events can be observed, and must be stopped
explicitly. Re-running is **not** guaranteed idempotent because this is
a command-line probe, not a durable production request bridge.

## Current acceptance state

- Native order creation, submission, optional cancel, callback source:
  implemented, awaiting compile and network tests.
- Native reduce-only and hedge-mode position mapping: confirmed in the
  pinned upstream source, not yet Binance exchange-tested.
- Conditional TP to USD-M algo endpoint: supported upstream, not yet
  independently exchange-tested from this entrypoint.
- Real accepted/rejected/partial fill/cancel and restart recovery:
  **not yet demonstrated**.
- Main SeeAlpha Runtime/Gateway/ledger integration: **not connected**.

References: [Nautilus Binance integration](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/docs/integrations/binance.md),
[Nautilus Rust Strategy API](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/docs/how_to/write_rust_strategy.md).
