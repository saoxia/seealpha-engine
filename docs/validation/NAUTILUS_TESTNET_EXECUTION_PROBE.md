# Nautilus Binance execution — isolated testnet probe (2026-11-23)

## Scope and non-goals

This is an opt-in, one-shot **Binance Testnet only** probe in the separately
built `engine/pipeline/sources/shared/nautilus/` crate (NautilusTrader
v2.0.0rc9, Rust crates 0.68.0). It is **not** wired to the production Engine,
Runtime, Gateway, SeeAlpha Ledger, or any production Docker image.

- Reuses the native `LiveNode`, `RiskEngine`, `ExecutionEngine`,
  `BinanceExecutionClientFactory`, order factory and strategy callbacks.
- Tests a single GTC `LIMIT` order or a futures `MARKET_IF_TOUCHED`
  conditional take-profit order. On native `OrderAccepted`, issues the
  native `cancel_order` command. Receipt of `OrderAccepted` or a cancel
  request **does not** prove a venue cancellation.
- `LIMIT + reduce_only` is the default safer way to buy back an existing
  short at a stated *maximum buy price*. A `MARKET_IF_TOUCHED` buy at a
  lower trigger uses Binance `TAKE_PROFIT_MARKET` via the adapter; it **does
  not guarantee an execution price**.
- For USD-M in one-way mode, `reduce_only` is an exchange field. In hedge
  mode, the upstream adapter maps a reduce-only buy into
  `positionSide=SHORT` instead of sending a forbidden `reduceOnly` wire
  parameter. Verify the account's **actual** position mode before using it.
- Binance Spot has no `reduceOnly`. The probe refuses Spot reduce-only and
  refuses conditional market take-profit orders without explicit USD-M
  `reduce_only` intent.
- Does not test or implement Spot OCO, USD-M arbitrary algo families, batches,
  order replacement, external request intake, durable server-side authorization,
  production recovery, or certified Ledger posting.

Upstream version-specific evidence:
- [Nautilus v2.0.0rc9 Binance integration](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/docs/integrations/binance.md)
- [Nautilus v2.0.0rc9 futures position-side mapping](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/crates/adapters/binance/src/futures/conversions.rs)
- [Binance USD-M official trading rules](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade)

## 1. Offline checks (no API credentials)

```sh
cargo test --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml --lib
cargo test --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml --test native_execution_node
cargo check --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml --bin native_order_testnet
```

The dry-run contract accepts a local JSON file. Create a dedicated, private
state directory. For example, in a new Testnet account **already known to be
one-way**, and with a verified existing short:

```sh
mkdir -p /tmp/seealpha-testnet-probe
chmod 0700 /tmp/seealpha-testnet-probe
cat >/tmp/eth-exit-probe.json <<'JSON'
{
  "request_id": "TESTNET-EXIT-0001",
  "product": "futures_um",
  "position_mode": "one_way",
  "symbol": "ETHUSDT",
  "account_id": "BINANCE-UM-TESTNET-001",
  "side": "buy",
  "kind": "limit",
  "quantity": "0.01",
  "price": "1000",
  "reduce_only": true
}
JSON
cargo run --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml \
  --bin native_order_testnet -- /tmp/eth-exit-probe.json /tmp/seealpha-testnet-probe
```

This invocation only parses and validates the intent; it will not connect.
The exchange's instrument precision, step size, min notional, position
side and account/position availability must also pass actual preflight
checks during an authorized online test.

## 2. Explicit one-shot testnet order send (manual operator only)

**Do not execute unless the Testnet account, current position and
permissions have been independently checked.** The program will try to
submit exactly one order and cancel after acceptance; fills may happen
before the cancel, and a failed disconnect may leave an order open.

Use a NEW `request_id` per authorized experiment, and never rerun the same
request. Supply the Testnet API key/secret through the process environment
(never commit them). The only possible venue environment is
`BinanceEnvironment::Testnet`.

```sh
export SEEALPHA_BINANCE_TESTNET_KEY='...'
export SEEALPHA_BINANCE_TESTNET_SECRET='...'
cargo run --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml \
  --bin native_order_testnet -- /tmp/eth-exit-probe.json \
  /tmp/seealpha-testnet-probe --submit-testnet-once
```

A `request_id.attempt` file is atomically created before the client can
connect and the native `client_order_id` is fsynced immediately before
the submit command. An occupied marker (including one from an interrupted
startup) **always blocks replay**; resolve exchange state manually. The
account-scoped file lock prevents concurrent probes using the same state
root, but is **not** a cross-service or cross-host account ownership lock.
The operator must ensure no other sender uses the account concurrently.

The binary leaves the native node running to receive events until it is
stopped explicitly. Stop it gracefully, then inspect exchange order status
and fills by native client/venue IDs. It does not mark any real Ledger
balance as settled.

## 3. Acceptance matrix

| Case | Required evidence | Current state |
| --- | --- | --- |
| LIMIT validation/creation | Offline config, OrderApi compile | Implemented source; CI pending |
| Submit -> Accepted/Denied/Rejected | Testnet native event log and exchange match | Not run (no account credentials) |
| Accepted -> Cancel request -> Canceled | Both venue and native confirmed | Not run |
| Partial/Full fill | Native fill plus exchange trade ID, dedup | Not run |
| Unknown submit and disconnect | No blind retry; reconcile by client ID | Not run |
| One-way reduce-only | Native field and Binance order-side match | Source inspected; Testnet not run |
| Hedge-mode short close | Native BUY reduce-only maps SHORT leg | Source inspected; Testnet not run |
| Conditional TP | Native MARKET_IF_TOUCHED to Binance algo endpoint, cancel pre-trigger | Source inspected; Testnet not run |
| Restart and durable recovery | Native cache, venue reports, unknown responses, no double send | Not run |
| Production readiness | Authorization, evidence, recovery, external ownership, risk limits | Explicitly **not ready** |

This is a testing vehicle, not a replacement for SeeAlpha's missing production
intention authorization, cross-process serialization, real-fill provenance
and reconciliation acceptance gates. The production gateway and engine
must remain unable to submit orders until those gates are closed.
