# Portfolio/Basket Rust integration contract (research-only)

This crate does not execute orders and does not authenticate external facts. The
Ledger/Runtime integration must provide an authenticated execution adapter and
a durable transaction boundary. Do **not** infer venue fills from acknowledgements,
bars, prices, funding rates, or target publications.

## Coordinator (single instrument)

- Construct `coordinator::Coordinator::new(increment, risk_limit)` for a
  scoped account/instrument, then register strategy members.
- Publish signed absolute member targets with `report_target`. The
  `risk_exit` flag cannot reverse or increase the member's existing target.
- `accept_order` and `modify` maintain virtual orders. Conditional orders
  arm after their creation/amendment bar, not on that same bar.
- Once all strategies have published for a closed bar,
  `reconcile(bar, authoritative_account_position, command_id)` produces at
  most one `Command::Submit` or `Command::Cancel` intent. Before any
  external effect, persist the returned state and unique intent identity with
  an atomic Ledger CAS transaction. Never transmit based on a rejected CAS.
- Only a verified execution update may call `apply_venue_fill`; its
  `cumulative` input means **cumulative quantity for the particular working
  venue order**, not last-fill quantity. It is not an ACK. Callers authenticate
  its order, execution identity, freshness, and account before passing it in.
- A rejected venue fill must never commit even a partial virtual allocation;
  only the freeze reason may change, and the original in-flight order identity
  remains unresolved for independent native order/account reconciliation.
- Partial fills keep `flight` in place. Completed request IDs are retained
  across snapshots and cannot be reused for later intents. Unattributable
  venue quantity, conflicting event IDs, an unknown order, and cancellation
  denial freeze the coordinator; they do not create a replacement order.
- Persist with `coordinator_snapshot::encode_coordinator`. Decode only
  same-scope records; `flight` and freeze survive restart. A decoded state is
  not permission to resubmit its outstanding order.

## Basket (multi-product)

1. Build a `basket::Plan` with exact Decimal quantities and authoritative
   market evidence. `Plan::executable` must remain false.
2. Before starting each phase, call
   `phase_gate::gate_next_phase(checkpoint, positions, quotes, limits, pairs)`.
   The caller must supply an authenticated same-account position snapshot,
   bounded quote ages, and **explicit disjoint hedge pairs** when a portfolio
   contains more than one Spot/UM pair.
3. The gate uses each leg's no-fill/full-fill endpoints independently to
   upper-bound gross exposure and hedge drift. With options, it assumes the
   protective legs may fill **after** futures. Admission can therefore fail
   even when the final target is perfectly hedged. Reduce the phase lot size,
   increase independently justified transient risk budget, or stage and confirm
   protection first. Do not silently bypass the gate.
4. `Checkpoint::begin(request_id)`, `order_evidence::bind_phase_orders`,
   and the updated `journal::encode_checkpoint` must be persisted in the
   *same durable CAS pre-registration* before the external adapter sees any
   bound order intent. A duplicate CAS or unresolved request is not a new
   submission.
5. Query all venue orders and the same-account positions via a **trusted**
   adapter. `reconcile_order_evidence` consumes terminal status, order ID,
   execution ID, cumulative fill, timestamps, and account positions. It
   rejects static acceptance as a fill and freezes on partial, canceled,
   unknown or inconsistent multi-leg outcomes. The partial-fill account
   check is performed even when one counter-leg has not filled.
6. Evaluate `risk::observe_phase` on each authoritative state update
   (including partial fills and heartbeat/timeout). Persist the returned
   frozen checkpoint with CAS before any further action. No admission,
   recovery or order retry occurs while frozen or pending.
7. `journal::decode_checkpoint` verifies a versioned checksum and
   deterministically replays events. Pending/frozen state is retained across
   restarts. A completed journal is immutable.

## Python comparison

| Behavior | Python reference | Rust |
| --- | --- | --- |
| Signed targets, netting, Decimal steps | `target_netting.py`, `portfolio_coordinator.py` | `lib.rs`, `coordinator.rs` |
| Virtual market/limit/stop and next-bar eligibility | `portfolio_coordinator.py` | `coordinator.rs` |
| Bound order identities, partial fills, failed/unknown outcome | `basket_order_evidence.py` | `order_evidence.rs` |
| Multi-leg stages and replay | `basket_targets.py`, `basket_reconciliation.py` | `basket.rs`, `journal.rs` |
| Realized hedge drift and option coverage | `basket_risk_monitor.py` | `risk.rs` |
| Worst-case independently filled phase admission | No corresponding pre-order admission function | `phase_gate.rs` (deliberately stricter) |
| Cancel rejection | Keeps an in-flight order and suppresses same-bar replacement | Freezes original in-flight order until independent reconciliation (deliberately stricter) |

## Unresolved boundaries and verification

- Authentication of venue order/execution/position records is NOT provided by
  this crate. No signature, account attestation, or live order transport exists.
- An integration must implement durable CAS for every transition and ensure a
  trade is never sent merely because a function returned an order intent.
- `Coordinator::confirm_terminal` still relies on a trusted caller to verify
  the terminal event and account snapshot; it must not be wired to a placement
  ACK. A separately authenticated terminal-reconciliation adapter is required.
- Fees, cash ledger/available margin and portfolio-wide cross-account risk
  require authoritative Ledger/Runtime accounting; this crate's notional
  guard is not a margin engine.
- Python/Nautilus callback scheduling and session-level fill attribution have
  not yet been exercised end-to-end against the Rust Runtime.
- The fault-injection tests here use offline synthetic data. They are NOT
  proof of Binance behavior or a production-compatible venue adapter.

Required verification on an environment with a Rust toolchain:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test -p seealpha-execution --locked
cargo test --workspace --locked
python3 -m unittest discover -s tests -t . -p '*_test.py' -v
```

## TargetHolding migration slice (2026-11-21)

The native `seealpha-execution` crate now exposes `TargetHolding::new` and
`TargetHolding::signed_quantity`, mirroring the legacy
`engine/execution/targets.py` contract for absolute signed quantity or bounded
weight-based quantity. A target must specify exactly one of `quantity` and
`weight`; weight must be between -1 and 1 (inclusive), and the allocation
must be strictly positive even when an absolute quantity is supplied. Rust
`Decimal` is inherently finite and uses checked multiplication.

These functions are an incremental parity primitive, **not** a claim that
the legacy strategy publishing / coordinator / HTTP routes have been
replaced end-to-end. No production entrypoint is changed and the legacy
Python module remains available until differential tests and owner-approved
retirement. The new Rust tests exercise valid and invalid quantities, but
have not been executed in the GitHub editing environment. Before cutover run:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test -p seealpha-execution --locked
cargo test --workspace --locked
python3 -m unittest discover -s tests -t . -p '*_test.py' -v
```

## Native bar-synchronous holding replay

The Runtime calls `replay_holdings_by_bar` to resolve member quantity/weight
publications, retain the last target for each member within a closed bar,
net member targets once per bar, and generate paper deltas. Each batch rejects
mixed symbols, out-of-order bar indices, inconsistent close prices and invalid
allocations before applying fills.

The obsolete per-publication `replay_holdings` entry point was removed:
it could create artificial same-bar churn and was not called by Runtime.
`replay_targets` remains the underlying paper execution primitive and is
not a live venue execution path. Live orders and executions must use the
Nautilus-integrated Runtime path; do not treat paper fills as exchange facts.

## Historical carry data boundary

Spot/UM bar alignment and observed funding-rate merging now live in
`seealpha-backtest::carry_history`, not in execution. They are historical
observations and must not generate exchange fills or funding cash movements.
Execution uses authoritative position/order evidence; Ledger records actual
exchange-confirmed funding and settlement movements.

## Native account evidence boundary

Removed the unused `execution::account_evidence` implementation that
decoded Binance Spot/UM account JSON into a separate account view. Account
events and runtime balances belong to Nautilus `AccountState`, with typed
observations already supplied by `pipeline/sources/shared/nautilus` and
subscribed through the Binance account pipeline. The SeeAlpha execution crate
must not parse exchange account JSON, create an alternative live account
cache, or treat a reported event as proof of venue authenticity.

Independent tenant/account/venue provenance checks and immutable economic
movement evidence remain required before Ledger attribution or settlement.
The deletion does not assert that the native live account session has passed
production acceptance or that any genuine account reconciliation is complete.

## Option exercise settlement ownership

The uncalled `execution::settlement` offline cash/payoff reconciliation
entrypoint has been removed. Exchange-signed option exercise evidence is
normalized in `engine/connection/binance-options/src/settlement.rs`.
Durable economic events and strategy attribution belong to Ledger; native
account state and reports must be independently reconciled. Do not infer
account cash movements or mark settlement complete from modelled option
payoff formulas.
