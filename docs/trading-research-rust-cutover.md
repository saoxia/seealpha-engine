# Trading and research Rust cutover (2026-11-21)

## Scope

Retire the inactive Python source directories `engine/trading/` (3 files)
and `engine/research/` (10 files). The primary entrypoint is the Rust
`seealpha-engine` binary; `python -m engine` already refuses startup.
No Python module is imported into the Rust binary. The strategy package host
under `python/strategy_host/` is **not** deleted.

This is a **source retirement**, not a claim of event-level or API parity with
the old Python/NautilusTrader runtime. The remaining legacy Python scripts and
tests that import these deleted modules are not supported runtime entrypoints.
Historical source remains accessible in Git history.

## Trading ownership

| Retired Python source | Native Rust owner |
| --- | --- |
| `engine/trading/runtime.py`: tenant/runnable checks, account snapshots, order commands, reconciliation and worker orchestration | `application/runtime/src/lib.rs`, `accounts.rs`, `trading.rs`, `session.rs`, `jobs.rs`; `connectivity/venue/` |
| `engine/trading/store.py`: local request/order/fill/position/event persistence, tenant scoping and strategy/backtest records | `accounting/ledger/` plus `application/runtime/src/order_journal.rs` and job/session adapters |
| `engine/trading/__init__.py` | No native functionality |

The Rust order path retains pre-send durable UNKNOWN registration,
request-id replay, scoped reconciliation and fail-closed error handling.
**External live order writes remain disabled by the Rust runtime**; no
legacy live-order code is made available as a fallback.

## Research ownership and known gaps

| Retired Python source | Native Rust owner or disposition |
| --- | --- |
| `auth.py` | `application/research/src/lib.rs` (signed grants) and `scope.rs` (tenant/task/dataset checks) |
| `mcp.py` | `gateway/mcp/src/research.rs` (native research MCP route) |
| `basket_research_session.py`, `issued_basket.py` | `portfolio/`, `alpha/strategy/` and `application/runtime/` own native basket, issued strategy and offline replay workflows; Python callback parity **not** asserted |
| `research_native.py` | `backtest/`, `market_data/market/`, and `application/runtime/src/verified_replay.rs`; detailed Nautilus L2/Greeks parity **not** asserted |
| `candidate_package.py`, `candidate_template.py`, `python_candidate.py` | `alpha/strategy/src/lib.rs` provides native archive inspection and host policy, and `application/research/src/scope.rs` validates the research envelope. The native research candidate **submission endpoint is currently unavailable** |
| `snapshots.py` | `application/research/src/scope.rs` validates snapshot ownership, bounds and evidence. Native HTTP snapshot create/read endpoints **currently return unavailable**; there is no claim of native Parquet export parity |
| `__init__.py` | No native functionality |

The native research data-set list and restricted backtest read/submit/cancel
routes remain the supported research API subset. Snapshot exports, candidate
submission and old Python-only research behaviors must be implemented and
tested separately before they can be advertised as supported Rust features.
Never silently present synthetic/fallback data as authoritative snapshots.

## Verification and operational boundary

Review the full tracked-file inventory above and re-check references on an
actual checkout. CI workflows are not evidence of a run. This GitHub-connected
editing environment cannot execute Cargo tests; therefore **no Cargo check,
Clippy, test, release build, Python regression or end-to-end parity run is
claimed**.

On a Rust-capable checkout run:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
```

Do not enable production live trading or deploy from this source deletion.
