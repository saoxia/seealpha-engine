# Python-directory Rust cutover ledger

This ledger treats a Python directory as a migration unit, **not** as permission to delete the directory. The default Engine is Rust; the opt-in legacy Python Engine still carries behavior not proven equivalent. The migrated code must be invoked by the active Rust runtime rather than merely exist in a crate.

## First migration unit: `engine/portfolio/`

| Legacy responsibility | Rust target | Current evidence | Cutover gate |
| --- | --- | --- | --- |
| `engine/portfolio/targets.py` immutable target, selection, bounded weight, allocation conversion | `portfolio/core/src/lib.rs` `TargetHolding` | Unit coverage and `portfolio/core/tests/legacy_target_parity.rs` native fixture tests | Run both implementations on the same fixture matrix, compare values and error behavior; confirm every import site has migrated |
| Target netting, paper delta and clipping | `portfolio/core/src/lib.rs` | Native unit tests exist | Compare replay order, rounding, netting, persistence and error paths against legacy scenario fixtures |
| Portfolio coordinator and failure handling | `portfolio/core/src/coordinator.rs` and application integration | Rust implementations exist | Inventory legacy call sites and externally visible side effects, including cancel/unknown-order behavior; prove parity on isolated stores |

The complete legacy directory file inventory and all import sites **must be determined from the checked-out tree before marking this unit complete**. Do not infer completion from these representative rows.

## Directory-by-directory process

1. Inventory every tracked `.py` file under one legacy directory, its imports, callers, HTTP routes, callbacks, state tables and shared contracts. Record file counts and Python LOC at the starting commit.
2. Create a fixture matrix from the legacy implementation, including boundary decimals, temporal ordering, malformed payloads, restart/idempotency behavior and error responses.
3. Implement the corresponding Rust domain and wire all production consumers to the Rust path. Any temporary Python host must have a documented narrow interface; legacy runtime remains isolated.
4. Validate `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, `cargo build --workspace --locked --release`, the focused Python legacy tests, and side-by-side offline parity tests. GitHub Actions are currently paused: no green check may be assumed.
5. Only after route/storage/failure parity and explicit owner approval, remove the obsolete Python files, imports, scripts and dependencies. Verify no remaining reference and measure Python LOC deleted. Never share writeable live state or automatically retry unknown exchange orders.

## Native coordinator integration (follow-up)

`portfolio/core/src/coordinator.rs` now exposes `Coordinator::report_holding` to accept a validated `TargetHolding`, explicit allocation and risk-exit intent. The Rust coordinator computes signed quantity and applies existing risk-exit rules; invalid allocation or risk-exit attempts leave its state unchanged. Regression tests live in `portfolio/core/tests/coordinator_holdings.rs`.

**Not cut over:** no legacy Python caller or HTTP/application caller has yet been rewired to this new method. It is an opt-in Rust entrypoint, not a proven end-to-end replacement for the legacy coordinator. The legacy directory still requires full file/call-site inventory and cross-language replay parity. No Python code is removed in this batch.

## Active paper-session weight path (follow-up)

`application/runtime/src/session.rs` previously parsed every host publication as nonempty `quantity`, even though the restricted Python strategy host and the issued historical-backtest path accept either `quantity` or `weight`. The paper-session adapter now reads each strategy's validated `prepared.allocation`, constructs `TargetHolding` for either field, and uses Rust `replay_holdings` for all paper fills. The published report keeps the old `quantity` JSON shape for quantity targets and reports `weight` for weight targets. Tests cover both views and invalid allocation.

This is **real application-layer integration**, but it does not route this historical paper replay through `Coordinator::report_holding` (that remains a separate persisted coordinator API). The legacy Python Engine, Python strategy host, and user-authored Python packages are unchanged. A focused Rust integration test now constructs an isolated Python strategy package that publishes weight=0.5 with allocation=2, runs through the real strategy host, native paper replay and ledger, and checks one paper fill, no exchange orders and idempotent request replay. The test has been **added but not executed**. Same-fixture legacy Python versus Rust parity remains required.

## Bar-synchronous paper execution correction

The original ordered publication replay in `portfolio/core/src/lib.rs` produces a simulated fill after **each** publication, even if two members publish offsetting targets at the same bar close. The already-native historical replay groups publications by bar; paper sessions now follow the same principle via `IndexedHoldingPublication` and `replay_holdings_by_bar`.

- Strategy publications carry the original `bar_index` and close price. A batch with mixed symbols, decreasing bar index, or inconsistent same-bar prices fails closed.
- A member's latest target persists across bars; repeated publications by that member on one bar use the final one. The entire batch is validated before generating any fills.
- At each bar close, calculate the signed net of the latest member targets and emit **at most one** delta fill after the existing risk clipping and quantity increment rules. A fully offsetting simultaneous pair generates no paper fill.
- New tests cover same-bar offsets, full hedging, repeated member publications, cross-bar continuation, inconsistent evidence and invalid allocation.

The change affects the Rust Engine's paper-session accounting semantics and must be compared against expected production behavior. Tests are committed but **not executed**; do not merge before cargo checks and regression review. The legacy Python Engine remains intact.

## Subsequent directories

- `engine/market_data/repair/`: existing Rust repair decisions and planning are partial; legacy Catalog reads/writes and orchestration remain.
- `engine/market_data/` and its child subdirectories: progressively cut over source supervision, history, completeness, repair and Nautilus v2 storage compatibility.
- Other legacy Engine folders: prioritize after an actual tree-and-import inventory rather than guessing a deletion sequence.

## Current batch status

**Partial: no Python deleted.** The native coordinator now validates membership before removing resting orders in `flatten`, with failure-atomicity and member-isolation tests in `portfolio/core/tests/coordinator_atomicity.rs`. This is a Rust safety fix, not a Python cutover. The added `legacy_target_parity.rs` covers the native target selection, bounds, signed quantity and allocation cases. The native fixture suite has **not been executed** in this connector session, and no identical-input Python-versus-Rust comparison or legacy import inventory has been run. Do not treat this as a completed folder cutover or enable live trading based on it.
