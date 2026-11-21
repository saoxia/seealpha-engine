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

## Subsequent directories

- `engine/market_data/repair/`: existing Rust repair decisions and planning are partial; legacy Catalog reads/writes and orchestration remain.
- `engine/market_data/` and its child subdirectories: progressively cut over source supervision, history, completeness, repair and Nautilus v2 storage compatibility.
- Other legacy Engine folders: prioritize after an actual tree-and-import inventory rather than guessing a deletion sequence.

## Current batch status

**Partial: no Python deleted.** The native coordinator now validates membership before removing resting orders in `flatten`, with failure-atomicity and member-isolation tests in `portfolio/core/tests/coordinator_atomicity.rs`. This is a Rust safety fix, not a Python cutover. The added `legacy_target_parity.rs` covers the native target selection, bounds, signed quantity and allocation cases. The native fixture suite has **not been executed** in this connector session, and no identical-input Python-versus-Rust comparison or legacy import inventory has been run. Do not treat this as a completed folder cutover or enable live trading based on it.
