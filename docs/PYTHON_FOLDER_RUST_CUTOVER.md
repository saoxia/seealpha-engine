# Python-directory Rust cutover ledger

This ledger tracks replacement by Python module within a directory. The default Engine is Rust. When the corresponding native Rust functionality is already implemented and integrated, the redundant Python source is removed without waiting for unrelated Decimal formatting or cleanup verification. The separate legacy Python Engine may lose compatibility when its imported modules are retired.

## First migration unit: `engine/portfolio/`

| Legacy responsibility | Rust target | Current evidence | Cutover gate |
| --- | --- | --- | --- |
| `engine/portfolio/targets.py` immutable target, selection, bounded weight, allocation conversion (**deleted on this PR**) | `portfolio/core/src/lib.rs` `TargetHolding` | Rust model wired to paper runtime and native coordinator; fixture tests authored but unexecuted | Follow up on any remaining legacy-only imports and adjust/remove stale tests |
| Target netting, paper delta and clipping | `portfolio/core/src/lib.rs` | Native unit tests exist | Compare replay order, rounding, netting, persistence and error paths against legacy scenario fixtures |
| Portfolio coordinator and failure handling | `portfolio/core/src/coordinator.rs` and application integration | Rust implementations exist | Inventory legacy call sites and externally visible side effects, including cancel/unknown-order behavior; prove parity on isolated stores |

The complete legacy directory file inventory and all import sites **must be determined from the checked-out tree before marking this unit complete**. Do not infer completion from these representative rows.

## Directory-by-directory process

1. Inventory every tracked `.py` file under one legacy directory, its imports, callers, HTTP routes, callbacks, state tables and shared contracts. Record file counts and Python LOC at the starting commit.
2. Create a fixture matrix from the legacy implementation, including boundary decimals, temporal ordering, malformed payloads, restart/idempotency behavior and error responses.
3. Implement the corresponding Rust domain and wire all production consumers to the Rust path. Any temporary Python host must have a documented narrow interface; legacy runtime remains isolated.
4. Validate `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, `cargo build --workspace --locked --release`, the focused Python legacy tests, and side-by-side offline parity tests. GitHub Actions are currently paused: no green check may be assumed.
5. Once a specific legacy module's behavior is present in the active Rust path, remove that redundant Python module. Review its remaining references and fix or retire legacy-only callers as part of follow-up cleanup. Continue to keep independent exchange safety and accounting checks; never share writable live state or automatically retry unknown exchange orders.

## Native coordinator integration (follow-up)

`portfolio/core/src/coordinator.rs` now exposes `Coordinator::report_holding` to accept a validated `TargetHolding`, explicit allocation and risk-exit intent. The Rust coordinator computes signed quantity and applies existing risk-exit rules; invalid allocation or risk-exit attempts leave its state unchanged. Regression tests live in `portfolio/core/tests/coordinator_holdings.rs`.

**Not cut over:** no legacy Python caller or HTTP/application caller has yet been rewired to this new method. It is an opt-in Rust entrypoint, not a proven end-to-end replacement for the legacy coordinator. The legacy directory still requires full file/call-site inventory and cross-language replay parity. The Python target holding module was removed in a subsequent commit; other legacy Python modules remain.

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

## Durable native portfolio event integration

The existing `application/runtime/src/portfolio.rs` persists coordinator snapshots with optimistic revisions. Previously its `NativePortfolioEvent::PublishTarget` accepted only an absolute quantity. It now also accepts `PublishHolding { member, holding, allocation, risk_exit }`, validates `holding.symbol == instrument` before mutation, and delegates to `Coordinator::report_holding`. The original absolute-target event remains supported.

A focused runtime test creates a development ledger, registers a member, publishes a 0.5 weighted target at allocation 4, reloads persisted coordinator state, reconciles an offline target delta of 2, and verifies a wrong-symbol publication returns a conflict without advancing the stored revision. **Test committed, not executed.** This is a Rust-native persisted API pathway, not a live venue order route.

The persisted coordinator and the paper replay remain separate application workflows. This does not prove that all legacy Python Portfolio callers were migrated, or justify deleting the entire `engine/portfolio/` folder; only the separately replaced target module has been removed.

## Validation environment and Decimal assertion review

Static review of the weighted paper host-to-ledger integration test found that serialized `rust_decimal::Decimal` values may preserve scale (e.g. `1.0` instead of `1`), including after quantity-increment quantization. Assertions now parse report quantities back into `Decimal` and compare numerically; publication protocol strings are still compared literally where relevant. This change prevents false-negative tests and preserves user-facing report representation.

The current connected container does not have `cargo` or `rustc`, and its git remote fails DNS resolution. The draft PR head has no reported combined commit statuses. **No test suite or build is claimed to have run.** Before deleting any legacy Python files, obtain a working Rust checkout and run the documented full validation matrix.

## Subsequent directories

- `engine/market_data/repair/`: existing Rust repair decisions and planning are partial; legacy Catalog reads/writes and orchestration remain.
- `engine/market_data/` and its child subdirectories: progressively cut over source supervision, history, completeness, repair and Nautilus v2 storage compatibility.
- Other legacy Engine folders: prioritize after an actual tree-and-import inventory rather than guessing a deletion sequence.

## Current batch status

**Partial: `engine/portfolio/targets.py` deleted (41 source lines).** The native coordinator now validates membership before removing resting orders in `flatten`, with failure-atomicity and member-isolation tests in `portfolio/core/tests/coordinator_atomicity.rs`. This is a Rust safety fix, not a Python cutover. The added `legacy_target_parity.rs` covers the native target selection, bounds, signed quantity and allocation cases. The native fixture suite has **not been executed** in this connector session, and no identical-input Python-versus-Rust comparison or legacy import inventory has been run. Do not treat this as a completed folder cutover or enable live trading based on it.

## First actual Python module retirement

- Deleted: `engine/portfolio/targets.py` (41 lines, immutable target definition plus `signed_quantity`).
- Replacement: `portfolio/core/src/lib.rs::TargetHolding`, already used by `application/runtime/src/session.rs` and the persisted portfolio event adapter.
- Independent fixes (Decimal report string scale, coordinator atomicity) are **not** blockers to deleting replaced Python source.
- Remaining impact: the opt-in legacy Python runtime may still import this module. That import graph could not be enumerated using the available GitHub connector search; do **not** claim the legacy Python entrypoint remains operational after its removal. The primary Rust runtime does not import `engine/portfolio/targets.py`.
- No Cargo or legacy-Python test run has been observed in this editing session.

## Complete directory retirement: `engine/market_data/quality/` (2026-11-21)

Removed the legacy Python quality directory (`validate_data.py`, `monitor.py`, `__init__.py`) and its duplicate non-workspace Rust crate. The active `market_data/quality` crate owns closed OHLCV/Trade validation, strict maker flag parsing, inclusive catalog gap computation and read-only service status observation (`monitor::observe_status`, `monitor::read_monitor`). The monitor requires caller-supplied process supervision evidence; it never starts a source or modifies market data. Legacy Python Engine imports of the retired module are intentionally unsupported until their containing folders are cut over.

This is a directory-level source retirement, **not** proof of feature-for-feature compatibility or green CI. In particular, native supervisor wiring and full catalog coverage checks should be iterated separately. Decimal/precision cleanup is independent and does not block retirement. Cargo commands were not run in this connector environment.

## Empty legacy namespace retirement: `engine/feed/` and `engine/pipeline/` (2026-11-21)

The checked-in tree contained only `__init__.py` in each directory. Both namespace markers are removed as a follow-up clean-up after native Rust feed and pipeline orchestration landed under `market_data/feed/` and `market_data/market/src/pipeline.rs`. The five former Python pipeline forwarders were already removed in an earlier change. **This batch removes no remaining Python business logic** and does not assert that every legacy entry point is compatible with Rust. No live data or exchange state is touched.

Validation: exact tree inventory and diff checked through GitHub; no local Rust/Python test execution in this connector session.

## Calculation folder: staged Rust cutover (2026-11-21)

- Inventory: `engine/market_data/calculation/` has five tracked Python files: `aggregation.py`, `bar_features.py`, `feature_source.py`, the `pair_calculation.py` import alias and `__init__.py`.
- The import-only `pair_calculation.py` alias was retired. Native feed and market pipeline functions are in `market_data/feed/src/features.rs` and `market_data/market/src/pipeline.rs`.
- Native feed Trade aggregation now rejects invalid intervals, negative timestamps/IDs, nonpositive prices/quantities, negative quote values and arithmetic overflow, checking rows before last-ID-wins de-duplication. Added a focused regression test.
- **Not a complete folder replacement**: legacy SQLite rollup persistence, daily Parquet feature materialization and the legacy ZIP/BarDataWrangler adapter are still in `aggregation.py`, `bar_features.py` and `feature_source.py`. Keep those files until native dataflow/storage integration exists; native source reading and feature publication are distinct from arithmetic helpers. Legacy imports of the retired alias are unsupported.
- Cargo tests are unexecuted in this connector session; no production data or live order flow was changed. Decimal-scale optimization remains a separate task.

## Native persistent minute calculation increment (2026-11-21)

Introduced `market_data/feed/src/persisted_rollup.rs`: a native SQLite closed-minute OHLCV store, 1m/5m/15m aggregation, revisioned materialization and contiguous minute checkpoint updates within one transaction. The new native tables do not share legacy Python writer state. Duplicates are idempotent, conflicting source minutes fail closed, gaps remain incomplete, and derived results never independently authorize new risk. Added offline SQLite regression tests for gap recovery, five-minute aggregation, invalid input and source conflict.

**Scope limitation:** this provides native persistence infrastructure but is not yet wired into the application source supervisor, and it does not replace the legacy `aggregation.py` call sites or `bar_features.py` Parquet generation. Do not delete the directory until those integrations exist. No Cargo execution is claimed in this connected environment.

## Native application rollup bridge (2026-11-21)

The Rust `application/runtime/src/catalog.rs::commit_market_bars` 1m history path now sends *successfully committed* spot/USD-M closed bars to `application/runtime/src/native_calculation.rs`. The bridge maps native Catalog decimal OHLCV bars to the Rust `seealpha-feed::persisted_rollup` SQLite cache, isolated by a stable SHA-256 key over tenant/product/symbol. The cache advances only contiguous source-minute checkpoints; no READY flag, source confirmation or trading permission is generated. The canonical Parquet/compatibility Catalog commit remains authoritative; a failed derived-cache write is logged for subsequent repair, not misreported as an uncommitted Catalog import. Duplicate and conflicting minute handling uses the Rust-native persistence gate. No Python process is invoked.

This is an **actual Rust application write-path integration**, not a full replacement of the old Python calculation folder: real-time collector ingestion, daily feature Parquet materialization, and ZIP/BarDataWrangler compatibility remain outstanding. The changes were reviewed via GitHub tree/diff only; Cargo and integration tests were not executed in this environment.

## Native live calculation-to-rollup mirror (2026-11-21)

The existing `binance-native-calculation` process now consumes its successfully processed source-owned `candles` journal and mirrors contiguous, **closed and complete** 1m OHLCV records into the isolated Rust `PersistentRollups` database at `<pair-root>/calculation/native-rollups.sqlite`. Mirroring occurs only while the source's exchange-clock confirmation is unexpired and unresolved raw trade-ID gaps are zero; a missing/incomplete minute holds the in-memory cursor until repaired. The derived watermark is shown separately in degraded calculation status. The mirror is capped at 256 candles per pass, and crashes replay idempotently from the beginning. A conflicting prior derived candle fails closed rather than being overwritten. Offline unit tests cover gap repair and incomplete/future exclusion.

This is a real Rust source-worker integration for calculation, but is not a proven equivalent to the full Python `aggregation.py` SQLite callback API. Daily Parquet feature publication, legacy Trade ZIP adapter and complete directory deletion remain outstanding. No Cargo tests or production source sessions were run in the connected editor.

## Offline Binance Trade ZIP adapter in Rust (2026-11-21)

Added `market_data/sources/binance/src/trade_archive.rs` and the offline executable `binance-trade-archive-bars <spot|futures_um> <archive.zip> <interval_ms>`. It reads one bounded ZIP CSV member, rejects unexpected row shape, invalid maker flags, malformed decimal/Trade IDs, and row count/size violations, then aggregates the source into Rust-native decimal OHLCV using `seealpha_feed::features::aggregate_trades`. Output is JSON for downstream offline feature generation. The archive path is read-only; no venue, journal or Catalog is mutated. Locked `zip` and `csv` dependencies were already in the workspace lockfile and the source crate dependency list was updated.

**Partial cutover only**: this is a runnable offline Rust alternative for the *CSV ingestion and aggregation* subset of `feature_source.py`, not the legacy Nautilus BarDataWrangler output nor the `bar_features.py` Parquet schema/materialization workflow. The three remaining Python calculation modules are retained until full native consumers exist. No Cargo compilation was performed in this connected environment.

## Offline native Feature Parquet publication (2026-11-21)

New `market_data/sources/binance/src/trade_features.rs` builds Arrow feature rows directly from Rust-validated Binance Trade ZIP aggregation, including OHLCV, quote and taker-buy quantities, trade count, moving-average/std/return/ATR/Bollinger/buy-ratio calculations. Warmup values stay Arrow null and gaps reset the indicator state. An explicit offline CLI `binance-trade-feature-publish <product> <archive.zip> <interval_ms> <catalog-root> <safe-identity>` uses `seealpha_catalog::Catalog::append_record_batches` to publish an append-only `NativeTradeFeaturesV1` feature dataset. It is bounded to one UTC day and 50k feature rows and retains the Catalog's overlap/atomic-write protections; no automatic source subscription, exchange orders, or READY permission.

**Important scope:** `NativeTradeFeaturesV1` is deliberately distinct from legacy Nautilus Python `BarFeatures`. Its schema/metadata, indicator warmup across UTC days, full feature column set and validity-chain handling are not yet equivalent. The CLI is opt-in, not installed as an automatic strategy input. Keep `engine/market_data/calculation/bar_features.py`, `aggregation.py` and `feature_source.py` until the native consumers and source/rebuild contracts are completed. Cargo compilation and runtime execution were not available in this editor.
