# Rust domain-first integration matrix (2026-11-21)

## Provenance
Integration branch: `integration/rust-domain-cutover-20261121`, from `main@83a19d857bca0ec16faf1a731ccb9835081672ab`.
Domain changes are copied exactly at file level from the branch associated with each PR, not squashed/merged as original PR histories. Original PRs remain independently reviewable.

| Layer | Domain PR | Source branch | Landed crate(s) | Runtime wiring and acceptance |
| --- | --- | --- | --- | --- |
| Catalog | #76 | feat/rust-catalog-parquet-interop-20261121 | seealpha-catalog | Parquet write/read and Python interop still require test; single-writer policy |
| Binance source | #73 | feat/binance-source-offline-hardening-20261121 | binance-source | WS/REST handover, gaps, durable checkpoint/READY still incomplete |
| Market/Feed | #75 | feat/rust-market-feed-evidence-20261121 | seealpha-market, seealpha-feed | READY only with persistent source evidence; no empty-as-complete |
| Strategy/Research | #77 | feat/ai8-strategy-research-rust-20261121 | seealpha-strategy, seealpha-research | Runtime accept_strategy checks issuance; historical Catalog replay now calls run_issued_host on trusted Ledger issuance; archive-backed paper sessions now also use Ledger-issued host; development-only sample directories still use lower-level host |
| Backtest | #74 | feat/backtest-evidence-execution-20261121 | seealpha-backtest | Verified replay is available, Runtime task worker not yet switched |
| Portfolio | #72 | feat/portfolio-evidence-risk-20261121 | seealpha-portfolio | Cross-leg phase gate present; Rust Runtime state/evidence lifecycle still needs integration |
| Venue | #80 | feat/rust-venue-order-evidence-20261121 | seealpha-venue | Old write path intentionally fails closed; no live orders |
| Ledger | #79 | feat/ledger-parity-cas-20261121 | seealpha-ledger | Atomic pre-registration present; Runtime IntentJournal adapter not yet connected |
| HTTP | #78 | feat/http-v2-research-v1-parity-ai9-20261121 | seealpha-http | Research task auth/cancel implemented; 501 for missing snapshot and other capabilities; routes not all equivalent |

## Fail-closed integration blockers
1. **No live send permission.** `Runtime::runnable("live")` is disabled and `Runtime::execute_order` refuses every non-loopback venue URL before mutating state; localhost is permitted strictly for synthetic fixture traffic. `Runtime::execute_order` still uses legacy Venue writes, which Venue #80 deliberately rejects. Before a live path is enabled, Runtime must construct an exact immutable request, atomically register it with Ledger (#79), implement Venue `IntentJournal` over that committed evidence and obtain a one-shot `PreparedWrite`. Timeouts and duplicate IDs are reconciliation-only. Do not patch around this guard to make old endpoints 'work'.
2. **Data readiness** must be based on durable catalog coverage, unresolved source gaps, raw trade identity and complete journal checkpoint. Do not assert READY because sockets are connected or REST returns an empty array.
3. **Strategy issued execution**: historical Catalog replay now reads immutable Ledger issuance and calls `run_issued_host`; archive-backed paper sessions now enforce Ledger issuance and immutable parameter equality, and untrusted Python still requires OS-level isolation. Damaged stored parameters fail closed rather than falling back to `{}`.
4. **Verified backtest path** requires complete time-ordered quote/trade/funding/settlement evidence and instrument metadata; do not promote hypothetical OHLC matching to verified execution.
5. **Portfolio** must pass cross-leg admission and fresh external position/evidence checks before the order intent can be registered.
6. **HTTP** routes must surface a real Runtime result or an explicit error, not a fabricated success or completed research snapshot.
7. **Recovery** order: open durable Ledger -> quarantine prior synthetic fills -> restore interrupted jobs as unresolved -> reconcile unknown order intents -> verify source journal/checkpoints -> resume authorized jobs. No unknown-order blind resend.
8. **Python** only the dedicated user strategy host; no legacy Python Engine fallback. Production keys, signed writes and production data remain off-limits.

## Integration-first status
- Domain source files landed on the integration branch (not yet compile-tested).
- Runtime issuance-preparation authorization guard added.
- Historical Catalog replay and archive-backed paper sessions use Ledger-issued `run_issued_host`; malformed persisted parameters now fail instead of silently defaulting.
- Runtime live runnable reporting disabled; signed order endpoint restricted to parsed loopback `SocketAddr` target prior to writing any order intent.
- Cargo.lock and deployment configuration left untouched pending dependency resolution.
- No Cargo/rustc toolchain in the current execution container; repository Actions are paused. No fmt, Clippy, unit, integration, Docker or Python/Rust parity check has been executed; no claim of buildability.
- No real exchange credentials/orders, production DB or historical files touched.

## Dedicated test/fix pass (not yet executed)
```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
python3 -m unittest discover -s tests -t . -p '*_test.py' -v
```
Then run synthetic scenarios for restart after write timeout; duplicate request IDs; verified partial fills and conflicting order observations; cross-leg risk failure; data-gap recovery; invalid grants and cross-tenant task reads; complete historical replay vs Python fixtures; Docker cold start. Record every failed test, root cause and owning domain before merging main.

## 2026-11-21 follow-up assessment: remaining cross-module blockers
- **Order admission bridge:** Venue's `IntentJournal::admit` requires a durable and identical Ledger order intent before send; the current Runtime uses legacy `save_order_request` and its original transport methods. This is **not wired**. Any workaround that enables old writes is prohibited.
- **Portfolio evidence:** Rust coordinator persists CAS snapshots, but basket phase `gate_next_phase` has not been made a prerequisite for an order intent, and account evidence must remain distinct from hypothetical virtual fills.
- **Market data READY:** Existing source and feed adapters retain explicit DEGRADED state. Journal gap closure, Catalog checkpoint and stream identity must be reconciled before a READY signal is useful.
- **Verified replay:** Research HTTP schedules a durable worker; the worker currently drives a bar-level `run_backtest_with_curve`, **not** the new evidence-only `verified::replay`. Do not equate these model types.
- **Restart:** `restore_interrupted_data_jobs` stops blind re-execution but job resume, source checkpoint evidence, strategy session recovery and account reconciliation are not fully orchestrated.
- **Merge readiness:** All of these items are required to claim complete native Rust path. This branch is not verified with Cargo/CI and is not a safe full-parity merge candidate yet.

## Integration increment: Ledger / Venue and verified execution
- New `seealpha-runtime/src/order_journal.rs` implements the Venue `IntentJournal` contract over Ledger `preregister_order_intent` and append-only `venue_send_attempt` events. It accepts only exact scoped NEW order identity, with immutable order parameters supplied separately, and maps repeat admission to `Seen`. A synthetic restart/timeout test ensures there is no second transport attempt. This is **not yet connected to production dispatch**, because Portfolio phase authorization and verified account state are still absent. Old live writes remain blocked.
- New `seealpha-runtime/src/verified_replay.rs` exposes the native `timeline::verified::replay` engine through Runtime, with explicit dataset provenance, bounded event window, positive coverage assertion, offline-only status and Ledger report persistence. This does **not** silently upgrade the legacy OHLC HTTP task worker: the corresponding complete Quote/Trade/Mark/Funding/OptionSettlement ingestion and trusted completeness certification are still needed.
- `session.rs` now binds archived paper execution to exact Ledger issuance (bytes, parameters and account scope) and routes through `run_issued_host`; development sample directories remain separate.
- **Not completed:** risk → admission → single-send → signed fill → authoritative state → portfolio reconciliation; source journal→Catalog/Feed trusted READY; historical job to verified replay evidence bundle; automatic restart reconciliation. All of these must be wired and verified before merging as a complete Rust replacement.

## Additional wiring (integrated but not compilation-verified)
- `runtime/src/basket_phase.rs`: reloads basket state from Ledger, replay-verifies immutable journal/revision, checks tenant/account leg scope, calls Portfolio `gate_next_phase`, CAS-persists the next phase in `awaiting_confirmation`. No Venue dispatch authority is returned. Authenticated position provenance is still the responsibility of the trusted account coordinator.
- `ledger::active_tenant_ids` and `http::resume_queued_research_jobs`: on startup, restore only persisted `queued` backtests for active tenants (honouring tenant pinning), using the existing bounded task executor. Previously `running` tasks stay interrupted and must be explicitly retried. No exchange credentials or blind order retries.
- `runtime::subscribe_strategy_market` now checks Ledger-issued scope and purpose itself (not only HTTP validation); strategy subscription status reports `data_ready=false`, `allow_new_risk=false` pending proven Feed/Catalog continuity.

### Remaining full-cutover gate
1. Atomically bind **authenticated** Portfolio account/market evidence and CAS Basket phase to a specific Ledger/Venue order intent before one-shot dispatch. The adapter is not a sender.
2. Ingest native Source/Feed journals into Catalog with durable checkpoint/gap reconciliation, prove readiness stream-by-stream; do not infer READY from subscription, source process or historical bars.
3. Materialize a trusted immutable dataset evidence bundle for the verified replay worker. Existing HTTP backtests are still OHLC-mode.
4. Complete signed account reconciliation and order-by-order verified fills before mutating authoritative portfolio state; restore unresolved external orders explicitly.
5. Run Rust toolchain verification, test failures and fault scenarios on this integrated code before marking parity or merging as completed migration.

## Catalog coverage bridge (this iteration)
- `Runtime::inspect_catalog_coverage` calls the authoritative Nautilus v2 Parquet `Catalog::bar_gaps` for a caller-scoped closed UTC window. It surfaces missing ranges and never infers market readiness from a successful history read. `source_checkpoint_verified=false`, `feed_ready=false` and `allow_new_risk=false` are deliberate until a separate persisted source proof exists.
- **Cross-module test pending:** the read-only Catalog check is not itself a source journal reconciliation or a Feed `MarketProof`. The source coordinator must verify raw journal completeness, clock, TTL, gap repair, durable publication and exact stream identity before confirming READY.
- Toolchain still unavailable; no Cargo compilation evidence exists. This audit must not be treated as release acceptance.

## Follow-up (2026-11-21): account evidence and native journal inspection
- `Runtime::reserve_basket_phase` now requires a specific environment and exact CAS account snapshot revision. The persisted snapshot must be `signed_exchange_account_query` (or `paper_simulator` for paper), fresh against the phase clock, and contain every basket leg with matching fixed-point Decimal quantity. Arbitrary caller-submitted positions are rejected; the phase remains `executable=false` and is CAS-persisted before any possible external effect.
- `binance-source::store::inspect_pair_journal` opens the existing SQLite source journal **read-only**, verifies symbol/base-level ownership and reads trade count, last ID, unresolved gaps, pending calculation and conflict counts. It never acquires the writer lease or implicitly creates a source.
- `Runtime::inspect_source_catalog_window` joins the existing journal inspection and `Catalog::bar_gaps`, requiring a persisted product policy and canonical in-root source directory. The result remains `data_ready=false`, `allow_new_risk=false`: a closed-bar Catalog check and zero journal gaps are insufficient to prove REST/WS overlap and complete committed publication.
- **Still blocking full cutover**: no atomic durable binding of the basket phase CAS to an order intent and its one-send permit; no independent source checkpoint-to-Catalog publication proof; no trusted multi-product event bundle feeding the HTTP Verified Replay Worker; no end-to-end account reconciliation/fault acceptance or Rust compile/test result. No main merge has been performed.
