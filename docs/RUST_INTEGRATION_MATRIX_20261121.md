# Rust domain-first integration matrix (2026-11-21)

## Provenance
Integration branch: `integration/rust-domain-cutover-20261121`, from `main@83a19d857bca0ec16faf1a731ccb9835081672ab`.
Domain changes are copied exactly at file level from the branch associated with each PR, not squashed/merged as original PR histories. Original PRs remain independently reviewable.

| Layer | Domain PR | Source branch | Landed crate(s) | Runtime wiring and acceptance |
| --- | --- | --- | --- | --- |
| Catalog | #76 | feat/rust-catalog-parquet-interop-20261121 | seealpha-catalog | Parquet write/read and Python interop still require test; single-writer policy |
| Binance source | #73 | feat/binance-source-offline-hardening-20261121 | binance-source | WS/REST handover, gaps, durable checkpoint/READY still incomplete |
| Market/Feed | #75 | feat/rust-market-feed-evidence-20261121 | seealpha-market, seealpha-feed | READY only with persistent source evidence; no empty-as-complete |
| Strategy/Research | #77 | feat/ai8-strategy-research-rust-20261121 | seealpha-strategy, seealpha-research | Runtime accept_strategy checks issuance; historical Catalog replay now calls run_issued_host on trusted Ledger issuance; generic paper sessions still use lower-level host |
| Backtest | #74 | feat/backtest-evidence-execution-20261121 | seealpha-backtest | Verified replay is available, Runtime task worker not yet switched |
| Portfolio | #72 | feat/portfolio-evidence-risk-20261121 | seealpha-portfolio | Cross-leg phase gate present; Rust Runtime state/evidence lifecycle still needs integration |
| Venue | #80 | feat/rust-venue-order-evidence-20261121 | seealpha-venue | Old write path intentionally fails closed; no live orders |
| Ledger | #79 | feat/ledger-parity-cas-20261121 | seealpha-ledger | Atomic pre-registration present; Runtime IntentJournal adapter not yet connected |
| HTTP | #78 | feat/http-v2-research-v1-parity-ai9-20261121 | seealpha-http | Research task auth/cancel implemented; 501 for missing snapshot and other capabilities; routes not all equivalent |

## Fail-closed integration blockers
1. **No live send permission.** `Runtime::runnable("live")` is disabled and `Runtime::execute_order` refuses every non-loopback venue URL before mutating state; localhost is permitted strictly for synthetic fixture traffic. `Runtime::execute_order` still uses legacy Venue writes, which Venue #80 deliberately rejects. Before a live path is enabled, Runtime must construct an exact immutable request, atomically register it with Ledger (#79), implement Venue `IntentJournal` over that committed evidence and obtain a one-shot `PreparedWrite`. Timeouts and duplicate IDs are reconciliation-only. Do not patch around this guard to make old endpoints 'work'.
2. **Data readiness** must be based on durable catalog coverage, unresolved source gaps, raw trade identity and complete journal checkpoint. Do not assert READY because sockets are connected or REST returns an empty array.
3. **Strategy issued execution**: historical Catalog replay now reads immutable Ledger issuance and calls `run_issued_host`; generic paper sessions are not yet issuance-enforced, and untrusted Python still requires OS-level isolation. Damaged stored parameters fail closed rather than falling back to `{}`.
4. **Verified backtest path** requires complete time-ordered quote/trade/funding/settlement evidence and instrument metadata; do not promote hypothetical OHLC matching to verified execution.
5. **Portfolio** must pass cross-leg admission and fresh external position/evidence checks before the order intent can be registered.
6. **HTTP** routes must surface a real Runtime result or an explicit error, not a fabricated success or completed research snapshot.
7. **Recovery** order: open durable Ledger -> quarantine prior synthetic fills -> restore interrupted jobs as unresolved -> reconcile unknown order intents -> verify source journal/checkpoints -> resume authorized jobs. No unknown-order blind resend.
8. **Python** only the dedicated user strategy host; no legacy Python Engine fallback. Production keys, signed writes and production data remain off-limits.

## Integration-first status
- Domain source files landed on the integration branch (not yet compile-tested).
- Runtime issuance-preparation authorization guard added.
- Historical Catalog replay switched from bare `run_host` to Ledger-issued `run_issued_host`; malformed persisted parameters now fail instead of silently defaulting.
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
