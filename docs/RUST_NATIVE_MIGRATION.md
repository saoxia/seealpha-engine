# SeeAlpha Engine Rust-native migration

## Status

The default resident process is `seealpha-engine`, and its Python usage is limited to the strategy host. **A separate legacy Python Engine has been restored from `f18e48e` pending native feature parity.** It can be run only as an explicitly selected alternative process/image, not as an automatic fallback within Rust; see [LEGACY_PYTHON_RECOVERY.md](LEGACY_PYTHON_RECOVERY.md) for isolation and safety. The native Rust runtime and the restored Python runtime have different capability boundaries. The notes below preserve the original migration plan and historical checkpoints; older statements about files being removed refer to the pre-recovery state.

## Decision

**Historical cutover statement (now superseded):** the Python engine implementation was removed by the native runtime switch. It has since been restored for explicit legacy compatibility until equivalent Rust functionality and tests are verified. The normal Rust image still only includes the Python strategy host; the optional legacy image includes the old Python Engine.

Maintain the existing repository layout and ownership boundaries, including engine/market_data/sources/binance_perpetual, binance_spot, binance_options and binance_shared. Rust crates can live inside the same domain directories, not in a new parallel business tree. Existing Python implementation remains authoritative until Rust has passed parity and integration tests.

This change does not authorize live trading, deployment, historical source rewrites, production credentials, or changes to Platform.

## Baseline and truth in this PR

- On 2026-11-20 the repository has 238 Python files (including tests), approximately 1,995,336 Python source bytes, and one pre-existing Rust converter source.
- The quality Rust crate under engine/market_data/quality/rust is an isolated first implementation. It currently does not replace or invoke the active Python quality, data-access, or trading paths.
- No speed or memory improvement is claimed. Its microbenchmark generates synthetic normalized trades; meaningful comparison requires matched Python and Rust workload, RSS and CPU measurement.
- A Rust executable, native NautilusTrader engine, Platform integration or new Docker image is NOT yet available on this branch.

## Domain migration matrix

| Existing ownership | Rust target | Status |
| --- | --- | --- |
| core/ | Domain, market, order and API contracts with exact decimal semantics | Pending |
| engine/connectors/ | Exchange protocol adapters and reconnect/reconcile semantics | Pending |
| engine/market_data/sources/binance_perpetual/ | Native collection/calculation/history maintainers | Pending |
| engine/market_data/sources/binance_spot/ | Native collection/calculation/history maintainers | Pending |
| engine/market_data/sources/binance_options/ | Native collection/option evidence/history maintainer | Pending |
| engine/market_data/sources/binance_shared/ | Shared transport/parsing, without shared runtime lifecycle | Pending |
| engine/market_data/control/ | Durable DataPolicy and independently recoverable tasks | Pending |
| engine/market_data/collection/, calculation/, repair/ | Streaming and event processing with bounded queues | Pending |
| engine/market_data/storage/ | Existing SQLite/Parquet/Arrow layout and evidence provenance | Pending |
| engine/market_data/access/ | Unified read-only data subscriptions and replay isolation | Pending |
| engine/market_data/quality/ | Quality rules, parity and integration | Rust candidate only |
| engine/strategies/ | Native strategy package validation and target interface | Pending |
| engine/backtesting/ | Rust NautilusTrader backtest/streaming parity | Pending |
| engine/portfolio/ | Decimal-safe netting, basket targets and risk phases | Pending |
| engine/trading/ | Ledger, order idempotency, no blind retry, recovery and risk gates | Pending |
| engine/research/ | Research task authorization, snapshots and workers | Pending |
| engine/api/ | HTTP v2, Research v1 and MCP-compatible native boundary | Pending |
| engine/infrastructure/ | Native supervisors, bounded task queues, recovery | Pending |
| engine/__main__.py and legacy feed/pipeline CLI | Replace Python entrypoints after native cutover | Pending |
| tests/, docker/, CI | Native test pipeline, standalone build/image, Platform acceptance | Pending |

## Required equivalence / safety gates

1. **Rust without Python installation**: Engine production binary can run market data, historical repair, data access, backtests, portfolio, execution, research and APIs without Python. Python strategies alone run in a separately opted-in worker with a narrow, authenticated, bounded event/target contract.
2. **Pin upstream parity**: verify NautilusTrader Rust APIs at the project's pinned 2.0.0rc9 source revision. Do not infer coverage from later 'latest' or 'develop' docs. Document unavailable features before substituting any implementation.
3. **No protocol breakage**: retain the existing HTTP v2, Research v1, MCP auth/scope semantics, error responses, tenant separation and Platform-to-Engine contract. Perform cross-repository offline compatibility checks.
4. **No data migration surprise**: preserve catalog paths, manifests, journal ordering, SQLite formats, Arrow/Parquet schemas, recorded evidence, precise timestamps, locks, gap flags and provenance. Read-side subscriptions never start or stop producer tasks.
5. **No trading safety regression**: request pre-registration, order idempotency, unknown-result reconciliation without blind replay, partial-fill handling, cancel-denied semantics, risk gating and crash recovery remain tested.
6. **Decimal correctness**: monetary and order quantities must use exact bounded/checked decimal representations; do not replace them with float. Native f64 quality checks are only for normalized market-data quality and do not implement order sizing.
7. **Performance evidence**: record same-workload p50/p99 latency, events/sec, CPU, steady and peak RSS, queue pressure and backtest throughput before/after. Show long-running stability and fault recovery; never infer speedup from language choice.
8. **Complete cutover**: all pending rows above become verified and integrated, native image starts independently, old Python Engine implementation is retired, and explicit final review authorizes the merge. Until then this PR stays draft.

## Implementation process in this PR

For each module: capture deterministic baseline fixtures and failure behavior; add equivalent Rust tests; measure CPU, allocations and peak memory; integrate behind an offline/synthetic test gate; verify the consuming modules; only then retire the Python implementation. Keep one source of truth for business rules, avoid parallel permanently divergent implementations. Python strategy compatibility must never let a strategy control market-data collection or bypass portfolio/execution risk gates.

Run the isolated candidate tests:
    cargo test --locked --release --manifest-path engine/market_data/quality/rust/Cargo.toml

Run the synthetic benchmark (not proof of production speedup):
    cargo run --locked --release --manifest-path engine/market_data/quality/rust/Cargo.toml --example quality_bench

The existing Python tests, native Rust converter tests and Docker synthetic checks remain in place until a complete Rust replacement exists.


## Native trade archive converter cutover (2026-11-20)

The **historical Binance trade ZIP -> Nautilus TradeTick/quote/Bar Parquet**
conversion path now requires the existing `nautilus-data-converter` Rust binary.
The former Python CSV-to-TradeTick writer and in-place trade aggregation
fallback have been removed from the active conversion module. When ticks
already exist but a requested Bar interval is missing, reuse of a compatible
existing smaller Bar interval remains unchanged; when raw trades must be
reprocessed, the missing Bar is now generated by Rust in a private staging
folder, without rewriting ticks. Native output is checked before installation
and existing Catalog manifests, checksums, locks and force boundaries remain.

The binary is built by the existing Docker and CI paths. Missing or explicitly
disabled Rust binary fails closed (`RustConversionError`); it does **not**
silently switch to a second writer. The conversion source still uses a Python
orchestrator for Catalog staging and atomic publication, and may use the
existing Python Bar-from-Bar aggregation when source bars are already present.
That remaining aggregation should only be retired after a separate native
parity implementation is verified.

**This is not a cutover of live market-data collection.** Spot and USD-M native
WebSocket/SQLite/calculation workers still lack production-ready exchange-clock,
REST handover, history/Catalog bridge and READY evidence, while Options has
only partial protocol support. They must remain disconnected from the default
Engine service until their safety and compatibility gates pass. API, research,
backtesting, trading and the Engine Docker entrypoint remain Python.

## Next required code

Prioritize a version-pinned NautilusTrader native-API gap audit, shared Python-versus-Rust fixture corpus, and data-source-driven native producer/read-only access integration. Then migrate portfolio, strategy, backtest, trading and API, preserving exact decimal/ledger semantics. Complete the standalone native Docker build and Platform smoke suite before requesting final review.

## Binance source manifest Rust candidate (2026-11-20)

A dependency-pinned native source kernel now lives in
`engine/market_data/sources/binance_shared/rust/`. It provides **offline**
manifest-v1 parsing and validation for USD-M perpetual, Spot, and Options;
canonical symbol rules (including option expiry calendar and positive strikes);
disabled/duplicate-policy rejection; deterministic cold-start socket capacity
(128 for perpetual/spot; 64 for options); and a pure start/stop/keep planner.
It caps input manifests at 5 MiB and has read-only inspection CLI and Rust unit tests.

**Not a cutover.** The live source manager, Binance sockets, queues,
journals, aggregation, option recovery, history, quality checks, Catalog,
access and production process supervisor still run through Python.
The native kernel is intentionally not invoked by the active Engine until
equivalent fixture/contract tests and storage/recovery acceptance exist.
No speed, memory or feature-parity claim is made from this candidate alone.

Next: add Python-vs-Rust fixture corpus; move the event transport and
backpressure/fail-closed mechanics per product into native workers; preserve
SQLite/Parquet/Arrow, exact timestamps, source integrity, locks and checkpoints;
then gate the process switch with fault-injection tests, sustained ingestion,
RSS/CPU comparisons and the full Engine/Platform suite.
