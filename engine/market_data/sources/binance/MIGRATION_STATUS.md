# Binance market data: Nautilus-first current implementation

## Current ownership

- NautilusTrader `nautilus-binance` owns Spot/USD-M real-time trade subscriptions, bounded recent trade REST requests, native TradeTick model, Feather streaming, and Parquet Catalog persistence.
- SeeAlpha `archive_fetch.rs` downloads official daily Binance public ZIP files and validates their CHECKSUM and archive identity.
- The isolated native Rust `integrations/nautilus_binance_data/src/bin/import_archive.rs` validates archive rows, UTC dates, Trade IDs, Instrument precision, SHA-256, and uses the **native** Nautilus Catalog writer. Its importer refuses overlapping intervals and is not data READY.
- Runtime retains tenant-scoped policies, explicit source process supervision, deterministic job IDs, provenance, and fail-closed `archive_fetch` / `archive_import` tasks.
- There are no SeeAlpha Spot/USD-M WebSocket drivers, raw SQLite Tick Journal, old calc process, old Catalog publisher, or custom Nautilus Tick/Quote schema converter in this source crate.

## Deployment boundary

The Engine's default Docker image only includes the official offline ZIP fetch CLI. The Nautilus read-only Rust worker/import CLI must be compiled, validated and installed separately. `ENGINE_EXPERIMENTAL_SOURCE=1` and an explicit `ENGINE_NATIVE_NAUTILUS_SOURCE_BIN` path are mandatory to start live source supervision. There is **no automatic legacy fallback**.

Retained data capabilities outside this cleanup: the bar-level Catalog/rollup code still used by historical research and backtests, public K-line/funding/exercise backfills, option market data modules, and order/evidence/risk code. Do not delete these simply because a legacy Binance WebSocket was removed.

## Open release gates

1. Real Binance Spot `@trade` and USD-M `@aggTrade` fixtures and live multi-symbol subscription, disconnect, restart, and flush tests.
2. Verified WS/REST/ZIP handover, Trade ID continuity, overlap deduplication, and multi-day correction/recovery checkpoints.
3. Native Catalog read-side parity for existing strategy and research consumers.
4. CI compile/test/Clippy/Docker on the cleanup PR; original production risk gates remain unchanged.

The source process only reports `experimental_degraded`; `data_ready=false` and `allow_new_risk=false` are not negotiable until verified independently. See [ARCHIVE_BACKFILL.md](ARCHIVE_BACKFILL.md).
