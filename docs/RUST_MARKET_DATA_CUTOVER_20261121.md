# market_data: Rust directory cutover (2026-11-21)

## Scope

Retire the complete legacy `engine/market_data/` directory, including its nested, legacy Rust helper crates. Retain and use the first-class Rust workspace under `market_data/`:

- `market_data/sources/binance`: Spot / USD-M public market-data source and persisted journal
- `market_data/catalog`: Nautilus v2 Parquet catalog read, write, cleanup and repair verification
- `market_data/converter`: native data conversion
- `market_data/market`: closed-bar / funding / option parsing, exchange clock and source evidence
- `market_data/feed`: strategy subscription, replay isolation and delivery
- `market_data/quality`: quality monitoring

This is a **folder-level, deliberate non-parity cutover**. Old Python entry points under `engine.market_data` have been removed, not converted to Python wrapper imports. Remaining consumers of those imports need to migrate to the Rust HTTP/runtime or Rust crates. In particular the opt-in legacy Python runtime and its Dockerfile must **not** be considered operational after this deletion.

## Boundaries

Rust remains the default executable and native source activation stays opt-in. Source status must remain `DEGRADED`, `data_ready=false`, and `allow_new_risk=false` without independently verified evidence. Do not enable exchange order submission or assert production READY because the Python tree has been deleted.

Non-parity items deferred by maintainer direction: exact decimal behavior, historical REST/WS overlap and extended gap backfill, full options source lifecycle, all record-type Parquet writes and Python error/result parity. The previous archive may be retrieved from Git history if required for forensic reference; it is **not** a runtime fallback.

## Verification

This repository change was made through the GitHub API. No local `cargo` toolchain, tests, clippy or live data handover verification were run in this environment. Before deployment run:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
```

The obsolete `docker/engine-legacy.Dockerfile` has been removed because it copied deleted Rust helper sources. Existing Python tests and surviving legacy Engine modules that import `engine.market_data` are still present for their own directory-by-directory cutovers, **not** executable compatibility paths. Do not run the legacy Python Engine after this cutover.
