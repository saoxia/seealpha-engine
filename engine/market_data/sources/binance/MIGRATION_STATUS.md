# Binance Spot / USD-M Rust source: integration and verification status

This crate is an **experimental, public-data-only** candidate. It does not
perform signed REST requests, access account credentials, submit orders, or
activate any source in the Engine Runtime. The existing
`ENGINE_EXPERIMENTAL_SOURCE=1` opt-in stays unchanged and off by default.

## Existing interfaces (unchanged)

- `binance-native-source <spot|futures_um> <manifest.json> <managed-root> [seconds]`
- `binance-native-calculation <spot|futures_um> <symbol> <base_level> <pair-root> <intervals> [iterations]`
- `binance-native-catalog-publish <spot|futures_um> <symbol> <pair-root> <catalog-root> <interval> <price_precision> <size_precision>`
- `ws::run_forever(product, channel, symbols, sender)` writes bounded `WireEvent` messages.
- `PairJournal::open` owns `writer.lock` throughout the writer lifetime;
  `journal.lock` protects raw SQLite and derived receipts.
- `PairJournal::last_trade_id()`, `unresolved_gap_count()` expose durable
  journal evidence to a future REST handover supervisor.
- `status.json` is atomically replaced with `DEGRADED`,
  `data_ready=false`, `allow_new_risk=false`, and a conservative gap count.
  `confirmed_ms` is usable for bounded calculation only, **not READY**.

## Changes in this branch

- Correct the subscription ACK deadline independently of the heartbeat.
  Loopback WebSocket fixtures cover ACK, missing ACK, pre-ACK trade, and close.
- Track trade-ID holes in the existing `gaps` SQLite table. Holes are narrowed
  or removed **only** by insertion of the corresponding real raw trade ID.
  Duplicate IDs remain idempotent and contradictory duplicates latch evidence.
- Keep unresolved gaps across writer restarts. Do not advance derived
  calculations or publish Nautilus Parquet bars over unresolved ID gaps.
- Serialize concurrent Catalog publishers for a source pair with an
  exclusive file lease. The existing journal publication checkpoint advances
  only after durable Parquet installation.
- Bounded output queue remains backpressured and cannot silently drop trades.
  New offline fixtures exercise the blocking writer and restart behavior.

## Not yet equivalent to Python; integration blockers

1. Add a **public REST recent-trade overlap** task on each new WS session,
   validate the exact ID handover, and only then reconcile the corresponding
   persisted gap ranges. The recent-trade endpoint is bounded; an older gap
   requires verified historical archive/API evidence, not synthetic ticks.
2. Add rate-limited historical archive/download and checksum validation for
   gaps beyond the public recent-trade window. This may require separate
   read-only history tooling and cross-language evidence acceptance.
3. Add durable per-session handover checkpoints, restart-safe gap repair
   state machine, stale clock and last-trade freshness checks, and
   a fully verified Catalog / Runtime READY handshake. None may be inferred
   from mere WebSocket subscription ACK or a fresh exchange clock.
4. Run the complete Rust workspace build, clippy, tests and Python parity suite
   in an environment with the Rust toolchain. CI is intentionally paused in
   the repository; no passing CI claim is made by this branch.
5. Confirm Linux Python/Rust `flock` interoperability, original historical
   data identity, and complete replay against offline Binance fault fixtures.

**Runtime integration:** keep the existing explicit source process launch and
stop behavior; read `status.json` and require `data_ready=true` only after an
independent handover/Catalog verifier is implemented. Do not change Runtime or
turn on `ENGINE_EXPERIMENTAL_SOURCE` in this migration.

## Offline commands when a Rust toolchain is available

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test -p seealpha-binance-market-data-source --locked
cargo test --workspace --locked
python3 -m unittest discover -s tests -t . -p '*_test.py' -v
```

These commands are **required but were not run** in this editing environment:
`cargo`/`rustc` are unavailable, and the GitHub Actions workflows remain
paused. Therefore **do not merge or enable production READY** solely on the
strength of these new, unexecuted tests.
