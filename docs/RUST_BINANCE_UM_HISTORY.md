# Rust Binance USD-M daily trade history (first migration slice)

The native source crate now provides an isolated, public-data-only daily ZIP archive
planner/downloader. It uses the original Python path convention under
\`data/futures/um/daily/trades/{symbol}/\` and only accepts fully completed UTC days.

\`\`\`bash
cargo run --locked -p seealpha-binance-market-data-source --bin binance-um-history -- \
  --symbols ETHUSDT,BTCUSDT --start 2026-11-18 --end 2026-11-20 \
  --output-dir data/binance/raw --plan-only

cargo run --locked -p seealpha-binance-market-data-source --bin binance-um-history -- \
  --symbols ETHUSDT --start 2026-11-18 --end 2026-11-20 \
  --output-dir data/binance/raw
\`\`\`

* \`--end\` is exclusive. \`--refresh\` validates again against the official
  remote CHECKSUM instead of trusting a previously validated local ZIP.
* SHA-256 verification is mandatory. Local offline reuse requires both an
  ordinary ZIP and a matching official CHECKSUM sidecar.
* No API keys, trade orders, strategy activation, Catalog mutation or READY
  state are involved.
* Per-archive filesystem locks and staging files prevent partially downloaded
  ZIPs from being treated as complete. No HTTP redirect to third-party hosts.
* Failure of any day makes the command fail. Other successfully verified days
  are retained for a later retry.

## Important migration boundary

This is **not** yet end-to-end equivalence with the Python historical pipeline.
The existing \`market_data/converter\` Rust CLI produces Nautilus-compatible
TradeTick/TradeQuote and Bar Parquet, but native scheduling, source checksum
conversion records, Catalog installs/rollbacks, full coverage reports, overlap
repair and REST fallback are not wired into this history CLI yet. Do not
decommission the Python history management on the basis of this slice.

The repository's Rust build/tests, network download, and live Nautilus Parquet
interop still require execution in a Rust-enabled CI environment.

## Offline durable Trade-ID gap repair

The Rust native source already collects public websocket trades into
\`<source-root>/<symbol>/<base-level>/collection.sqlite\`, and its
\`gaps\` table contains durable missing Trade ID intervals.

After stopping the live \`binance-native-source\` for the pair, use a
**completed UTC day** whose Binance trades ZIP has already passed SHA256
verification via \`binance-um-history\`:

\`\`\`bash
cargo run --locked -p seealpha-binance-market-data-source \
  --bin binance-um-gap-repair -- \
  ETHUSDT 2026-11-18 \
  data/sources/futures_um/ETHUSDT/tick \
  data/binance/raw tick
\`\`\`

The repairer requires exclusive writer ownership, matches symbol/day,
rechecks the official local checksum before opening the archive,
validates the complete CSV before committing rows, and calls the existing
journal ingest logic for trade IDs within recorded missing ranges.
Conflicting data are never silently overwritten. It reports the number
of remaining missing-ID ranges but **never marks the source READY**.

The repairer is intentionally **offline/manual**. The running source
supervisor has not yet scheduled archive repair on reconnection, and
today's intraday trade holes cannot be recovered from an incomplete
daily ZIP. Automatic REST/WS handover, live catch-up, raw tick Parquet
publication, and real Nautilus compatibility remain unverified.
Do not run the repair binary concurrently with the live writer.

Validation still required in a Rust toolchain:

\`\`\`bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
\`\`\`

The scripts and regression fixtures have been committed but **these
commands have not been executed in this session**.
