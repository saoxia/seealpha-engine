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
