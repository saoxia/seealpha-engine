# Rust migration increment: repair, connector parsing, protected replay evidence

Date: 2026-11-21. This is an incremental migration, **not** a complete Python cutover.

## Implemented native behavior

1. **Legacy `engine/market_data/repair/`**: `seealpha-catalog::missing_bar_windows` ports the read-only missing-window calculation to bounded, half-open UTC windows and is used by the native `Catalog::bar_gaps` read path for fixed-step intervals. Existing calendar-variable 1M calculation remains on its `bar_window` path. Includes empty/middle/tail, duplicate, misalignment and range rejection unit tests. This does **not** repair or rewrite archives.
2. **Legacy `engine/connectors/binance/options_parsing.py`**: `seealpha-market::option_stream::parse_option_trade_event` converts the public EAPI `optionTrade` stream's identity, aggressor maker side, exchange timestamp and exact decimal price/quantity into an existing Rust `OptionTrade`. It is an observation, **never** an order fill. Includes valid MARKET/BLOCK and fail-closed malformed event tests. This parser is publicly available to native Rust connectors but the live WebSocket ingestion path has **not** been switched to it.
3. **Legacy `engine/backtesting/option_history.py`**: `seealpha-backtest::option_quotes::align_protection_asks` implements bounded, ordered historical quote validation plus a two-leg last-at-or-before as-of join with max age and ask-size gates. It never borrows future quotes or invents executable premiums. Tests cover future quotes, insufficient depth, expired and crossed markets. It is a research building block: NautilusTrader matching, contract validation and full protection replay **remain in Python**.

## Scope and cutover gates

- No legacy business implementation or Python strategy-host code deleted; no live order or database write path changed.
- Fixed-step `Catalog::bar_gaps` uses this increment. Option stream and quote join are exported Rust functions, **not** proof of runtime integration.
- Still required: snapshot parity against Python fixtures; recurring archive downloader/repair reconciliation; live EAPI stream wiring and recovery; catalog read-to-option-quote conversion; original contract/exercise validations; fees, matching and full native NautilusTrader backtest parity; end-to-end API/Platform assertions.
- Do not delete `engine/market_data/repair/`, `engine/connectors/` or `engine/backtesting/` based on this increment. Never turn unknown execution results into retryable fills.

## Verification

Source reviewed with synthetic offline unit tests added. Cargo toolchain was unavailable in the authoring environment, therefore no local `cargo fmt`, `clippy`, `test` or Docker result is claimed. GitHub CI must confirm all checks before merge.
