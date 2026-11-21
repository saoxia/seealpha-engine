# Market/Feed Rust migration handoff (2026-11-21)

Scope: `crates/seealpha-market/` and `crates/seealpha-feed/` only. Baseline
`main@83a19d857bca0ec16faf1a731ccb9835081672ab`; Python remains intact.
This change is **not** a declaration of end-to-end parity or production readiness.

## Integration contract for the Runtime/source owners

1. Options: use `options::parse_options_klines` for exact closed candles,
   `options::parse_recent_option_trades` for observed public trades, and
   `options::parse_option_book_ticker` for EAPI BBO. Preserve the venue symbol
   (expiry/strike/C/P), exchange event time, exact Decimal values and raw
   provenance. `RecentOptionTrades::historical_complete()` is always false.
   Missing REST trades/quotes and disconnected BBO cannot be synthesized.
   A networked Options WS source, durable raw journal, and full native
   Catalog writer are **not** implemented in these crates.
2. USD-M funding: `funding::parse_funding_estimate` consumes the changing
   public mark-price stream and must **never** be journaled as settlement.
   `funding::FundingSource::history_observed` returns public REST observations.
   Convert only those rows with `FundingObservation::to_update()`; the
   resulting `FundingRateUpdate` uses exact millisecond->nanosecond conversion
   and `account_payment_confirmed=false`. `rateType` missing => `Unknown`,
   not a fabricated `Regular`. `FundingHistoryRead::historical_complete()`
   is always false. Neither a fixed 8-hour schedule nor an empty REST page
   can prove completeness, and a public settlement is not an account payout.
3. Trade integrity: `pipeline::audit_trade_ids` records suspicious gaps and
   out-of-order input while deduplicating byte-equivalent IDs. An ID reused
   with different contents fails. `aggregate_trades` uses only unique
   observations, never interpolates a missing execution.
4. Producer certification (bar-level, millisecond): call
   `Book::accept_page` with source-authored closed bars; then, **only after**
   durable journal checkpoint, REST/WS overlap reconciliation, healthy
   exchange clock, warmup coverage, zero unresolved gaps/backlog, and no
   conflicting evidence, call `Book::confirm_source(SourceProof)`.
   `Book::verified_ready(now_ms)` must be checked with an authoritative
   current time. A new page, failure, restart or expiry closes the gate.
   `SourceProof` is a trust-boundary message: strategies and API readers
   must not construct it. The proof currently does not carry a cryptographic
   signature; it assumes a trusted in-process coordinator.
5. Strategy certification (nanosecond): after reading the committed same-stream
   latest bar into `Hub::accept_closed_bar`, and **only** from the verified
   coordinator, call `Hub::confirm_market(Market, MarketProof)`. Update the
   clock via `Hub::advance_time(now_ns)` before polling
   `Hub::data_ready(market)` or `Hub::view(strategy)`. A mere subscription,
   received candle or heartbeat cannot certify READY. `View.allow_new_risk`
   remains false; actual execution authorization stays with Runtime and risk.
6. Delivery: `session::Delivery::certify_from_book` must precede a fresh
   live delivery; it rejects stale, replay-only and mixed backfill batches.
   No subscription or read operation starts a producer.
7. Replay: `Hub::begin_backtest` + `accept_replay_closed_bar(run,...)` +
   `replay_view(run,...)` + `end_backtest` uses a distinct bounded buffer,
   not the live series; all replay views prohibit new risk. A caller must
   release a run even after errors (use the existing `backtest_guard`).

## Important remaining integration gaps

- The existing `hub::Market { venue, symbol }` does not include period/kind.
  Until an agreed cross-crate identity migration is completed, the Runtime
  must not alias multiple intervals or tick/quote/book streams under a single
  `Market` key. Python `MarketStream` includes source/product/symbol/
  interval/kind. Introducing fields here would break external constructors.
- The existing `Product` enum covers Spot and USD-M. Options are exposed via
  a separate public adapter; adding Options to `Product` needs exhaustive
  Runtime/HTTP caller review outside this branch.
- Option streaming recovery, real BBO gap repair, FundingRateUpdate Parquet
  publication, persistent history source checksums, correct source
  lifecycle/clock certificates and end-to-end strategy delivery are still
  pending. Do not enable experimental source READY or new-risk permission.
- The Python `engine/` path remains authoritative until fixture-by-fixture
  parity is demonstrated. All new tests are synthetic and offline; no account
  credentials, private API endpoints, live trading, raw history rewrite or
  production state writes are used.

## Offline acceptance commands

```sh
cargo test --locked -p seealpha-market
cargo test --locked -p seealpha-feed
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
python3 -m unittest discover -s tests -t . -p '*_test.py' -v
```

Required follow-up fixtures: EAPI tuple/object mixed and empty-page history;
recent option trades with identical/conflicting IDs and gaps; crossed/stale BBO;
irregular and Special funding events; missing rateType, duplicate or regressed
funding times; disconnected source, post-reconnect overlap, journal commit
failure, clock expiry, backlog, missing candle, replay/live isolation and
delayed/corrupt/duplicate delivery.

### Verification status at authoring

This session's container has no `cargo`/`rustc` and no networked Git
checkout. GitHub Actions remain under `.github/workflows-paused/`.
These assertions were added as Rust unit tests but **were not executed**.
Do not merge this PR as validated until CI or a maintainer executes the
above commands and resolves failures. This document reports neither a
successful build nor end-to-end Python/Rust parity.
