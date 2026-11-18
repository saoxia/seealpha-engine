# NautilusTrader RC9 native capability adoption

Date: 2026-11-19. Engine repository only. This document covers capability changes
after the dependency-only RC9 upgrade. Deployment and live-trading authorization
are **not** included.

## Directly integrated

- **Order lifecycle**: local `OrderCancelDenied` and `OrderModifyDenied` are
  distinct from venue-side rejection. Manual cancel completes with explicit
  denial status; operation records retain denial reason and type. Coordinator
  does not discard working orders after a partial fill. Local and venue cancel
  refusals preserve the working order; do not blindly resubmit unknown orders.
- **Backtest input**: Engine HTTP v2 backtest endpoint retains its existing
  request/response contract. The implementation now iterates Arrow batches,
  runs `BacktestEngine.run(streaming=True)`, clears processed input, and calls
  `end()` once. Its read lease protects all batch loads and native replay.
  Strictly increasing Bar event times and one-instrument identity are checked.
- **Binance options reconciliation**: periodic EAPI order/account poll backs
  off up to 30 seconds on failed reconciliation; additional risk stays gated
  until a complete successful refresh.
- **Valuation**: the latest native Cache mark-price observation is preferred
  over the callback's copy when its event time is at least as recent; timestamp
  integrity and REST balance evidence remain project-level protections.

## Explicit opt-in offline research APIs

- `engine.pipeline.catalog.ParquetDataCatalog` now delegates native RC9
  FundingRateUpdate, OptionGreeks, OrderBookDepth, InstrumentStatus and
  InstrumentClose reads and writes. Existing immutable source, gap, fingerprint
  and conflict rules are not removed.
- `engine.pipeline.funding_data.fetch_history`: bounded, paginated Binance
  USD-M funding settlement observations from an injected public GET transport.
  It verifies symbol, event ordering, decimal finiteness and pagination. The
  caller must preserve provenance, check coverage and invoke
  `persist_history` explicitly; an empty response does not mean complete
  history. No exchange credentials or order endpoints are involved.
- `engine.strategies.loading.run_issued_history_stream(..., funding_updates=...)`
  injects explicitly supplied native funding events into chronological replay.
  Rates do not fabricate payments: the native engine uses event and settlement
  boundary semantics. Existing HTTP v2 backtests do **not** automatically
  include unverified funding history. Validate against actual settlement
  records before enabling it for standard strategy performance reports.
- `engine.strategies.research_native.greeks_exposures` groups signed,
  contract-multiplier-adjusted Greeks by underlying, rejecting missing, stale
  or non-finite native OptionGreeks. Callers must check the Greeks convention,
  vol surface and cross-currency units; this does not auto-hedge live accounts.
- `engine.strategies.research_native.protective_bracket` uses the RC9 native
  OrderFactory and checks both exits are reduce-only. It only builds orders;
  no venue submission is performed. Use a real strategy's configured
  `manage_contingent_orders`, protect failed child orders, and verify
  exchange-specific support before any production adoption.
- `engine.pipeline.options_data.export_depth_replay` turns immutable
  `ws/depth` option source snapshots into a separate native Parquet research
  Catalog, without rewriting historical evidence or pretending disconnected
  periods are complete. `run_depth_backtest` uses RC9 L2_MBP simulation,
  never live execution. Depth snapshot replay is a model of observed snapshots,
  not proof of exact queue placement or real fill probability.

## Verification and explicit limitations

PR CI runs `uv sync --locked`, Ruff, full standalone Engine Python regression
suites, Rust release converter tests, Docker build and authenticated synthetic
HTTP smoke. New tests use synthetic events, records and exchange fixtures only.
Retain 106+ upstream warning lines as separate follow-up if non-critical;
do not suppress warning failures without analysis.

Not asserted: production-ready execution routing, coverage-complete historical
funding, historical Greeks data sources, venue-native contingent order execution,
long-running live stability, exact queue-position realism, or compatibility with
the separate SeeAlpha Platform. They require distinct data-source and
cross-repository acceptance before enabling real trading.

References: https://github.com/nautechsystems/nautilus_trader/releases/tag/v2.0.0rc9

## Follow-up: bounded L2 replay and managed option book consumer

- `run_depth_backtest` now accepts a one-pass depth iterator and validates time
  ordering and instrument identity across batches of up to 50,000 snapshots
  (10,000 by default). It feeds native `BacktestEngine.run(streaming=True)`,
  clears processed input after each batch and calls `end()` exactly once.
  The offline export helper retains its list-based compatibility validator;
  the replay path does not use it.
- The single-instrument option `market_events` collector now explicitly
  requests `managed=True` depth-only L2 subscriptions, reads the native
  `Cache.order_book` as the managed-book readiness check, and continues
  archiving unmodified snapshot provenance. It does not create a second
  Python L2 cache. Native integration tests check the resulting best bid/ask.
- `batch_market_events` is a separate bounded combined-WebSocket raw evidence
  collector: it does not run a Nautilus DataEngine. Its high-volume transport
  and historical source bytes are unchanged. This change **does not** claim
  that all batch collectors now have DataEngine-managed books.
- These changes do not grant live execution, certify disconnected history,
  or establish accurate queue position. Full CI and offline integration
  tests are required before adoption in a deployed collector.
