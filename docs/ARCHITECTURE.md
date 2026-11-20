# SeeAlpha Engine architecture

The resident process is the Rust binary `seealpha-engine`. Python remains only
where a user-authored strategy package must run. The HTTP contract with
SeeAlpha Platform is still version 2
(`X-SeeAlpha-Engine-Contract-Version: 2`). `core/engine_contracts.py` and
`core/trading_contracts.py` are unchanged and must stay byte-identical with
Platform.

## Workspace

```
crates/
  quality/                 market-data quality checks (gap scan, kline and trade validation)
  seealpha-market/         public Binance feed and bar pipeline
  seealpha-portfolio/      decimal target netting, risk clip, paper deltas
  seealpha-ledger/         SQLite tenant, strategy, and paper-order book
  seealpha-strategy/       package checks and the Python host process
  seealpha-research/       research-grant verification
  seealpha-runtime/        paper session: feed -> pipeline -> strategy -> ledger
  seealpha-http/           HTTP v2 server
  seealpha-engine/         process entrypoint
  seealpha-bench/          hot-path measurements
  binance-source/          offline Binance source manifest and journal tools
  data-converter/          historical trade ZIP to Parquet converter
python/strategy_host/      one-shot strategy process and the small nautilus_trader surface
packages/                  example strategy packages, including ema_crossover
core/                      shared contract modules kept for Platform
```

## Request path

1. HTTP checks the contract header, then the pinned tenant, then the bearer token.
   Wrong contract is 426, another tenant is 403, a bad token is 401. Every
   response includes the v2 header.
2. A paper session loads public Binance spot or USD-M klines, recent trades, and
   a depth snapshot. No API key is sent.
3. The pipeline validates closed klines, aggregates trades into OHLCV bars, and
   computes decimal bar features. A read does not start a collector.
4. The strategy host loads the issued package, or the development-only sample
   package, and returns `publish` events. `submit_order` raises.
5. Portfolio code nets absolute targets, clips them to the risk limit, and
   quantizes the delta toward zero. The ledger pre-registers the session.
   A repeated fingerprint replays the stored result. An unresolved `unknown`
   session is not executed again.
6. Fills are marked `venue=paper`. Live order, connection, and snapshot routes
   return 409 and do not store credentials or call a signed exchange endpoint.

## Python that remains

`python/strategy_host/host.py` is the glue. Strategy packages such as
`packages/ema_crossover` are written against `prepare`, a `Strategy` subclass,
bar callbacks, and `publish`. The host supplies that surface, including the EMA
the sample package registers, and exits after one JSON request. It is not a
Nautilus matching engine and it has no exchange client.

## Intentionally retired

Signed Binance trading, credential retention, Nautilus catalog backtests, and
options execution are not invoked. Those paths placed or simulated exchange
orders inside the old Python process. This engine keeps the HTTP status and
error strings Platform already understands for "no result yet" (`还没有回测` and
the carry/option equivalents) and refuses live commands. Research grants are
still verified; `GET /research/v1/datasets` is the mounted research route when
`ENGINE_RESEARCH_SIGNING_KEY` is set.

## State

The book remains SQLite at `$ENGINE_STATE/book.sqlite` (WAL). Existing tenant,
order, and strategy-load tables are created if missing. Paper positions and
paper session idempotency use additional tables. Secrets are not columns.
