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
6. Fills from a paper session are marked `venue=paper`.
7. Signed order routes build a Binance HMAC request and send it only when
   `ENGINE_VENUE_BASE` is set. Credentials are not written to SQLite. A repeated
   request id replays the stored result and does not call the venue again.
   Transport failure leaves the intent `unknown` and reconcile queries; it does
   not submit a second order. With no gateway URL the route refuses rather than
   guessing a production host. Connection and account-snapshot routes still do
   not retain secrets.

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
If that directory is not writable, which is the case for the Platform
handshake mount under `--cap-drop ALL`, the process uses a temporary book
under `/tmp` and still serves HTTP.


## Target business-flow-oriented architecture (migration in progress)

The repository root should communicate the trading flow without requiring readers
first to understand Cargo crates. The canonical **target** layout is:

```text
market_data/  -> alpha/ -> portfolio/ -> risk/ -> execution/ -> connectivity/
                                    accounting/  (positions, fills, PnL and feedback)
application/  (shared use cases / authorization / orchestration)
interfaces/   (api, mcp, websocket, grpc protocol boundaries)
apps/         (trading engine, backtest CLI, optional service entrypoints)
backtest/     monitoring/     common/     config/     tests/     docs/
```

**This is a target architecture, not a claim that the Rust sources have been
moved.** The active Cargo workspace still uses `crates/*`; changing its paths
without migrating every crate source, internal `path` dependency, lockfile,
container path and CI script would break the engine.

| Target business boundary | Existing Rust implementation | Notes |
| --- | --- | --- |
| `market_data/` | `crates/seealpha-market`, `crates/seealpha-feed`, `crates/seealpha-catalog`, `crates/binance-source`, `crates/data-converter`, `crates/quality` | Preserve catalog and ingestion semantics |
| `alpha/` | `crates/seealpha-strategy`, Python strategy host and `packages/` | Strategy hosting is not an execution gateway |
| `portfolio/` | `crates/seealpha-portfolio`, runtime portfolio module | Targets and netting |
| `risk/` | Portfolio risk clipping and runtime order gates | Cross-cutting: do not bypass gates |
| `execution/` | Runtime order journal, reconciliation and execution functions | Evidence-first, idempotent behavior |
| `connectivity/` | `crates/seealpha-venue` and market source adapters | Separate signed exchange IO from public feeds |
| `accounting/` | `crates/seealpha-ledger`, runtime accounts/fills | Durable ledger owns financial facts |
| `application/` | `crates/seealpha-runtime` | Target shared use-case layer |
| `interfaces/api/` | `crates/seealpha-http` | Preserve HTTP contract version 2 |
| `interfaces/mcp/` | Research MCP endpoints within HTTP crate | No standalone server is implied |
| `apps/trading_engine/` | `crates/seealpha-engine` | Preserve `seealpha-engine` binary name |
| `backtest/` | `crates/seealpha-backtest` | Preserve existing benchmark and test entrypoints |
| `common/`, `monitoring/` | Shared code and existing operational instrumentation | Create dedicated crates only when justified |

### Migration requirements

1. Move existing sources as **complete crates** (or refactor into modules) and
   update workspace membership and all Cargo `path` dependencies together.
2. Update source paths in Dockerfiles, scripts, tests, and documentation; avoid
   introducing second implementations that drift from the running engine.
3. Keep the v2 HTTP contract, Python strategy-host compatibility, paper/live
   isolation, tenant authorization, durable reconciliation and fail-closed risk
   rules unchanged.
4. Validate `cargo fmt --all --check`, `cargo clippy --workspace --all-targets
   --locked -- -D warnings`, `cargo test --workspace --locked`, `cargo build
   --workspace --locked --release` and deployment smoke tests before cutover.
5. Do not claim live-trading readiness until the separate Rust parity and
   exchange end-to-end acceptance gates are satisfied.
