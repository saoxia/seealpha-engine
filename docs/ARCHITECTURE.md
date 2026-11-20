# SeeAlpha Engine architecture

The resident process is the Rust binary `seealpha-engine`. Python remains only
where a user-authored strategy package must run. The HTTP contract with
SeeAlpha Platform is still version 2
(`X-SeeAlpha-Engine-Contract-Version: 2`). `core/engine_contracts.py` and
`core/trading_contracts.py` are unchanged and must stay byte-identical with
Platform.

## Workspace

Rust crates are now grouped by trading/business flow at the repository root:

```text
market_data/market/             public data / bar pipeline
market_data/feed/               market data feed
market_data/catalog/            Nautilus-compatible historical catalog
market_data/sources/binance/    native Binance data source
market_data/quality/            data quality checks
market_data/converter/          historical conversion utility
alpha/strategy/                 strategy-package validation and host
portfolio/core/                target netting / risk clip
accounting/ledger/              durable SQLite state and paper orders
connectivity/venue/             signed exchange transport
backtest/core/                 matching / replay / backtest
application/runtime/            shared orchestration, paper sessions and reconciliation
application/research/           research grant verification
interfaces/api/http/            HTTP v2 / existing research MCP routes
apps/trading_engine/            seealpha-engine executable entrypoint
benchmarks/rust/                Rust performance measurements
risk/                           logical domain: implementation currently spans portfolio/runtime
execution/                      logical domain: implementation currently spans runtime/venue/ledger
```

The Cargo package names and executable names are unchanged. The Python
strategy host in `python/strategy_host/`, legacy Python runtime in `engine/`,
shared contract files in `core/`, and sample packages in `packages/` retain
their existing locations. Dedicated standalone MCP/WS/gRPC servers have not
been implemented by this reorganization.

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


## Business-flow architecture and remaining refactoring

The **physical directory relocation is complete**, but responsibility changes
are not implied by moving source files. For example, risk clipping still lives
in `portfolio/core`, signed exchange transport in `connectivity/venue`, and
order reconciliation in `application/runtime`. The `risk/` and `execution/`
directories document desired long-term domain boundaries; they are not yet
standalone Cargo packages.

`interfaces/api/http` continues to own the HTTP v2 boundary, including the
existing research MCP routes. `application/runtime` continues to coordinate
use cases; extracting thin adapters or separate server processes is a later
reviewed change.

### Required regression checks

1. `cargo fmt --all --check`
2. `cargo clippy --workspace --all-targets --locked -- -D warnings`
3. `cargo test --workspace --locked`
4. `cargo build --workspace --locked --release`
5. `python3 -m unittest discover -s tests -t . -p '*_test.py' -v`
6. `docker build -f docker/engine.Dockerfile -t seealpha-engine:dev .`

Do not enable live trading solely because the directory migration is merged.
Current Rust parity and production exchange acceptance restrictions still apply.
