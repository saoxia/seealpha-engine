# SeeAlpha Engine

Rust-based market-data and read-only trading research engine. The process binary is `seealpha-engine`.

## Modules

- `engine/apps/trading_engine/` — binary entry point, PostgreSQL bootstrap, active Store assembly, HTTP server and read-only market smoke.
- `engine/gateway/api/` — HTTP v2 boundary, routes, contract version, validation and domain calls.
- `engine/gateway/mcp/` — optional read-only Research MCP adapter.
- `engine/execution/` — standalone target quantity, netting and clipping utilities (no live order sender).
- `engine/pipeline/` — market ingestion, Catalog, subscription desired state and source interfaces.
- `engine/ledger/` — verified economic evidence, fills and audit history.
- `engine/database/` — PostgreSQL access and schema bootstrap.
- `engine/connection/` — external exchange integrations.

There is **no active custom strategy runtime, strategy issuance store, strategy preview, `engine/runtime` crate, or `engine/backtest` crate**. The bundled EMA demo and its dedicated `engine/strategy/` crate and HTTP routes were retired on 2026-11-23. When actual strategy execution is required, prefer NautilusTrader's native strategy lifecycle rather than recreating generic strategy infrastructure. No live order transmission is enabled.

## Build

```bash
cargo check --workspace --all-targets --locked
cargo test --workspace --locked
cargo build --workspace --locked --release
docker build -f docker/engine.Dockerfile -t seealpha-engine:dev .
```

## Local API

The service listens on `127.0.0.1:8766` by default and requires a PostgreSQL URL through `SEEALPHA_DATABASE_URL`. There is no bearer-token authentication. It is intended for a trusted local machine or restricted network boundary, **not for exposure on a public interface**.

```bash
curl http://127.0.0.1:8766/health
curl http://127.0.0.1:8766/pipeline/subscriptions
curl http://127.0.0.1:8766/research/v1/datasets
```

Main paths: `/accounts/{account_id}/orders` (GET only), `/accounts/{account_id}/fills`, `/data/catalog/*`, `/data/funding`, `/venue/options/*`. The `/strategy-packages/inspect`, `/strategy-loads`, and `/accounts/{account_id}/strategy-preview` paths are **removed** and return 404.

To bind outside loopback (including `ENGINE_HOST=0.0.0.0` in a container), operators **must** explicitly set `ENGINE_ALLOW_UNAUTHENTICATED_REMOTE=1` and enforce network access controls externally. This flag does not provide authentication.

## Data and execution safety

The application has a single installation scope. Pipeline subscriptions are keyed by product and instrument. PostgreSQL state is initialized from the current schema at first start. This change no longer creates `strategy_issuances` in fresh deployments, but does **not** drop historical database tables, files or user state. No legacy database upgrade or data-conversion path is enabled.

Proven exchange fills, request idempotency and order-admission safety must be independently verified before any production execution cutover.

See [architecture](docs/ARCHITECTURE.md) and [Research interface](docs/RESEARCH_API.md).
