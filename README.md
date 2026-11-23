# SeeAlpha Engine

Rust-based market-data, strategy, backtest and read-only trading research engine. The process binary is `seealpha-engine`.

## Modules

- `engine/apps/trading_engine/` — binary entry point, PostgreSQL schema bootstrap, domain Store assembly, HTTP server and read-only market smoke.
- `engine/gateway/api/` — HTTP v2 boundary, routes, contract version, validation and direct domain calls.
- `engine/gateway/mcp/` — optional read-only Research MCP adapter.
- `engine/strategy/` — native strategy package inspection, immutable issuance and execution.
- `engine/execution/` — target quantity, netting and clipping.
- `engine/pipeline/` — market ingestion, Catalog, subscription desired state and source interfaces.
- `engine/ledger/` — verified economic evidence, fills and audit history.
- `engine/database/` — PostgreSQL access and schema bootstrap.
- `engine/backtest/`, `engine/connection/` — backtests and external integrations.

There is **no `engine/runtime` crate**, application-level Engine Token, or caller-supplied tenant. Strategy and pipeline operations use existing domain stores directly.

## Build

```bash
cargo check --workspace --all-targets --locked
cargo test --workspace --locked
cargo build --workspace --locked --release
docker build -f docker/engine.Dockerfile -t seealpha-engine:dev .
```

## Local API

The service listens on `127.0.0.1:8766` by default and requires a PostgreSQL URL through `SEEALPHA_DATABASE_URL`. There is no bearer-token authentication. It is intended for a trusted local machine or a restricted network boundary, **not for exposure on a public interface**.

```bash
curl http://127.0.0.1:8766/health
curl http://127.0.0.1:8766/pipeline/subscriptions
curl http://127.0.0.1:8766/research/v1/datasets
```

Main paths: `/strategy-packages/inspect`, `/strategy-loads`, `/accounts/{account_id}/strategy-preview`, `/accounts/{account_id}/orders` (GET only), `/accounts/{account_id}/fills`, `/data/catalog/*`, `/data/funding`, `/venue/options/*`.

To bind outside loopback (including `ENGINE_HOST=0.0.0.0` in a container) operators **must** explicitly set `ENGINE_ALLOW_UNAUTHENTICATED_REMOTE=1` and enforce network access controls externally. This flag does not provide authentication.

## Data and execution safety

Existing PostgreSQL `tenant_id` columns and immutable strategy record layouts remain for **non-destructive compatibility**. New callers cannot select a tenant; current engine writes use the fixed internal legacy storage namespace `local`. Existing records in another namespace remain retained but are not automatically migrated or exposed. No table is dropped or rewritten.

No real order transmission is enabled. A strategy preview only computes signals and target exposures, never fills or actual positions. Proven exchange fills, request idempotency and order-admission safety must be independently verified before any production execution cutover.

See [architecture](docs/ARCHITECTURE.md) and [Research interface](docs/RESEARCH_API.md).
