# PostgreSQL-only SeeAlpha runtime state

SeeAlpha stores its Ledger, Control and Strategy business state in the `seealpha`
PostgreSQL schema. NautilusTrader's native Cache schema stays separate in `public`.
Market Parquet/Catalog and signed strategy artifacts are not stored in these tables.

## Configuration

Set `SEEALPHA_DATABASE_URL=postgresql://user:password@host:5432/database`.
The single-container Docker entrypoint configures its own isolated loopback database.
A standalone Engine requires an existing PostgreSQL server. There is no SQLite fallback.

Runtime bootstraps the actively used tables from
`docker/seealpha-postgres-schema.sql`. The current schema creates 11 tables:
tenants, order_intents, order_observations, orders, fills,
quarantined_unverified_fills, account_evidence_events, account_event_allocations,
account_settlements, pipeline_subscriptions, and strategy_issuances.
Immutable order intentions, pending reconciliation and authenticated economic
evidence must not be bypassed or replaced by synthetic fills.

## Retired compatibility state

The old SQLite-to-PostgreSQL importer and unused legacy/Paper tables have been
removed from the **new schema**. No legacy data import is part of startup.
The Docker entrypoint refuses to start if old SQLite state files are present,
rather than silently replacing potentially important order evidence.

The bootstrap is create-only: it does not execute destructive `DROP TABLE`
operations against a database that was initialized by an older release.
Fresh deployments do not create retired tables; any already-existing,
unreferenced tables need a separately authorized operational cleanup.

## CI / required checks

- `cargo check --workspace --all-targets --locked`
- `cargo test --workspace --locked --no-fail-fast` with an isolated PostgreSQL database
- `cargo fmt --all --check` (existing formatting debt is advisory in CI)
- `cargo clippy --workspace --all-targets --locked --keep-going -- -D warnings`
- `cargo build --workspace --locked --release`
- Docker cold-start and restart smoke tests
