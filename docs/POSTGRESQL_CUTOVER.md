# PostgreSQL-only SeeAlpha state cutover

The Engine uses PostgreSQL for its own Ledger, Control, and Strategy metadata.
NautilusTrader's PostgreSQL tables stay in the `public` schema; the SeeAlpha
state tables are in the `seealpha` schema. Parquet/Catalog files and immutable
strategy artifacts remain filesystem-backed.

## Configuration

Set `SEEALPHA_DATABASE_URL=postgresql://user:password@host:5432/database`.
The single-container Docker entrypoint exports this automatically for its
private loopback PostgreSQL instance. A raw Engine binary requires the variable
and an already running PostgreSQL server. No SQLite fallback is supported.

Startup initializes 29 tables in the `seealpha` schema. A shared PostgreSQL
cluster has separate tables for the Nautilus native Cache and SeeAlpha business
state; do not merge or manually rename those tables.

## Existing installations (offline one-time import)

1. Stop **all** Engine writers and take a backup of the complete persistent
   state volume, including legacy `-wal` files.
2. Start PostgreSQL in maintenance mode **without starting Engine**. Initialize
   `docker/seealpha-postgres-schema.sql` as the database owner:
   `psql -v ON_ERROR_STOP=1 -d "$SEEALPHA_DATABASE_URL" -f docker/seealpha-postgres-schema.sql`.
3. Run from an operator environment with Python 3 and `psql`:
   `python3 scripts/migrate_legacy_sqlite_to_postgres.py --state /path/to/old/ENGINE_STATE --backup-dir /safe/audit/seealpha-cutover-YYYYMMDD`.
   The destination backup directory must not already exist.
4. The importer snapshots SQLite with its committed WAL, checks integrity,
   imports current tables in a PostgreSQL transaction, validates row counts,
   and writes `.postgres-cutover-complete` into the old Engine state directory.
   It does **not** delete SQLite originals or alter Nautilus tables. The strategy
   index rebuilds from validated immutable issuance files on Engine startup.
5. Verify tenant and account scopes, pending order recovery, recorded fills,
   and external exchange reconciliation. Only then restart the new Engine and
   independently archive/remove legacy SQLite files after audit sign-off.

If `book.sqlite` exists without the cutover marker, the Docker entrypoint
refuses to start the new Engine: silently creating an empty ledger would
discard order idempotency and outstanding reconciliation information.

Do not treat row-count equality alone as evidence of correct trading balances.
The old database and snapshots are audit evidence until venue reconciliation
and explicit operational acceptance.

## CI / required checks

- `cargo check --workspace --all-targets --locked`
- `cargo test --workspace --locked --no-fail-fast` using an isolated PostgreSQL
  database (test fixtures use isolated PostgreSQL schemas)
- `cargo clippy --workspace --all-targets --locked --keep-going -- -D warnings`
- `cargo build --workspace --locked --release`
- Docker cold-start and restart smoke, plus a test with a legacy book that must
  refuse startup before migration.

Before merge, commit the regenerated `Cargo.lock` and verify the lockfile,
compiled binaries and Docker container no longer rely on `rusqlite`.
