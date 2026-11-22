# ClickHouse historical market-data connection

This crate is a **read-only** adapter to the existing `binance_trade` ClickHouse table.
It does not own schema migrations, data ingestion, backfill scheduling or Catalog writes.

Environment variables (set in the server environment, never pass credentials in Data Job payloads):

- `SEEALPHA_CLICKHOUSE_URL`: HTTP(S) endpoint such as `http://127.0.0.1:8123` (omit to disable)
- `SEEALPHA_CLICKHOUSE_DATABASE`: defaults to `default`
- `SEEALPHA_CLICKHOUSE_USER`: defaults to `default`; use a restricted read-only user
- `SEEALPHA_CLICKHOUSE_PASSWORD`: optional

Use `ClickHouseConfig::from_env()`, then `ClickHouseConnection::new(config)` and
`read_spot_trades(symbol, start_ms, end_ms, after, limit)`. Time windows are half-open UTC
milliseconds, pagination is ordered by `(dt, trade_id)`, and the largest page is 10,000 rows.
Pass the last row's `TradeCursor` for the next page. The query uses `FINAL` to handle
ReplacingMergeTree duplicates.

Scope: **Binance Spot raw trades only**. Futures aggregate trades require a separate
table/contract and must not be inferred from this table. `Decimal(32,12)` is returned
as exact strings; convert to Rust Decimal only if values fit its range. Historical
data is evidence, not proof of completeness; downstream repair must validate coverage
and conflicts before import. This connector does not automatically enable a backfill job.
