# ClickHouse USD-M historical trade connection

数据表的完整 DDL、字段语义、交易对映射、去重限制和历史修复边界详见 [SCHEMA.md](SCHEMA.md)。

This is a **read-only** connection to the user's existing `binance_trade` table.
No migrations, source ingestion, automatic Catalog writes or live collector changes.

Configuration (server-side environment only):

- `SEEALPHA_CLICKHOUSE_ENABLED`: **defaults to off**; set exactly `true` to opt in (URL alone never enables fallback)
- `SEEALPHA_CLICKHOUSE_URL`: HTTP(S) endpoint such as `http://127.0.0.1:8123` (omit to disable)
- `SEEALPHA_CLICKHOUSE_DATABASE`: defaults to `default`
- `SEEALPHA_CLICKHOUSE_USER`: defaults to `default`; use read-only credentials
- `SEEALPHA_CLICKHOUSE_PASSWORD`: optional

Use `ClickHouseConfig::from_env()`, `ClickHouseConnection::new(config)`, and
`read_um_trades(storage_symbol, start_ms, end_ms, after, limit)`.
The helper `um_storage_symbol(engine_symbol)` maps `ETHUSDT` to
`ETH/USDT:USDT` and `ETHUSDC` to `ETH/USDC:USDC`.
This applies to USDT / USDC-settled perpetual symbols only; unsupported pairs
return an error. The observed stored examples include ETH, BTC, SOL, and DOGE
for USDT, and ETH, BTC, SOL for USDC. Do not treat the examples as a complete
list of actually available symbols.

Windows are half-open UTC milliseconds, pages are sorted by `(dt, trade_id)`,
maximum 10,000 rows. Use the last returned row's `TradeCursor` to page. Queries
use `FINAL` for `ReplacingMergeTree` duplicate versions. `Decimal(32,12)`
values remain strings to avoid narrowing precision.

The **existing Binance official ZIP remains the first choice**. Only a failed
USD-M ZIP fetch triggers the bounded ClickHouse read of a suspect minute.
ClickHouse is **disabled by default**, even with a configured URL. Only `SEEALPHA_CLICKHOUSE_ENABLED=true` opts in. It is optional and best-effort: absent URL, invalid settings, connection
failure, query error, unsupported symbol or zero rows all skip the fallback
without replacing the original ZIP failure. A successful official ZIP path never
queries ClickHouse. No connection attempt is made at service startup.
Currently this is evidence-only: no Catalog import or completeness guarantee.
**Important:** the table is raw `trade`, while the current USD-M Nautilus
repair/import path uses `aggTrades`. Never pass these rows through that importer
or conflate `trade_id` with aggregate trade ID. Spot fallback is not enabled.
