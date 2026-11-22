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


## HTTP API 配置

使用 Engine HTTP v2 现有的 Bearer Token 身份认证，并要求
`ENGINE_AUTH_MODE=token` 和 `ENGINE_TENANT_ID`（固定租户）。
接口按租户配置，不允许未固定租户的 Engine 修改数据库凭据。

- `GET /tenants/{tenant_id}/data/clickhouse/config`：读取启用状态与非敏感配置。
- `PUT /tenants/{tenant_id}/data/clickhouse/config`：保存配置及启用/关闭状态。

PUT JSON 示例：

```json
{
  "enabled": true,
  "url": "http://127.0.0.1:8123",
  "database": "default",
  "user": "reader",
  "password": "replace-me"
}
```

`enabled` 必填；省略其他字段会保留当前值，故可通过
`{"enabled":false}` 关闭。默认关闭；只配置 URL 不会自动启用。
密码只写：GET 和 PUT 的响应只返回 `password_configured`，不回显密码。
首次启用必须指定有效 URL；保存不会主动访问 ClickHouse。

API 配置按租户存储在 Engine 的 `control.sqlite`，重启后仍然有效。
API 配置优先于环境变量；尚未通过 API 配置的租户仍可使用
`SEEALPHA_CLICKHOUSE_ENABLED=true` 显式启用环境变量配置。
ClickHouse 不可用时保留原有 ZIP 获取失败原因，不会改变其他数据任务。

**安全注意：目前密码以明文保存在本机 SQLite 状态库中**，并非加密密钥仓库。
应保护 Engine 状态目录、备份及管理员 Token，且仅通过 HTTPS 或可信本地
通道发送凭据；不要对不可信租户开放此管理接口。
