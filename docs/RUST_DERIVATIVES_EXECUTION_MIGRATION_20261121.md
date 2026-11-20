# Rust 期权、资金费与真实成交迁移（2026-11-21）

## 背景与边界

本批次继续基于旧 Python Engine `f18e48e` 的行为迁移；Rust Engine 是默认运行时，Python Engine 暂不删除。GitHub Actions 在 `.github/workflows-paused/` 中保持停用。**此批代码尚未通过 Cargo 编译或端到端验收，绝不能据此启用未验证的实盘订单。**

Binance 公共 REST API 已核对：

- 期权合约详情：`GET https://eapi.binance.com/eapi/v1/exchangeInfo`。
- 期权历史 K 线：`GET https://eapi.binance.com/eapi/v1/klines`，带 `symbol/interval/startTime/endTime/limit`。
- 期权公开行权参考事件：`GET https://eapi.binance.com/eapi/v1/exerciseHistory`，带 `underlying/startTime/endTime/limit`。
- 永续已结算资金费事件：`GET https://fapi.binance.com/fapi/v1/fundingRate`，带 `symbol/startTime/endTime/limit`。

不同产品拆分成 Rust 只读模块：

- `crates/seealpha-market/src/options.rs`：合约标识和期权历史行情。
- `crates/seealpha-market/src/funding.rs`：资金费结算事件，处理 `Regular`/`Special`，不假定每 8 小时固定发生。
- `crates/seealpha-ledger/src/funding.rs`：多租户、原子、冲突拒绝式持久化结算记录。
- `crates/seealpha-backtest/src/evidence.rs`：资金费跨市场持仓与买入 Put + 永续持仓按历史价格估值。
- `crates/seealpha-venue/src/trades.rs` 和 `crates/seealpha-ledger/src/verified_fills.rs`：交易所实际成交 ID、历史假成交隔离及订单对账。

## 已接入的 HTTP 操作

下例请求均需原 Engine 鉴权及有效租户，不包括密钥：

| 路径 | 用途 | 约束 |
| --- | --- | --- |
| `POST /tenants/{tenant_id}/venue/options/contracts` | 官方期权合约目录 | 只读 |
| `POST /tenants/{tenant_id}/venue/options/klines` | 官方期权闭合历史 K 线 | 只读；最多 5000 根，1m/5m/15m |
| `POST /tenants/{tenant_id}/data/control/job/start` | 现货、永续、期权历史 K 线补齐，以及永续结算资金费补齐 | 任务入账后独立执行，失败不覆盖已有冲突证据 |
| `POST /tenants/{tenant_id}/data/funding` | 从本地只读账本查询已结算资金费 | 最多 500 条最近记录或时间窗口数据 |
| `POST /tenants/{tenant_id}/data/options/exercises` | 查询已落盘的公开期权行权参考事件 | 非账户成交证据、非实际账户现金流 |
| `POST /tenants/{tenant_id}/data/coverage`, `/page`, `/chart` | 新增 options 产品支持 | 只读本地 SQLite 证据 |
| `POST /tenants/{tenant_id}/accounts/{account_id}/carry-backtests` | 真实现货/永续价格加真实资金费事件的组合纸面回测 | 传入历史日期时启用；需要显式 fee_rate |
| `POST /tenants/{tenant_id}/accounts/{account_id}/protected-option-backtests` | 买入 Put 保护永续多头的期权价格回测 | 传入历史日期时启用；不模拟行权/结算 |
| `POST /tenants/{tenant_id}/accounts/{account_id}/requests/{request_id}/fills/sync` | 已知委托的 Spot/UM 成交明细签名 GET 查询和入账 | 必须配置交易网关；不重发下单；不支持未经验证的期权成交 |

期权补齐任务例子：

```json
{
  "payload": {
    "action": "backfill",
    "product": "options",
    "symbol": "BTC-271231-90000-P",
    "interval": "1m",
    "start_ms": 1810000020000,
    "end_ms": 1810000080000
  }
}
```

*时间仅作请求格式示例；区间必须与交易所已闭合 K 线对齐并实际存在。*

资金费补齐：

```json
{
  "payload": {
    "action": "backfill",
    "data_kind": "funding",
    "product": "futures_um",
    "symbol": "ETHUSDT",
    "start_ms": 1763500000000,
    "end_ms": 1763586400000
  }
}
```

当前新增 `data_kind=exercise`、`product=options`、`symbol=BTCUSDT` 的显式历史任务，可以导入 **公开参考事件**。公开 exerciseHistory 并不代表该租户持仓发生行权，不能据此自动写 Fill 或增加账户余额。历史查询限定 90 天且满 100 条直接拒绝，避免漏分页。

实际的融资利息、逐档手续费、冲击成本、强平、期权行权与交割仍必须按正式的真实数据或特定模拟器另行处理。空资金费事件列表不能证明没有资金费；本地事件的完整性目前明确标为 *not certified*。

## 订单账本的重要纠正

旧 Rust 代码在交易所回传 `NEW` 时，会自动创建一个没有价格、数量和交易所 Trade ID 的 `fill-<client_order_id>`。这不是成交，可能污染仓位和收益。新版本：

1. 不再根据订单受理 ACK 生成虚构的 Fill。
2. 启动时将符合旧模式的无数量/价格占位 Fill 原样搬入 `quarantined_unverified_fills` 审计表。保留数据，不当真实成交计算。
3. 针对 Spot/UM 已知订单发起 **signed GET** 查单及 `myTrades/userTrades`，按交易所实际 `orderId`、`tradeId`、`symbol` 严格过滤。
4. 同一交易所 Trade ID 重放幂等；值变更冲突时整批事务拒绝。
5. 查询数量达到单页上限、交易所结果未知、已报成交数量与已获取成交明细不一致时，不宣称订单已完整对账。
6. HTTP 成交对账仅接受账户环境，使用已登记且只在内存保存的账户密钥；**禁止**在每次对账请求 JSON 中传递 API Key / Secret，避免新增加密密钥暴露面。
7. 暂不对期权 `userTrades` 冒险确认；尚未通过分页和鉴权边界验收。

这次修改**不会**回滚已向真实交易所发送过的订单，也绝不能在连接超时后自动重发订单。

## 仍未完成的等价迁移

NautilusTrader 原生 Parquet Catalog 与 tick/depth 回放；Binance Options 实时独立进程和账户签名数据完整兼容；资金费跨期历史完整性证明；订单成交分页超过 1000 条与长历史；真实保证金和清算模型；期权行权与结算；跨市场策略下发后的自动事件交接；租户数据导入/导出；全量 Platform 接口与 Research 能力。

本轮提交的含义是新增 Rust 运行路径和历史数据模型，不代表完整 Rust 迁移已完成或准许清理 Python 旧实现。

## 结束集中迁移后统一验收

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
uv run pytest -q tests/engine tests/connectors tests/feed tests/pipeline tests/core
```

必须补：完整日期/Kline 缺口、资金费特殊事件、订单取消与部分成交竞态、重启后的未确认订单、用户流更新幂等、期权真实行权及旧版回归夹具。CI/Actions 目前不自动运行。
