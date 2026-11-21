# Binance 数据源：Nautilus-first 清理状态

## 当前路径

- 实时 Spot / USD-M 成交、订阅与 REST 最近成交：`integrations/nautilus_binance_data` 中的 NautilusTrader Rust 原生客户端，**仅由管理员显式启用、始终 DEGRADED**。
- 历史归档下载：`archive_fetch.rs` 与 `binance-trade-archive-fetch`；下载官方每日 ZIP、SHA-256 校验、不执行账户操作。
- 官方 ZIP → 原生 TradeTick / Parquet：`integrations/nautilus_binance_data/src/bin/import_archive.rs`，必须通过来源及精度检查；`archive_import` 由现有租户任务系统执行。
- 策略配置：保留 `parse_manifest` 及只读 `binance-manifest-inspect`，由 Runtime 校验已授权交易对，不再自行计算 WS 分片与重连。
- 各路径仍不能自动宣称历史连续、Catalog 交接完整或可开放交易风险。

## 本次删除的旧代码

```text
src/ws.rs
src/protocol.rs
src/store.rs
src/bin/native_source.rs
src/bin/native_replay.rs
tests/feed_names.rs
```

并删除 `binance-native-source` 与 `binance-journal-replay` 的 Cargo 目标、`tokio-tungstenite` / `futures-util` 直接依赖、旧分片分组及自研版本迁移计划；生产 Docker 不再复制旧 WS 采集器。Runtime 的 source start **只有 Nautilus 原生可选**，不提供 `ENGINE_SOURCE_BIN` 自研回退。

## 刻意保留的过渡资产

- `src/calc.rs`、`src/bin/native_calculation.rs`：仍由 Runtime 的旧 SQLite K 线计算控制端点引用，仅适用于既有历史数据，不是 Nautilus 在线行情读取。
- `src/bin/native_catalog_publish.rs`：仍由 Runtime 历史发布任务引用，需要在真实 Catalog 消费链路迁移后另行移除。
- `src/trade_archive.rs`、`src/trade_features.rs` 及相关离线 CLI：离线研究或历史数据处理用途；不能直接假定为 Nautilus 原生 Tick 导入器。
- `source_root`：旧离线数据卷读取及部分 Runtime API 仍使用，移除前需证明调用链已切换。

## 验收前门禁

这次是**旧实现清理**，不是声明新实现通过了真实行情验收。必须核查：`cargo check/test/clippy --workspace`、Docker 构建、独立 Nautilus CI，以及真实 Spot/UM WS/REST/ZIP 的 Trade ID、断线重连、时间戳精度和 Catalog 查询一致性。

不得将 `data_ready`、`allow_new_risk` 提升为 `true`。不会访问交易凭据，也不会改动订单处理。
