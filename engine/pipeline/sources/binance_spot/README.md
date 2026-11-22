# Binance Spot

此目录仅负责 Binance 现货。Spot 与 USD-M 共享 NautilusTrader 和 K 线分层扫描算法，但**不共用 WebSocket、交易 ID 或历史成交数据集**。

| 数据链路 | Spot 实现 |
| --- | --- |
| 原生实时成交 | `native.rs`；NautilusTrader 以 `spot` 启动独立只读进程，订阅原始 `@trade`，合约如 `BTCUSDT.BINANCE` |
| REST 历史 K 线 | `../shared/market/` 访问 Spot `/api/v3/klines`；只有收盘的 K 线参与历史对照 |
| 近期 REST 成交 | `/api/v3/trades` 原始成交；不得与 USD-M 的 `aggTrades` 互换 |
| 官方历史 | `../binance/archive/` 下载校验 `spot/daily/trades` ZIP，再由 `../shared/nautilus/src/bin/import_archive.rs` 导入原生 TradeTick Catalog |
| 历史自维护 | `repair.rs` 固定 Spot 产品，复用 `../shared/market/src/repair.rs` 的分层扫描与 ZIP 触发规则 |

## 按日修复任务

Data Job 支持 `repair_spot_day`，参数必须包含 `product=spot`、`symbol`、`day`（UTC）及从 Nautilus Instrument 获取的 `price_precision`、`size_precision`。

从 `1d → 4h → 1h → 15m → 5m → 1m` 逐级比较，只进入异常父区间。当分钟数据缺失或已存分钟 OHLCV 与交易所不一致时，触发现货 `trades` ZIP 下载进行成交证据核对。对同成交 ID 内容冲突的情况仍不得静默覆盖。既有导入任务必须在本产品的原生实时写入进程停止后执行，不会对活跃的 Parquet Catalog 并发写入。

与 USD-M 当前实现相同，**整日 ZIP 的重叠成交增量合并、导入后的 K 线自动重算和周期性无人值守巡检仍未完成**。导入成功不自动表示所有 K 线已经恢复，也不改变 `data_ready=false`、`allow_new_risk=false` 等运行安全门禁。完整调用链由 Runtime 的通用 Data Job 调度接口管理。
