# Binance Options

期权历史行情**只获取和落盘 K 线（Bar / OHLCV）**，不通过历史行情任务保存逐笔成交、订单簿、报价、Mark/Greeks 快照或公开行权事件。

## 历史行情链路

`API → PostgreSQL pipeline_subscriptions → options Pipeline Worker → OptionsSource::history() → Binance 官方 /eapi/v1/klines → Catalog`。现有 Runtime backfill 仅作为旧接口兼容，非新配置主链路。

- 期权历史任务必须指定 `product=options`，并使用 `data_kind=bars`（不传时默认 `bars`）。还需指定完整期权合约 `symbol`、`interval`、`start_ms` 和 `end_ms`。
- 当前实现支持 `1m`、`5m`、`15m`；最多 5,000 根历史 K 线，且必须使用已经收盘的交易所数据。不能凭空填造零成交 K 线，交易所缺失历史时保持失败/缺失状态。
- 以前支持的公开行权历史 `data_kind=exercise` 不再通过行情任务采集及写入；包括已有但尚未执行的排队任务。已保存的公共行权证据读取、Ledger 历史记录与签名账户结算仍独立保留；Pipeline 旧公开行权 REST 抓取及无调用方的即时 Trade/BookTicker 解析入口已清除。
- `contracts()` 的期权合约列表只用于识别可用合约，并非历史行情数据。已有期权实时消息的解析函数不自动连接 WebSocket，也不自动落盘。
- 不对期权复用 Spot/永续的官方成交 ZIP 补齐链路，也不创建期权 TradeTick 历史存储。此限制不改变账户交易安全门禁。

本产品数据边界已在 [Pipeline 基线](../../BASELINE.md) 固定；当前已有的 `option_stream.rs` 仅为尚未接通的实时消息解析器，不构成历史成交采集或落盘服务。


## 自主管理（与 Spot / USD-M 共用配置 API）

可通过 `PUT /tenants/{tenant}/pipeline/subscriptions/options/{symbol}` 维护完整期权合约的 `history_start`、`enabled`、`realtime_enabled`。Docker 中的独立 `options` Pipeline Worker 每 30 秒读取 PostgreSQL。

- 历史数据调用现有 Binance Options EAPI K 线接口，按完整 UTC 日补齐 **15m Bar**，严格保留合约到期前的完整交易日，且不生成伪造的零成交数据。
- 期权历史没有 Spot/USD-M 的 1d 原生 K 线接口，不能声称每日 1d Bar；“按日补齐”指按日组织 15m Bar 检查与写入。
- 发布期权历史 Parquet 前必须在配置中提供来自权威 Instrument 元数据的 `price_precision` 和 `size_precision`；缺失时跳过，不猜测。
- 开始日期后移时，按严格的完整文件边界清理对应合约的旧 Bar；跨日期边界文件不删除、不裁剪。
- `realtime_enabled` 目前仅记录期望状态：期权实时 WebSocket 尚未实现，Worker 不会启动假采集器，也不会宣称 READY。
- 已到期期权不会请求其到期日及之后的完整 UTC 日；到期当天的部分会话处理仍需单独完善。
