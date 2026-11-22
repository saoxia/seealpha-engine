# Binance Options

期权与 Spot、USD-M 使用相同的 **Tick → Bar** 唯一来源规则。Binance EAPI `/eapi/v1/klines` 读取器可以用于独立研究、参考和格式校验，但 **不允许进入正式 Catalog Bar 持久化链路**。

## 数据配置

使用 `PUT /tenants/{tenant}/pipeline/subscriptions/options/{symbol}` 保存完整期权合约标识、`history_start`（UTC）、`realtime_enabled` 和 `enabled`。API 只修改 PostgreSQL；Options Worker 定期读取同一配置表，不创建任务队列。

目前 Options Worker 能识别合约并按安全文件边界执行既有 Bar 保留范围清理；**没有实现期权原生 TradeTick 采集、逐笔历史 ZIP 回填、以 Tick 生成全周期 Bar 或实时 WebSocket 订阅**。即使配置 `realtime_enabled=true`，也不能把期权行情当作 READY。

## K 线标准

期权未来应与其他产品共用基于 Tick 的 OHLCV 聚合器；最低 `1s`，支持交易所对应周期。`1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M`。有 Tick 时，尚未闭合的 Bar 立即更新 Open / High / Low / Close / Volume；没有 Tick 时不虚构 K 线。历史重放和实时接力都必须校验 TradeTick 唯一身份、顺序和覆盖完整性。

之前使用 EAPI 已收盘 15m K 线直接回填 Catalog 的路径已停用；底层 REST 读取代码暂留，但不是正式 Bar 数据源。若需要继续支持期权行情，下一步必须接入真实的期权 Tick 来源并完成质量验收，不能通过恢复交易所 Kline 写入来替代。
