# Binance Options

期权历史行情**只获取和落盘 K 线（Bar / OHLCV）**，不通过历史行情任务保存逐笔成交、订单簿、报价、Mark/Greeks 快照或公开行权事件。

## 历史行情链路

`OptionsSource::history()` → Binance 官方 `GET /eapi/v1/klines` → 现有 K 线校验 → Runtime 的 `backfill` → Catalog 的 Bar 持久化。

- 期权历史任务必须指定 `product=options`，并使用 `data_kind=bars`（不传时默认 `bars`）。还需指定完整期权合约 `symbol`、`interval`、`start_ms` 和 `end_ms`。
- 当前实现支持 `1m`、`5m`、`15m`；最多 5,000 根历史 K 线，且必须使用已经收盘的交易所数据。不能凭空填造零成交 K 线，交易所缺失历史时保持失败/缺失状态。
- 以前支持的公开行权历史 `data_kind=exercise` 不再通过行情任务采集及写入；包括已有但尚未执行的排队任务。已存在的行权证据、解析代码以及真正的账户结算职责保留，不与期权历史行情混合。
- `contracts()` 的期权合约列表只用于识别可用合约，并非历史行情数据。已有期权实时消息的解析函数不自动连接 WebSocket，也不自动落盘。
- 不对期权复用 Spot/永续的官方成交 ZIP 补齐链路，也不创建期权 TradeTick 历史存储。此限制不改变账户交易安全门禁。
