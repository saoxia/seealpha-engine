# Binance Futures（当前 USD-M）

`native.rs` 定义 USD-M Instrument ID、Binance ProductType 和原生客户端名称。`funding.rs` 负责公开资金费率、资金费结算数据处理；保留原 `seealpha-market::funding` API。

当前 `futures_um` 通过独立 Nautilus 进程采集 `@aggTrade`，并不复用 Spot 的连接。**尚未覆盖 COIN-M**；本次不新增 COIN-M WebSocket。
