# Binance Spot

现货产品专属 Instrument ID、Binance ProductType 和原生数据客户端名称位于 `native.rs`，由 `../shared/nautilus/` 引用。

当前 Spot `@trade` 订阅由共享的 Nautilus 二进制以 `spot` 模式启动独立进程，**并不与 USD-M 使用同一个 WebSocket**。REST 和归档共享的实现暂时位于 `../shared/`。
