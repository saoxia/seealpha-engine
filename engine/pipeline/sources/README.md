# 数据来源

目前按产品连接域组织：`binance_spot/`、`binance_futures_um/`和 `binance_option/`。Spot 与 USD-M 已使用不同的 Nautilus 原生子进程，Options WebSocket 尚未接通。

`shared/` 暂时保留跨产品复用的 Nautilus 与市场模型/REST crate，避免重复实现相同的采集持久化能力。以后逐个优化各产品模块时再下沉专用逻辑；**目录划分不是创建三个可运行连接的声明**。

`binance_futures_um/` 只对应 Binance USD-M Futures；未来 COIN-M 独立放在 `binance_futures_cm/`，不混用 WebSocket、合约 ID 或数据目录。

Spot 与 USD-M 现已具有各自的 `repair.rs` 入口，共用 `shared/market/src/repair.rs` 的大周期到小周期 K 线分层检查。Spot 的归档数据集是 `trades`，USD-M 是 `aggTrades`；两条数据链路不可混合。

Binance 官方 ZIP 下载代码保存在 `binance/archive/`，仅供 Binance 产品使用。Spot 与 USD-M 共享一份下载实现，但现货 `trades` 与 USD-M `aggTrades` 由各自产品入口区分，不作为跨交易所的 `shared/` 抽象。

`binance_option/` 历史市场数据仅使用 Binance 官方期权 K 线（`bars`），按期权合约与周期落盘；不对期权采集历史成交、订单簿或公开行权事件。合约元数据用于识别品种，不属于历史行情。

上述 Spot、USD-M、Options 的产品数据边界已记录于 [Pipeline 基线](../BASELINE.md)。冻结已有责任划分，不为未接通的接口创建平行采集或修复程序。
