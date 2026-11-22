# 数据来源

目前按产品连接域组织：`binance_spot/`、`binance_futures/`（目前仅 USD-M）和 `binance_option/`。Spot 与 USD-M 已使用不同的 Nautilus 原生子进程，Options WebSocket 尚未接通。

`shared/` 是当前共用的 Nautilus、历史 ZIP 与市场模型/REST crate，避免重复实现相同的采集持久化能力。以后逐个优化各产品模块时再下沉专用逻辑；**目录划分不是创建三个可运行连接的声明**。
