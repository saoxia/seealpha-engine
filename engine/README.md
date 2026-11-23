# Rust 引擎源码

SeeAlpha Engine 全部 Rust 业务领域和应用程序放在本目录，根目录保留 Cargo Workspace 配置、Docker、性能基准、测试及文档。

| 目录 | 职责 |
| --- | --- |
| [pipeline](pipeline/) | 完整行情服务：实时订阅、历史采集、质量认证和数据访问 |
| [strategy](strategy/) | 策略、信号与目标发布 |
| [execution](execution/) | 组合、目标持仓净额和风险裁剪 |
| [connection](connection/) | Binance Options 专属规范化接口；Spot/USD-M 标准执行由 Nautilus 原生工厂规划承接 |
| [ledger](ledger/) | 账户与账本事实及持久化 |
| [runtime](runtime/) | 跨领域用例编排 |
| [gateway](gateway/) | HTTP API、MCP 网关与研究授权 |
| [apps](apps/) | 可执行入口 |

独立的 `risk/` 和订单执行状态机仍是规划职责，不创建空目录。回测模块已退役，后续另行重建。详见 [领域布局](../docs/architecture/DOMAIN_LAYOUT.md) 和 [代码导航](../docs/architecture/MODULE_MAP.md)。

持久化行情文件位于仓库根级（或外部挂载卷）的 `data/`，不属于 `engine/` 源码目录；参见 [数据目录约定](../docs/architecture/DATA_LAYOUT.md)。
