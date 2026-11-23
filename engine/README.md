# Rust 引擎源码

SeeAlpha Engine 的当前 Rust 业务领域与应用入口位于 `engine/`。Cargo Workspace、Docker、测试和文档位于仓库根目录。

| 目录 | 职责 |
| --- | --- |
| [pipeline](pipeline/) | 实时与历史行情、数据质量、Catalog、订阅管理 |
| [execution](execution/) | 目标数量、净额及风险裁剪计算；不是实盘订单发送器 |
| [connection](connection/) | Binance Options 规范化接口；Spot/USD-M 继续采用 Nautilus 原生工厂 |
| [ledger](ledger/) | 账户事实、真实成交及审计证据 |
| [database](database/) | PostgreSQL 数据访问及 schema 引导 |
| [gateway](gateway/) | HTTP API、MCP 网关及只读研究入口 |
| [apps](apps/) | 可执行程序与启动装配 |

2026-11-23 已退役仅支持 EMA 示例的 `engine/strategy/`，同时移除其下发和预览 API。当前**没有运行中的策略执行器**，后续优先接入 NautilusTrader 的原生策略生命周期，而不是恢复独立通用执行框架。

旧 `runtime/` 与 `backtest/` 目录不存在；回测将来独立规划。不要为外观创建空目录或兼容层。持久化行情文件位于仓库根级 `data/`（或外部挂载卷），见 [数据目录约定](../docs/architecture/DATA_LAYOUT.md)。
