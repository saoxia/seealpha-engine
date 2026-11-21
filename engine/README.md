# Rust 引擎源码

SeeAlpha Engine 全部 Rust 业务领域和应用程序放在本目录，根目录保留 Cargo Workspace 配置、Docker、性能基准、测试及文档。

| 目录 | 职责 |
| --- | --- |
| [market_data](market_data/) | 行情与历史数据 |
| [strategy](strategy/) | 策略、信号与目标发布 |
| [portfolio](portfolio/) | 组合、资金与目标仓位 |
| [exchanges](exchanges/) | 交易所私有订单及账户协议（当前为 Binance） |
| [ledger](ledger/) | 账户与账本事实及持久化 |
| [backtest](backtest/) | 历史回放与模拟 |
| [runtime](runtime/) | 跨领域用例编排 |
| [gateway](gateway/) | HTTP API、MCP 网关与研究授权 |
| [apps](apps/) | 可执行入口 |

规划中的 `risk/` 和 `execution/` 暂未独立实现，不创建空目录。详见 [领域布局](../docs/architecture/DOMAIN_LAYOUT.md) 和 [代码导航](../docs/architecture/MODULE_MAP.md)。
