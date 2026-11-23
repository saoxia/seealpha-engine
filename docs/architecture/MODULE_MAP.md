# SeeAlpha Engine 代码导航

| 需求 | 代码位置 |
| --- | --- |
| 进程启动、数据库初始化、Store 装配 | `engine/apps/trading_engine/src/main.rs` |
| HTTP 服务参数、监听约束 | `engine/gateway/api/src/config.rs` |
| 路由、合同版本、HTTP 错误映射 | `engine/gateway/api/src/lib.rs`、`src/routes/` |
| 目标数量、净额与风险裁剪计算 | `engine/execution/target/`（当前未作为策略 API 挂载） |
| 行情标准化 | `engine/pipeline/sources/shared/market/` |
| 历史 Catalog | `engine/pipeline/catalog/` |
| Subscription 配置 | `engine/pipeline/catalog/src/subscriptions.rs` |
| 订单证据、验证成交和历史审计 | `engine/ledger/` |
| PostgreSQL 访问及共享 schema | `engine/database/`、`docker/seealpha-postgres-schema.sql` |
| Research 只读数据集发现 | `engine/gateway/api/src/research_v1.rs` |
| 可选 MCP 只读适配 | `engine/gateway/mcp/src/research.rs` |
| 原生 Binance / Nautilus 独立行情节点 | `engine/pipeline/sources/shared/nautilus/` |

当前没有策略下发、示例 EMA 执行、策略预览或通用自研策略宿主。若日后恢复策略服务，应直接复用 NautilusTrader 原生生命周期并重新设计 SeeAlpha 特有业务对接，而不是恢复已退役示例接口。

旧 Runtime / Backtest crate 与通用应用层租户、Token 授权服务不存在。不可用实盘发送及真实成交的可信证据认证属于独立交易安全边界。
