# SeeAlpha Engine 代码导航

| 需求 | 代码位置 |
| --- | --- |
| 进程启动、数据库初始化、Store 装配 | `engine/apps/trading_engine/src/main.rs` |
| HTTP 服务参数、监听约束 | `engine/gateway/api/src/config.rs` |
| 路由、合同版本、HTTP 错误映射 | `engine/gateway/api/src/lib.rs`、`src/routes/` |
| Strategy package / issuance | `engine/strategy/src/native_issuance.rs`、`store.rs` |
| 已下发策略只读预览 | `engine/gateway/api/src/strategy_preview.rs` |
| 目标持仓计算、净额与风险裁剪 | `engine/execution/target/` |
| 行情标准化 | `engine/pipeline/sources/shared/market/` |
| 历史 Catalog | `engine/pipeline/catalog/` |
| Subscription 配置 | `engine/pipeline/catalog/src/subscriptions.rs` |
| 订单证据、验证成交和历史审计 | `engine/ledger/` |
| PostgreSQL 访问及共享 schema | `engine/database/`、`docker/seealpha-postgres-schema.sql` |
| Research 只读数据集发现 | `engine/gateway/api/src/research_v1.rs` |
| 可选 MCP 只读适配 | `engine/gateway/mcp/src/research.rs` |
| 原生 Binance / Nautilus 独立行情节点 | `engine/pipeline/sources/shared/nautilus/` |

不再存在 Runtime crate、旧回测 crate 或通用应用层租户/Token 授权服务。历史 DB `tenant_id` 为待非破坏性迁移的兼容字段，不在 HTTP 路由中暴露。不要为了保持目录命名而重新创建空的中间层。禁用实盘发送和保证可信成交事实属于独立交易安全边界。
