# SeeAlpha Engine 代码导航

本页记录当前源码位置，不把历史迁移文件里的已删除路径当作现行模块。

| 行为 | 当前代码位置 | 边界 |
| --- | --- | --- |
| 二进制启动 / smoke | `engine/apps/trading_engine/src/main.rs` | 生产 HTTP 入口 |
| 配置与策略用例 | `engine/runtime/src/config.rs`、`lib.rs`、`session.rs` | 发行、预览、Ledger |
| HTTP 路由和认证 | `engine/gateway/api/src/lib.rs`、`src/routes/` | v2 合同、租户范围 |
| Research 签名与数据集权限 | `engine/gateway/api/src/research/` | 不提供回测任务 |
| Research HTTP / MCP | `engine/gateway/api/src/research_v1.rs`、`engine/gateway/mcp/src/research.rs` | 数据集查询、快照 501、MCP list_datasets |
| 原生策略和目标发布 | `engine/strategy/src/` | 受控 Rust 策略 |
| 目标净额及风险裁剪 | `engine/execution/` | 不执行模拟撮合 |
| 订单、成交、账本事实 | `engine/ledger/` | 必须可信证据 |
| Binance Options 规范化 | `engine/connection/binance-options/` | Spot/USD-M 接口位于 Nautilus |
| 现货及永续原生数据 | `engine/pipeline/sources/shared/nautilus/` | 独立 Cargo 工程 |
| 公共行情及校验 | `engine/pipeline/sources/shared/market/` | K 线、成交和质量规则 |
| 历史 Catalog | `engine/pipeline/catalog/src/` | Parquet 读取、库存、缺口检查 |
| 历史 ZIP 获取 | `engine/pipeline/sources/binance/archive/` | 数据导入，不运行回测 |

**已移除**：旧 `engine/backtest/` 和回测 API、模拟回放、模拟成交及回测结果。开发新回测时应新建明确边界，不将这些能力附着在现行行情读取或策略预览中。

持久化数据目录见 [DATA_LAYOUT.md](DATA_LAYOUT.md)。
