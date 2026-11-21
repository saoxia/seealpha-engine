# SeeAlpha Engine 代码导航

目标：从业务需求定位模块，而不是让目录结构复制一次业务流程。

| 要找的行为 | 首选位置 | 关联边界 |
| --- | --- | --- |
| MCP 协议与研究工具 | `engine/gateway/mcp/src/research.rs` | `engine/gateway/api/src/research_v1.rs` 复用授权与任务能力；现有 `/research/mcp` 路由不变 |
| HTTP 路由、权限和版本 | `engine/gateway/api/src/lib.rs`、`src/routes/` | `parity.rs`、`derivatives.rs`、`research_v1.rs` |
| Engine 启动与运行配置 | `engine/apps/trading_engine/src/main.rs`、`engine/runtime/src/config.rs` | `gateway/api` |
| Binance Spot / USD-M / Options 协议 | `engine/exchanges/binance/src/` | `spot/`、`usd_m/`、`options/`、`orders/`、`account/`；Cargo 包名及 Rust 公共模块路径保持不变 |
| 订单发送、重复请求、对账 | `engine/runtime/src/orders.rs` | `order_journal.rs`、`engine/exchanges/binance`、`ledger` |
| 回测协调、研究快照 | `engine/runtime/src/research.rs`、`history.rs` | `gateway/api/src/research/`、`engine/backtest` |
| 策略授权与原生策略 | `engine/strategy/src/` | `engine/runtime/src/session.rs` |
| 研究授权与操作范围校验 | `engine/gateway/api/src/research/` | `engine/gateway/api/src/research_v1.rs`、`engine/gateway/mcp/src/research.rs` |
| 目标持仓、篮子、风险裁剪 | `engine/portfolio/src/` | `runtime/src/basket*.rs` |
| 账户、订单和成交持久化 | `engine/ledger/src/` | `engine/runtime/src/accounts.rs` |
| Binance 行情进程入口 | `engine/market_data/sources/binance/src/bin/` | 同 crate 的 `lib.rs`、`protocol.rs`、`store.rs` |
| Feed 门禁、汇总、数据会话 | `engine/market_data/feed/src/` | `remaining.rs` 的测试在 `remaining/tests.rs` |
| 历史 Catalog 读取、写入和修复 | `engine/market_data/catalog/src/` | `market_data/converter`、`quality` |
| 独立撮合和验证回放 | `backtest/src/` | `engine/runtime/src/verified_replay.rs` |

详细的一级目录所有权及 Risk/Execution 规划见 [领域布局](DOMAIN_LAYOUT.md)。

表中 `src/routes/` 等局部路径是模块内的相对路径，完整 Rust 引擎源码均在 `engine/` 下。

## 组织原则

1. 顶层保留领域归属，Cargo crate 名称不因改目录而改变。
2. `lib.rs` 只应清晰表达模块入口与必要公共 API；体量大的 HTTP handler、测试及应用用例放入职责文件。
3. 同一业务操作跨不同边界是正常的：Runtime 协调、Ledger 记录事实、Venue 对接外部协议，不能因为名字相似就合并安全事务。
4. 不将测试从行为附近移到难以定位的单一大仓库测试目录：单元测试就近存放，跨 crate 场景保留在对应 `tests/`。
5. 不改变不可见的副作用：行情消费者不启动生产者；HTTP 读取不变成下单；未知订单只通过证据对账恢复。
6. 未实现的规划模块写在文档中，不建立空的 crate 假装存在。
