# SeeAlpha Engine 代码导航

目标：从业务需求定位模块，而不是让目录结构复制一次业务流程。

| 要找的行为 | 首选位置 | 关联边界 |
| --- | --- | --- |
| HTTP 路由、权限和版本 | `engine/interfaces/api/http/src/lib.rs`、`src/routes/` | `parity.rs`、`derivatives.rs`、`research_v1.rs` |
| Engine 启动与运行配置 | `engine/apps/trading_engine/src/main.rs`、`engine/application/runtime/src/config.rs` | `interfaces/api/http` |
| 订单发送、重复请求、对账 | `engine/application/runtime/src/orders.rs` | `order_journal.rs`、`engine/connectivity/venue`、`accounting/ledger` |
| 回测协调、研究快照 | `engine/application/runtime/src/research.rs`、`history.rs` | `research`、`engine/backtest/core` |
| 策略授权与原生策略 | `engine/strategy/src/` | `engine/application/runtime/src/session.rs` |
| 研究授权与操作范围校验 | `engine/research/src/` | `engine/application/runtime/src/research.rs` |
| 目标持仓、篮子、风险裁剪 | `engine/portfolio/core/src/` | `application/runtime/src/basket*.rs` |
| 账户、订单和成交持久化 | `engine/accounting/ledger/src/` | `engine/application/runtime/src/accounts.rs` |
| Binance 行情进程入口 | `engine/market_data/sources/binance/src/bin/` | 同 crate 的 `lib.rs`、`protocol.rs`、`store.rs` |
| Feed 门禁、汇总、数据会话 | `engine/market_data/feed/src/` | `remaining.rs` 的测试在 `remaining/tests.rs` |
| 历史 Catalog 读取、写入和修复 | `engine/market_data/catalog/src/` | `market_data/converter`、`quality` |
| 独立撮合和验证回放 | `backtest/core/src/` | `engine/application/runtime/src/verified_replay.rs` |

详细的一级目录所有权及 Risk/Execution 规划见 [领域布局](DOMAIN_LAYOUT.md)。

表中 `src/routes/` 等局部路径是模块内的相对路径，完整 Rust 引擎源码均在 `engine/` 下。

## 复用与迁移导航

- **当前实际运行代码**：`engine/connectivity/venue` 的 `VenueClient` 和 `engine/application/runtime/src/orders.rs` 的直接调用仍为旧 Binance 自研链路；不是 Nautilus 原生执行。
- **当前原生复用缺口**：Spot、USD-M、行情 WS/REST、订单/成交报告及一般回放等，应首先核对锁定版本 Nautilus 的实现并替代自研，而不是按交易所增加另一组适配器文件。
- **确需扩展**：Binance Options 等上游尚未提供完整 live client 的产品面及 SeeAlpha 专属多租户/持久化证据逻辑；仅实现经审计的差异。
- **迁移规则和验证门禁**：[NAUTILUS_REUSE](NAUTILUS_REUSE.md)。未通过等价验证前不能将目标状态写成实际实现。

## 组织原则

1. 顶层保留领域归属，Cargo crate 名称不因改目录而改变。
2. `lib.rs` 只应清晰表达模块入口与必要公共 API；体量大的 HTTP handler、测试及应用用例放入职责文件。
3. 同一业务操作跨不同边界是正常的：Runtime 协调、Ledger 记录事实、Venue 对接外部协议，不能因为名字相似就合并安全事务。
4. 不将测试从行为附近移到难以定位的单一大仓库测试目录：单元测试就近存放，跨 crate 场景保留在对应 `tests/`。
5. 不改变不可见的副作用：行情消费者不启动生产者；HTTP 读取不变成下单；未知订单只通过证据对账恢复。
6. 未实现的规划模块写在文档中，不建立空的 crate 假装存在。
