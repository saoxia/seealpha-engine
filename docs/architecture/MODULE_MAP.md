# SeeAlpha Engine 代码导航

目标：从业务需求定位模块，而不是让目录结构复制一次业务流程。

| 要找的行为 | 首选位置 | 关联边界 |
| --- | --- | --- |
| HTTP 路由、权限和版本 | `interfaces/api/http/src/lib.rs`、`src/routes/` | `src/routes/parity.rs`、`derivatives.rs`、`research_v1.rs` |
| Engine 启动与运行配置 | `apps/trading_engine/src/main.rs`、`application/runtime/src/config.rs` | `interfaces/api/http` |
| 订单发送、重复请求、对账 | `application/runtime/src/use_cases/orders.rs` | `order_journal.rs`、`connectivity/venue`、`accounting/ledger` |
| 回测协调、研究快照 | `application/runtime/src/use_cases/research.rs`、`src/services/history.rs` | `application/research`、`backtest/core` |
| 策略授权与原生策略 | `alpha/strategy/src/` | `application/runtime/src/use_cases/session.rs` |
| 目标持仓、篮子、风险裁剪 | `portfolio/core/src/` | `application/runtime/src/use_cases/basket*.rs` |
| 账户、订单和成交持久化 | `accounting/ledger/src/` | `application/runtime/src/use_cases/accounts.rs` |
| Binance 行情进程入口 | `market_data/sources/binance/src/bin/` | 同 crate 的 `lib.rs`、`protocol.rs`、`store.rs` |
| Feed 门禁、汇总、数据会话 | `market_data/feed/src/` | `remaining.rs` 的测试在 `remaining/tests.rs` |
| 历史 Catalog 读取、写入和修复 | `market_data/catalog/src/` | `market_data/converter`、`quality` |
| 独立撮合和验证回放 | `backtest/core/src/` | `application/runtime/src/verified_replay.rs` |

## 组织原则

1. 顶层保留领域归属，Cargo crate 名称不因改目录而改变。
2. `lib.rs` 只应清晰表达模块入口与必要公共 API；体量大的 HTTP handler、测试及应用用例放入职责文件。
3. 同一业务操作跨不同边界是正常的：Runtime 协调、Ledger 记录事实、Venue 对接外部协议，不能因为名字相似就合并安全事务。
4. 不将测试从行为附近移到难以定位的单一大仓库测试目录：单元测试就近存放，跨 crate 场景保留在对应 `tests/`。
5. 不改变不可见的副作用：行情消费者不启动生产者；HTTP 读取不变成下单；未知订单只通过证据对账恢复。
6. 未实现的规划模块写在文档中，不建立空的 crate 假装存在。

## Runtime 物理目录（保持原 Rust 模块路径）

```text
application/runtime/src/
├── lib.rs                 # Runtime 公开接口、模块声明和核心状态
├── config.rs              # 运行配置
├── use_cases/             # 从业务动作查入口：账户/订单/组合/回测/会话
│   ├── accounts.rs
│   ├── basket.rs
│   ├── basket_phase.rs
│   ├── orders.rs
│   ├── portfolio.rs
│   ├── research.rs
│   └── session.rs
└── services/              # 被用例使用的数据、证据与后台基础能力
    ├── catalog.rs
    ├── data.rs
    ├── evidence.rs
    ├── fills.rs
    ├── history.rs
    ├── inspection.rs
    ├── jobs.rs
    ├── native_calculation.rs
    ├── order_journal.rs
    ├── sources.rs
    └── verified_replay.rs
```

使用 `#[path = "..."] mod ...;` 保留原模块标识（例如 `crate::orders`），避免把本次物理目录调整变成业务逻辑与 API 迁移。后续按需缩小单文件体量，但不得为满足目录对称而新增空层。
