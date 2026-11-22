# Ledger / NautilusTrader 重叠能力审计 — 第一批（2026-11-22）

> 历史设计记录：此文件描述的 Accounts SQLite 快照和 Basket 旧 CAS 实验已在后续 PR #193 中退休。当前状态请以 `engine/ledger/README.md` 和原生 Nautilus Observer 为准；下列旧矩阵不代表当前生产功能已完成。


基线：main @ 32fdb064008e2a27d6eeb44ebc84ecf21b4569df。Nautilus 固定为 Rust crate 0.68.0 / 2.0.0rc9；本审计不以较新版本功能代替固定版本的构建验证。

## 归属原则

- Nautilus 原生 ExecutionEngine、RiskEngine、Order/OrderFilled、Position、AccountState、Cache、Reconciliation、Streaming/Catalog 是交易引擎和标准执行事件的首选实现。
- SeeAlpha Ledger 仅保留无法由上述能力满足的租户级不可变提交预登记、独立账户结算证据、成交核验和特有策略协调持久化；策略业务状态机由 Portfolio/Runtime 拥有。
- Nautilus 的事件记录不能直接提升为交易所**签名认证的资金或成交事实**。既不能由 ACK 合成 Fill，也不能由公共费率合成账户收入。
- 迁移按证据链和调用方完成，不按名称直接删库或绕开安全门禁。

## 逐项重叠矩阵

| 领域 | Nautilus 已有能力 | SeeAlpha 当前实现 | 处理 | 退出条件 |
| --- | --- | --- | --- | --- |
| 原生订单事件记录 | StreamingConfig.record_types、NautilusRecordType、原生 Feather/Parquet Catalog | ledger/native_events.rs 仅在测试中写入未认证 metadata | **第一批移除重复写入**，由 Nautilus 模块统一选择记录类型 | 实际 Streaming LiveNode 运行、关闭、持久化与重启测试；事件溯源与证据检验 |
| 订单生命周期 | Order/ExecutionEngine/OrderStatusReport | ledger/order_intents.rs 的 order_observations，runtime/orders.rs 的旧兼容视图 | **暂缓移除**订单状态副本；将来以 Nautilus 为主，Ledger 仅保留不可变发送前指令 | Native 执行接线、部分成交与撤单竞争、断线未知、恢复完整核验 |
| 成交和部分成交 | OrderFilled/FillReport/TradeId、Cache | ledger/verified_fills.rs、records.rs | **暂缓**；真实成交证据依赖交易所认证来源及完整性证明 | 认证 Trade ID、费用、账户归属、分页完整性、重启去重 |
| 当前账户/持仓 | AccountState、Position、Cache | ledger/account_snapshots.rs、parity.rs | **暂缓**；Basket 版本 CAS 仍依赖账户快照 | 启动/持续对账、快照时效、跨租户与组合风险一致性 |
| 对账操作 | Live Execution Reconciliation | ledger/order_reconciliation_tasks.rs | 保留唯一需要的持久恢复任务队列；**不要再造订单状态机** | 原生检查与任务调度接通并证明故障重启不重复发送 |
| 下单前不可变预登记 | 原生引擎订单状态与事件持久化，不构成同步已落盘发送授权 | ledger/order_intents.rs 的 preregister_order_intent | **保留** | 若未来 Nautilus 提供等价的同步持久提交+单次发送约束，再重新评估 |
| 资金费/期权结算 | Nautilus 可处理账户、成交和部分事件，但未证实覆盖所有 Binance 私有资金流水 | ledger/settlements.rs、account_events.rs | **保留** | 逐产品确认身份、签名来源、历史补齐和财务对账 |
| Portfolio/Basket 状态 | 交易所持仓和执行状态，不包含 SeeAlpha 全部多策略业务不变量 | ledger/basket_journal.rs、coordinator_state.rs | **保留数据持久化；业务归 Portfolio** | 无需以 Nautilus 原生持仓代替跨腿风险状态机 |
| 策略净额与内部成交分配 | NETTING/HEDGING、策略虚拟持仓；不自动解决 SeeAlpha 跨策略内部撮合的成本规则 | Portfolio coordinator 和尚待补充的 allocations | 保留 SeeAlpha 业务逻辑，仅将真实发单交给 Nautilus | 明确虚拟成交价格、费用、净头寸守恒和历史回放 |

## 第一批实际变更

1. 删除没有生产调用者的 ledger/src/native_events.rs 及公开 UnverifiedNativeOrderEvent 写入 API。
2. 对新的 Ledger SQLite 不再创建 native_order_event_quarantine；原有数据库的表和数据不 DROP、不修改、不迁移。该表不会再收到任何 SeeAlpha 新写入。历史归档/删除需要另外审计。
3. 在 Nautilus native_execution.rs 中定义 native_execution_record_types()，集中复用 NautilusRecordType，包括 AccountState、订单生命周期、OrderFilled/OrderFillVoided、OrderStatusReport、FillReport、PositionStatusReport。
4. 更新离线 LiveNode StreamingConfig 测试，使用该函数；验证原生记录选择包括成交、账户和报告。
5. 更新 Runtime 测试：仅持久化意图绝不生成经过认证的成交，也不能绕过 ReconcileOnly。

## 未完成与不得宣称的能力

- **未执行 Cargo 测试、Clippy 或运行中 Streaming 实测**。当前运行环境无 Cargo 编译器；需要在固定 Rust/依赖版本的 CI 或隔离主机验收。
- 原生 Streaming 的测试目前只校验 Builder 契约。rc9 下曾发生包含 Streaming 的 LiveNode 销毁异常，不能宣称执行事件已实际写盘或可恢复。
- Runtime 的原生订单仍只可预登记、不可触网发送；没有已认证的 Nautilus-to-Ledger 实际成交写回。
- Event Store / Cache 的异步记录与订单发送前 SQLite 事务门禁不是等价能力，不能因此删除 preregister_order_intent。
- 不得依据 Nautilus OrderFilled 或未认证 FillReport 独立产生真实资金分录。成交和结算的可信度继续由原有 Ledger 核验路径守卫。

## 后续替换顺序与验收

1. 构建并运行带执行 Streaming 的 Nautilus sandbox 节点，验证记录 schema、账户分区、权限、Catalog 写入、关机/崩溃恢复；解决 rc9 生命周期问题。
2. 串接 Nautilus ExecutionEngine 和 Runtime 的发送前安全门禁，在模拟网络下验证一次发送、重试只核验、不重复委托。
3. 将可信 Nautilus 执行报告与交易所交易历史核对，逐笔校验 Trade ID、数量、手续费、账户和时间，再切断旧订单状态与成交副本。
4. 接入 Nautilus Account/Position 的恢复与持续对账，确认 Basket CAS 风控仍然成立，再评估 account_snapshots/parity 的替换。
5. 最后逐项检查结算/策略归属、备份与历史审计的缺口；保留真正独有的 Ledger 数据。

官方设计参考：
- https://nautechsystems.github.io/nautilus_docs/latest/concepts/execution/policies/
- https://nautechsystems.github.io/nautilus_docs/latest/concepts/cache/
- https://nautechsystems.github.io/nautilus_docs/latest/concepts/event_sourcing/
