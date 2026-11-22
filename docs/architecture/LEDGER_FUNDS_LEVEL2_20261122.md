# Ledger / Funds — 二级业务抽象（2026-11-22）

> **方向澄清：** 一级模块为 SeeAlpha 的「账本与资金」，本文件只定义它内部的二级业务职责；不重划整个交易系统的一级模块。PR #185 当前源码归组（execution_guard / exchange_evidence / portfolio_state / compatibility）只是一项保留行为的过渡性整理，不代表最终二级业务抽象。

## 设计依据

- **NautilusTrader**：Accounting/AccountState 负责运行时余额、保证金、PnL 和账户事件；Portfolio/Position 与执行引擎维护运行状态和交易生命周期。SeeAlpha 优先复用已固定的 Nautilus Rust crates 0.68.0，实际支持情况必须以该版本的构建与端到端测试为准。
- **LEAN**：CashBook 作为组合持有的货币现金与估值视图；不是独立的交易所资金审计凭证。
- **TigerBeetle**：Account、Transfer、Ledger 分离，账户余额由可核验转账事实产生；分录纠错依靠反向或调整事实。SeeAlpha 目前不要求自建复式记账数据库，但应保持资金事实不可随意覆写。
- 用户业务需求：交易所一笔真实净额成交，对应多个策略意图或内部交叉撮合；真实交易与策略虚拟账分离，且必须可解释、可对账。

## 建议的四个二级业务功能块

| 块 | 中文 | 核心问题 | 输入/权威来源 | 数据与职责 | 不负责 |
| --- | --- | --- | --- | --- | --- |
| Accounts | 账户与资金状态 | 现在各账户有多少资产/可用资金/保证金？ | Nautilus Account/AccountState、认证交易所快照；账户归属配置 | AccountId/venue/tenant/environment 映射、状态可读接口、必要快照及证据指针、时效标记 | 重写 Nautilus 余额/保证金/盈亏计算，不以自行构造的数值覆盖真实账户 |
| Movements | 资金与资产变动事实 | 为什么发生了一笔经济变动？ | Nautilus verified execution reports + 交易所独立认证的成交/结算/历史现金流水 | 实际成交证据、费用、资金费、期权结算、转入转出、充提等具有 origin/provenance 的不可变事实；区分资产转移和损益 | 把限价订单 ACK、目标持仓或公开资金费率当成已实际结算的资金 |
| Allocations | 策略内部归属账 | 真实或内部成交、费用和收益归属哪个策略？ | 已认证外部成交/结算 + Portfolio 明确的内部撮合与分摊决策 | strategy_id/run_id、虚拟成交、内部撮合、外部 fill 多对多分配、手续费/资金费归属、规则版本和可重建日志 | 再次向交易所下单；将虚拟成交标记为交易所真实成交 |
| Reconciliation | 账账与账实核对 | 真实账户、外部流水和策略账能否解释一致？ | Nautilus 原生执行/持仓核验 + 交易所认证账户快照和流水 + Allocations | 成交完整性、持仓净额、资金余额及费用守恒；缺失证据、未分配额、冲突、恢复与审计差异 | 自动伪造缺失资金、盲目重复发送订单、丢弃证据来强制对平 |

这四块是**业务逻辑边界**，不是四套数据存储。SQLite/Catalog/Cache 是共享基础设施，旧接口 Compatibility 是临时迁移区，不应列成业务功能块。

## 典型资金流

1. Portfolio 的策略 A 计划买入 2 ETH，策略 B 计划卖出 1 ETH。
2. Portfolio 可决定以约定内部撮合规则处理部分对冲需求；外部仅需净买入 1 ETH。具体时点、内部价格和对手归属由 Portfolio 决定。
3. Nautilus 执行和记录实际 1 ETH 外部订单及 OrderFilled / FillReport；Movements 仅收录有来源证明的外部交易事实与实际收取的费用，不把内部抵消当外部成交。
4. Allocations 记录哪些虚拟成交/策略归属来自该外部 1 ETH，哪些来自内部交叉撮合。策略持仓净和与真实净敞口可以核对。
5. 交易所真实资金费、期权结算或划转到来后，Movements 保留来源，Allocations 根据独立已定义规则在策略之间分摊。
6. Accounts 显示 Nautilus/交易所认可的账户状态；Reconciliation 比较资产、持仓、费用总量与策略归属。

## 最小不可变对象与身份

- 外部身份：tenant_id、environment、venue、account_id、product、instrument_id、venue_trade_id / source_settlement_id、source_uri/ref、event_ts、ingested_ts。
- 内部归属：strategy_id、run_id、internal_trade_id、external_fill_ref（可空，内部撮合须明确标记）、allocated_qty、allocated_fee、attribution_policy_version。
- 差异：reconciliation_run_id、scope、as_of、difference_type、difference_amount、source_evidence_refs、resolution_status。
- 货币数量必须使用精确 Decimal，区分原币金额、折算估值与价格；每笔记录必须区分 exchange_verified / nautilus_runtime / internal_simulated / backtest_provenance。

## 守恒/一致性约束

- 已完全分配的单个外部成交：sum(external_allocated_qty) = venue_fill_qty；未完全分配时有明确 unallocated_qty。
- 同一账户、产品、标的、仓位模式和时间点：内部策略净仓位的合计能够解释实际持仓，差异需形成记录；不可简单地把期权 Delta 当实际持仓数量。
- 真实费用与结算总额 = 策略分配额 + 未分配额；跨账户划转不应同时被算作交易利润。
- 实际账户余额与流水推出的值不等时，优先记录来源和缺失区间，不能直接伪造一笔差额流水。
- 订单 ACK、外部 UNKNOWN、未认证的 FillReport 不得自动记成真实交易和现金。

## 当前代码映射（PR #185 过渡布局）

| 旧代码 | 最终二级归属建议 | 处理 |
| --- | --- | --- |
| exchange_evidence/account_snapshots.rs | Accounts | 逐步用 Nautilus AccountState 接入；保留签名证据与账户快照版本门禁所需数据 |
| exchange_evidence/verified_fills.rs | Movements | Nautilus OrderFilled/FillReport 只作输入，保留必要独立交易所认证及去重 |
| exchange_evidence/settlements.rs | Movements | 保留资金费、期权结算证据；补划转/充提需有可验证来源 |
| exchange_evidence/account_events.rs | Movements | 目前只是对经过核验的成交/结算的可分页引用，**不是**完整现金复式分录 |
| execution_guard/order_intents.rs | **不属于二级资金业务**；Execution Guard | 发送前幂等门禁继续保留，最终迁至执行领域，底层可复用 Ledger 存储事务 |
| execution_guard/order_reconciliation_tasks.rs | **不属于资金对账**；Execution | 保守订单恢复任务不应与资金 Reconciliation 混为一谈 |
| portfolio_state/basket_journal.rs、coordinator_state.rs | **不属于二级资金业务**；Portfolio | 后续迁移业务归属，底层 CAS 可以复用 |
| compatibility/parity.rs、records.rs | Compatibility（临时） | 按调用方与历史恢复逐一替代并移除 |
| Allocations、资金 Reconciliation | 暂无成熟的独立实现 | 先定义事实与守恒契约，不新增空模块或重复状态机 |

## 分批落地计划

1. **本 PR**：只保留低风险的文件归组与 schema 外置，不改变 API、数据和交易行为，明确本文件的二级业务设计。未通过 Cargo 测试不合并。
2. Accounts：对照 pinned Nautilus AccountState 与原账户快照/账户安全门禁，选择单一运行时权威状态和保留证据。
3. Movements：先逐笔验证 Fill/Settlement 的真实来源、幂等、现金影响类型、费用与资产精度，再切掉重复写入；不能先删 verified_fills。
4. Allocations：实现多策略分配、内部交叉成交与不可变归属策略版本，不与交易所 Fill 混写。
5. Reconciliation：对外部成交/资金事实、Nautilus 状态和内部策略账分别对账，允许未分配和待核验状态。
6. 迁移过渡模块的调用方，在安全回归与历史重建验收后删除原代码；历史表删除须另行迁移审批。

## 资料

- https://nautilustrader.io/docs/latest/concepts/accounting/
- https://nautilustrader.io/docs/latest/concepts/events/account_state/
- https://www.quantconnect.com/docs/v2/writing-algorithms/portfolio/cashbook
- https://docs.tigerbeetle.com/coding/data-modeling/
