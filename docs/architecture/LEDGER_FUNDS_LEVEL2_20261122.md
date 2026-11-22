# Ledger / Funds：四个二级业务功能块

> 历史设计记录：此文件描述的 Accounts SQLite 快照和 Basket 旧 CAS 实验已在后续 PR #193 中退休。当前状态请以 `engine/ledger/README.md` 和原生 Nautilus Observer 为准；下列旧矩阵不代表当前生产功能已完成。


## 目标

一级领域：**账本与资金（Ledger / Funds）**。此层仅划分资金及经济事实的内部业务职责，不重新划分 SeeAlpha 一级模块，不自行重写 NautilusTrader 已有的 OMS、账户和持仓引擎。

唯一的二级业务抽象为 **Accounts、Movements、Allocations、Reconciliation**。

| 二级功能块 | 核心问题 | 权威来源、职责 | 现状 |
| --- | --- | --- | --- |
| **Accounts** | 当前账户持有什么资产和保证金？ | 优先 Nautilus Account / AccountState 及已认证交易所快照；存储账户观察与版本证明，不重复计算余额与 PnL | 已归类 `account_snapshots.rs` |
| **Movements** | 哪一笔真实经济事件导致资金/资产变化？ | 保留交易所认证成交与真实结算证据；费用、资金费、期权结算等必须有来源和精确 Decimal | 已归类 `verified_fills.rs`、`settlements.rs`、`account_events.rs` |
| **Allocations** | 实际成交和费用归属于哪些策略？ | 多策略真实成交分配、内部交叉成交、成本归属及规则版本；不可将虚拟成交冒充交易所 Fill | 已预留模块边界，尚无已验收的实现 |
| **Reconciliation** | 系统记录能否解释交易所账户和内部虚拟账？ | 优先 Nautilus 原生订单/持仓对账；额外核验现金/结算、策略归属与未分配额 | 已预留模块边界，尚无已验收的资金专属实现 |

## 边界与复用原则

- NautilusTrader 是执行订单、标准 OrderFilled、账户运行时状态和基础持仓对账的首选实现。代码目前固定 Rust crates 0.68.0 / 2.0.0rc9；迁移前必须以当前版本的真实接口、运行和重启验收为准。
- AccountState 是账户**观察/运行时状态**，Movements 是有来源的**事件与流水证据**。两者不能相互代替。
- `record_account_evidence_event` 目前是指向已认证成交/结算的分页引用，并非完整的现金复式分录；不应为了模块名称而称其为会计总账。
- `Allocations` 仅持久化归属事实及依据；内部撮合、目标仓位等决策仍由 Portfolio 定义。完全外部分配需要满足数量守恒；尚未分配时必须显式保留缺额。
- `Reconciliation` 记录外部事实、Nautilus 状态与内部归属之间的差异；无法证明的数据保持待核验，不凭推断补造资金和真实成交。
- `order_intents.rs`、`order_reconciliation_tasks.rs` 等订单安全相关文件不属于此处的资金业务二级分类；本次不移动、不删改。Basket/Coordinator、旧查询兼容代码也原位保留，不据此增加额外资金业务功能块。
- 具体文件夹划分不是新增四套数据库或四个常驻进程；本次保持原 `Ledger` API、SQL Schema、历史数据不变。

## 当前业务目录布局

`engine/ledger/{accounts,movements,allocations,reconciliation}/` 四个业务目录与 `src/` 并列，`src/lib.rs` 使用显式 `#[path]` 模块属性加载它们。以下历史变更说明保留原业务拆分含义，不代表四块仍位于 `src/` 下。

## 这次提交的具体变更

1. `accounts/`：将 `account_snapshots.rs` 原样移动，并继续在根 `lib.rs` 重导出 `AccountSnapshot`。
2. `movements/`：将 `account_events.rs`、`verified_fills.rs`、`settlements.rs` 原样移动。
3. `allocations/`、`reconciliation/`：只建立明确的业务边界文档，**不引入空数据库表、伪造业务接口或尚未实现的交易逻辑**。
4. 其他文件不变，原 `Ledger` 上的公开方法仍由对应子模块的 `impl Ledger` 提供。
5. 不引入 Schema 迁移，不改变生产开关，不启用实盘交易，不删除原始认证数据。

## 逐块完善和删除旧实现的准则

- Accounts：用 Nautilus AccountState / Cache 对照可信快照、租户与账户身份、风险门禁版本，验证后只删重复的实时状态实现；保留必要的历史证据。
- Movements：以 Nautilus 执行报告配合交易所真实 Trade ID / 手续费 / 结算对账，完成后删重复订单成交视图；不得以 ACK 或未认证 FillReport 直接入账。
- Allocations：明确内部虚拟成交、真实成交、部分分配、策略资金费用归属的守恒条件，再实现最小存储事实。
- Reconciliation：验证外部成交/结算、账户快照、策略归属的一致性，保留缺口与纠错来源，不自动强制凑平。
- 各块必须先完成固定版本编译、重复/乱序/重启/故障回归、消费者迁移与历史数据完整性验证，再在同一后续 PR 删除等价旧代码。**本次只是结构边界调整，不是功能替代验收。**

## 参考设计

- NautilusTrader：账户、执行、事件持久化与对账（https://nautilustrader.io/docs/latest/）
- QuantConnect LEAN：组合 CashBook、TransactionManager（https://www.quantconnect.com/docs/v2/）
- TigerBeetle：Account / Transfer / Ledger 金融记账原语（https://docs.tigerbeetle.com/coding/data-modeling/）
