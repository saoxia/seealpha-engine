# Accounts：账户状态与可信快照

## 职责

Accounts 是 Ledger / Funds 的二级功能块。它负责**账户状态的来源、版本、有效期与读取约束**，而不是实现第二套交易账户引擎。

- **运行时账户状态**：优先使用 NautilusTrader `Account`、`AccountState`、`Cache` 提供的余额、锁定余额、可用余额、保证金及持仓相关状态。
- **持久化可信观察**：当前 `account_snapshots.rs` 存储原先就存在的、由调用方认证的交易所完整余额/持仓快照；`paper` 使用独立的模拟来源。
- **交易风险的状态门禁**：根据租户/账户/环境、预期快照修订版、来源标记、观察时刻与最大年龄，拒绝使用过期、未来、错误来源或错误版本的快照。
- **恢复与审计**：读取快照的 `evidence_id`、来源与时间戳，以便判断其可否被某个明确时点的风险决策引用。

## 与 NautilusTrader 的关系

Nautilus 已经管理交易运行时账户的 `total / locked / free`、保证金和 PnL；Ledger 不重新计算这些值，也不将自行保存的 JSON 快照视为可直接覆盖 Nautilus 账户状态的权威指令。

Nautilus `AccountState` 可能来自**交易所报告**，也可能由 Portfolio 本地重新计算（请核对固定版本的实际字段/行为）。即使数据来源标记为 venue-reported，也**不等于** SeeAlpha 已经认证了交易所账户身份、完整性和历史区间。

当前 `source = signed_exchange_account_query` 是现有 Ledger 数据契约的一个**已声明的来源标记**，并非此文件夹内发生了签名验证。签名与账户身份认证必须在未来 Nautilus 原生账户适配/报告消费边界实现；在该链路接通、复核前，Runtime 的远端账户查询继续 fail-closed。

## 当前实现与 API

| 文件或接口 | 作用 | 状态 |
| --- | --- | --- |
| `account_snapshots.rs` | 验证并 CAS 保存账户观察；读取当前快照 | 已实现 |
| `Ledger::save_account_snapshot` | 要求精确 Decimal 字符串、合法来源、独立环境、单调时间戳、版本 CAS；同一当前 evidence ID 不允许变更内容或时间 | 已实现 |
| `Ledger::account_snapshot` | 只按租户、账户、环境读取存储的最新快照，**不承诺新鲜度** | 已实现 |
| `Ledger::account_snapshot_for_risk` | 读取后核查预期版本、来源及 `now_ns/max_age_ns`；无快照、未来、过期或溢出即拒绝 | 本批新增 |
| Nautilus 原生 AccountState 消费、账户连接及历史恢复 | 验证 report 可信度、账号身份，处理重连后恢复 | 尚未接通 |

## API 使用规范

```rust
// `now_ns` 和 `max_age_ns` 必须由调用方显式提供。
// 返回已存储的账户观察，不访问交易所，不授予下单权限。
let checked = ledger.account_snapshot_for_risk(
    tenant, account, environment, expected_revision, now_ns, max_age_ns,
)?;
```

- `now_ns` 使用纳秒；持久化 `observed_at_ms` 使用毫秒，转换需做溢出检查。
- `paper` 只接受 `paper_simulator`，`production/sandbox` 只接受 `signed_exchange_account_query`；模拟数据不得通过生产门禁。
- CAS 仅防止**当前保存的同一条账户观察**被覆盖或重用；由于目前每个账户只保留最新一条快照，不能声称已有全历史 `evidence_id` 唯一性保证。
- Snapshot 不等于 Flow：余额变化不能自动推导为成交、资金费、划转或收益。资金经济事实属于 Movements。
- Snapshot 不是交易授权：检查通过仅说明已有观察满足指定的版本/时效条件，外部发送、风控预算和交易所报告认证仍需独立验收。

## 本批完成与后续

**已完成：** 将 Runtime 中 Basket 阶段预留和账户参与的 Basket 对账所重复的快照时效、来源、版本判断统一至 Accounts；增加边界时刻、未来时间、过期、跨账户/跨环境、模拟环境及当前证据 ID 冲突的回归测试。没有新增 Schema、资金计算逻辑或真实网络调用。

**后续：** 在固定版本 Nautilus 0.68.0 的原生执行节点上，先打通 AccountState 的报告订阅、账户身份映射、真实性分类、断线恢复和账户 Cache 观察，再评估是否能替代 `account_snapshots` 的某些运行时读取。风险所需的不可变来源、版本和时效约束在证明等价之前不能删除。

父模块文档：[Ledger README](../README.md)。

## Nautilus AccountState 的下一步接入（独立堆叠 PR）

固定版本 Nautilus 模块提供 `native_account::inspect_native_account_report`，对 `AccountState` 的 `account_id`、`account_type`、`is_reported` 与纳秒时效进行只读候选过滤。**这只是原生事件候选筛选，尚未接入 Runtime 认证事件消费、余额/保证金同步和持久化，不等同 `signed_exchange_account_query`。** 后续必须绑定实际原生 ExecutionClient 与租户账户环境、核验事件来源和必要的历史覆盖，再将已经通过认证的事实按 Ledger 契约记录。本阶段不会自动把 Nautilus AccountState 写入 `account_snapshots`。
