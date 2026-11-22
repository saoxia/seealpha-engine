# Ledger / Funds

Ledger 只维护 SeeAlpha 特有的持久业务事实，不复制 NautilusTrader 的账户、持仓和订单运行时状态。

## 职责

| 组件 | 数据归属 | 说明 |
| --- | --- | --- |
| NautilusTrader | Account / AccountState / Position / Cache / 原生 PostgreSQL Cache | 账户与持仓的唯一运行时权威来源；真实交易前须完成原生执行报告与交易所对账 |
| `movements/` | 有认证来源的成交、费用、资金费和结算 | 不依据 ACK、余额差值或公共费率推断资金事实 |
| `allocations/` | 策略内部归属（目前仅定义边界） | 不将内部成交冒充交易所成交 |
| `reconciliation/` | 资金与归属核验边界 | Nautilus 基础订单/持仓对账之外的证据完整性检查 |
| `src/order_intents.rs` | 不可变发送前委托意图和未知状态恢复 | 单次预登记、重复请求只核验、不重复发送 |
| `src/basket_journal.rs` / `src/coordinator_state.rs` | SeeAlpha 业务检查点 | 不是交易所资金余额缓存 |

## Accounts 迁移

2026-11-22 起不再编译 `accounts/account_snapshots.rs`，也不再为新数据库建立 `account_snapshots` 表。原生账户模块位于 `engine/pipeline/sources/shared/nautilus/`。删除了无生产调用方的 Ledger 快照 Basket 对账、旧版本 CAS 预登记实验接口，保留通用 Basket Checkpoint 和通用不可变订单意图记录。

**这不是生产 Runtime 的原生账户接线验收。** 当前独立 Nautilus Observer 为测试网只读；Runtime 尚未消费可信原生 Account/Position 数据，故需要实时账户证据的 HTTP 操作继续拒绝，不能凭旧 SQLite 快照放行订单。不得将用户提交的 JSON、`is_reported` 或原生缓存存在视为交易所认证的持仓完整性证明。

升级已有数据库不会自动删除历史 `account_snapshots` 表及数据；待明确历史保留和备份策略后由独立迁移处理。

## 安全原则

- Nautilus Account / Position 管理运行时余额、保证金和持仓；SeeAlpha 不再新增另一套账户快照引擎。
- 订单发送前意图必须持久化；未知结果只允许查证，不得自动重发。
- 未接通的原生执行报告和余额服务必须 fail-closed，不能以模拟数据替代实盘认证。
- 历史成交及结算证明归 Movements；资产余额变化不等于已经确认的成交或结算。
