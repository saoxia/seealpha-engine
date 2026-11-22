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

## 四个最小业务核心（2026-11-22）

- **Accounts**：`account_registry_view` 复用现有租户和连接登记，只返回业务元信息；不存余额、持仓、账户快照，不授予发送权限。原生账户状态仍由 Nautilus 维护。
- **Movements**：沿用经过认证的成交、资金费、结算及 `account_evidence_events`，没有上游证据时拒绝生成事件，既不凭 ACK 入账也不建一套现金余额账。
- **Allocations**：`attribute_account_event` 将已登记的可信账户经济事件按整数 basis points（总计最多 10000）不可变地归属策略；跨租户/账户/环境、重复改写和超额分配均拒绝；尚不计算策略盈亏或合成虚拟成交。
- **Reconciliation**：`reconcile_account_attribution` 汇总已存证据的已归属/未归属情况，明确标记历史完整性、交易所余额和持仓对账均**未完成**；这不是通过真实账户对账的证明。

本轮只搭建 Ledger 的数据核心，不新增 HTTP 路由、自动执行任务或与 Nautilus 并行的账户状态机。现有 Ledger 库在打开时创建必要表；已有历史表保持原状。

## 旧账户接口收敛（2026-11-22）

- Gateway `account_health` 复用 Ledger `account_registry_view`，仍保留旧响应的 `status`、`workers`、`connections` 字段；附加显式的非实盘认证标记。
- Runtime 的账户注册和测试网只读快照不再接收无效的凭据 / `venue_base` 参数；HTTP 快照继续拒绝非空内联凭据。
- 旧 `sync_registered_fills` 代理已移除，Gateway 成交同步路由统一调用 `sync_verified_order_fills`，仍拒绝未经核验的原生成交，不要求配置一个实际上不会使用的旧 Venue URL。
- 未改变订单发送门禁、旧历史数据或实盘账户状态；这些修改需要 Cargo / CI 验证后才可合并。

## Accounts 迁移

2026-11-22 起不再编译 `accounts/account_snapshots.rs`，也不再为新数据库建立 `account_snapshots` 表。原生账户模块位于 `engine/pipeline/sources/shared/nautilus/`。删除了无生产调用方的 Ledger 快照 Basket 对账、旧版本 CAS 预登记实验接口，保留通用 Basket Checkpoint 和通用不可变订单意图记录。

**当前只完成测试网只读 Runtime 接线**：独立 Nautilus Observer 可通过私有 Unix Socket 提供已筛选的 AccountState 余额/保证金观察，Runtime 校验账户范围、环境和 60 秒时效。它不是交易所完整认证流水或 Position Cache；生产账户请求与测试网实时持仓查询仍 fail-closed，不能凭旧 SQLite 快照放行订单。不得将用户提交的 JSON、`is_reported` 或原生缓存存在视为交易所认证的持仓完整性证明。

升级已有数据库不会自动删除历史 `account_snapshots` 表及数据；待明确历史保留和备份策略后由独立迁移处理。

## 安全原则

- Nautilus Account / Position 管理运行时余额、保证金和持仓；SeeAlpha 不再新增另一套账户快照引擎。
- 订单发送前意图必须持久化；未知结果只允许查证，不得自动重发。
- 未接通的原生执行报告和余额服务必须 fail-closed，不能以模拟数据替代实盘认证。
- 历史成交及结算证明归 Movements；资产余额变化不等于已经确认的成交或结算。
