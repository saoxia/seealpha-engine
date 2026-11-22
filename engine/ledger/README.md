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
| `runtime/src/state_store/` | Basket/Coordinator/Paper/Research 业务状态 | 由 Runtime 持久化，不归 Ledger Funds |

## 四个最小业务核心（2026-11-22）

- **Accounts**：`account_registry_view` 复用现有租户和连接登记，只返回业务元信息；不存余额、持仓、账户快照，不授予发送权限。原生账户状态仍由 Nautilus 维护。
- **Movements**：沿用经过认证的成交、资金费、结算及 `account_evidence_events`，没有上游证据时拒绝生成事件，既不凭 ACK 入账也不建一套现金余额账。
- **Allocations**：`attribute_account_event` 将已登记的可信账户经济事件按整数 basis points（总计最多 10000）不可变地归属策略；跨租户/账户/环境、重复改写和超额分配均拒绝；尚不计算策略盈亏或合成虚拟成交。
- **Reconciliation**：`reconcile_account_attribution` 汇总已存证据的已归属/未归属情况，明确标记历史完整性、交易所余额和持仓对账均**未完成**；这不是通过真实账户对账的证明。

本轮只搭建 Ledger 的数据核心，不新增 HTTP 路由、自动执行任务或与 Nautilus 并行的账户状态机。现有 Ledger 库在打开时创建必要表；已有历史表保持原状。

## Accounts 迁移

2026-11-22 起不再编译 `accounts/account_snapshots.rs`，也不再为新数据库建立 `account_snapshots` 表。原生账户模块位于 `engine/pipeline/sources/shared/nautilus/`。删除了无生产调用方的 Ledger 快照 Basket 对账、旧版本 CAS 预登记实验接口，保留通用 Basket Checkpoint 和通用不可变订单意图记录。

**当前只完成测试网只读 Runtime 接线**：独立 Nautilus Observer 可通过私有 Unix Socket 提供已筛选的 AccountState 余额/保证金观察，Runtime 校验账户范围、环境和 60 秒时效。它不是交易所完整认证流水或 Position Cache；生产账户请求与测试网实时持仓查询仍 fail-closed，不能凭旧 SQLite 快照放行订单。不得将用户提交的 JSON、`is_reported` 或原生缓存存在视为交易所认证的持仓完整性证明。

升级已有数据库不会自动删除历史 `account_snapshots` 表及数据；待明确历史保留和备份策略后由独立迁移处理。

## 旧 `src/` 接口清理（2026-11-22）

- 删除不再被生产调用的 `records.rs::gaps()`、`records.rs::events()` 及可绕过专用认证入口的旧 `upsert_fill_row()`；保留 `gaps_for_scope()`、`event_page()`、`import_verified_fills()` 和 Paper Session 内部模拟成交写入。
- 删除 `parity.rs::positions()` 对历史 `positions` 表的读接口；Gateway `/positions` 在 Nautilus 原生持仓核验尚未接线前明确失败关闭，包含 `paper` 环境，不能把缺少证据的空仓位响应当作已核实状态。
- **不执行 DROP 表或历史数据迁移**：既有 `positions` 和 `order_observations` 数据保留供审计及后续受控迁移。
- `order_intents.rs` 的订单观察与恢复链路暂时保留：虽然没有生产观察写入调用方，`order_recovery_page()` 依赖历史观察、且带有禁止未知状态重发的安全约束；必须由已验收的 Nautilus 持久报告与恢复功能替代后再删。

## 应用状态职责迁出（2026-11-22）

- 研究快照与回测结果：`engine/runtime/src/state_store/research.rs`；由 Runtime 持有并持久化，不属于资金账本。
- Basket Journal 与 Portfolio Coordinator CAS 检查点：`engine/runtime/src/state_store/{basket_journal,coordinator_state}.rs`；作为 Runtime 的 Portfolio 用例状态，原有并发修订规则保持不变。
- Paper Session 和模拟持仓：`engine/runtime/src/state_store/paper.rs`；仅用于独立模拟，不能被当作真实账户余额或 Nautilus Position Cache。
- 上述存储继续使用相同 PostgreSQL schema 和既有表，**没有重建、复制或删除历史数据**。目前由 Runtime 在 Ledger 初始化后建立自己的数据库连接。当前不为目录整理而增设多个长期运行服务。
- Ledger 暂时保留不可变原生订单意图及只读对账任务队列，因为其属于未完成原生执行链路的安全恢复门禁；待 Nautilus/执行领域有真实替代并经过验证后再迁出。
- Legacy 订单、缺口、事件兼容读写暂留现有持久化入口，避免无收益的全仓库 API 重写。本阶段变更尚须通过 CI/数据库回归验证。

## 安全原则

- Nautilus Account / Position 管理运行时余额、保证金和持仓；SeeAlpha 不再新增另一套账户快照引擎。
- 订单发送前意图必须持久化；未知结果只允许查证，不得自动重发。
- 未接通的原生执行报告和余额服务必须 fail-closed，不能以模拟数据替代实盘认证。
- 历史成交及结算证明归 Movements；资产余额变化不等于已经确认的成交或结算。
