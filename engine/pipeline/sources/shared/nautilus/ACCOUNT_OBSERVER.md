# Nautilus 原生账户订阅、余额观察与恢复（测试网只读）

这个实现位于独立 Nautilus crate 中，不经过被删除的自研 Binance REST 客户端；使用固定的 NautilusTrader Rust `0.68.0` `BinanceExecutionClientFactory`、`LiveNode` 以及 `msgbus::subscribe_account_state` 按明确的账户主题观察 `AccountState`，不再使用全账户通配主题。

## 已实现

- **账户事件订阅**：`native_account_session.rs` 使用固定版本的 *typed* AccountState 消息总线订阅；`src/bin/native_account_observer.rs` 启动只读的 Binance **Testnet** 原生执行节点，不添加交易策略、不提交订单。节点使用 Nautilus 自带断线连接管理和 `with_reconciliation(true)`。
- **账户绑定与余额同步**：由单一原生节点连接**一个显式 AccountId**；过滤 AccountType、`is_reported`、观察时效，按原生 AccountBalance 的 `total/locked/free`、MarginBalance 的 `initial/maintenance` 读取数值，检查同币种重复与金额守恒，以原始 Nautilus `AccountState` 作为**唯一进程内账户状态**，按需生成只读 JSON 展示。绝不根据 ACK 或余额差值生成真实结算和成交。
- **重启恢复**：独立文件中原子持久化上次接受的 *账户 ID、事件 ID、纳秒时戳*；重启后旧余额不可用，仅较新、来源与身份检查通过的完整 AccountState 可重新形成视图。过期、未来、重复或乱序报告 fail-closed；`mark_disconnected()` 可在已知断线时立即冻结。Native `with_reconciliation(true)` 会请求其原生执行恢复，但是否真正获得完整交易所区间仍须单独验证。

## 如何运行（仅测试网）

先准备单独、仅当前系统账户有权写入的状态目录和 Binance Testnet 凭据：

```bash
mkdir -p ./nautilus-account-state
export SEEALPHA_BINANCE_TESTNET_KEY='...'
export SEEALPHA_BINANCE_TESTNET_SECRET='...'
cargo run --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml --bin native_account_observer -- spot BINANCE-SPOT-001 ./nautilus-account-state
```

USD-M 当前只支持显式的 **OneWay/Netting** 测试观测。已有 Hedge 双向账户必须先扩展并核对独立头寸模式映射，不能悄悄按 Netting 处理。当前不支持 Options。

只读输出的 `balances` 和 `margins` 直接来自 Nautilus AccountState；没有确认来源的事件不会成为 `Ledger::save_account_snapshot` 的可信财务证据。测试网凭据只在当前进程内使用，程序不写入凭据、不连接生产环境。

## 严格的边界与未完成的验收

1. `is_reported=true` 和消息总线上的事件本身**不是交易所签名验证**。隔离进程、原生客户端连接和身份绑定构成可信来源的前提，但本版还没有实现不可伪造的跨进程身份凭证，不能从外部任意发送的 JSON 认定真实资产。
2. 余额同步为原生节点内部的**只读视图和 JSON 输出**，而不是已完成 SeeAlpha Runtime 的账户余额同步或 Ledger 财务入账；当前原 Runtime `account_snapshot()` 继续 fail-closed。
3. 自动的网络断线由 Nautilus 原生客户端负责；本模块有可调用的 `mark_disconnected()`，但尚未对 Nautilus 每一种内部 socket 断线事件安装一一对应回调。未观测到断线时只能通过时效窗口拒绝旧快照。
4. 重新收到新 `AccountState` 不能单独证明断线期间**每一笔成交、资金费、划转**都完整；这项核验属于 Reconciliation 和 Movements。账户状态进入可读，也不代表可以下单。
5. 原生节点已支持可选 PostgreSQL Cache；独立文件持久化的是防重放恢复游标，而不是另一套资金账；SQLite 生产表、历史 Ledger 快照及订单安全门禁均未改变。原生缓存写入、恢复及 Runtime 端到端衔接尚未验收。
6. 文件游标是本地单进程单写者；同目录重复启动、锁定、防损坏文件安全策略以及生产级进程监督尚未验收。生产部署必须有独占写入保护和被审计的环境配置。
7. 首次测试网联机、断线注入、不同持仓模式、长期离线补齐和跨账户隔离的端到端验证仍需单独执行。

## 离线验收

`native_account_session.rs` 测试涵盖实际 Nautilus 类型与消息总线订阅、账户绑定、资金值视图、过期/错账户、重启后重放拒绝、新事件恢复和损坏游标拒绝。所有测试均不需要真实账户、不会提交订单。

工作完成前保留现有 Ledger Accounts 的 CAS 和可信快照门禁；不能因为这个只读观察进程存在就删除历史账户证据。

## Accounts 去重迁移边界（2026-11-22）

原生 Observer 已有 AccountState 订阅、原生余额和保证金读取、以及可选 PostgreSQL Cache。不应再新增一套资金余额计算系统。独立 JSON 输出只是未认证的测试网观察，不能直接用于生产 Runtime。

迁移和删除旧账户快照模型的前提：

1. 绑定执行节点与 tenant/account/environment，证明唯一生产者和跨进程报告可信来源。单靠 is_reported 或 JSON 标记不能构成认证。
2. Runtime 从原生 Account/Position/Cache 读取完整一致状态，并在重启后完成交易所对账；PostgreSQL Cache 不是交易所证明。
3. 保持 Basket 的账户版本 CAS、时效、来源和发送前原子登记约束。当前 preregister_basket_order_intent 在 SQLite 事务中读取账户 revision，不能只删除快照写入而保留此依赖。
4. 验证断网、乱序、重复、账户隔离及 Spot、USD-M Netting/Hedge；无法保证的产品继续 fail-closed。
5. 完成 basket.rs、basket_phase.rs、order_intents.rs 的一致迁移与回归后再移除 SQLite account_snapshots；历史表需另行制定保留/迁移方案，不直接 DROP。

本批仅收紧原生账户订阅范围并增补隔离测试；并未完成 Runtime 接线或旧快照删除。

## 原生类型读取接口（后续改造）

`NativeAccountSession::fresh_native(now_ns, max_age_ns)` 返回当前已经通过该隔离会话校验、仍处于时效窗口内的原始 Nautilus `AccountState` 引用。重启未获新报告、主动断线或超时均返回 `None`。这为后续在同一可信进程中直接使用 Nautilus 类型提供接入点，避免将 JSON projection 当成新的权威账户模型。

当前实现只保留最新原生事件，JSON 投影在读取时临时构建，不再缓存第二份 balances/margins；不会写入另一套账户数据库。该 API 本身**不认证交易所来源，不包含 Position Cache，不提供跨进程 Runtime 访问或下单权限**，不得直接替换 Basket 原子账户版本门禁。

## 简化记录（2026-11-22）

已删除 `NativeAccountSession` 中重复保存的 `ReportedAccountView` 状态字段，改为只缓存 Nautilus `AccountState`。`fresh_native` 直接读取原生数据；`fresh` 只为现有 CLI JSON 输出按需生成投影。`accept` 在更新游标前验证余额一致性和重复币种/保证金，仍然先持久化游标再公布最新事件。此变更不启用 Runtime 原生交易或删除 Basket 的事务版本检查。
