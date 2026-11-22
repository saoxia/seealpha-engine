# Nautilus 原生账户订阅、余额观察与恢复（测试网只读）

这个实现位于独立 Nautilus crate 中，不经过被删除的自研 Binance REST 客户端；使用固定的 NautilusTrader Rust `0.68.0` `BinanceExecutionClientFactory`、`LiveNode` 以及 `msgbus::subscribe_account_state` 观察 `AccountState`。

## 已实现

- **账户事件订阅**：`native_account_session.rs` 使用固定版本的 *typed* AccountState 消息总线订阅；`src/bin/native_account_observer.rs` 启动只读的 Binance **Testnet** 原生执行节点，不添加交易策略、不提交订单。节点使用 Nautilus 自带断线连接管理和 `with_reconciliation(true)`。
- **账户绑定与余额同步**：由单一原生节点连接**一个显式 AccountId**；过滤 AccountType、`is_reported`、观察时效，按原生 AccountBalance 的 `total/locked/free`、MarginBalance 的 `initial/maintenance` 读取数值，检查同币种重复与金额守恒，形成**进程内只读视图**，并作为 JSON 输出。绝不根据 ACK 或余额差值生成真实结算和成交。
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
5. 持久化的是恢复游标，而不是另一套资金账；SQLite 生产表、历史 Ledger 快照及订单安全门禁均未改变。
6. 文件游标是本地单进程单写者；同目录重复启动、锁定、防损坏文件安全策略以及生产级进程监督尚未验收。生产部署必须有独占写入保护和被审计的环境配置。
7. 首次测试网联机、断线注入、不同持仓模式、长期离线补齐和跨账户隔离的端到端验证仍需单独执行。

## 离线验收

`native_account_session.rs` 测试涵盖实际 Nautilus 类型与消息总线订阅、账户绑定、资金值视图、过期/错账户、重启后重放拒绝、新事件恢复和损坏游标拒绝。所有测试均不需要真实账户、不会提交订单。

工作完成前保留现有 Ledger Accounts 的 CAS 和可信快照门禁；不能因为这个只读观察进程存在就删除历史账户证据。
