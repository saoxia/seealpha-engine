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

只读输出的 `balances` 和 `margins` 直接来自 Nautilus AccountState；没有确认来源的事件不会被标记为可信交易所财务证据。测试网凭据只在当前进程内使用，程序不写入凭据、不连接生产环境。

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

旧 Ledger Accounts SQLite 快照实现已从编译目标移除；历史表不会自动 DROP。生产 Runtime 尚未接通，依赖原生账户证据的路径仍必须 fail-closed。

## Accounts 去重后现状

- `NativeAccountSession` 仅在进程内保留 Nautilus 原生 `AccountState`，JSON 展示按需计算，事件订阅以具体 AccountId 为主题。
- 旧 Ledger `account_snapshots` 代码、新库建表和无生产调用方的旧 Basket 快照 CAS 原型已移除；既有数据库的历史表不自动 DROP。
- PostgreSQL Cache 使用 Nautilus 自身配置；独立游标只防止重启后重放旧报告，不是另一套余额账本。
- **仍未完成生产 Runtime 的原生账户与持仓接线**：Observer 是 Testnet 只读实验节点，无法凭本地 JSON 或 `is_reported` 通过生产认证，不能下单。
- 通用 Basket 状态检查点、不可变订单意图与真实成交/结算证据仍由 SeeAlpha 维护；这些不是重复账户余额缓存。

后续生产接线应直接消费可信 Nautilus Account / Position / Cache，并将状态验证与实际发送统一交由原生执行节点处理。禁止重新引入旧余额 JSON 快照表。

## Runtime 只读接线（本地 Unix Socket）

在 Linux/macOS 上，原生观察进程和 Engine 使用**同一个仅服务账户进程所属用户可访问的私有目录**（0700），例如 `/var/lib/seealpha/native-account-sockets`；目录必须预先创建，不接受符号链接。运行测试网原生观察进程时配置：

```bash
export SEEALPHA_ACCOUNT_SOCKET_DIR=/var/lib/seealpha/native-account-sockets
export SEEALPHA_TENANT_ID=tenant1
export SEEALPHA_ENGINE_ACCOUNT_ID=account1
# 另外提供观察器原有的 Binance Testnet 凭据
cargo run --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml \
  --bin native_account_observer -- spot BINANCE-SPOT-001 ./nautilus-account-state
```

Engine 启动时为同一目录配置 `ENGINE_NATIVE_ACCOUNT_SOCKET_DIR`。Runtime 在账户快照查询中只接受 `sandbox`，不再收取 API 密钥；它读取由固定账户的原生节点提供的 `tenant1--account1--sandbox.sock`，校验来源、账户、环境和 60 秒时效。Socket 权限为 0600，目录权限为 0700；目录内已有同名 Socket 时原生进程拒绝覆盖。节点退出时移除 Socket。没有原生有效报告则返回冲突错误，而不是回退到旧 SQLite 或可伪造的外部 JSON。

这是单机部署中的**只读测试网账户余额和保证金查询**。目录访问控制并非跨主机或同 Unix UID 进程之间的强身份认证，因此不应允许不可信程序以同一 Unix 用户运行。它没有读取 Nautilus Position Cache、没有完成资金费/成交历史对账，也没有接通实盘或发送订单；任何真实交易均继续 fail-closed。
