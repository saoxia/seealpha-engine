# seealpha-ledger Rust 等价迁移审计 / Runtime 接入契约

- 审计日期：2026-11-21
- 基线：`main` @ `83a19d857bca0ec16faf1a731ccb9835081672ab`
- 修改范围：仅 `crates/seealpha-ledger/`。未修改 Runtime、Venue、Portfolio、Python 或生产数据。
- 对照源：`engine/trading/store.py`、`engine/portfolio/basket_journal_store.py`、
  `engine/portfolio/basket_order_evidence.py`、
  `docs/RUST_CUTOVER_AUDIT_20261121.md`。
- **状态：Draft / 尚未 Cargo 编译与端到端验收。** 本文不是生产可切换证明。

## 逐项真实差异矩阵

| 领域 | main 已有能力 | 本分支新增/修复 | 仍未等价或要求调用方接线 |
|---|---|---|---|
| 租户、账户环境 | `tenants`、`connections` 的作用域字段 | 新证据接口严格绑定 tenant/account/environment；请求 ID 在同一租户账户下跨环境冲突而不是重用 | 原始 `events`、`order_requests` 的 Python v1 表没有环境复合主键；Runtime 不得把它们当作隔离后的实盘成交流 |
| 下单前订单意图 | Paper Session 的 UNKNOWN 登记及 CAS | `preregister_order_intent` 在事务中写入 `order_intents` 与 Python 可读 UNKNOWN 请求；仅第一次返回 `Fresh`；重启/重复返回 `ReconcileOnly`；参数指纹冲突拒绝 | 这不是下单授权或交易所签名；Risk/Venue 必须在下单前确认一次性发送权 |
| 请求幂等 | Python `save_request` UPSERT；Rust `save_order_request` 原先也覆盖 | 原请求现为插入不可变；未知请求不能被后写的成功状态覆盖 | 旧 Runtime 如依赖原 UPSERT，必须改用显式独立观察事件；禁止对已未知的请求重发 |
| 订单生命周期 | `orders` 原始存储；没有可信的订单状态证据流 | `upsert_order_row` 只接受首次或相同内容；`append_order_observation` 添加来源绑定、时间顺序、终态冲突保护；`NEW/ACCEPTED/TRIGGERED` 不生 Fill | 具体交易所撤单拒绝、OTO/OCO/Algo 组合状态归并由 Venue/Runtime 对账；`FILLED` 必须在观察前导入真实 trade IDs |
| 实际 Fill / 部分成交 | `import_verified_fills` 已要求 signed trade 来源、真实 ID、Decimal 数量价格并去重；合成 Fill 隔离 | 禁止 live 环境使用原始 `upsert_fill_row`；重复/冲突写入拒绝；关联真实 Fill 后才记录 `PARTIALLY_FILLED/FILLED` 观察 | 实际账户仓位由完整权威快照决定，不能从订单 ACK 计算；成交分页完整性需 Venue 证明 |
| 资金费 | 已有公共 `funding_observations` 与独立签名 `account_settlements`，支持特殊资金费观测 | `record_account_evidence_event` 将已存的签名 funding settlement 转成幂等、环境隔离的证据事件 | 公共费率观测不是账户收入；尚不提供真实余额记账/每日权益核算或完整 income 分页 |
| 期权结算 | 已有公共行权参考 `option_exercise_observations`、签名账户 `account_settlements` | 仅可将签名且明确 account-confirmed 的行权结算写到账户证据事件 | 不能由公共行权记录合成现金、权利金、交割腿；完整 Options 会计仍需核实 |
| 账户与仓位 | 原 `positions` 按租户/账户/环境读取，Paper 位置使用 Decimal | 新 `account_snapshots` 的 balances/positions 全部 Decimal 字符串，签名查询来源、时间单调、版本 CAS；断线和重启可读取 | 只保存完整观察快照，不根据 Fill/资金费自行虚构账户余额；不支持不完整增量自动合并 |
| 账户事件 | 原 Python `events` 按 tenant/account 的全局自增游标 | 新 `account_evidence_events` 来源必须是已存的真实 Fill / 签名 Funding / 签名 Options 结算；记录幂等，环境隔离，游标分页 | 与旧无环境游标的逐字节 HTTP 契约尚未接线；证据事件不是复式现金分录 |
| Basket/Coordinator Journal | main 已有研究 Basket 和 Coordinator SQLite CAS | 保持原 CAS，不复制业务状态机；强调调用方需先验证 journal hash/事件重放 | Python 文件权限/文件锁、防跟随、SHA256 包裹与 SQLite 数据格式并不字节等价；Portfolio 负责状态重放验证 |
| 旧 SQLite 兼容 | 原 Rust `open` 仅 `CREATE IF NOT EXISTS`，旧表缺 tenant_id 会报错 | `legacy::initialize` 使用一次 IMMEDIATE 事务重命名、创建、复制、保留原事件 ID、补策略包字段、失败回滚 | 尚未在真实历史镜像上验证；禁止在生产共享 SQLite 卷或 Python 并发写入时自动转换 |
| 租户导出/导入 | Rust 无对应 Python Store v1 导出导入 | Python 9 表 JSON 字段兼容，`strategy_loads.archive` base64 字节保真，原子 create-only 导入，混租户/覆盖拒绝 | Python v1 不包含 Rust 新增的 OrderIntent、签名结算、审计证据、Catalog/任务；检测到 Rust-only 行会拒绝 v1 导出。**完整 Rust v2 备份/恢复尚缺失** |
| 故障恢复与 CAS | Basket/Coordinator 事务 CAS、Paper Session unknown | 新 Intent 断线后只对账、账户快照 stale CAS 冲突、订单终态冲突和重复证据均 fail-closed | 交易所查询和未知结果恢复是 Venue/Runtime 外部接线，Ledger 绝不自发发送请求 |

## Runtime / Venue / Portfolio 接入顺序

1. 每个运行实例以独立、受权限保护的 SQLite 文件打开 `Ledger::open(path)`；请勿同时由 Python/Rust 写相同文件，也不要直接使用生产状态卷验证迁移。调用 `ensure_tenant` 创建 active 租户。生产和 sandbox 独立配置/访问权限。
2. 在任何 **NEW / CANCEL / AMEND / REPLACE** 请求的外部副作用之前，完成风控/账户身份验证，生成稳定 `request_id`、`client_order_id` 和 SHA256 指纹；调用
   `preregister_order_intent(tenant,account,environment,request_id,client_order_id,fingerprint,&intent)`。
   `intent` 必须包含 action、environment、client_order_id、symbol；NEW 还需 BUY/SELL、Decimal 字符串数量，价格如存在亦必须是 Decimal 字符串，拒绝 JSON 浮点。
3. 只有 `IntentStart::Fresh` 代表**首次登记成功**，不是交易所下单成功。其上层在通过 Venue 的一次性安全门禁后最多进行一次发送。若传输超时、断开或进程异常，状态保持 UNKNOWN；任何重复请求返回 `ReconcileOnly`，只查询交易所，不自动再次提交订单。
4. 用已认证的 Venue 交易历史和用户流证据执行 `import_verified_fills(tenant,account,environment,venue_order_id,rows)`。每笔必须有稳定的 `binance:...` 实际成交 ID、`signed_exchange_user_trades` 来源和 Decimal 字符串数量价格。不得用 `executedQty`、订单 ACK 或内部 order_id 伪造交易 ID。
5. `append_order_observation` 仅接收 `signed_exchange_order_query` 或 `authenticated_user_stream`，并绑定原始 NEW 意图和递增观察时间。真实 Fill 已入库且 trade ID 对应同一个 venue_order_id 后，才可把订单观察标记为 PARTIALLY_FILLED/FILLED。任何 CANCELED、REJECTED、EXPIRED、UNKNOWN 都不能自动改造成成功或触发补单。
6. 交易所权威账户快照调用 `save_account_snapshot`；`expected_revision=None` 只允许首次创建，此后必须传旧版本号。读取 `account_snapshot` 后对账，冲突必须重新读+核对。快照字段 `balances` / `positions` 仅接受 Decimal 字符串。
7. 签名 Funding/Options 结算分别使用现有 `import_account_settlements`；如需环境隔离事件页，再调用 `record_account_evidence_event` 与 `account_evidence_event_page`。它们不自动发起余额借贷，必须由独立会计核算模块依据账户查询证据对账。
8. Portfolio 仍使用现有 `save_basket_journal` 和 `save_coordinator_state` 的 expected revision；应用写入之前必须在 Portfolio 内完成完整 checkpoint 事件重放和腿间风险约束。不要把本模块当成下单执行器。
9. Python 格式备份用 `export_tenant` 与 `import_tenant_new`。只允许在**不含任何 Rust-only 证据**的 v1 子集间移动；目标租户必须不存在，冲突和事件 ID 重复必须人工处理，绝不清空已有记录。完整 v2 数据迁移是未完成的独立工作。

## 测试清单与执行结果

已编写、**未在该环境运行的 Rust 测试**：

- `legacy.rs`：旧表升级、事件游标与 raw Fill 原文、二次打开、损坏迁移回滚；
- `order_intents.rs`：12 并发预登记只允许一个 Fresh，租户/环境冲突；NEW ACK 不生成 Fill，真实 Fill 后接受 FILLED，拒绝终态倒退与浮点意图；
- `account_snapshots.rs`：8 并发 CAS 只允许一个成功，超时证据、浮点金额、跨租户/账户环境隔离；
- `account_events.rs`：签名资金费事件幂等、重复、不同环境和租户不可见；
- `tenant_transfer.rs`：Python base64 历史策略 archive 字节保真、迁入/重复/混租户回滚、存在 Rust-only 订单意图时拒绝有损 v1 导出；
- `records.rs`：旧接口重复/冲突写入、禁止 raw live Fill、未知订单重启后无第二次发送许可；
- `verified_fills.rs`：原有合成 ACK Fill 的隔离回归，fixture 改为仅在测试中直接写旧表。

本次实际执行：

| 检查 | 实际结果 |
|---|---|
| Python `sqlite3` 辅助契约烟测（作用域主键、CAS 过期、旧 Fill 原文迁移） | **3/3 PASS**；仅证明独立 SQLite SQL 契约，不代表 Rust 逻辑执行 |
| `cargo fmt --all --check` | **NOT RUN**：本执行容器 `cargo: command not found` |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | **NOT RUN**：同上 |
| `cargo test -p seealpha-ledger --locked` | **NOT RUN**：同上 |
| `cargo test --workspace --locked` | **NOT RUN**：同上 |
| Python/Rust 实际数据库镜像双语对照、故障注入、Workspace/HTTP E2E | **NOT RUN**：缺本地克隆/编译器及执行链路 |
| GitHub Actions | 仓库工作流仍在 `.github/workflows-paused/`，没有启用 CI；本分支未修改工作流 |

## 验收前必须处理

- 在有 Rust 1.100.0 的隔离环境，对最新 main 重新验证编译、格式、Clippy、单测、Workspace 测试；根据编译反馈修复问题；在临时卷上跑实际旧 Python Store 读写与 Rust 导入/导出对照。
- 补全 Rust v2 全量租户证据备份/恢复及跨数据根目录清单；当前 Python v1 导出会因 Rust-only 证据而 fail-closed。
- 实测 Venue 的分页完成标志、成交 ID、资金费发生时间和 Option exercise 账户证据；断线未知仅能按订单身份对账。
- 与 Runtime/Portfolio 负责人评审兼容变更：旧 `save_order_request`、`upsert_order_row` 改为不可覆盖；原 `upsert_fill_row` 仅 paper；全套活跃订单状态仍需适配新 observation API。
- 禁止真实密钥/真实下单/生产 SQLite 迁移，禁止在上述验收前合并或切换默认 Runtime。

