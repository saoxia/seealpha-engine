# Runtime — 用例编排

Runtime 负责租户状态检查、策略下发/预览的跨模块编排。策略 ZIP 解包、Manifest 检查、参数准备、授权下发及可信执行校验集中在 `strategy/src/native_issuance.rs`。HTTP / Catalog 查询由 Gateway 负责；原生订单生命周期属于 NautilusTrader，目标仓位计算归 Execution，可信经济证据归 Ledger。

## 2026-11-23 精简

- 删除 `state_store/` 包装目录及未使用的 `active_tenant_ids()`；Runtime 直接通过 Database 读取和更新 `tenants` 表。保留租户作用域与 active/archived 门禁，不改动数据库数据。

- 删除 `orders.rs`、`native_handoff.rs`、`state_store/order_intents.rs`。此前的订单实现仅登记意图，不连接 Nautilus 发送端或真实认证恢复。
- Gateway 退役订单提交、Spot 订单提交、交易控制和请求对账的 POST 路由；同路径的 POST `/orders` 返回 405（GET 仍保留），其余已退役 POST 路由经身份/租户门禁后返回 404。只读历史订单与可信成交查询仍保留。
- `strategy-preview` 仍只输出信号和目标仓位，不构造 Paper 成交与持仓。
- 不执行 DROP、数据迁移或删除历史记录。PostgreSQL 中的 `order_intents`、`order_observations`、`orders` 和 `fills` 表保留供受控审计与归档；Runtime 已无旧意图恢复入口。

## 租户状态退役（2026-11-23）

- 删除租户登记、`active/archived` 状态及其独立数据库连接；新数据库不再创建 `tenants` 表。
- `tenant_id` 仍用于策略授权及历史数据隔离；Gateway 的 `/tenants/{tenant_id}` 请求必须匹配 `ENGINE_TENANT_ID`，不配置则拒绝。
- 策略下发和预览依赖可信 `ENGINE_TENANT_PURPOSE`（development/production），并继续检查下发账号与授权一致性。
- 已有的 PostgreSQL `tenants` 表不执行 DROP，历史行保留而不再读取/修改。

## 安全边界

真实订单继续禁用。删除不完整的 Runtime 订单试验链路**不意味着** Nautilus 发送前原子授权、风控、幂等恢复及真实成交认证已经完成。今后启用实盘之前必须另行完成这些端到端接线与验收。
