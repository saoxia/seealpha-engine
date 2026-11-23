# Runtime — 用例编排

Runtime 负责租户状态检查、授权策略包加载和只读策略预览。HTTP / Catalog 查询由 Gateway 负责；原生订单生命周期属于 NautilusTrader，目标仓位计算归 Execution，可信经济证据归 Ledger。

## 2026-11-23 精简

- 删除 `orders.rs`、`native_handoff.rs`、`state_store/order_intents.rs`。此前的订单实现仅登记意图，不连接 Nautilus 发送端或真实认证恢复。
- Gateway 退役订单提交、Spot 订单提交、交易控制和请求对账的 POST 路由；通过现有身份/租户门禁后返回 404。保留只读历史订单与可信成交查询。
- `strategy-preview` 仍只输出信号和目标仓位，不构造 Paper 成交与持仓。
- 不执行 DROP、数据迁移或删除历史记录。PostgreSQL 中的 `order_intents`、`order_observations`、`orders` 和 `fills` 表保留供受控审计与归档；Runtime 已无旧意图恢复入口。

## 安全边界

真实订单继续禁用。删除不完整的 Runtime 订单试验链路**不意味着** Nautilus 发送前原子授权、风控、幂等恢复及真实成交认证已经完成。今后启用实盘之前必须另行完成这些端到端接线与验收。
