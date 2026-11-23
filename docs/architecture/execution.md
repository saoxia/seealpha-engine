# Execution / Order Management

## 当前实现归属（2026-11-23）

- `engine/execution/target/`：策略成员目标、净额和账户仓位调整规划；不发送订单，不维护 OMS。
- `engine/pipeline/sources/shared/nautilus/`：原生 Binance Spot/USD-M 执行工厂、事件/身份契约及离线节点构造测试。尚无验收完成的生产发送及重启恢复链路。
- `engine/ledger/movements/verified_fills.rs`：可信成交及资金事实；不能从订单 ACK 或未认证事件推导 Fill。
- `engine/runtime/`：租户范围、授权策略加载和只读预览；旧订单登记/恢复试验代码已移除。
- `engine/gateway/api/src/routes/orders.rs`：历史订单与 Ledger 已核验成交只读查询，不是实时 Nautilus 订单状态源。
- `engine/connection/binance-options/`：Options 专属适配，不是 Spot/USD-M 原生执行替代。

## 安全约束

2026-11-23 删除 Runtime `orders.rs`、`native_handoff.rs`、`state_store/order_intents.rs` 和 Gateway 对应的 POST 写入/对账路由。保留历史 PostgreSQL 表和数据，不 DROP、不迁移；旧 Runtime 不再读取订单恢复清单。

真实交易仍然禁用。未来 Nautilus 发送端必须实现并验收**发送前原子不可变授权**、账户/品种身份和风控校验、未知结果仅核验不重发、真实 Trade ID 对账、异常/部分成交/重启恢复；不能将离线节点构造或删去旧代码当作生产可用的证明。

参见 [Ledger / Nautilus 历史重叠审计](LEDGER_NAUTILUS_OVERLAP_20261122.md)（2026-11-22 快照）。
