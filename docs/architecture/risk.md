# Risk Management

**当前是逻辑业务边界，不是独立的 Rust crate。**

- `engine/portfolio/core/src/risk.rs`：组合目标的风险裁剪与基本限额。
- `engine/portfolio/core/src/phase_gate.rs`、`basket.rs`：篮子阶段和相关门禁。
- `engine/runtime/src/orders.rs`、`basket_phase.rs`：账户操作准入、运行与恢复阶段的业务控制。
- `engine/connectivity/venue/src/order_guard.rs`：发送前的交易所协议/订单约束。

风控不等于一个顶层 `risk/` 目录。未来如拆出 crate，应先明确它对组合目标、订单准入、最大敞口、幂等和未知成交恢复的职责，确保不会改变当前的事务边界。
