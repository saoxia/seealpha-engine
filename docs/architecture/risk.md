# Risk Management

**当前是逻辑业务边界，不是独立 Rust crate。**

截至 2026-11-23，原 `engine/execution/` 目标数量与基础裁剪计算已退役；以前的 `engine/execution/src/risk.rs`、`phase_gate.rs`、`basket.rs` 及 `engine/runtime/` 路径也不是当前有效实现。

未来优先使用 NautilusTrader 原生 RiskEngine、账户/订单缓存及执行前检查。只有明确超出原生能力的 SeeAlpha 业务要求（如跨策略预算与组合风险、账户级权限和多腿保护约束）才增加专门实现；不得复制原生订单状态或交易所恢复机制。

当前**没有已验收的生产下单风险门禁**。不能因为某个策略已加载、订单 ACK 或离线测试通过，就授权真实账户发送。实际运行前需要验证风险约束、账户/订单对账与未知结果恢复。
