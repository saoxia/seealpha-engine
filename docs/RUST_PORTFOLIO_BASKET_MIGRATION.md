# Rust Portfolio Coordinator 与 Basket 迁移状态

此变更新增 `seealpha_portfolio::coordinator` 和 `seealpha_portfolio::basket`，两者是**离线纯状态机**，不触发任何交易所下单或生产账户副作用。

## Rust 已覆盖

- 成员绝对目标净额、虚拟订单登记/改单/撤单、K 线延迟触发限价与止损、同方向按步长分配、反向内部撮合。
- 外部账户差额订单的单一在途门禁、部分成交累计、成交事件去重与冲突冻结、风险退出撤单意图、状态快照恢复。
- 跨 Spot / USD-M / 期权的同账户目标净额、完整仓位与行情证据核验、总名义预算及阶段内峰值预算约束。
- Generic 减险优先、Funding Carry 协同阶段、Option Protected 先确认证券保护腿再扩大方向敞口。
- Basket 阶段唯一请求登记、逐腿累计成交、仅在终态和权威仓位完全一致后推进、冻结、事件回放恢复。

## 未完成、不能宣称完成 Python 替代

- 尚未逐项移植 `basket_journal_store.py` 的数据库 CAS 原子持久化、`basket_risk_monitor.py` 的全部动态风险监控、`basket_order_evidence.py` 与结算/历史记录模块。
- 尚未把原生协调器接入 `seealpha-runtime` 事件循环、交易策略入口与订单事实账本；未处理多组合并发调度和可恢复事务。
- Basket generic hedge pair 的专用分阶段排序与 funding carry 允许漂移容忍区间仍需移植；目前默认保守、未开放。
- Rust 侧状态机只返回订单意图，调用方须先持久化并校验，再调用权威执行器；未知委托必须只读核对，不得重发。
- 没有执行 `cargo fmt`、`cargo clippy`、`cargo test` 或生产/沙盒验证；当前执行环境不具备 Rust toolchain。以上不构成构建成功声明。

## 约束

任何 Basket 仍为 `executable=false` 的研究态计划。不能通过此模块直接启用真实下单。生产接线前必须落实 CAS 日志、完整终态证据、重启查询、交易所限制、保证金与费用的动态再验证。
