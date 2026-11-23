# Ledger / Funds

Ledger 只保存 SeeAlpha 特有的、经过认证的经济事件。NautilusTrader 是 Account、AccountState、Position 和订单运行时状态的权威来源。

## 业务边界

- `movements/`：交易所认证成交、资金费、期权结算和经济事件证据；不从 ACK 或余额变化推断成交。
- `allocations/`：按照整数 basis points（0–10000）将已认证事件不可变地归属策略；拒绝重复改写、越额分配、跨账户和跨环境引用。
- `reconciliation/`：只核验本地经济事件归属完整性，不代表已完成交易所余额、持仓和全部交易历史对账。
- Ledger 不维护租户登记、租户身份、租户状态或多租户隔离。业务数据只按 `account_id` 与 `environment` 隔离；策略归属仍保留 `strategy_id`。
- Ledger 不复制 NautilusTrader 的余额、保证金、Position Cache 和订单状态机。

## PostgreSQL 单实例结构（2026-11-23）

Ledger 的表 `fills`、`quarantined_unverified_fills`、`account_settlements`、`account_evidence_events`、`account_event_allocations`，以及旧订单历史表 `orders`、`order_intents`、`order_observations` 已移除 `tenant_id` 字段及相关复合主键和索引。相关 Ledger 公共方法不再接收 tenant 参数。

共享 schema 由应用启动时的 `seealpha_database::bootstrap_engine_schema` 初始化。同一事务中会将已有单租户历史表无损转换为单实例表，重建约束和索引；**如果历史账本出现多个不同 tenant 值，启动时拒绝合并**，防止不同账户证据混淆。迁移前应备份数据库；不会自动删除历史经济证据。

仅对账户和环境进行隔离是有意设计：同一个 `account_id` 的 `production` 与 `sandbox` 不能共享成交和资金费证据。

## 执行安全

订单发送前必须持久化意图；未知交易所执行结果只能核验，不能自动重发。原生执行报告与订单恢复循环目前仍未完整接通，因此 Ledger 的存在不意味着具备可用的实盘执行能力。

旧的无生产消费者的订单恢复逻辑、模拟资金状态与快照已从 Ledger 移除。对账结果中的 `history_completeness_verified`、`exchange_balance_reconciled`、`position_reconciled` 必须明确为 false，直到真正完成认证。
