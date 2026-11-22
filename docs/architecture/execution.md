# Execution / Order Management

**当前是跨多个 crate 的逻辑业务边界，不存在独立 `seealpha-execution` crate。** 以前的 `engine/execution/` 只封装交易所订单状态和累计数量校验，实际业务链路未调用，故已移除。此项清理不等于原生下单或订单恢复已经完成。

## 当前实现归属

- `engine/runtime/src/orders.rs`：订单命令门禁、原生意图转交、未完成订单的保守对账检查；真实外部发送被阻止。
- `engine/runtime/src/native_handoff.rs`：Nautilus 原生订单意图的严格验证和 Ledger 预登记；**不会发送真实订单**。
- `engine/runtime/src/fills.rs`：原生真实成交证据链尚未验收时失败关闭。
- `engine/ledger/src/order_intents.rs`、`order_reconciliation_tasks.rs`、`verified_fills.rs`：不可变下单意图、待恢复任务、经核验的真实成交；不再复制未接线的 Nautilus 原生订单事件元数据。
- `engine/pipeline/sources/shared/nautilus/`：原生 Binance Spot/USD-M 工厂、事件类型清单和 Streaming/Catalog 离线配置测试；尚无可运行的 Runtime 实盘执行闭环。
- `engine/connection/binance-options/`：Options 专属规范化能力，不能视为 Spot/USD-M 通用订单执行引擎。

## 安全约束与后续集成

订单必须先原子登记不可变意图，发送权限必须显式核验；未知外部结果不得盲目重发。请求 ACK、未认证的 Nautilus 事件、订单累计成交量都不能单独作为真实成交入账依据。只有可信、已核验的实际成交证据可以改变 Ledger 事实。

后续接入 Nautilus 原生下单和事件回写时，应统一验证账户/合约身份、原生订单生命周期、累计数量单调性、撤改和部分成交、幂等与重启恢复、风控门禁以及真实成交 Trade ID 的核对；不能仅凭删除未使用代码声称替代完成。

## 第一批去重（2026-11-22）

- 移除仅供测试写入的 native_events.rs 及创建 native_order_event_quarantine 新表的代码；已有 SQLite 历史表不会被删除。
- 统一选择 native_execution_record_types() 中的 Nautilus 原生记录类型，避免另一条自研订单事件 JSON 持久化流。
- 不修改 Ledger 不可变下单意图、已验证成交、资金结算及业务协调状态，也不启用原生发送。Nautilus 的 Record 类型配置尚未经过实际运行、写盘、重启验证。
- 其他潜在重叠功能应先完成运行时证据链与回归验收后再逐步迁移，不能按表名直接删库。

参见 [Ledger / Nautilus 功能重叠审计](LEDGER_NAUTILUS_OVERLAP_20261122.md)。
