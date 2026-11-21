# Execution / Order Management

**当前是跨多个 crate 的逻辑业务边界，不存在独立 `seealpha-execution` crate。** 以前的 `engine/execution/` 只封装交易所订单状态和累计数量校验，实际业务链路未调用，故已移除。此项清理不等于原生下单或订单恢复已经完成。

## 当前实现归属

- `engine/runtime/src/orders.rs`：订单命令门禁、原生意图转交、未完成订单的保守对账检查；真实外部发送被阻止。
- `engine/runtime/src/native_handoff.rs`：Nautilus 原生订单意图的严格验证和 Ledger 预登记；**不会发送真实订单**。
- `engine/runtime/src/fills.rs`：原生真实成交证据链尚未验收时失败关闭。
- `engine/ledger/src/order_intents.rs`、`order_reconciliation_tasks.rs`、`verified_fills.rs`、`native_events.rs`：不可变意图、待恢复任务、经核验的真实成交及尚未认证的原生事件隔离。
- `engine/pipeline/nautilus/`：单独编译的 NautilusTrader 原生 Binance Spot/USD-M 工厂、执行身份映射与离线构建测试；未与 Runtime 构成可发送实盘订单的完整链路。
- `engine/connection/binance-options/`：Options 专属规范化能力，不能视为 Spot/USD-M 通用订单执行引擎。

## 安全约束与后续集成

订单必须先原子登记不可变意图，发送权限必须显式核验；未知外部结果不得盲目重发。请求 ACK、未认证的 Nautilus 事件、订单累计成交量都不能单独作为真实成交入账依据。只有可信、已核验的实际成交证据可以改变 Ledger 事实。

后续接入 Nautilus 原生下单和事件回写时，应统一验证账户/合约身份、原生订单生命周期、累计数量单调性、撤改和部分成交、幂等与重启恢复、风控门禁以及真实成交 Trade ID 的核对；不能仅凭删除未使用代码声称替代完成。
