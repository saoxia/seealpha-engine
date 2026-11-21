# Execution / Order Management

**当前是逻辑业务边界，不是独立的 Rust crate。**

- `engine/runtime/src/orders.rs`：请求门禁、订单命令、按请求 ID 对账和账本同步编排。
- `engine/runtime/src/order_journal.rs`：订单意图预登记与外部发送控制。
- `engine/exchanges/binance/src/`：交易所订单协议、条件单、撤单与外部观察接口。
- `engine/ledger/src/order_intents.rs`、`order_reconciliation_tasks.rs`、`verified_fills.rs`：不可变意图、恢复任务与真实成交证据。

不得因为传输响应、请求 ACK 或未知结果猜测成交。不盲重发，不跳过租户、风控、精度及持久化门禁。日后若独立 `execution` crate，须先固定事务/故障恢复边界和回归测试，不能仅按文件名挪代码。
