# Runtime / 用例编排

- 本目录是当前 Rust Engine 用例编排入口：订单命令及证据对账在 `src/orders.rs`；历史研究/回测在 `src/research.rs`；市场数据、策略、纸面会话等由对应的内部模块负责。
- 研究授权和范围校验位于 [gateway/api/src/research](../gateway/api/src/research/)；跨模块研究用例编排继续由 `src/research.rs` 负责。

接口层调用 Runtime；Runtime 组合领域 crate。源码按用例分文件不等于改变业务安全门禁或引入新的事务边界。目前 HTTP 适配器尚未完全转为纯薄层，这属于独立的后续重构任务。

精简原则：没有调用方且始终失败的占位方法直接移除；原生真实成交核验仍未接通，任何未认证成交都不得写入账本。订单预登记、未知状态恢复、租户隔离和实盘发送禁用门禁保持不变。

本轮继续移除没有实际入口的研究快照读写、旧模拟回测包装、单独的 NativeTradeFeaturesV1 查询门面和未接线的原生订单诊断门面；保留已调用的 Catalog、订单预登记、恢复查询及实盘禁用门禁。现有数据库表不做破坏性迁移。

策略包预检现在只解析原生 Rust manifest（可以直接提交 JSON，或提交包含原生 manifest 的 ZIP），不再运行 Python host，也不再需要 `ENGINE_PYTHON`。纸面会话仅支持原生发行包和内置 Rust 示例策略。

第二轮清理：删除账户快照/连接管理、实验性行情子进程监督、ClickHouse 设置与回退、候选演示回测、异步回测任务、应用事件及异常日志。保留租户隔离校验、官方历史行情任务所需的 Catalog 数据操作，以及真实数据回测所需的只读 K 线能力；不运行实盘订单。

模拟订单由 `state_store/paper.rs` 事务写入数据库 `orders`、`fills`；Gateway 订单列表按租户、账户和环境直接查询数据库，已移除 Runtime 的 `StoredOrder`/`stored_orders` 与旧请求历史读取兼容接口。`order_requests` 表仍由订单意图预登记使用，不能随旧查询接口删除。

对账调度精简：删除未接入实际 Worker 的 `state_store/order_reconciliation_tasks.rs`（入队、任务租约、重试状态机）。待恢复订单仍通过 `order_intents.rs` 中的 `order_recovery_page()` 按租户分页直接查询 PostgreSQL；保持订单意图预登记、未知结果禁止重复发送、真实成交必须认证等安全约束。现有 `order_reconciliation_tasks` 数据表暂不删除，以避免破坏性迁移。

历史证据回测精简：移除 Runtime 中的 `evidence.rs`（现货/永续资金费证据与期权保护历史回测），相关按日期提交的 Carry / Protected 请求现在明确拒绝，不以模拟数据替代；普通历史策略回测和模拟窗口仍保留。底层 `seealpha-backtest::evidence` 暂不在本次清理范围。

历史策略回放精简：删除 `src/history.rs` 中仅服务旧接口的 Catalog→已下发策略→自定义回测编排；指定日期而未提供 `bars` 的回测请求现在明确拒绝，不自动编造输入。保留显式传入 `bars`/`targets` 的回测及 Catalog、Strategy、Backtest 领域能力。历史结果表不作迁移。
