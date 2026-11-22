# Runtime / 用例编排

- 本目录是当前 Rust Engine 用例编排入口：订单命令及证据对账在 `src/orders.rs`；历史研究/回测在 `src/research.rs`；市场数据、策略、纸面会话等由对应的内部模块负责。
- 研究授权和范围校验位于 [gateway/api/src/research](../gateway/api/src/research/)；跨模块研究用例编排继续由 `src/research.rs` 负责。

接口层调用 Runtime；Runtime 组合领域 crate。源码按用例分文件不等于改变业务安全门禁或引入新的事务边界。目前 HTTP 适配器尚未完全转为纯薄层，这属于独立的后续重构任务。

精简原则：没有调用方且始终失败的占位方法直接移除；原生真实成交核验仍未接通，任何未认证成交都不得写入账本。订单预登记、未知状态恢复、租户隔离和实盘发送禁用门禁保持不变。

本轮继续移除没有实际入口的研究快照读写、旧模拟回测包装、单独的 NativeTradeFeaturesV1 查询门面和未接线的原生订单诊断门面；保留已调用的 Catalog、订单预登记、恢复与对账队列及实盘禁用门禁。现有数据库表不做破坏性迁移。

策略包预检现在只解析原生 Rust manifest（可以直接提交 JSON，或提交包含原生 manifest 的 ZIP），不再运行 Python host，也不再需要 `ENGINE_PYTHON`。纸面会话仅支持原生发行包和内置 Rust 示例策略。
