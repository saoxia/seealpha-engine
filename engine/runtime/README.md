# Runtime / 用例编排

- 本目录是当前 Rust Engine 用例编排入口：订单命令及证据对账在 `src/orders.rs`；历史研究/回测在 `src/research.rs`；市场数据、策略、纸面会话等由对应的内部模块负责。
- 研究授权和范围校验位于 [gateway/api/src/research](../gateway/api/src/research/)；跨模块研究用例编排继续由 `src/research.rs` 负责。

接口层调用 Runtime；Runtime 组合领域 crate。源码按用例分文件不等于改变业务安全门禁或引入新的事务边界。目前 HTTP 适配器尚未完全转为纯薄层，这属于独立的后续重构任务。
