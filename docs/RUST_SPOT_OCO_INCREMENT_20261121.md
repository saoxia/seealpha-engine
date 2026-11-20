# Spot OCO Rust 订单列表迁移（2026-11-21）

新增 Rust `SpotOcoIntent`：对两个条件腿的类型、价格、停止价格、TIF、数量和列表 ID 进行校验，衍生确定性的两个客户子订单 ID。提交使用 `POST /api/v3/orderList/oco`；查询使用 `GET /api/v3/orderList`；撤单后再次 GET 以核实结果。

Rust Runtime 在对外提交前登记请求 ID 与 `order_list=oco` 恢复标志；在未知结果下按列表 ID 查询，不盲目重发。成交核对读取列表两个子订单的交易所状态和各自真实成交 ID，验证累计数量及不可逆状态；只有 `ALL_DONE` 且两个子订单终态和成交数量均完整时标记 ledger_synced。列表状态本身从不被转化为虚构成交。

**仍未等价：** Spot OTO、OTOCO、OPO/OPOCO 订单列表，改单/撤单极端竞态，实时用户流、跨账户任务调度。未执行 Cargo 构建、fmt、clippy、测试或真实交易所鉴权；CI 暂停。保留旧 Python 实现，不把本增量当作全量切换成功。
