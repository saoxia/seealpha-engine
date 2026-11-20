# Rust USD-M Algo 条件单链路（2026-11-21）

## 本分支

1. 独立 `UmConditionalIntent` 及校验：STOP/STOP_MARKET、TAKE_PROFIT/TAKE_PROFIT_MARKET、TRAILING_STOP_MARKET；限价触发价格、数量、单向/双向持仓、ReduceOnly 与 ClosePosition 互斥、跟踪回调范围、客户端 ID。
2. 签名提交至 `POST /fapi/v1/algoOrder`；普通 `/fapi/v1/order` 禁止处理以上条件单类型。
3. Rust Runtime 请求预登记先保存 `algo_order` 可恢复标签，再提交一次；撤单调用 `DELETE /fapi/v1/algoOrder` 并按客户编号重新 GET 核实最终状态。
4. 未知结果经 `GET /fapi/v1/algoOrder` 恢复，不盲目重试。
5. 已触发 Algo 从 `actualOrderId` 经签名查询真实子订单，并读取真实成交明细；不会把 TRIGGERED 作为 FILLED，已核实的子订单状态单独审计。
6. 净额同步仅在子订单终态和数量对账齐全时标记完成。

## 仍然缺口

- 未接入用户数据流的实时触发推送、重启后统一调度器或全部条件单列表恢复。
- Spot OCO/OTO/OTOCO 和订单列表没有替代。所有产品的更改/撤单竞态仍需端到端场景。
- 交易所账户实际测试和 Rust cargo fmt/test/clippy 未运行（CI 暂停，当前 GitHub 编辑能力不包含构建执行环境）。
- 不删除 Python 旧实现，不能宣称全部交易所链路 Rust 等价完成。
