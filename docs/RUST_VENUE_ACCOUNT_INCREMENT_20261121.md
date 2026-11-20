# Rust 交易所与账户链路迁移：本轮增量（2026-11-21）

本轮变更是账户与交易所读侧的增量实现，**不是 Spot / USD-M / Options 全部 Python 等价迁移完成的声明**。Python 代码继续保留。

## 新增

- `seealpha_venue::reconciliation`：按产品读取签名账户快照。Spot 使用 `/api/v3/account`，USD-M 复用 `/fapi/v2/account`，Options 分别读取 `/eapi/v1/account`、`/eapi/v1/position`。
- 使用 `rust_decimal::Decimal` 比较账户余额与仓位；现货余额包含 free + locked；USD-M 的 LONG / SHORT 仓位独立比较。重复 key、缺字段、无法解析金额一律拒绝作为完整证据。
- USD-M 条件单只读查询与显式撤单走 `/fapi/v1/algoOrder`，按 clientAlgoId 核实返回标的及身份，无法确认时返回 `Unconfirmed`，不自动重发。
- 新增离线单元测试覆盖多空对账、错误/重复证据及十进制精度。

## 尚未完成

- 账户快照尚未接入 HTTP v2 多产品路由、持久化对账作业及差异审批工作流。
- USD-M 条件单的提交、触发事件和恢复状态机；Spot OCO/订单列表；Options 全量查询/撤改单和成交分页。
- 资金费、期权行权/到期/结算的交易所权威流水拉取及账本幂等记账。
- 用户数据流与 REST 补偿、真实资金/仓位长期核对；本轮没有真实交易所验证。
- CI/Actions 按仓库原有暂停策略没有恢复；当前 GitHub-only 编辑环境未运行 cargo 检查。

## 安全边界

这次只新增独立读侧及显式撤单 API，未新增可自动触发真实订单的路径；不迁移密钥入库，不以本地推算成交替代交易所证据，未知结果只能按交易所 ID 核对。
