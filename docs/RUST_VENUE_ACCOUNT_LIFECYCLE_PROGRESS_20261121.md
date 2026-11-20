# Rust 交易所/账户链路继续替代（2026-11-21）

## 本分支新增

- 订单生命周期检查：统一 NEW、PARTIALLY_FILLED、FILLED、CANCELED、REJECTED、EXPIRED 和 UNKNOWN；禁止订单 ID 变化、累计成交回退、终态回退，以及重复或伪造的终态成交量。
- 在 Runtime 的已核实成交同步前使用该检查，避免乱序交易所响应覆盖历史证据。
- 使用签名 GET 收集 USD-M `/fapi/v1/income` 的 FUNDING_FEE 账户实际收付，校验 tranId、精确 Decimal、币种、时间与重复事件。页满即拒绝声称读取完整。
- 使用签名 GET 收集 Options `/eapi/v1/exerciseRecord` 的账户行权证据，验证 ID、数量、价格、费用和窗口。不把公开行权参考数据当作实际账户结算。
- Runtime 账户方法：使用已注册的进程内凭据对 Spot、USD-M、Options 拉取账户快照并与指定基线比较，写入差异审计事件，不改动账户余额。

## 未完成的等价能力（不得宣称替代完成）

1. Options `/eapi/v1/userTrades` 全量成交分页与 Trade-ID 幂等入账、订单撤改单与错误恢复。
2. USD-M 条件单 Algo Service 的提交/触发追踪/重启恢复，Spot OCO 与 List Order。
3. 资产、仓位快照与账本的后台周期性对账，结算记录的账户环境隔离和幂等持久化，流水完整分页。
4. 期权行权对资产变动的准确应用、资金费账户净值的会计分录，以及端到端 HTTP API 路由。
5. 全量 Python parity / Cargo 编译 / 集成测试。当前环境无法运行 cargo，CI 仍按仓库决策停用。

## 原则

不删除 Python 旧路径，不猜测真实成交或结算，不在未验证状态下自动重试，不接触生产密钥或实盘交易。所新增 Rust 读侧方法不能视为生产就绪。
