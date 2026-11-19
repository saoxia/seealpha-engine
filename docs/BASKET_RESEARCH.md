# 跨产品组合目标与历史数据同步（Research-only A / B1）

关联 Platform [P0 总任务 #107](https://github.com/saoxia/SeeAlpha/issues/107)。

## 当前已实现的纯规划器

`engine/strategies/basket_targets.py` 将多个成员目标按 **租户 + 账户 + 产品 + 合约** 分组，只对同一腿进行代数净额，不会把现货和永续多空直接抵消。

- `LegId(tenant_id, account_id, product, symbol)`：支持 `spot`、`futures_um`、`options`。
- `MemberLegTarget`：成员自己的带符号 Decimal 目标，可多成员净额。
- `LegEvidence`：每腿严格提供价格、最小下单步长、合约乘数和观测时间。
- `plan_basket(...)`：拒绝不完整账户仓位、未来/过期行情、非法精度、不足的期权保护或毛名义预算超限；返回纯研究用 `BasketPlan`。

当前只定义两种**明确且严格**的计划：

1. `funding_carry`：同标的现货多头和 USD-M 永续空头，要求 BASE 数量相等，当前腿偏差不超过传入上限。涉及调整时只给出 `coordinated_spot_perpetual_required` 意图，**并不把两条腿依次直接发往交易所**。
2. `option_protected`：同标的永续加同到期日看跌/看涨期权多头；目标与当前仓位都要求两翼按乘数覆盖永续数量。建议阶段顺序为「先降低方向敞口 → 买入并确认保护 → 增加方向敞口 → 最后释放保护」。这只是顺序要求，不是已实现的成交确认状态机。

`BasketPlan.executable` 永远是 `False`；没有下单或发布入口，不能将 `BasketPhase` 直接转换为实盘订单。这个阶段也不注册任何新增 HTTP 接口，不改变现有单品种 `PortfolioCoordinator`。

## 已增加：双市场 Catalog 历史对齐（B1）

Engine 内部 `EngineRuntime.iter_funding_carry_history(...)` 仅在开发租户和双产品历史订阅均具备时读取，使用两个产品各自的数据读取锁，无 HTTP、交易所订单或在线行情连接。

- 从真实本地 NautilusTrader Catalog 并行流式读取同一标的的 `spot` 和 `futures_um` K 线，精确按闭合时间戳配对，拒绝时间不匹配、重复、倒序及中段缺口；不填充缺失数据
- 从同一 Catalog 读取已有的 `FundingRateUpdate`，以其**真实事件时间戳**发出单独记录，不从 K 线推断费率；缺少费率观测默认拒绝
- 序列为 `CarryHistoryEvent(kind="bars" | "funding")`，仅供**后续真正的回测执行器**使用：这不是订单成交、资金费划转、组合净值或收益率回测
- 不修改旧单品种回测、策略包入口、HTTP v2 或生产授权

**时区与覆盖限制：** 使用 UTC 时间窗口 `[start,end)`；当前校验两个产品数据的首尾窗口和中段连续性，但仍不证明资金费观测是无缺口的最终结算事实。回测执行前仍须补齐这些检查，不可用这批输入宣称真实收益。

## B2：双独立账户原生模拟（开发租户研究版）

Engine 增加特殊的已下发 `demo_funding_carry` 策略组合研究入口：

`POST /tenants/{tenant_id}/accounts/{account_id}/carry-backtests`

请求必须明确提供 `start`、`end`、`interval`、`starting_spot_balance`、`starting_perpetual_balance` 和 `max_gross_notional`。不接受交易密钥、生产租户、未下发或其他策略包。结果保留在 Engine `carry_backtest` 事件里，可通过 `GET .../carry-backtests/latest` 查询。

实现方式：使用单个 NautilusTrader BacktestEngine，借由**模拟专用 Venue ID** 将同一交易所的 Spot `CASH` 与 USD-M `MARGIN` 分隔，合约及 K 线数据来自获准的真实 Catalog；仅重映射 Venue 身份，不重写撮合。使用原生 MarketOrder、FeeModel、Account、FundingRateUpdate 和净持仓事件；计算独立账户和合并的资产曲线。发出多腿目标之前验证真实当前仓位、步长、组合名义金额和腿间偏差；若模拟撮合发现不对冲或拒单，停止回测并拒绝返回成功报告。最多 20000 根对齐 K 线，以限制同步 HTTP 回测工作负载。

**重要边界：**
- B2 研究入口从已由 Platform 授权并下发的 demo 参数计算组合目标，**没有调用 Demo 包自身的 `on_market_snapshot` 或 `publish_basket`**；同策略代码运行的统一协议仍需完成
- 交易所没有原子跨产品成交；原生模拟的双订单和失败中止不意味着真实交易所的原子性/异常补偿能力
- 报告中的 `funding_observations` 是原始历史费率证据，资金费结算由 Nautilus 原生机制模拟；尚无单独经对账的资金费归因或生产适配
- 成交依赖 K 线模拟撮合（而非 Tick/L2 真实成交）；滑点、盘口冲击和真实部分成交仍不具备可用证据
- 不开放实盘、不改变原单合约执行，不宣称 Funding Carry 策略已具备生产权限

## 尚未实现的关键条件

- 真实的多腿 `publish_basket`、`subscribe_market_bundle`、独立现金/保证金与资金费/期权权利金/到期模拟。
- 多产品 Catalog 已具备双市场 1m/5m 严格时间对齐输入，但尚无撮合、独立账户、资金费结算与统一权益曲线；目前两个跨产品 Demo **仍不可实际回测**。
- 多腿成交状态机、持久幂等、部分成交和超时结果未知的冻结恢复；真实交易所跨产品并不存在跨账户原子成交保证。
- 真实行情合约的期权到期日、卖盘深度、报价和乘数核验、价格滑点/费用/保证金门禁，以及订单/资金对账。
- Platform 的授权、开发与正式环境区分、AI 研究工具入口及任何真实资金权限。

## 验证

单元测试：`uv run pytest -q tests/engine/basket_targets_test.py tests/engine/basket_history_test.py`。

测试覆盖同一腿跨成员净额、跨产品保留独立腿、先保护后开仓、退出先减险、预算、步长、当前仓位缺失、偏差以及期权覆盖与到期标识。尚不构成多市场回测或生产验收。
