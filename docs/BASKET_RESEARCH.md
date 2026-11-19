# 跨产品组合目标（Research-only Stage A）

关联 Platform [P0 总任务 #107](https://github.com/saoxia/SeeAlpha/issues/107)。

## 当前已实现的纯规划器

`engine/strategies/basket_targets.py` 将多个成员目标按 **账户 + 产品 + 合约** 分组，只对同一腿进行代数净额，不会把现货和永续多空直接抵消。

- `LegId(account_id, product, symbol)`：支持 `spot`、`futures_um`、`options`。
- `MemberLegTarget`：成员自己的带符号 Decimal 目标，可多成员净额。
- `LegEvidence`：每腿严格提供价格、最小下单步长、合约乘数和观测时间。
- `plan_basket(...)`：拒绝不完整账户仓位、未来/过期行情、非法精度、不足的期权保护或毛名义预算超限；返回纯研究用 `BasketPlan`。

当前只定义两种**明确且严格**的计划：

1. `funding_carry`：同标的现货多头和 USD-M 永续空头，要求 BASE 数量相等，当前腿偏差不超过传入上限。涉及调整时只给出 `coordinated_spot_perpetual_required` 意图，**并不把两条腿依次直接发往交易所**。
2. `option_protected`：同标的永续加同到期日看跌/看涨期权多头；目标与当前仓位都要求两翼按乘数覆盖永续数量。建议阶段顺序为「先降低方向敞口 → 买入并确认保护 → 增加方向敞口 → 最后释放保护」。这只是顺序要求，不是已实现的成交确认状态机。

`BasketPlan.executable` 永远是 `False`；没有下单或发布入口，不能将 `BasketPhase` 直接转换为实盘订单。这个阶段也不注册任何新增 HTTP 接口，不改变现有单品种 `PortfolioCoordinator`。

## 尚未实现的关键条件

- 真实的多腿 `publish_basket`、`subscribe_market_bundle`、独立现金/保证金与资金费/期权权利金/到期模拟。
- 从多产品 Catalog 同步生成一致的历史快照、撮合与统一权益曲线；目前两个跨产品 Demo **仍不可实际回测**。
- 多腿成交状态机、持久幂等、部分成交和超时结果未知的冻结恢复；真实交易所跨产品并不存在跨账户原子成交保证。
- 真实行情合约的期权到期日、卖盘深度、报价和乘数核验、价格滑点/费用/保证金门禁，以及订单/资金对账。
- Platform 的授权、开发与正式环境区分、AI 研究工具入口及任何真实资金权限。

## 验证

单元测试：`uv run pytest -q tests/engine/basket_targets_test.py`。

测试覆盖同一腿跨成员净额、跨产品保留独立腿、先保护后开仓、退出先减险、预算、步长、当前仓位缺失、偏差以及期权覆盖与到期标识。尚不构成多市场回测或生产验收。
