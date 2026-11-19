# SeeAlpha 通用组合研发：Strategy 成员、历史时钟与财务账本

> #107 的 A/B 开发研究增量。**不可执行真实订单**，不授予生产权限。
> 这套能力没有代替现有两种已接入 NautilusTrader 原生模拟撮合的专用 Demo。

## 通用策略成员

- `basket_targets.plan_basket(..., policy="generic_research")` 允许同一
  tenant/account 下的任意 Spot、USD-M、Options 组合腿，目标按
  `tenant/account/product/symbol` 净额、Decimal 数量/步长/乘数校验、
  执行阶段毛名义风险预算检查。现货/期权仍为买方现金支持，无卖空或裸卖期权。
  **没有假定**所有跨产品组合都已对冲：通用模式首先减险，再增加其他模拟敞口；
  需要资金费中性对冲或期权保护先行的策略仍须使用对应专用模式门禁。
- `GenericBasketRuntime` 在同一个完整市场快照中遍历 1–16 个已绑定成员，
  使策略经 `ResearchBasketMember.publish_basket` 发布自己全部注册腿目标，
  包括零目标。所有成员完成后才统一出研究计划。缺行情、额外产品、重复成员、
  别的账户和时间倒退都拒绝。
- `BasketResearchStrategy(Strategy)` 是 NautilusTrader 策略层的受约束
  `on_basket(bundle)`、`publish_basket(targets=...)` 适配器。
  `NativeBasketBarGate` 按真实 `Strategy.on_bar` 回调等待全部已注册
  策略的 `BarType` 在同一时间点出现，再用独立的可信行情/仓位提供者
  构造快照并调用上面的协调器。不能通过成员 `publish_basket` 直接发送订单；
  这不是自行开放任意第三方 Strategy 的生产订单路由。

## 多产品历史与交易成本

- `read_generic_quote_history` 要求调用方分别提供各腿账户实际
  taker 手续费率和全部 UM 腿初始保证金比例。Nautilus 原生合约元数据不保证
  带有账户级费率，不得在通用收益研究中隐式使用默认手续费。
- `read_generic_quote_history` 从原生 Parquet Catalog 取出真实产品合约、
  NautilusTrader `QuoteTick` 和参考 Spot/UM `Bar` 时钟（1m/5m，
  最多 24 小时）；三种产品都必须具备真实 BBO 和有效卖买深度。
  只允许过期时间不超过上限的历史 as-of 报价，不向未来借报价、
  不用 K 线 close 冒充买价或卖价。测试覆盖缺口拒绝与 PUT/CALL/UM 同步。
  **重要**：本研究加载器以 BBO 中间价作为 *估值代理*，不是交易所强制平仓
  或资金费账户结算的官方标记价。
- `BasketFinanceLedger` 为 Spot 现货现金、UM 独立保证金、
  Options 期权权利金分别计账，逐腿保存签名头寸、均价、已经实现/
  未实现 PnL、手续费及资金费现金流。每一次 fill 都核验原生数量步长和
  费率、确认账户现金或独立保证金充足并检查资金守恒。
  `GenericBasketReplay` 只使用已提供 BBO 深度模拟市价单，
  采用明确的滑点 bps；缺深度时最多模拟可见部分并立即冻结，
  不虚构后续补偿或假定“原子成交”。所有模拟 fills、目标、阶段日志和
  产品权益曲线都带有显式的非生产身份。
- 资金费必须由带原始身份、利率及实际签名入账金额的记录提供，
  并与执行时持仓及标记价核对。调用方可同时提交**完整的预期结算时间清单**；
  缺失时绝不声称资金费完整，结果标记 `funding_schedule_verified=false`。
  `basket_settlement.reconcile_verified_option_expiry` 已接入通用现金账本：
  只有期权真实合约、完整行权数据、交割指数、费用、期权仓位、
  结算前后现金同时闭合，且任何 UM 腿已归零，才可记录到期现金；
  不凭历史 OHLC 猜测行权或到期价格。
- 每个通用研究阶段都生成可重放 `BasketCheckpoint`。可提供
  `BasketJournalStore` 私有 CAS 存储；模拟委托阶段先登记再确认，
  部分成交只登记确认过的模拟增量并冻结；重启检测到既有阶段日志
  不会再次发送模拟委托，也不会静默把历史资金当作恢复后的余额。

## 仍需另行交付才算 B 全量验收的内容

1. 通用研究匹配模型尚**不是** NautilusTrader 原生 BacktestEngine 撮合器。
   Nautilus 原生 `on_bar` 成员网关与通用离线交易账本是可复用组件，
   但未被统一为一个多市场原生撮合且独立实际成交回报的最终回测执行服务。
2. 资金费实际逐笔账、期权行权/交割及独立保证金**历史源认证与批量提取**
   仍由可信数据适配器负责；没有证据时不计算为零或伪造结果。
3. 原生交易所层级的滑点/成交冲击、清算、维持保证金、跨币种账户 FX、
   资金划转与账户级可用资金完整重建需要更多验证。
4. 通用组合仍未作为 Platform/Harness 生产或公开回测端点开放；
   原有专用 Demo 的 Platform 回测链路及隔离政策保持不变。

## 离线回归

```bash
uv run pytest -q \
  tests/engine/basket_generic_replay_test.py \
  tests/engine/basket_generic_history_test.py \
  tests/engine/basket_native_member_test.py
```

原则：任何由用户提供的历史数据、费率、原生 Strategy 参数都必须先在
受控开发环境验证。本研究模型不得用来发起 Binance 真实订单。
