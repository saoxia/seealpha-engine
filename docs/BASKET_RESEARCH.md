# 跨产品组合研究：目标规划、历史模拟与可恢复事件账本

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

## B3-A：期权保护历史报价证据门禁

`engine/strategies/option_history.py` 读取已有的原生期权合约、完整 1m/5m 永续及双翼期权 K 线，并按事件时点绑定真实历史 `QuoteTick`。交易所期权 K 线结束标记为永续 K 线收盘时间减 1ns，因此显式校验此差异；不填补缺失行情。

必须满足：合约标的与报价币种一致、PUT/CALL 各一且到期相同、整个研究窗口早于到期、数量匹配各自合约步长、卖盘报价 `ask_price` 有效且 `ask_size` 足以覆盖每条保护腿、报价没有来自未来且未过期。任何证据缺口直接拒绝，而不是用期权 K 线收盘价推断可成交成本。

原生 Catalog 包装层现支持 `QuoteTick` 的读写，以便后续经审核的数据采集器持久记录可追溯的历史买卖盘口。

**这只是期权保护组合的历史数据证据门禁；尚未提供一条有历史报价覆盖的生产采集链路、真正的多腿期权模拟撮合、行权与到期记账或实盘执行。** 当前 `demo_option_hedged_bollinger` 仍不标注为可回测。

## B3-C：原生保护腿先成交的期权组合研究回测

新增 `run_native_protected`，将单个开发租户、同账户已授权 `demo_option_hedged_bollinger` 归档内的 `BollingerModel` 用作信号提议源（不复制策略算法），然后由独立的 NautilusTrader `BacktestEngine` 在 `SIM_OPT` 的 USDT CASH 账户以及 `SIM_UM` 的 MARGIN 账户模拟订单和净额仓位。读取先前验证过的 native Catalog 期权真实 QuoteTick（含买卖数量）和原始永续 K 线，保留期权双翼合约乘数与不同产品间净额隔离。

组合调度器以真正的 `OrderFilled` 事件推进阶段，不把下发目标或订单接受当成交：**先买 PUT 和 CALL 并逐笔确认足够覆盖 → 才可扩大永续方向仓位**，退出时先减永续再释放保护。订单被拒、撤销、剩余数量未知、腿间覆盖不足或资金/权利金超预算直接停止回测，不发布成功回测记录。报告含订单、逐笔成交、原生费用、净持仓、按可观察 bid 估值的权益曲线和成员目标时间戳；正式交易权限与既有 Demo `on_start` 仍保持禁用。

仅允许 `POST /tenants/{tenant_id}/accounts/{account_id}/protected-option-backtests` 开发内研究入口：必须有三腿历史订阅与合约、原始策略归档哈希、独立两账户资金和总名义预算；`GET .../protected-option-backtests/latest` 查询归档结果。

**边界：** 本阶段不包含已到期/到期当日的行权与结算价核算。因为无可核验的交易所交割和行权证据，`read_option_protection_evidence` 会拒绝涉及到期的区间，报告标注 `settlement_accounting=not_in_window_expiry_requires_verified_settlement`。也不能从现有公开 API 恢复任意断线期的历史 BBO，缺口由历史报价时效门禁拒绝。订单按 native 模拟运行，并非真实交易所跨产品原子订单或实际部分成交补偿。

## 阶段账本：研究态恢复与并发写入保护（后续独立切片）

现有 `engine/strategies/basket_reconciliation.py` 对**不可执行**的 BasketPlan 记录阶段 begin、可信累计成交、最终成交确认及全部腿权威仓位快照。未知订单状态必须冻结，不自动重发；在完整确认前不能进入下一风险增加阶段。

`engine/strategies/basket_journal_store.py` 现提供明确的研究态文件存储：

- `BasketJournalStore(private_directory).save(basket_id, checkpoint, expected_revision=...)`：比较并交换式写入，初次保存需 `expected_revision=None`；同内容重复提交幂等，不同内容的旧 revision 明确报冲突。
- `load(basket_id, plan)`：校验内容哈希、计划指纹和完整事件回放；重启后待确认阶段仍为待确认，冻结状态不会自行恢复。
- 跨进程 POSIX 文件锁、同目录临时文件 + `os.replace`、文件及目录 `fsync`；记录和目录仅允许当前用户访问，拒绝符号链接和不规范的 basket_id。

调用者必须在任何可能引发外部副作用的行为**之前**提交 begin 阶段持久化，并基于 Engine/交易所权威订单与仓位证据推进状态。这里不包含任何执行器、网络接口或真实订单适配器。SHA-256 revision 只能发现意外损坏或并发版本冲突，**不能**防止已获得同 UID 文件写权限的恶意进程伪造历史；研究 Worker 仍非强安全沙箱。落盘后仍需外部备份与恢复演练，不等于具有完整事务语义的正式资金订单账本。

## 阶段级毛名义风险预算（Research-only）

规划器不仅校验目标总名义金额，也校验**已有仓位**和每个阶段内可能出现的最大临时敞口。各腿按已验证的价格和合约乘数计量；同一阶段即使允许多腿目标，也不能假设交易所跨产品原子成交，因此对任意腿间先后次序取保守上界。超过预算则拒绝新增风险，不允许依赖净额抵消掩盖实际资金占用。如果已有仓位因行情变动等原因超出预算，只允许逐阶段严格、单调降低毛名义敞口的退出计划，不能阻断纯减险平仓。

这是静态、研究态的风险上界：没有模拟成交期间的报价变化、手续费、保证金追加或交易所动态风险档位，也不能据此放开实盘。真正的执行需要在每笔订单前实时重验行情、现金、保证金和全账户风险，并与 [可重放研究阶段审计](../engine/strategies/basket_reconciliation.py) 对接持久订单事实。

## 紧急全平研究规划（不具备下单能力）

当受控回测或日后可信执行器发现**现有**现货/永续腿偏差超过常规阈值，或永续原有仓位已失去期权双翼完整覆盖时，普通策略调整仍应拒绝。不过，对既有非零仓位的纯归零退出，不能因为现有风险状态已经损坏而完全无计划可用。

`plan_basket(..., emergency_unwind=True)` 显式允许研究态的**全部腿目标数量必须为零**的退出计划：仅为已有仓位生成减险阶段，永续先减险，剩余期权最后释放；对资金费两腿标记 `emergency_coordinated_unwind`，不把它误当作原子成交。仍严格要求同一账户产品身份、合法数量步长、真实且未过期报价以及无裸卖空；超预算时仍只能逐阶段单调减小毛名义敞口。该标记不是可信交易权限，**不会创建或发送任何订单**，不能跳过真实交易环境下的最新保证金、账户和成交核对。

生产状态下的实际紧急退出策略、独立的减仓 API、已挂单处置、权威仓位恢复和断网报警仍需单独实现、实测及明确授权。

## 通用多策略研究意图发布协议（A/D 基础）

`engine/strategies/basket_research_session.py` 新增 `BasketResearchSession`：
同一个开发研究快照内，多个成员以其真实发行标识登记 `member()`，使用与两个 Demo 一致的 `subscribe_market_bundle` / `publish_basket` **参数语言**，分别提交当前租户、账户和产品合约绑定的完整目标。缺少任何成员目标、缺少报价/实际仓位、跨账户/产品、重复腿、不安全约束、过期行情或不相容的保护覆盖将直接拒绝。相同成员和历史时间戳下未提供请求号时产生稳定的**研究意图标识**，而不是交易所委托号。成员快照发布后不可覆写；严格对全部成员按 `tenant_id/account_id/product/symbol` 求和并交给原生 `plan_basket` 阶段规划；返回 `BasketResearchDecision.executable=False`。

`research_checkpoint(request_id)` 为研究阶段生成待确认日志；后续所有成交/权威仓位核对仍必须经 `basket_reconciliation`，没有订单发送、没有资金或保证金划转。该适配器是**纯 Python 意图收集与审计**，不是实际 NautilusTrader Strategy 注入，也不从 Catalog 自行抓取数据，且 **不等同于两个 Demo 已经改用这一运行时或跨产品回测全部贯通**。

上线前还需要把原生 Nautilus Strategy 生命周期、严格历史行情 Bundle、撮合回报与持久审计真实接通；真实交易权限及生产风控须另行验收。

两种已有开发回测现改为经这个研究发布协议汇总目标：Carry 将 1–16 个授权成员的目标绑定到统一发布会话，原生组合撮合前核对净目标；受保护期权将模型目标以同一协议校验后，继续沿用原生先保护再开仓的模拟成交序列。Carry 报告额外输出目标变化历史 `target_events`（成员数、有符号净目标、不可执行标记），不等同于模拟订单成交，更不是交易所订单。

## 离线组合阶段安全监督（C 的前置件，不是生产执行）

`engine/strategies/basket_risk_monitor.py` 的 `observe_basket_phase` 读取完整仓位快照与行情证据、阶段检查点和调用者**可信提供**的 pending 时间，重新核对当前毛名义、Spot/UM 腿偏差、期权双翼覆盖、累计成交/仓位一致性与最长待确认时间。异常返回一个必须经持久化层记录的冻结检查点；不能依靠订单 accepted 或历史价格推断已成交。已完成日志如果出现新风险异常则直接拒绝，不尝试非法修改不可变事件。

该函数未连接 Binance 订单入口、交易所权威查询、账户资金证明或服务进程计时器；没有实时报警和生产幂等事务保证。它不授权实盘，不满足 #107 C 的最终验收。真实账户沙盒、断网/未知订单状态故障注入、手续费/保证金实时核算和长期运行测试仍必需。

## 尚未实现的关键条件

- 通用策略包 `publish_basket` / `subscribe_market_bundle` 协议及跨任意策略的统一 Engine 运行时。现有 Funding Carry 与保护期权回测是**特化研究入口**，并非通用策略实盘接口。
- 真实多腿下单/撤单、状态机与交易所回报的持久连接、价格与资金重新验证、部分成交补偿、未知状态对账，以及真实资金权限。研究态阶段账本**不得**直接接入生产订单发送。
- 期权到期行权/交割结算、市场冲击与真实部分成交的可靠模型，长期历史 QuoteTick 覆盖和经过验证的资金费归因。
- 跨环境多版本恢复演练、真实账户沙盒测试、长期运行故障注入与生产独立审批。

## 验证

单元测试：`uv run pytest -q tests/engine/basket_targets_test.py tests/engine/basket_history_test.py tests/engine/basket_reconciliation_test.py tests/engine/basket_journal_store_test.py`。

测试覆盖同一腿跨成员净额、跨产品保留独立腿、先保护后开仓、退出先减险、预算、步长、当前仓位缺失、偏差以及期权覆盖与到期标识。尚不构成多市场回测或生产验收。
