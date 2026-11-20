# 原生多标的组合回测（开发环境，v1）

## 为什么采用一个 Nautilus BacktestEngine

NautilusTrader 2.0.0rc9 原生允许同一 BacktestEngine 注册多个 Instrument、Venue、Strategy，并按事件时间归并数据流，内部负责撮合、订单生命周期和分 Venue 账户核算。SeeAlpha 不再需要对每个标的独立运行回测然后人为相加，也不需要重复写一个撮合内核。

本实现保留原 Engine 已有的 `LegId(tenant,account,product,symbol)`、`MemberLegTarget`、`BasketPlan`、`BasketResearchSession`、`issued_basket_members`，增加通用 `policy=generic`。不同产品或不同合约永不直接净数量；多个策略仅在同一腿求和。通用计划先减少旧风险、然后增加新风险；按合约价格和乘数保守检查每个阶段的毛名义金额。它不声称跨市场执行具备原子性，也不把风险计划转换为生产委托。

## 开发 HTTP API

`POST /tenants/{tenant_id}/accounts/{account_id}/generic-portfolio-backtests`

示例请求：

```json
{
  "issuance_ids": ["my-multi-strategy-1"],
  "legs": [
    {"product": "spot", "symbol": "ETHUSDT"},
    {"product": "futures_um", "symbol": "BTCUSDT"}
  ],
  "start": "2026-11-01T00:00:00+00:00",
  "end": "2026-11-01T06:00:00+00:00",
  "interval": "1m",
  "starting_spot_balance": "1000",
  "starting_perpetual_balance": "1000",
  "max_gross_notional": "1000"
}
```

所有腿必须位于同一个开发租户/虚拟账户，具有独立历史订阅及已验证原生合约。仅接收 1–16 个本账户已下发的开发策略和至多 8 条 Spot/UM 腿。保持历史闭合K线精确对齐，拒绝任意开头、中间或尾部缺口。历史最多 20000 个同步时间点；不注入真实交易密钥，不接入正式订单。

策略包需实现以下纯目标协议，不直接发送订单：

```python
def on_start(self):
    self.subscribe_market_bundle(
        legs=(
            {"product": "spot", "symbol": "ETHUSDT"},
            {"product": "futures_um", "symbol": "BTCUSDT"},
        ),
        data=("closed_bars", "account_positions"),
    )

def on_market_snapshot(self, snapshot):
    # snapshot["markets"]["spot:ETHUSDT"] gives PREVIOUS closed-bar price
    # snapshot["positions"] gives current native simulated position quantities
    self.publish_basket(targets=[
        {"product": "spot", "symbol": "ETHUSDT", "quantity": "0.1"},
        {"product": "futures_um", "symbol": "BTCUSDT", "quantity": "-0.1"},
    ])
```

每个策略只需声明自己使用的市场腿，并完整发布这些订阅腿的目标（允许数量 0）；组合全部腿必须至少由一个成员覆盖。这样单标的策略与多标的策略都能在同一次原生模拟中参与净额与共享账户预算。每次快照使用上一根已闭合 K 线形成目标；随后交给同一个原生回测引擎接收新的模拟行情和订单回报。报告分别返回策略目标、原生委托、原生成交、各腿实际仓位、按产品账户权益与总权益。前一个阶段未确认时，不允许悄悄执行新的目标。研究计划和实际成交仍必须分开展示。

## 严格限制

- **本阶段绝不授予实盘下单权**；真实交易所的多腿成交不保证同时成功，实际委托未知状态和补偿仍独立设计、验收。
- 通用接口暂只验证 USDT 现货与 USD-M 永续；期权专用报价验证、双翼保护、到期核算继续走已有的保护组合研究入口。**不得把简单的通用毛名义预算当作期权保护策略充分风控**。
- 为免虚构历史结算，本次通用回测结果明确给出 `funding_accounting=NOT_INCLUDED...`。因此总权益轨迹不是已通过真实资金费结算核验的可投资收益，不能用于宣称套利净盈利。
- 使用 Bars 原生模拟撮合而非历史 L2/Tick 的真实成交；订单簿冲击、排队、复杂部分成交和流动性压力模型不包含在本阶段。
- 同一产品下的多个标的共享原生模拟账户与现金；Spot CASH 和 UM MARGIN 仍是两个独立账户。跨币种计价尚未验收。
- 当前入口是 Engine 自身的受保护开发 API。Platform 业务授权、用户可操作页面、长期运行验证和 AI 自动研究仍需跨仓库接线与完整验收。
- 运行用户策略 Python 仍需要可信开发执行沙箱；该通用入口没有降低现有信任边界。

## 回归

```bash
uv run pytest -q tests/engine/generic_portfolio_targets_test.py tests/engine/basket_research_session_test.py tests/engine/generic_simulation_test.py
uv run ruff check engine/strategies tests/engine/generic_portfolio_targets_test.py tests/engine/generic_simulation_test.py
```

设计依据：https://nautilustrader.io/docs/latest/concepts/backtesting/apis-and-runs/ 及 https://nautilustrader.io/docs/latest/concepts/execution/



## 显式 Spot + USD-M 同标的数量对冲（开发研究）

通用 `policy=generic` 仍默认让不同标的独立运行。需要把同一标的现货多头与 USD-M
空头视为严格的对冲腿时，已授权策略可通过现有 `publish_basket` 约束协议显式声明，
无需新增 HTTP 路由或修改发行授权。

```python
self.publish_basket(
    targets=[
        {"product": "spot", "symbol": "ETHUSDT", "quantity": "0.2"},
        {"product": "futures_um", "symbol": "ETHUSDT", "quantity": "-0.2"},
    ],
    constraints={"spot_perpetual_hedges": [{"symbol": "ETHUSDT"}]},
)
```

声明成员必须自己订阅该同标的两腿。Engine 在组合内净额合并**全部**策略成员后校验
每组目标 BASE 数量严格相等（不允许以不同合约、不同币种的相近美元价值冒充完全对冲），
沿用原有量化步长与账户毛名义预算。建仓先模拟买现货并确认成交，再增加永续空头；
退出先平永续，再减少现货。每个阶段必须确认原生订单完成，阶段稳定后检查
`spot_qty + perpetual_qty == 0`，连续三个 K 线周期仍未完成的对冲阶段直接终止回测。
报告 `declared_hedges` 和 `hedge_verification` 区分主动声明对冲与没有声明的普通组合。

该顺序**不是跨 Venue 原子成交**，过渡阶段可能持有单腿敞口；数量中性也不等于
资金费、价差、交易成本、滑点、强平和基差风险全部被抵消。当前只支持相同标的的
现货多头/永续空头；不同币种的统计配对、期权 Greeks Delta 与生产实盘
仍需分别验证，不能借用这一声明冒充完美对冲。


## 单一策略内的跨标的、跨产品价格关联

通用 `subscribe_market_bundle(legs=...)` 声明的是产品+合约的集合，不是预设的资金费、
对冲或价差策略类型。一个已下发策略可以同时读取其订阅的不同标的、不同产品市场，
计算价差、比价、滚动统计信号，然后通过一次 `publish_basket` 发布多个目标数量。
仅对相同产品、相同合约执行目标净额；对冲数量关系是**可选声明**，而非默认规则。

策略可在现有订阅接口增加 `lookback_bars`（1–256，默认 1），通用
`on_market_snapshot(snapshot)` 此时会额外收到
`snapshot["history"]["spot:ETHUSDT"]` 等成员自己的历史序列：
每条记录含 `ts_ns/open/high/low/close/volume`，数值为十进制字符串；
`snapshot["markets"]` 的 `price/ts_ns` 以及 `snapshot["positions"]` 仍保留。
每个成员只收到自己声明的订阅腿，不泄漏共享组合中其他策略的行情/仓位。
数据按完整同步 Bar 的时间线进入原生模拟后才放入历史；下一次决策使用的
永远是前一根（及更早）已闭合 K 线，绝不包含当前即将模拟成交的 Bar。
窗口未达到模型所需长度时应当输出零目标而不是使用未来数据。

这拓宽的是**策略编程与研究信号能力**，而非绕过底层产品接入的安全约束。
目前通用原生模拟验证 Spot/UM 的 USDT 合约，期权缺少的报价与结算核验仍保持拒绝。
后续产品扩展应接入市场数据与执行适配器，不再按策略类别创建专用发布协议。


## 共享订阅、按策略分发的第一阶段

现在通用回测通过 `engine.feed.subscription_hub.SharedMarketData` 管理共享行情：
所有开发发行成员首先声明所需 `MarketStream(source/product/symbol/interval)`，
同一市场在一个回测实例内只保留一份闭合历史缓存；
`requirements` 合并各成员需求，并记录需要的最大 `lookback_bars`。
随后各成员只收到自己授权、订阅的行情与持仓快照，不会读取别的成员的未订阅腿。
这将原先回测内的共享历史与逐策略裁剪提升为可复用的独立数据层，
不改变策略包 `subscribe_market_bundle` / `publish_basket` 的既有协议。
策略没有交易所密钥，行情也不会因为其他成员停止而被清空（除非是最后一个订阅者）。

本阶段进一步引入 `EngineMarketDataService`：由每个 `EngineRuntime` 分别持有，
`generic-portfolio-backtests` 使用它的 `backtest(run_id, ...)` 生命周期管理回放。
同一回测内多策略共享数据，同一 Engine 的不同历史任务分别拥有隔离的回放缓存，
正式行情保留独立 `live` 域；用户运行两个 Engine 则没有跨 Engine 单例或
隐式网络连接共享。回测结束自动释放相应范围。

限制：正式策略尚未连接到此读侧服务，`live_requirements` 也尚未驱动
`engine.feed.managed` 的实际上游订阅/重连与实时交付；旧回测入口仍使用原
Catalog 路径。隔离 Backtest Worker 是单独进程，不跨进程复用 Python 内存，
更不会将回放行情写入正式缓存。统一的生产级服务交付仍需这几条链路验收；
既有采集配置和 Catalog 继续作为权威来源。
