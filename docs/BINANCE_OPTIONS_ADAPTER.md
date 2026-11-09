# Binance 期权行情、账户与执行适配器

2026-10-29，按用户确认的“行情与账户”范围实施。适配 NautilusTrader `2.0.0rc7` 的
公开 Python 客户端接口，不改引擎源码，不替换既有 USD-M 客户端。
同日用户进一步明确要求增加下单、撤单，新增独立的原生执行客户端；不自动启用实盘。

2026-11-10 RC8 升级：行情订阅及取消订阅使用原生 `instrument_ids` / `bar_types` 列表字段；
批量命令逐合约建立或撤销原有独立订阅，仍受 32 条流限制。
历史合约、成交与 K 线请求暂只支持单一对象，收到批量请求时明确拒绝；
未改交易指令、风险资格或期权账户接入。

## 能力与边界

| 功能 | 当前支持 |
| --- | --- |
| 合约目录 | EAPI 目录 → 原生 CryptoOption，保留单位、精度和报价/结算币种 |
| 近期成交请求 | 最多 500 笔，不接受任意历史时间范围，不代表完整历史 |
| 历史 K 线 | LAST 1m/5m/15m，有界请求最多 31 天、50000 根，分页并只返回闭合记录 |
| 实时成交 | `optionTrade` → TradeTick，保留成交编号、实际接收时间，最多缓存 10000 个编号去重 |
| 最优报价 | `bookTicker` → QuoteTick |
| 盘口 | L2 前 5/10/20 档完整 OrderBookDepth 快照，100ms；不是增量全深度盘口 |
| 实时 K 线 | 1m/5m/15m，仅发布已闭合记录；重复闭合值冲突时停止订阅 |
| 只读账户 | 资产、持仓、当前委托、单笔订单、最近 5 天已完成订单的显式分页 |
| 下单 | LIMIT GTC/IOC/FOK，GTC Post Only 映射为 GTX；保留 Reduce Only |
| 撤单 | 单笔撤单；按原生策略、账户、合约范围逐笔撤销，核实成交与撤单竞争结果 |
| 状态同步 | 原生 Submitted/Accepted/Fill/Canceled/Rejected，REST 轮询与启动对账 |
| 暂不支持 | 市价/触发/关联/冰山/报价金额委托、原地改单、账户设置修改 |

本适配器可被其他代码注册和调用；本轮不新增 HTTP 接口、前端账户产品选项、持续采集服务、
自动持久化或策略接线。原有页面的期权近期/历史数据管理仍可使用原先入口。
2026-11-07 已将期权接入管理端持续采集及官方闭合 K 线补齐，原 USD-M 连续分钟线门禁保持不变。
此前页面的期权“实时未接入”指当时的持续采集管理链路，
不能因为底层已有适配器就改成 READY。

## 原生节点注册

```python
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import TraderId
from engine.connectors.binance.options_client import (
    BinanceOptionsDataClientConfig,
    BinanceOptionsDataClientFactory,
)

# 合约由调用方从当前目录选择；此处 symbols 是用户选择的完整合约代码列表。
builder = LiveNode.builder("options-data", TraderId("OPTIONDATA-001"), Environment.LIVE)
builder.add_data_client(
    "BINANCE_OPTIONS",
    BinanceOptionsDataClientFactory(),
    BinanceOptionsDataClientConfig(symbols=symbols),
)
node = builder.build()
# 添加需要的 DataActor / Strategy，然后 await node.run_async()。
```

Actor 使用标准 `subscribe_trades`、`subscribe_quotes`、`subscribe_book_depth`、
`subscribe_bars`、`request_instrument(s)`、`request_trades` 和 `request_bars`。
显式传入 `client_id=ClientId("BINANCE_OPTIONS")`，避免与同为 BINANCE 场所的永续路由混淆。
盘口使用 `BookType.L2_MBP, depth=5/10/20`。请求 K 线的原生起止边界按事件纳秒过滤，
保持 request_id、params 及原请求范围；历史 ts_init 与现有 Catalog 一样按事件时间索引，
实时数据的 ts_init 为接收时间。近期成交与历史请求不会作为实时订阅数据发布。

构造函数和 factory 不联网；只有节点连接时加载目录、订阅时建立流。
配置支持原生 importable round trip。空 symbols 表示启动时加载当前完整目录；单次最多配置
32 个显式合约、最多同时 32 条流，每种订阅占一条流。
依赖已有 `websockets`，已从开发依赖移至运行依赖，没有新增第三方库或升级版本。

## 生命周期与缺口

WebSocket 使用 Binance 当前 `/public/stream` 与 `/market/stream` 分区，等待明确订阅确认，
有界消息和确认前缓冲。断线后按 1–30 秒退避重新连接并重放原订阅；退出和取消订阅会关闭
socket 并等待后台任务结束。格式、身份或精度校验失败会停止该流，显式报告错误。

`client.stream_status` 是每条订阅的只读状态快照，包括 `connected`、`receipt_age_seconds`、
`last_event_ns`、`event_age_seconds`、`gap_count` 和 `last_error`。
`connected` 仅代表订阅已确认，收到新数据才能判断新鲜度。原生 `is_connected` 是启动生命周期
标识，不能替代每条流的网络/新鲜度判断。重连后 gap_count 保留、成交缺口没有自动补齐；
前 5/10/20 档以新完整快照替换，不拼接断线前盘口。适配器不发放交易就绪资格。

## 只读账户查询

```python
from engine.connectors.binance.options_account import BinanceOptionsAccountClient

# account 由现有后端账户服务鉴权并解密后提供，不在前端传递、不硬编码凭据。
credentials = account["credentials"]
client = BinanceOptionsAccountClient(
    credentials["apiKey"], credentials["secret"], environment=account["environment"],
)
assets = await client.assets()
positions = await client.positions()  # 可传 symbol
page = await client.open_orders(limit=1000)  # 可传 symbol
order = await client.order(symbol, order_id=order_id)  # 或 client_order_id，二选一
history = await client.order_history(symbol, start_ms, end_ms, order_id=cursor, limit=1000)
```

本类只接受 production；期权测试环境尚未验证，拒绝把凭据发送到推测地址。
只允许固定 EAPI 主机上的白名单 GET，HMAC-SHA256 签名，以交易所时间校时，
限制时差/接收窗口、请求间隔、响应大小、超时与限流重试。私有请求禁止重定向，
错误不会包含密钥、签名 URL 或交易所原始错误正文。
凭据仅由调用方在内存中提供，不在本模块存储、不配置 execution client。

响应保留交易所字段名和精确十进制字符串。空仓是有效结果；资产缺字段、方向与数量不符、
重复持仓或订单编号等会报错，不补零、不返回部分成功。
订单列表返回 `possibly_truncated`；该值为真时表示页面已满，需要调用方继续查询，
不是“已加载所有订单”。historyOrders 官方只承诺最近 5 天已完成订单，不能作为完整审计账本。
资产按币种保留，不混合求和，也不把交易所 equity 字段冒充本地计算的“预估总资产”。

## 验证与官方依据

```bash
.venv/bin/python -m ruff check engine/connectors/binance engine/pipeline/options_data.py tests/connectors/binance
.venv/bin/python -m pytest tests/connectors/binance tests/pipeline/options_data_test.py -q
```

测试用真实 Nautilus LiveNode 和本地 WebSocket 模拟端点覆盖原生缓存交付、历史回调、
订阅确认、重复成交、断线重连、取消订阅及退出。账户签名测试仅用虚构凭据；
未用用户真实期权账户验证权限或余额，不代表私有账户已完成线上验收。
本轮真实公开连接在原生节点观察 30 秒，收到 10 次最优报价与 12 次盘口快照并完整退出；
该窗口没有该合约成交，实时成交解析与重连行为由本地模拟端点验证。

## 执行客户端（新增授权范围）

`options_execution.py` 实现 Nautilus 公共 `ExecutionClient`、config、factory；
`options_orders.py` 是与只读账户客户端分离的写入传输，固定允许 `POST /eapi/v1/order`
和 `DELETE /eapi/v1/order`。既有 `BinanceOptionsAccountClient` 的公开查询方法仍然只读。
两者复用签名发送实现，没有复制凭据持久化或引擎订单状态机。

```python
from nautilus_trader.live import LiveExecutionEngineConfig
from engine.connectors.binance.options_execution import (
    BinanceOptionsExecutionClientConfig,
    BinanceOptionsExecutionClientFactory,
)

# builder 是上文已配置期权 DataClient 的节点 builder。
# 凭据由现有服务器鉴权、解密后提供，不把配置 JSON 写入日志或文件。
builder.with_exec_engine_config(LiveExecutionEngineConfig(
    reconciliation=True,
    reconciliation_startup_delay_secs=0,
    reconciliation_lookback_mins=60,
))
builder.with_timeout_disconnection_secs(30)
builder.add_exec_client(
    "BINANCE_OPTIONS",
    BinanceOptionsExecutionClientFactory(),
    BinanceOptionsExecutionClientConfig(
        account_id="BINANCE-OPTIONS-001",  # 与永续账户的引擎标识分开
        symbols=symbols,
        api_key=credentials["apiKey"],
        api_secret=credentials["secret"],
    ),
)
```

Strategy 使用原生 `order_factory.limit(...)` 和
`submit_order(order, client_id=ClientId("BINANCE_OPTIONS"))`；
撤单用 `cancel_order(order.client_order_id, client_id=ClientId("BINANCE_OPTIONS"))`。
Post Only 使用 `time_in_force=TimeInForce.GTC, post_only=True`，
Reduce Only 显式传入 `reduce_only=True`。策略和风险预算仍由原生 RiskEngine 管理。
配置的 repr 隐去凭据；原生配置序列化接口可包含密钥，调用方必须保护该配置。

### 发送与状态语义

- 原生客户端订单号与独立账户标识共同生成稳定的 `sa-` 编号。发送前查询同编号订单，
  找到已有订单就恢复观察，不重新下单。同一原生命令在本客户端内不会再次 POST。
  应继续使用原生唯一订单号；交易所历史保留期之外、引擎缓存丢失后的永久去重不在保证范围。
- 写请求只有一次发送机会，超时、429、5xx 不自动重发。只有明确的交易所拒绝证据与
  同编号订单不存在相符时才报告 Rejected；结果不明时保留 Submitted/待核实并通过查询恢复。
- HTTP 成功不直接报告成交或撤单成功。按真实订单报告与 `userTrades` 明细交给原生引擎处理；
  累计成交数量必须等于明细数量，手续费按报价币种、原始精确值和实际成交编号处理。
  币安负 fee 表示扣费，映射为原生正 commission；返佣保留负 commission。
- 账户和活动订单每 2 秒尝试一次 REST 核对，实际间隔受网络、限流和订单数量影响。
  这是轮询同步，没有声称私有 WebSocket 级延迟。核对失败、超过 15 秒未成功同步或有待核实
  订单时禁止新增风险。撤单和 Reduce Only 不受新增风险就绪门禁阻挡。
- 新增风险还要求配置内未到期期权、合法精度/步长/数量/价格范围及 15 秒内有效双边报价。
  需由消费方订阅报价；仅启动行情客户端但没有新报价，不满足新增风险条件。
- 关闭时不发新单，也不自动撤销所有账户订单。已进入网络发送的请求会等待线程结束，
  不把取消 await 当作撤回交易所请求。未核实的状态留给重启对账处理。

### 对账范围

启动先读取原生账户与配置合约的持仓、挂单、已完成订单和成交明细，明确携带
`lookback_start` 与完整性标记。已完成订单最多核对最近 5 天；满页、缺少成交明细、
未知合约、重复方向净持仓或证据冲突会阻止完整性声明与新增风险，不把局部快照当完整历史。
成交明细分页最多 20 页，持续核对复用原生真实成交编号去重。
私有调用不做重定向，签名与错误信息不会包含凭据或完整签名 URL。

账户原生余额使用交易所 `marginBalance`/`available` 与保证金字段，来源保留在 account info；
不会把这些字段冒充本地“预估总资产”。本轮不改现有账户估值或操作账本。
执行节点使用 NETTING，仅覆盖明确配置的期权合约。单笔撤单核实账户、策略和合约归属；
批量撤单只遍历原生缓存中命令范围内的订单，不调用交易所全账户清空接口。

执行测试仅使用虚构凭据与模拟交易所，覆盖原生风险/执行引擎中的下单、部分成交、
手续费、撤单和退出，以及超时查重、关闭期间禁止新单、撤单成交竞争、历史截断和状态恢复。
没有对真实账户下单、撤单；真实私有权限及线上成交仍未验收。

- [Nautilus Python adapter interface](https://nautilustrader.io/docs/latest/developer_guide/python_adapters/)
- [Binance 期权 public WebSocket](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/ws-streams/public)
- [Binance 期权 market WebSocket](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/ws-streams/market)
- [Binance 期权账户 REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/rest-api/account)
- [Binance 期权订单与持仓 REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/rest-api/trade)


2026-11-07 已授权批量行情管理：`batch_market_events` 将至多 64 个合约分组，
共享公开 K 线与 Tick 连接，可同时订阅前 10 档盘口（100ms）；动态订阅/退订须获得匹配确认，
新合约不会重连其他合约。2026-11-08 管理端默认同时开启 Tick 和盘口，不受所选 K 线粒度限制。
盘口经原生解析校验后交由 Pipeline 原子保存 SHA256 JSON 来源快照，Tick 继续写原 Catalog。
盘口历史不从 REST K 线恢复，断线缺口保留；现有数据页不提供盘口历史预览。
原生 DataClient 与批量入口共用 `options_parsing.trade_event/bar_event`，不重复精度或单位逻辑，
不配置账户/执行客户端，不改变原生交易与风控引擎。单合约公开入口仍可用。
模拟 1,514 合约全开 Tick 使用 48 条连接，另验证两个真实公开合约的共享订阅确认；
后者仅为公开协议检查，不代表私有权限或全量长期采集已验收。


2026-11-08 按用户要求接入首页期权持仓与操作账本。只读客户端新增
`history_pages("bill" / "userTrades" / "exerciseRecord", start_ms, end_ms, symbol=...)`，
真实成交必须指定合约，bill 按 USDT 查询，行权可按全账户读取。
API 只建立一次持仓基准，后续以真实成交与行权推算，不将期权持仓加入分钟快照；
订单、成交及期权资金证据复用原操作账本，不使用执行客户端发送命令。
前文执行适配器测试与验收边界保持不变；本次不注册期权执行客户端，也不改资产估值曲线。
详见[期权操作与持仓推算](OPERATIONS_API.md#期权操作与持仓推算2026-11-08)。
