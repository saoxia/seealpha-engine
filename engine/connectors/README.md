# SeeAlpha 市场连接器

本目录只维护项目需要的接入配置、原生适配器桥接和少量补充接口。
已有 USD-M 行情传输、历史请求及连接状态复用 NautilusTrader 原生适配器；
Binance 期权通过 Nautilus 公开 Python 扩展接口接入，见下文。

连续行情入口目前支持 Binance USD-M 公开行情，不配置账户或执行客户端。
账户查询、手动交易和估值节点由常驻 `engine` 调用 `binance/queries.py`、
`manual.py` 和 `valuation.py` 创建；不向现有公开行情节点加入账户或执行配置。

- `binance/native.py`：data-only LiveNode、原生订阅、历史请求和连接生命周期。
- `binance/transport.py`：共享原生节点的历史/实时入口，以及独立服务器时间查询。
- `binance/queries.py`：原生品种加载及只读账户快照（余额、已对账仓位）。使用公开
  LiveNode、配置和 factory，不调用适配器私有 HTTP 客户端，不依赖 API、数据库或旧后端。
- 公共分钟线与错误类型来自 `core.market.models`，市场命名来自 `core.market.instruments`。

连续性、补洞编排、数据就绪、回放交付和持久化属于 `engine/feed/`；
历史归档下载、转换、校验、特征生成和入库属于 `engine/pipeline/`。
连接器不依赖这两个业务模块，不在这里重复实现原生订单和账户接口。
账户快照复用原生启动对账并等待就绪，查询结束后关闭和释放节点。
账户查询及操作历史同步的节点在工作线程内使用独立事件循环，创建、运行和释放均在同一
线程完成，避免并发 HTTP 查询与后台同步违反原生每线程单节点限制。调用取消后，工作线程
继续按原查询超时完成清理；不把原生异常对象和带凭据的异常原文跨线程传回调用方。
异常清理同时遍历 `__cause__` 和 `__context__`，覆盖宿主运行与退出错误，避免原生线程绑定对象
遗留在原因链中、随后被其他线程垃圾回收；处理异常链环，不改变原生风控或订单重试策略。
对账可生成本地缓存条目以恢复外部仓位；观察策略不发送任何下单、撤单、杠杆或保证金修改命令。
测试使用本地 HTTP/WebSocket 模拟端点和虚构凭据验证实际原生客户端，覆盖空仓、单向及双向仓位。

原生标准仓位不提供旧页面全部字段，缺失的标记价、强平价、杠杆及逐仓模式不在 connector 中推算。
资产汇总、权限、凭据保存和缓存由上层服务负责。交易命令和历史归档不属于本轮查询接入。

`binance/assets.py` 为 LIVE 显示补充固定只读账户资产明细及资产 USD 指数，
填补原生 Python 快照未发布的资产模式、分币种钱包/浮盈亏字段；不替换原生仓位或交易路径。
connector 核对币种身份、有限数值和指数时间，不做估值、价格缓存或小额资产过滤；
USDT 换算、5 USDT 阈值及展示失败处理归现有 API 汇总服务。

2026-10-31 用户明确要求接入资金变动：`binance/income.py` 补充 USD-M 固定白名单
只读 GET 收支历史，填补锁定版本公共 Python 接口未暴露的资金流水能力，不替换原生账户/订单查询。
保留币种、有符号金额、未知收入类型、交易所流水身份和时间；七日窗口满页后按时间二分，
单毫秒饱和及非法数据拒绝确认覆盖。连接器只交付证据；持久化、检查点和补齐调度仍由 API 拥有，
不依赖 API、Pipeline 或数据库，不发送划转、充值提现或交易命令。

面向 SeeAlpha 的多资产定位，新增交易所或经纪商时，在独立子目录优先接入其
NautilusTrader 原生适配器；行情接入与账户、执行接入分别说明和验收。
公共命名覆盖 Binance USD-M 与期权合约；新增其他市场须显式扩展映射。

```bash
.venv/bin/python -m pytest tests/connectors -q
```


`binance/options.py` 补充原生 loader 暂不支持的 Binance 期权公开目录、近期成交和历史 K 线。
固定端点白名单、响应大小限制、超时/限流重试，返回公开 metadata 或 native CryptoOption；
不配置账户、不查询私有接口、不启动持续服务，持久化与来源一致性属于 pipeline。

2026-10-29 按用户确认的“行情与账户”范围增加期权适配器：
`binance/options_client.py` 提供原生 DataClient config/factory、历史请求与实时成交/报价/
盘口快照/闭合 K 线订阅，`options_parsing.py` 统一离线和实时的精确数值转换；
`options_account.py` 提供单独的白名单 GET 只读资产、持仓与订单查询。
行情与只读账户客户端不注册 ExecutionClient，不接通策略交易，不改变 USD-M Feed。
单合约 `options_client.market_events` 保留 data-only 原生节点公开入口。
批量管理通过 `batch_market_events` 每组至多 64 个合约复用公开连接，共享 DataClient
所用的 TradeTick/Bar/OrderBookDepth 解析、合约单位与精度验证；
管理端同时订阅 Tick 和前 10 档盘口（100ms），共享公开连接；
新增/移除合约使用原连接订阅/退订并核对确认。
连接器只交付精确行情与传输状态，有界队列施加背压；落盘、补齐和缺口恢复仍归 Feed/Pipeline。
调用方式、连接状态和未验证边界见 [期权适配器](../../docs/BINANCE_OPTIONS_ADAPTER.md)。

同日按用户明确要求增加下单、撤单：`options_execution.py` 提供单独的原生执行客户端及
config/factory，`options_orders.py` 允许固定端点的单次签名发送；保留只读账户客户端边界。
支持限价单、Post Only/Reduce Only、归属范围内撤单、真实成交明细对账及未知结果恢复。
只有调用方显式注册执行客户端后才具备写入能力，本轮没有在正式服务注册、启动或实盘验证。

管理端分钟 Feed 可选择启用 NativeMarketData.on_partial 回调：MinuteActor 对原生
成交流做临时分钟聚合并限制交付频率，不改变既有闭合 Bar 和心跳入口。
临时值不承诺完整覆盖，完整性标记、持久化和策略门禁仍由 Feed 负责。

`binance/pair_data.py` 的 `PairREST` 保留公开 HTTP 白名单 `trades`、`klines`、`time`，
复用响应上限、超时和限流退避。`PairTradeSocket` 补充固定 USD-M `@trade` WebSocket：
交付连接/断线事件和真实逐笔编号，断线指数退避，取消时关闭连接。
它补足固定版本原生 aggTrade 门面无法提供的原始逐笔证据，不混用两种编号，
不调用带密钥的 historicalTrades，不增加账户/执行客户端。
归档转换、存储、聚合、缺口和健康状态仍由 Pipeline/Feed 负责。


2026-11-06 用户确认 Binance USD-M 永续手动交易：`binance/manual.py` 通过公开
LiveNode / Strategy / 原生 Binance factory 提交显式命令，继续经过原生风控及执行状态机。
`queries.account_node` 提取既有节点配置供读取与手动入口复用，只读查询不安装手动策略。
合约选择器汇总行情使用固定公开白名单；图表行情由浏览器直连，旧快照查询已移除。
下单等待独立的新鲜原生报价，不使用显示行情作为风险报价。
connector 不依赖 API/数据库；命令登记、账户互斥和账本保存仍属 API。
功能及验证边界见 [手动交易](https://github.com/saoxia/SeeAlpha/blob/main/docs/MANUAL_TRADING.md)。

手动交易 `binance/manual.py` 在现有原生执行路径中只读核对实际持仓模式并匹配开/平仓方向，
不改变交易所账户设置。固定 GET 补充公共 Python 节点未暴露的持仓模式、损益两平/强平价；
后两者仅供图表显示，原生持仓身份、数量和开仓价一致时才附加，不作为风险报价。

手动账户快照另以固定签名 GET `/fapi/v1/symbolConfig` 读取所选合约的实际杠杆倍数，
仅作显示；空仓时也可读取。缺失、身份不符、无效值或读取失败返回空值，不影响原生交易，
提交/撤单路径不增加该显示查询，不修改杠杆设置。

手动节点的未分类异常只记录动作、异常类型与代码位置，不记录异常原文、签名 URL、凭据或账户数据。


`binance/valuation.py` 为 USD-M 资产曲线持有独立原生 data-only 行情节点，订阅持仓标记价。
余额与仓位复用 `assets.asset_account(..., positions=True)` 固定只读 GET，每 5 秒检测变化，
价格每 250ms 合并观察；原生回调不做网络/数据库 I/O。该节点不配置执行客户端，不操作 listenKey，
避免与既有短时账户查询的私有会话清理互相干扰。默认资产明细查询的返回字段保持不变。
断线、账户读取失败或有界队列溢出结束事件流，由 API 保存缺口并重建基准。
原生对象仅在所属工作线程创建和释放，不发送交易或账户配置命令。
持久化、Decimal 估值、资产指数折算、校准、任务和恢复继续属于 API。
详见 [资产估值与重算](https://github.com/saoxia/SeeAlpha/blob/main/docs/EQUITY_VALUATION.md)。


2026-11-08 首页及操作记录增加期权只读接入：`options_account.history_pages` 提供
资金、真实成交和行权的有界分页证据，`operations.normalize_option_record` 复用既有账本契约。
只允许固定 EAPI 主机的签名 GET，保持重定向拒绝、限流重试、精确数值和错误脱敏；
满页二分时间，单毫秒饱和或身份/范围冲突拒绝确认覆盖。
一次初始持仓、操作调度、持仓推算和存储仍归 API，不为展示注册执行客户端。
详见 [期权操作与持仓推算](https://github.com/saoxia/SeeAlpha/blob/main/docs/OPERATIONS_API.md#期权操作与持仓推算2026-11-08)。
