# SeeAlpha 连续行情服务 `feed`

当前服务仅接入 Binance USD-M 永续合约公开行情，输出闭合分钟 OHLCV。
这是 SeeAlpha 多资产系统中的现有行情实现；其他市场、交易时段和行情类型仍需扩展与验收。

独立接通本地历史、REST 补齐和 WebSocket 实时分钟行情，为后续交易运行层提供
连续的已闭合 K 线及就绪状态。仅使用公开行情接口，不读取账户密钥、不下单，
不修改研究冻结源、历史 Catalog 或原有策略。

2026-10-27: Bar subscriptions and historical requests use a data-only native Binance LiveNode. Native trade events supply freshness heartbeats; the published/persisted contract remains closed-minute OHLCV. SocketStateChanged drives disconnect gating.

原代码包 `market_data` 已更名为 `feed`，启动入口相应变为
`python -m engine.feed.service`。已有持久化数据仍使用
`data/market_data/binance_um_1m/`，无需迁移或重新预热。

## 模块边界

Binance 接入已迁至 [`connectors/binance/`](../connectors/README.md)，复用 Nautilus 原生
适配器；本目录保留校时、消息校验、历史衔接、连续性、补洞编排、存储和交付控制。
公共分钟线类型和命名规则位于 [`core/`](../../core/README.md)。
`history.py` 通过 `engine.pipeline.catalog` 读取历史 Catalog，这是显式的存储桥接；
服务入口不再依赖离线管道的配置和命名模块。现有启动命令及数据目录保持不变。

## 本版范围

统一单位为 Binance USD-M 永续 **1 分钟 OHLCV**。唯一键为交易对和 UTC 开盘时间；
转换为 Nautilus Bar 时，事件时间沿用历史管道的收盘边界前 1ms。
策略可继续从同一条分钟流聚合 5m、15m、小时及日线。

本版不是逐笔行情或盘口层，不混合历史 `trades` 与实时 `aggTrades` 的编号，
不生成伪造 TradeTick，也不承诺逐笔成交数、主动买卖量等特征一致性。
现有指标缓存不自动切换；消费方从回放分钟线恢复自己的指标。

## 运行

使用冻结的本地环境，无需安装或升级依赖。默认 BTC/ETH、135 天分钟预热。
首次运行会读取已有 Catalog，再通过 REST 补齐其缺失部分；Catalog 为空也可补齐，
但首次请求和磁盘写入会更多。预热长度必须覆盖具体策略的最长状态窗口，135 天不是
适用于所有未来策略的自动保证。短预热参数仅适用于连通性验证。

```bash
.venv/bin/python -m engine.feed.service serve
.venv/bin/python -m engine.feed.service status
```

有界连通性验证使用独立目录，不与正式数据混用：

```bash
.venv/bin/python -m engine.feed.service serve \
  --root /tmp/nautilus-minute-smoke --warmup-minutes 5 --duration-seconds 90
```

配置项包括 `--symbols BTCUSDT ETHUSDT`、`--catalog`、`--root`。
同一数据目录的交易对清单固定；更换清单应使用新目录。重启保留原起点与所有缺口，
不会把起点推进到最新时间以假装完成补齐。增加预热长度若超出已保存历史，应建立
更早起点的新目录；缺少所需预热时不会进入 READY。

服务在前台运行，Ctrl+C 停止；本次实现不自动安装常驻服务。
`--duration-seconds` 限制采集主循环时长，初始化连接、网络超时和清理时间另计。

## 数据与就绪

默认目录 `data/market_data/binance_um_1m/`：

- `minutes.sqlite`：闭合分钟线、来源核验记录、连续检查点、缺口及冲突证据。
- `status.json`：原子更新的状态、各市场连续进度、实时消息时间、后台任务及最后错误。
- `writer.lock`：进程级排他写锁；第二个采集进程直接拒绝启动。

SQLite 使用 WAL 和 FULL 同步。数据和连续检查点在同一事务提交；重启重新检查
已提交数据的连续前缀。重复输入只增加来源核验，不重复新增分钟线；乱序到达先落盘，
前面的洞补齐后才推进连续检查点。未闭合分钟线只作为实时连接心跳，不保存为闭合行情。

启动顺序：校时 → 连接 WebSocket 并开始缓冲 → 分日读取 Catalog → REST 补洞和重叠核对
→ 消费方回放 → READY。REST 分页有固定上限，空页、内部分钟缺口、异常值均阻止就绪。
历史加载与 REST 请求期间仍接收实时消息；缓冲溢出触发降级和重新补齐。

| 状态 | 含义 |
|---|---|
| WARMING_UP | 启动后历史或消费方状态尚未准备完成 |
| READY | 所有配置市场的分钟覆盖、预热、实时心跳、时钟及来源校验均通过 |
| DEGRADED | 缺口、断线、过期行情、异常时钟、冲突或消费方错误；禁止新增风险 |
| STOPPED | 进程已正常停止 |

READY 仅代表本层的数据条件，**不代表账户对账、订单恢复或策略盈利条件已通过**。
断线、停更、重连后必须重新补齐及核对重叠；单凭收到一条新行情不会重新就绪。
连接正常而缺少某个市场消息，也会降级。默认实时消息最大陈旧时间 15 秒，分钟闭合
保留 2 秒交付宽限；服务器时钟以单调时间续算，校时有效期 120 秒。

`status` 命令会拒绝过期的 READY。文件是运维视图，不是交易授权；交易消费者应使用
进程内 `Feed.check()` 和每个 `Delivery.allow_new_risk`，不能缓存 JSON 中的 READY。

## 策略接入契约

`Feed` 接受 `on_delivery(Delivery)` 回调；`Delivery.bar.to_nautilus(instrument)`
转换为现有策略使用的标准 Bar。回调按每个市场的时间递增调用，分钟不重复。

- `allow_new_risk=False`：启动预热、重启恢复、断线补齐或过期数据，只更新指标和状态。
- `allow_new_risk=True`：就绪期间新到达、连续且及时的 WebSocket 闭合分钟线。
- 初次进入或恢复 READY 时，所有积压线一律回放；下一根及时的实时线才可能允许新增风险。
- 消费方抛出异常后锁定降级，需重启并恢复消费方状态，不能自动重试可能已产生副作用的调用。

**不能把回放回调直接接到现有策略会下单的 `on_bar()`。** 后续交易运行层需要把
“更新指标/状态”和“允许交易决策”明确分开，并在执行端再次检查门槛。本层未改写冻结策略，
也不保证外部下单副作用的跨进程 exactly-once；那属于订单幂等及账户恢复阶段。
撤单与风险退出不应由本层数据就绪门槛阻断。

## 日终核验与冲突

运行期间核对前一个完整 UTC 日：读取当日已有 Catalog，再对照官方 REST K 线，
保存来源核验记录，不重写历史 Catalog。新出现的迟到历史归档可通过
`Coordinator.audit_day()` 再核对；本层不自动下载逐笔 ZIP，也不将分钟数据伪装成逐笔归档。

同一分钟价格必须一致；成交量只允许 `max(1e-8, |volume| × 1e-12)` 的数值误差。
超出误差的来源差异会回滚整页，持久保存原值、新值、来源和时间，锁定 DEGRADED；
重启不会清除此冲突。不自动选一个来源覆盖已消费行情。需要查明原因后保留原库，
以独立目录和明确的新起点重建、重放，并重新验收。

持续停机跨过多个自然日时，分钟缺口仍全部补齐；自动日终核验只针对最近完整日，
不是全历史每日归档复审。真实无成交、停牌等导致缺分钟时同样暂停，不自行造零成交线。

## 验证

```bash
.venv/bin/python -m pytest tests/feed -q
.venv/bin/python -m pytest -q
```

覆盖启动交接、REST 分页与缺口、重复/乱序、重启检查点、双市场门槛、断线补齐不追单、
来源冲突持久化、消费方失败、时钟失效、限流退避、Catalog 桥接与 Nautilus 时间戳。
实际网络联通和长时间运行验收须独立记录，离线测试通过不能代替网络验收。

接口参考（2026-09-28 核对）：
[Binance USD-M REST 行情](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#klinecandlestick-data)、
[WebSocket K 线](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market#klinecandlestick-streams)。

## 管理端按交易对采集

`engine.feed.managed` 为 API 的固定启动入口。旧 `auto` 配置继续使用原分钟 Feed、Store、
Delivery 和 1m/5m/15m 聚合，60 分钟窗口不替代策略自己的预热检查。

新显式最小粒度使用 `pair_service.py` 和 `pair_store.py`，日志在原 managed 目录的
`<symbol>/<base_level>/collection.sqlite`，按最小粒度及额外周期保存，改粒度不删除旧日志。
实时采集通过 connector 的原始逐笔 `@trade` WebSocket；REST 用于校时，以及每次连接后的
一次近期最多 1,000 条成交核对。请求内部有限次重试耗尽后保留失败状态，不按定时器重复
补取；仅新的连接可再尝试。未覆盖的历史交由归档核验，不声明近期片段补齐了全部缺口。
不再定时轮询各周期 K 线。读写之间使用有界队列施加背压，不静默丢弃成交。
逐笔按成交编号去重，接受乱序和跳号，不检查编号连续性，也不因跳号创建缺口、补洞或降级。
已有编号缺口记录保留，但不再参与实时健康及派生计算判断；不伪造成交。

成交写入在同一 SQLite 事务内通过触发器登记受影响的秒桶；秒数据变化直接触发分钟及
所有更高周期重算，不再由分钟线驱动高周期。同批秒变化按目标窗口合并，输入先按原始 trade ID 去重，再覆盖受影响窗口，
不把成交量重复累加；目标写入成功才确认待办，重启恢复待办及最近工作集。
1s 长期保存在原采集库并公开展示；非 Tick 配置的临时逐笔仍只保留两分钟。
高周期聚合顺序读取秒线，不把整周/月秒线展开成列表。旧内部缓存按原保留标记清理，
已承诺保留的秒线和其他周期不自动删除。
未闭合 K 线仅存在内存和状态快照，由下一次源数据变化确认闭合，不制造无成交 K 线。
源成交冲突和已确认的交易所闭合来源冲突保留原值；本地派生值可随源数据变化覆盖重算。
版本重启后递增。月线按自然月、周线按 UTC 周一；缺失子周期与部分启动窗口不会标记完整。

所有新管理更新禁止新增交易风险，不改变原分钟 Feed 的就绪门禁。库存/预览只读访问日志，
缺失文件不会自动创建。期权管理端持续采集见下文，原分钟 Feed 门禁不变。详见
[数据管理 API](https://github.com/saoxia/SeeAlpha/blob/main/docs/DATA_MANAGEMENT_API.md#按交易对配置与分钟实时管理2026-10-29)。


实网原始流中价格或数量为零的事件按正常过滤项处理，不写入有效 Tick、不参与聚合，
不因此产生冲突。过滤回执复用原 SQLite 证据记录并写运行日志；旧版零值隔离证据按同一
规则识别并保留，不删除原记录。负值、非有限数值、结构、时间和身份冲突仍保留严格校验。
本规则仅取消管理端逐笔成交编号连续性检查；原分钟 Feed 的连续门禁和 K 线闭合边界不变。


## 独立计算与文件监控（2026-10-29）

显式策略由 `engine.feed.managed --role collection` 只接入并提交证据、登记源变化；
`--role calculation` 独立消费，保留 `combined` 作为已有 CLI 的默认兼容行为。
两者共享原 collection.sqlite，短写入与确认持 `journal.lock`，不因计算落后提前清理原始工作集。
计算进程从采集状态读取交易所确认水位，不用本机时钟擅自闭合实时 K 线。
历史变化通过原 `history.py` 桥转交 Pipeline 计算，成功后确认文件通知，不跨越存储边界。
计算状态在 `calculation/status.json`，API 内监控定期读取三个服务的状态并写本地快照，
均不写业务数据库。细节与切换步骤见[四个服务](https://github.com/saoxia/SeeAlpha/blob/main/docs/DATA_MANAGEMENT_API.md#四个服务与配置切换2026-10-29)。

历史重算在计算进程内异步消费文件通知，耗时的历史读取与聚合放在线程中执行；
实时 SQLite 待办继续按交易所确认水位处理，不等待一整段历史计算结束。
历史写入线程持有自己的文件许可，取消协程不提前释放仍在写入的保护。

历史 Tick 派生通过原 Pipeline 桥按小时读取、每次最多转换 1,000 行，先汇总 1s，
再直接供所有高周期共享；超出已确认覆盖的周/月窗口在读取前跳过。1s 与目标 K 线均保存，
原始来源、精度、闭合冲突和计算成功后才确认文件通知的规则保持。

Tick/秒来源的历史归档按 UTC 两日前边界调度：UTC 日期 D 仅处理 D-2 及以前，
不提前查询昨天或当天的归档。CHECKSUM 或 ZIP 尚未发布时，等待状态与 30 分钟后的
探测时间保存在原历史检查点；等待期间不重新执行整天核验，重启仍遵守该时间。
文件可用后沿原核验、下载和原子写入路径处理；其他网络/校验失败保留原异常重试语义。

历史核验比较已保存的 K 线，不再为核验重聚合 Tick。完成标记仍由原 Pipeline 历史服务
写入逐日审计文件；存在计算通知时先等待本计算进程成功确认，再复核保存结果。
一致或修复后复核通过的日期不再自动扫描；失败或冲突日期不会因通知已生成就被标记完成。


## 当前管理端 K 线周期（2026-11-08 更新）

显式 USD-M 配置统一生成 1m、3m、5m、15m、30m、1h、2h、4h、6h、8h、12h、1d、3d、1w、1M，
并增加由真实 Tick 聚合的本地 1s，共 16 个周期，不再自定义周期。
实时与 Tick 历史均为 Tick → 1s → 各高周期，秒线长期保存并在数据页展示；
仍核对原始源覆盖，启动残缺窗口不会标完整，迟到数据覆盖重算。
旧秒线数据、显式旧 CLI 与未完成待办的恢复保留兼容；原分钟 Feed/Delivery 门禁不变。
历史进程按官方 K 线分层定位差异，只补确认缺失的来源，仍经 `history.py` 文件通知桥交由
计算进程生成派生数据。来源及已有闭合派生冲突保留，详情见数据管理 API 最新说明。


交易页的本地 K 线展示通过数据管理 API 读取 `journal_chart` 保留证据及有效形成中状态；
查询使用只读 SQLite 连接，不创建库、不闭合或修复 K 线，不改变策略 Delivery / 就绪语义。


## 期权管理采集（2026-11-07）

管理端使用 `engine.feed.managed --options-config`，在 `options_service.py` 的一个共享进程中
动态管理所有期权。connector 每组至多 64 个合约，使用共享公开连接和既有原生解析
订阅闭合 1m/5m/15m K 线、真实 Tick 和前 10 档盘口快照（100ms）；
Tick 和盘口不再受最小 K 线粒度限制，动态订阅与退订不重连其他合约。
单合约 `--policy` 仍通过原公开 DataActor/LiveNode/DataClient 入口，保留兼容。
不配置执行客户端，不改变 USD-M Feed/Delivery 或交易就绪门禁。
有界队列满或来源冲突显式失败；采集批次经既有 `history.py` 桥由 Pipeline 保存来源及 Catalog，
不在 Feed 实现第二套行情存储。断线与重启范围保存在 managed 文件检查点，
重新订阅后恢复可用官方闭合 K 线及近期成交；Tick 始终不声明完整历史。
盘口精确价格、数量、交易所时间/更新编号及接收时间按批次原子保存到
`raw/options/<合约>/depth/<UTC日>/` 的 SHA256 JSON 来源快照；不覆盖为单个最新值，
不把盘口当成交或派生 K 线。Tick 仍保存原始来源并写入既有原生 Catalog。
盘口历史仅保存已收到的快照，断线缺口保留；当前数据页不提供盘口历史预览。
到期停止实时节点，历史进程继续按日补齐官方可获取范围，空来源或错误不推进检查点。

每个合约仍持有自己的 writer/access 锁及恢复文件，批量进程只持全局托管锁。
暂停或删除通过动态配置取消对应工作，等待仍在写入的线程结束后才释放访问锁。
历史参数调整不重启其他实时工作；损坏运行快照保留原工作并等待 API 重新发布。
实时来源继续接收，断线补取另限最多两个在途工作，排队期间不声明数据就绪。
