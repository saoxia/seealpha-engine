# 原生能力复用评估

本文是原生化改造前的评估记录；后续落实情况见 [原生能力改造记录](NATIVE_CAPABILITY_ADOPTION.md)。文中引用的 `strategy_python/` 路径已在 2026-11-08 随该目录删除，只保留为当时的审阅记录。

日期：2026-10-26。基准：本机实际安装的 NautilusTrader **2.0.0rc7**。
本轮进行了源码审阅、已安装 Python 接口核对、原生引擎小样本行为对照和合成数据微基准。未改动交易逻辑；未做多年行情的完整性能压测。网上文档仅作设计参考，接口存在性以本机安装包及实际调用为准。

Implementation completed on 2026-10-27: [native adoption and acceptance](NATIVE_CAPABILITY_ADOPTION.md). The review below records the pre-change findings.

## 结论

项目有明确的重复实现，且部分是升级时新增的适配层，仍有继续原生化的空间。优先级最高的是：**统一流式回测入口、Arrow 批量转换、基础指标原生化**。其次是原生 Catalog 查询、策略限定批量撤单。完整周期校验、风险承诺预算、行情补洞与冲突检测属于项目语义，不能因为原生存在相似接口就删除。

## 1. 常规回测全量加载：优先级高

位置：[数据加载](/Users/lingyu/Documents/ChatGPT/SeeAlpha/strategy_python/_backtesting.py:198)、[引擎装载](/Users/lingyu/Documents/ChatGPT/SeeAlpha/strategy_python/_backtesting.py:289)。

当前先把整个请求区间转换成 Python Bar 列表，再用列表推导筛选时间，随后一次性 add_data/run。多年分钟线、秒线、逐笔数据会使常驻内存随数据总量增长；数据存到外置硬盘并不能解决加载时的内存问题。这是结构性判断，尚未测量真实大数据峰值 RSS。

rc7 已提供 BacktestRunConfig.chunk_size、BacktestNode，以及 BacktestEngine.run(streaming=True)、clear_data()、end()。建议将有界批次加载收敛到公共回测入口，确保全局时间排序、跨批次状态连续、仅最终 end、边界不重复和报告统计一致。组合配置和研究输出较复杂时，可先保留低层引擎，用原生 streaming 接口，而不强制全部改为 BacktestNode。

项目[研究入口](/Users/lingyu/Documents/ChatGPT/SeeAlpha/strategy_python/_research/four_strategy_20260914/streaming.py:111)已经采用流式引擎，但批次按 7/180 天划分，而且仍累积 marks；不能把它直接当成全流程内存有界的实现。应复用它的生命周期处理，再按行数/字节限制批次和报告缓冲。

## 2. Python 逐行 Bar/TradeTick 转换：重复且实测较慢

位置：[两个自写 Wrangler](/Users/lingyu/Documents/ChatGPT/SeeAlpha/pipeline/catalog.py:124)。

当前每行调用 Python 的 Bar/TradeTick、Price、Quantity 构造器，并通过 float 中转价格数量。旧 Cython/DataFrame 接口移除后，项目补写了这层循环，但 rc7 实际仍有原生 BarDataWrangler 和 TradeTickDataWrangler，入口已改为 process_record_batch_bytes。

建议保留项目的 CSV 校验、字段规范化和时间定义，将已规范化数据按原生 Decimal128/UTC 纳秒 schema 批量编码为 Arrow IPC，交给原生解码。避免额外的二进制浮点往返；精度一致性需覆盖真实价格/数量边界。

本机 30,000 根 Bar 对照：Python 循环 **30.19 ms**；原生路径连同 DataFrame→Arrow 转型及 IPC 序列化 **4.30 ms**，约 **7.0 倍**。全部输出相等。该样本使用可精确表示的固定 OHLCV；不包含 CSV 解压解析，也没有测 TradeTick 的倍率。

重要范围：已启用的 Rust 归档转换器本身采用流式写 Parquet，不经过这段逐行 Python 路径。因此不能宣称整个下载转换链路都会提高 7 倍。

## 3. 基础滚动指标：重复、复制多、已有明显计算收益

位置：[RollingMean/RollingBollinger](/Users/lingyu/Documents/ChatGPT/SeeAlpha/strategy_python/mean_reversion_long_5min/indicators.py:7)、[趋势指标](/Users/lingyu/Documents/ChatGPT/SeeAlpha/strategy_python/trend_following_long_5min/indicators.py:27)。

当前 SMA 反复扫描窗口，布林带反复求和并计算总体方差；均线交叉还会复制 deque。rc7 原生提供 SimpleMovingAverage、BollingerBands、AverageTrueRange 等，并支持注册到策略自动更新。

排除历史报告、历史研究及研究副本后，仍发现 RollingMean 和 RollingBollinger 各 **15 处类定义**；MaDeviationModel、HourlyTrendModel、MaCrossModel 各 **16 处类定义**。这些并非已经证明字节完全一致，但说明修改和核对成本很高。

本机 100,000 次更新、3 次运行中位数：

| 基础计算 | 当前 Python | 原生（含 Python 属性读取） | 局部倍率 |
|---|---:|---:|---:|
| SMA，窗口 99 | 30.33 ms | 6.19 ms | 4.90× |
| 布林带，窗口 20、倍数 2 | 128.36 ms | 12.90 ms | 9.95× |

此次输入下预热输出时机一致，最大数值差约 4.8e-12 / 4.1e-12；并非位级一致。阈值附近仍可能改变交易信号，必须做逐根特征与逐笔成交回归。

建议只原生化基础计算，将“历史最大偏离、小时趋势、恢复条件、周末过滤”等领域规则保留在薄层中。ATR 必须显式选择与现有实现一致的均线类型、首个 TR 处理及缺口重置行为；本轮未测 ATR 等价性。不要把含额外逻辑的 MaDeviationModel 整体替换为一个 BollingerBands。

## 4. Catalog 区间查询和自定义数据：可收缩适配层

位置：[区间查询](/Users/lingyu/Documents/ChatGPT/SeeAlpha/pipeline/catalog.py:95)、[自定义数据读取](/Users/lingyu/Documents/ChatGPT/SeeAlpha/pipeline/catalog.py:105)。

- get_intervals 自行 glob 和解析 Parquet 文件名；原生已经有 get_intervals、query_files、get_missing_intervals_for_request。本轮两段合成数据的区间结果一致。建议直接委托原生，减少对文件命名布局的耦合，不将“文件覆盖区间”误认为“区间内没有数据缺口”。尚未证明原生区间查询一定更快。
- custom_data 当前递归读取全部文件、to_pylist 后才筛选时间，再包成 SimpleNamespace。范围越小、历史越长，读取放大越明显。原生存在 query_custom_data、Arrow 查询/流接口；rc7 也实际提供 model.custom.customdataclass 与 register_custom_data_class，本轮注册并写读一个自定义特征对象成功。
- 现有 BarFeatures 尚需 schema/注册适配才能直接使用原生持久化；不能只替换函数名称。若只是列式研究表，直接使用 PyArrow Dataset 的列投影、过滤和 batch scanner 也合理，无须强制变成引擎事件。

调用范围需区分：custom_data 主要是查询便利接口；[read_bar_features](/Users/lingyu/Documents/ChatGPT/SeeAlpha/pipeline/bar_features.py:439)已经按日、按列读取，较前者合理，但仍汇总所有请求日的 DataFrame 后 concat，超长区间可进一步分批。

## 5. 手写“仅撤本策略订单”：rc7 已有等价方向

位置：[撤单循环](/Users/lingyu/Documents/ChatGPT/SeeAlpha/strategy_python/trend_following_long_5min/execution.py:115)。

代码先取出本策略订单，再循环过滤并 cancel_order；注释认为批量撤单可能影响其他策略。rc7 的 cancel_all_orders 已提供 **strategy_only=True**（默认也是 True），另有 cancel_orders 和 modify_orders。

本轮仅在独立测试进程中将该函数替换为原生策略限定撤单，已有“停止多头不撤空头订单、不平空头仓位”集成测试通过。可优先统一此类公共操作；多腿中仅撤指定订单集合时仍应用显式 ID 列表。性能收益尚未测量，已确认的收益主要是减少执行层代码和过时假设。

## 6. 最小名义金额检查：原先保留理由已过时，但不宜无条件删除

位置：[开仓边界检查](/Users/lingyu/Documents/ChatGPT/SeeAlpha/strategy_python/_execution/entry_constraints.py:1)。

文件开头声称原生 margin 风控会在 instrument notional 检查前返回。针对当前 rc7 的原生保证金回测，提交 **2 USDT** 限价单，合约最小名义金额为 **10 USDT**，原生直接返回 **NOTIONAL_BELOW_MINIMUM / DENIED**。所以“原生完全不检查”这一依据已不成立。

建议区分：原生风控负责最终拒单；策略前置过滤负责避免反复发送必然被拒的开仓意图。若该函数仅重复最终风控，可收缩；若用于调整状态机、拒绝放大已批准风险量，仍有业务价值。需补齐市价、无行情价格、反向合约、reduce-only、精度、最小/最大数量等矩阵后再决定删除。本轮只证明上述限价保证金场景。

FixedRiskSizer 与项目风险定仓也有基础公式重叠，但它不能直接代表[按权重扣减未释放承诺的共享预算](/Users/lingyu/Documents/ChatGPT/SeeAlpha/strategy_python/_risk/shared_budget.py:7)。

## 7. 自写 K 线聚合：可复用计算，必须保留严格完整性语义

位置：[CompletedBarAggregator](/Users/lingyu/Documents/ChatGPT/SeeAlpha/strategy_python/_indicator/bars.py:31)。

原生支持 INTERNAL 与 composite BarType，例如 5-MINUTE-LAST-INTERNAL@1-MINUTE-EXTERNAL。多个策略可订阅同一聚合流，减少每个实例重复聚合的工作。

实际对照（2026-08-01 UTC 起，11 根合成分钟线，时间戳为收盘前 1 ms）：完整输入时两者的五分钟结束边界一致；删掉第三分钟后，项目不输出第一个不完整五分钟，而原生仍输出一个成交量为 4 的五分钟 Bar。原生配置 time_bars_build_with_no_updates=False 只处理无更新情形，并不等于验证每一分钟完整。

因此建议“原生聚合 + 统一的连续性检查”，而不是删除 CompletedBarAggregator 后直接订阅。还需验证首尾不完整周期、事件触发时刻、日线 UTC、跨批次时钟和缺口后的指标重置。当前自写完整性保护具有明确价值。

## 8. 手写止盈/止损联动：适合局部改用原生订单组合

位置：[保护单同步](/Users/lingyu/Documents/ChatGPT/SeeAlpha/strategy_python/mean_reversion_short_v2_5min/execution.py:67)。

项目自行维护 entry/stop/target ID、成交数量、另一保护单撤销、修改拒绝后的退出。rc7 原生有 OrderFactory.bracket、OCO/OUO、submit_order_list、manage_contingent_orders，能覆盖“单笔入场 + 固定止损 + 固定止盈”的机械联动。

本轮核实接口存在，但未证明该策略与 bracket 完全等价。当前仍包含部分成交、保护失效后市价退出、晚到成交、12 小时退出等额外行为；其他策略还会加仓、分段止盈和动态移动保护。建议先选择最简单的固定保护策略做原生组合订单对照，不能直接把所有执行状态机删掉。回测联动支持与交易所实盘支持也须分别验证。

## 9. 自建行情客户端：传输重复，数据质量层具有独立价值

位置：[REST/WebSocket](../engine/connectors/binance/transport.py)、[Feed](../engine/feed/feed.py)、[Store](../engine/feed/store.py)。

rc7 已有 BinanceDataClientConfig / Factory 和 LiveNode 接入能力，策略可通过原生请求和订阅获得行情。长期实盘集成时，复用原生适配器可以减少自维护的消息解析、订阅和重连代码。

但当前服务是独立、持久化的闭合分钟行情层，额外实现了冲突拒绝、重启游标、历史与实时重叠核对、补洞、时钟不确定性和允许新风险的就绪控制。没有证据证明原生 Cache/自动重连覆盖了这些语义。通用 WebSocketClient 不在 rc7 的 Python 导出中，也不能因 Rust 内部有网络客户端就直接 import 使用。

建议将原生适配器作为可替换的数据来源，质量和就绪判定留在项目中；当前不应为减少代码量而整体移除此服务。

## 另外两个直接影响性能的工程点

1. **优化后端存在但可能没被使用。** [find_rust_converter](/Users/lingyu/Documents/ChatGPT/SeeAlpha/pipeline/rust_trade_converter.py:49)未设置环境变量时查找仓库内 target/release，而实际产物在外置硬盘。本轮未激活 shell 时返回 None，但外置盘二进制存在；此时会回退 Python。应让 Python 入口读取统一存储配置或明确报告所选后端，避免性能依赖调用者是否 source activate.sh。这属于项目配置问题，不是 Nautilus 缺少能力。
2. **首次处理原始归档会重复读取。** [prepare_data](/Users/lingyu/Documents/ChatGPT/SeeAlpha/pipeline/prepare_data.py:133)先转换行情，然后生成特征；特征缓存未命中时，[prepare_bar_features](/Users/lingyu/Documents/ChatGPT/SeeAlpha/pipeline/bar_features.py:426)再次从 ZIP 汇总成交量和指标输入。可考虑一次解压输出标准行情及最小中间统计，再计算特征。有效特征缓存命中时已跳过重读，因此不能说每次都重复解析。主动买卖量等逐笔特征不能仅从 OHLCV 恢复，优化时不能丢失这些输入。

## 应继续保留或暂缓替换的部分

- Rust 日归档转换器：已有流式优势，负责 Binance ZIP、校验、顺序、聚合和发布；不是把一个原生函数简单重写了一遍。可评估复用原生 schema/编码，不能据此断言删掉更快。
- `_config.py`：原生配置持有运行字段，项目层补充领域参数声明、严格参数名及序列化。需限制其规模，但当前没有证据表明原生基类完整覆盖这些项目约定，也不是热路径。
- `_strategy.py`：针对当前 rc7 stop 借用问题的生命周期处理，上一轮已有触发场景。属于版本限制的局部兼容；应随引擎修复移除，而不是为“纯原生”现在删掉。
- 原生 generate_*_report 已在使用；中文展示、按方向归因、项目质量报告可继续自定义。平仓收益回撤、现金余额回撤和逐时盯市权益回撤不是同一口径，不能直接换一个 MaxDrawdown 类就认为等价。
- 信号、加仓规则、周期风险承诺、源数据校验与审计：都是本项目需要表达的规则，不属于通用原生基础设施的重复。

## 建议实施次序与验收

1. 统一转换器发现路径和后端可见性；Catalog 区间委托；批量撤单原生化。改动较小，先用现有隔离/区间测试验证。
2. 公共回测入口分批：同时检查批次内存、报告缓存、跨批次成交与定时器一致性。对此项目即将下载的大规模数据，优先级高于追求单个指标跑分。
3. Arrow Wrangler 和基础指标：按类逐个替换，保留相同预热、时间边界、精度和重置规则，做特征序列及成交路径回归。
4. 最后再改聚合、保护单和行情来源等语义较重的架构。先做小样本等价试验，再逐步切换。

局部基准不能外推为整套策略的提速比例。整体性能结论需要实际规模的输入、逐阶段耗时和峰值内存数据。

## 证据

- [微基准结果](validation/native_capability_benchmarks.json)：当前机器、引擎版本、样本数量、耗时与误差。
- [行为对照结果](validation/native_capability_behavior.json)：原生最小名义金额拒单、策略限定撤单、Catalog 区间、自定义数据往返、缺口聚合及转换器发现路径。
- 一次性探针脚本与临时运行日志已清理；评估结果保留在上述 `validation/` 文件中，未将探针作为生产代码接入。
- 已安装接口：`.venv/lib/python3.14/site-packages/nautilus_trader/{persistence,indicators,backtest,trading,common,risk,adapters/binance}/__init__.pyi`。
- 官方参考：[数据与 Arrow Wrangler](https://nautilustrader.io/docs/nightly/concepts/data/)、[回测流式 API](https://nautilustrader.io/docs/nightly/concepts/backtesting/apis-and-runs/)、[聚合概念](https://nautilustrader.io/docs/latest/concepts/data/)。nightly 文档可能领先于发行版，本报告列为可用的接口均已核对本机 rc7。
