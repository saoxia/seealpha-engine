# 原生能力改造记录

完成日期：2026-10-27（北京时间）。引擎为 NautilusTrader 2.0.0rc7。
本次按用户要求落实原生能力评估中的全部类别，包含有行为差异的条件性项目。具体实现如下。

| 类别 | 实现与范围 |
|---|---|
| 转换后端发现 | 默认使用本机 engine/pipeline/rust_converter/target/release 下的 Rust 构建产物；直接运行项目 Python 即可发现，支持环境变量显式指定路径或 off，不依赖数据存储位置。 |
| Catalog 区间 | 委托原生 get_intervals；不再在该查询中自行解析文件名。项目发布命名和指纹校验仍使用自己的文件工具。 |
| Bar / TradeTick 转换 | DataFrame 列式规范化为 Arrow，原生 process_record_batch_bytes 创建模型对象；移除逐行 Python 构造。浮点列先明确量化，再经十进制字符串转换，防止 3000.10 被截成 3000.09。 |
| 公共回测 | 原生 Arrow C Stream 扫描，每批最多 50,000 条；run(streaming=True)、clear_data、最终 end。开始闭、结束开，保留跨批次定时器、订单和持仓状态。 |
| 基础指标 | SMA、BOLL、ATR/TR、Wilder、EMA、ROC、效率比和极值使用原生指标。保留预热、缺口重置及原来的 ATR 平滑类型；共享趋势模型取代 16 组重复类定义。可执行研究控制副本也使用共享基础实现。 |
| 撤单 | 当前策略的批量撤单改用 cancel_all_orders(strategy_only=True)，多策略隔离测试保留。 |
| 开仓边界 | 当前执行层移除手写交易所最小金额/数量预检查，交由原生 RiskEngine 拒单；测试实际检查 DENIED 事件且数量没有放大。离线研究的旧成交合规审计谓词仍保留，不再用于当前订单执行。 |
| 严格聚合 | 价格极值改由原生 DonchianChannel 维护，项目薄层负责 UTC 分桶、首尾身份及连续输入门控。**没有直接改为 INTERNAL 订阅**：rc7 Python 未导出独立 TimeBarAggregator，而直接切换订阅会改变同步收盘回调和缺口语义。此方案复用原生计算，同时保留“不完整周期不参与信号”的规则。 |
| 自定义特征 | 使用原生 customdataclass、类型注册和 write_custom_data；原生 Catalog 按时间过滤，Arrow 批次先选列再汇总。撤掉 SimpleNamespace 仿制数据对象和全文件 to_pylist 后过滤的读取路径。 |
| 保护单 | 固定保护的 mean_reversion_short_v2_5min 使用 bracket + OTO/OUO + manage_contingent_orders，由原生负责子单激活、数量联动和同组撤单；保留超时、保护失效、晚到成交等业务退出规则。复杂加仓/多目标策略继续使用原生订单与改单操作表达各自目标，不强行套成单一三腿 bracket。 |
| 行情接入 | 独立行情服务使用原生 LiveNode、BinanceDataClientFactory 和 DataActor，实时 Bar 和历史 request_bars 均走原生适配器；订阅原生 SocketStateChanged，任一已知端点断开时关闭就绪门控。数据节点不配置执行客户端。 |
| 原始归档重复解析 | Rust 首次解析同时输出原始 quote_quantity 补充 Parquet；Python 回退也在同次解析写补充数据。特征生成只扫描已提交的原生行情和补充列，按 trade_id 对齐，避免再解压 CSV，也不从舍入后的 price×quantity 推测原始成交额。 |
| 研究回测报价与统计 | 原先按天加载和累计 marks 的研究入口也采用原生有界批次。盯市权益、费用压力及缺口统计逐批折叠，只保留日/年汇总；保持已有现金流定义。 |

## 保留的项目语义

原生化不删除策略信号、周期风险承诺、敞口/费用预算、数据补洞、来源冲突与只允许已闭合分钟参与交易等业务规则。`_config.py` 保留领域参数约定；`_strategy.py` 的 rc7 生命周期兼容仍有实际触发场景。原生接口负责通用运行机制，项目层只表达这些规则。

Binance 服务器时钟探测仍使用标准库 HTTP，服务只允许该入口请求公共 time；它用于独立时钟不确定性校验。当前 rc7 Python 没有导出对应公共 HTTP 时钟客户端。K 线 REST、WebSocket 协议解析、订阅、重连和限流由原生适配器处理，websockets Python 包仅保留在开发依赖中用于本地模拟服务器。

## 数据布局与已有缓存

行情和状态继续位于外置盘的无版本后缀目录：

- 行情：`data/catalog/data/bars`、`data/catalog/data/trades`。
- 原始成交额补充：`data/catalog/supplements/<InstrumentId>/data/custom/TradeQuote/`。
- 原生特征子 Catalog：`data/catalog/features/<BarType>/data/custom/BarFeatures/`。
- 特征续算和来源指纹：与日 Parquet 同名的 `.metadata.json`。

补充数据与 canonical TradeTick 的行数、时间范围及成交 ID 对齐。修复缺失 ticks 时验证并复用相同的补充文件；强制重建仅替换请求日。特征 Parquet 和元数据均先暂存，发布错误会回滚到此前文件。

转换器版本为 4，特征格式版本为 2。新格式增加了无法从普通 TradeTick 恢复的原始 quote 信息；已有旧转换结果若缺少该补充，需要针对相应日期显式 `--force` 重建。仅重建特征不能凭空恢复原始 quote_quantity。未改写历史研究快照，也未下载大规模行情。

## 验证

- 正式入口 `python -m pytest -q`：源码/历史归档核验、Ruff 和全量测试通过。
- Rust release 构建及 3 项单元测试通过。
- 原生 Binance 适配器连接本地模拟 HTTP/WebSocket，验证合约加载、实时 Bar、历史请求、自动重连、SocketState 门控及正常退出。
- 原生 Arrow 分批与单次回放对照：成交、定时器、停止回调相同，最终 end 仅执行一次；批次大小与结束时间排除规则有独立测试。
- 特征测试覆盖原始 CSV 不被二次解析、原始成交额保留、跨日续算、缺口预热、列读取、幂等恢复、写入失败及元数据发布失败回滚。
- 固定保护策略覆盖止损、止盈、跳空、超时、保护失效和回测结束退出；历史收益未据此宣称已完整复现。

Web 项目检查的源码核验、测试数量和日志路径见 `logs/web/checks/latest.json`；
直接运行 `python -m pytest -q` 时结果显示在终端。

## 实际替换后的局部性能

同一机器、3 次预热运行取中位数，比较改造前项目适配层与当前项目适配层：

| 项目 | 样本 | 改造前 | 改造后 | 吞吐倍率 |
|---|---:|---:|---:|---:|
| RollingMean / SMA | 100,000 | 30.82 ms | 7.68 ms | 4.01x |
| RollingBollinger | 100,000 | 131.43 ms | 14.28 ms | 9.20x |
| BarDataWrangler | 30,000 | 30.24 ms | 11.81 ms | 2.56x |

Bar 值逐项一致；指标最大浮点差约 4.8e-12。此处包含当前适配层开销，因此不同于最初直接调用原生接口的跑分。没有把局部倍率外推成整套回测、整个归档下载或所有特征计算的提速比例。

原始结果见 [性能记录](validation/native_adoption_benchmarks.json)。
