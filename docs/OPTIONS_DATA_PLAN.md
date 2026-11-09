# Binance 期权数据接入

2026-10-28：用户明确批准方案及冻结来源更新后实施。期权仅接入公开只读行情，
没有新增账户、交易执行、数据库迁移或后台采集服务。

## 已实现

- 产品 → 标的交易对 → 具体期权合约。合约可按到期日、CALL/PUT 和行权价筛选，
  无默认交易对。目录来自 Binance EAPI，保留合约单位、报价/结算币种、精度与保证金参数。
- 使用 Nautilus 公开 `CryptoOption` 模型承载支持小数数量的期权合约，保留原始
  `underlying_type` / `contract_type`。例如 `BTC-261225-85000-C.BINANCE`，
  不套用永续的 `-PERP` 标识。未知挂牌时间以 `activation_time_known=False` 标注。
- 近期 Tick：保存官方 `/eapi/v1/trades` 返回的最多 500 笔，而非某个任意历史时段。
  返回的成交可能较早，按实际 UTC 成交日期保存，并在任务结果展示实际起止时间。
  重复编号必须与已有值一致；时间倒退或跨已有文件的补写拒绝覆盖。
- 历史 K 线：直接从 `/eapi/v1/klines` 分页获取 1m / 5m / 15m 数据，
  仅处理已完成的完整 UTC 日，保留官方零成交量 K 线，不填造缺口或反推 Tick。
- 复用现有任务、并发锁、子进程、取消/恢复、WebSocket 进度和 native Catalog。
  支持本地读取、预览、来源一致性校验及带文件指纹确认的删除。
  已到期的本地合约仍可查看、校验和删除，这些操作不依赖在线目录。

## 数据边界

近期接口无法证明任意历史 Tick 完整。本轮不提供期权持续实时采集，也不从不完整
近期 Tick 物化历史 K 线。官方返回空结果时不会生成空数据、虚假覆盖或成功的完整性声明。
目录或行情网络失败时显示错误，不回退预设合约或样例数据。

API 的 `product=options, intervals=[]` 准备任务表示**当前发起一次近期获取**；
请求 start/end 作为提交时效检查，不是交易所支持的历史 Tick 查询窗口。
历史 K 线的 start/end 才是实际查询范围。本地预览/校验/删除始终按实际事件时间过滤。

## 存储与恢复

来源快照位于配置 raw 目录的 `options/<合约>/<级别>/<UTC 日>/<SHA256>.json`。
快照包含官方响应、对应 native 合约定义、请求范围和真实获取时间；
写入后不覆盖，通过文件名 SHA256 校验内容和合约/日期归属。
保存来源之后才将完整的临时 Parquet 文件通过硬链接原子安装到现有 Catalog，
不覆盖已有文件。中断留下的来源可以重试安装；冲突会停止并保留证据。

历史 native 数据与现有 Pipeline 保持相同的事件时间索引约定（ts_init = ts_event），
真实获取时间独立留在来源快照，不将离线记录当实时信号。
期权删除只处理所选合约、完整 UTC 日期内的文件；可选清除来源，默认保留以供恢复。
合约定义、其他期权、永续数据、账户和研究报告保留。

## 冻结来源授权

用户本轮明确批准扩展 `pipeline/models.py` 的 options 市场与产品校验，并登记到
`tests/baselines/source_updates.json`。原始 previous_sha256、历史源码 ZIP、研究清单和
lint 基线保持原字节；既有永续请求的默认市场与命名保持兼容。

## 验证

现有检查与新增测试覆盖 CALL/PUT、到期过滤、单位/精度、分页和空结果、
原生 Catalog 读写、近期重复成交、来源冲突/损坏、写入中断重试、跨产品身份与删除隔离，
并回归 USDT / USDC 永续。真实公开接口已核对目录、近期成交和 K 线格式；
隔离目录的真实任务保存了 100 笔 Tick 和 1440 根一分钟 K 线，二者均通过本地来源一致性校验。
还验证了存储中途消失时拒绝重建目录。

2026-10-29：另按用户授权增加了可注册到 Nautilus 节点的期权行情适配器与只读账户查询。
离线解析已复用 connector 的相同精度转换；此处数据管理任务和持续 Feed 边界保持不变。
实时订阅、账户查询和验收限制见 [期权适配器](BINANCE_OPTIONS_ADAPTER.md)。

## 官方依据

当前安装的 NautilusTrader 公开 Binance 品种 loader 不接受 OPTIONS；本接入在既有
connector 职责内使用固定公开 REST 端点补充，不调用上游私有客户端或复制引擎。

- [NautilusTrader Binance 集成](https://nautilustrader.io/docs/latest/integrations/binance/)
- [Binance 期权市场数据接口](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/rest-api/market-data)
