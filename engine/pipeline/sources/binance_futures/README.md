# Binance USD-M Futures

此目录承载 Binance USDⓈ-M 永续合约**产品专属**逻辑，使用现有的 NautilusTrader、REST、历史归档和 Catalog 模块；不建立一套自研 WebSocket/Parquet 体系，也不把 Spot 的原始成交混入 USD-M 聚合成交数据。

## 现有数据处理链路

| 路径 | 入口 | 数据与用途 | 当前可信性 |
| --- | --- | --- | --- |
| 实时 | `native.rs` + `../shared/nautilus/` | `futures_um` 独立 Nautilus LiveNode；`@aggTrade`，Instrument 为 `BTCUSDT-PERP.BINANCE` 类 | 启动先取最多 1,000 条近期成交用于重叠；不足以证明无缺口 |
| REST 快照 | `agg_trades.rs` + `../shared/market/` | `GET /fapi/v1/aggTrades` 的 `a/p/q/f/l/T/m` 聚合成交；与 WS/ZIP 保持同类 ID | 只代表有限近期快照，不能证明完整历史 |
| REST 历史 K 线 | `../shared/market/` | `GET /fapi/v1/klines`，按交易所时钟确认收盘，精确区间校验后写入 Catalog | 验证单个请求窗口；不等同于持续实盘 READY |
| 官方历史成交 | `../shared/archive/` + `../shared/nautilus/src/bin/import_archive.rs` | Binance `futures/um/daily/aggTrades` ZIP，校验 SHA-256，验证数据与 Instrument 精度，原生 TradeTick Parquet | 成功导入不等于与 WS/REST 无缝衔接 |
| 资金费率 | `funding.rs` | `GET /fapi/v1/fundingRate`，解析已结算的公共 Funding；解析 `markPriceUpdate` 的预估资金费率 | 预估费率不等于真实结算；公开记录不等于账户付款 |
| 历史读取 | `../../catalog/` | Parquet、派生 K 线缓存、范围查询/缺口检查 | 需要单独维护不同产品、来源和记录粒度的身份 |

目前支持 USD-M，**不包含 COIN-M**。行情源进程与 Spot 分开管理；在同一 USD-M 进程内部，可订阅多个交易对，不要求每个符号独立 WebSocket。

## 既有处理逻辑与迁移差异

早期 Python 版 `engine/feed/futures_service.py` 曾管理两个 Futures 订阅通道（原始 `@trade` 和 `@markPrice@1s`）、按合约独立的队列/Journal/Checkpoint 和恢复状态。`engine/pipeline/pair_repair.py` 实现过从日线到分钟线逐级比较、只对异常分支修复的逻辑。它们在 Rust 迁移时退出生产代码，不能误认为当前已经存在对应的自动恢复服务。

新的 Nautilus TradeTick 采用**聚合成交**，不能把旧版 `@trade` 的原始 Trade ID 直接用于证明 `@aggTrade` 序列完整。此次把 REST 快照由 USD-M 原始 `trades` 改为 `aggTrades`，并加入严格字段与 ID 语义校验；Spot 的 REST `trades` 不变。

## 后续需实现的闭环（当前尚未完成）

1. 对原生 Nautilus 实时流、有限 REST 衔接窗口和官方 ZIP 同时保留来源证明，按聚合成交 ID/时间及 Instrument 对齐；重叠冲突必须拒绝自动判定为 READY。
2. 基于 Catalog 覆盖范围识别历史缺口，按优先级从 REST / 官方 ZIP 补充；缺失时间段内没有成交时，不能仅凭无记录判定历史连续。
3. 将可信状态和过期状态交给 Runtime READY/风险门禁；在验证完成前始终 `data_ready=false`、`allow_new_risk=false`。
4. 如果需要 `markPrice@1s` 的持续实时估计，应优先确认 Nautilus 原生订阅能力；不能把 `funding.rs` 的消息解析函数视为已经启动的 WebSocket。

原则：**来源相互补足，但不得因为多源存在就默认数据准确。** 当前保持只读、实验性，未开启实盘订单，也不恢复已删除的 Python 采集器。

## 按日层级修复（Rust 数据任务）

使用现有 Data Job 接口提交 `repair_futures_day`，参数含 `product=futures_um`、`symbol`、`day` 和经过 Instrument 核对的 `price_precision/size_precision`。该任务实现旧版逐级扫描顺序 `1d → 4h → 1h → 15m → 5m → 1m`：仅对上一层异常范围进行更细周期的比较，每个下钻层最多读取一整天官方收盘 K 线（总计不超过 6 次 REST 历史请求）。

仅识别到**缺失分钟 K 线且无现存分钟数据冲突**时，任务才自动创建并执行 `archive_fetch`，使用 Binance 官方 `futures/um/daily/aggTrades` 及 SHA256；然后创建 `archive_import`，复用既有 Nautilus 原生导入程序。已有实时 Catalog 写者运行时不强行导入，而将导入子任务留在 `queued` 并在结果中返回 `archive_import_job_id`，后续需在安全停止 LiveNode 后运行该任务。

ZIP 导入成功只是 Tick 数据写入，不代表已自动重新生成分钟/小时/日 K 线；任务返回 `needs_recalculation=true`。已有分钟记录与交易所 K 线不一致时不自动覆盖，返回 `minute_conflicts_require_review`。上层不一致但下层完全一致时返回 `parent_bar_recalculation_required`。这是历史数据维护流程，不改变实盘 READY、安全门禁及交易授权。
