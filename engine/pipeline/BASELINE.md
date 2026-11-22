# Pipeline 三产品基线（2026-11-22）

此处“锁定”表示 **保持现有数据契约与职责边界，清理无调用方的重复实现**，并非宣称完成实盘生产验收。需要变更时以明确的新需求、对应测试和单独提交进行。

| 产品 | 获取 | 落盘/标识 | 历史维护 |
| --- | --- | --- | --- |
| `binance_spot` | 独立 Nautilus Spot `TradeTick`（原始 `trade`）；REST 近期数据与收盘 K 线 | 按租户/产品隔离的 Feather → 原生 Parquet；`SYMBOL.BINANCE` | 共用分层扫描 `1d→4h→1h→15m→5m→1m`；按需使用 `binance/archive` 的 `spot/daily/trades` ZIP；由 Nautilus 原生导入器写入 |
| `binance_futures_um` | 独立 Nautilus USD-M 聚合成交 `TradeTick`；REST `aggTrades` / 收盘 K 线；既有 Funding | 按租户/产品隔离的 Feather → 原生 Parquet；`SYMBOL-PERP.BINANCE` | 同一分层扫描；按需使用 `binance/archive` 的 `futures/um/daily/aggTrades` ZIP；与 Spot 不混用成交 ID |
| `binance_option` | Binance EAPI `/eapi/v1/klines` 历史 OHLCV 与合约元数据 | 只允许 `product=options,data_kind=bars` 的历史 `Bar`；`1m/5m/15m` | 不获取历史逐笔成交、L2/BookTicker 或公开行权事件，不采用 Spot/USD-M ZIP 成交补齐 |

## 代码所有权

- `sources/binance_spot/repair.rs` 和 `sources/binance_futures_um/repair.rs` 只固定产品类型，复用 `sources/shared/market/src/repair.rs` 算法。
- `sources/binance/archive/` 仅包含 Binance 官方 ZIP，跨 Binance Spot / USD-M 复用一份，不是跨交易所公共抽象。
- `sources/shared/nautilus/` 提供原生只读 LiveNode 和 TradeTick ZIP 导入；`catalog/` 管理历史 Bar/Tick 数据与读写；`runtime/` 只协调 Data Job、租户、进程及写入互斥，不持有产品修复算法。
- 期权仅保留 K 线历史抓取及暂未接通的实时消息解析；签名账户成交与行权结算归 `engine/connection/binance-options/`、`engine/ledger/`，不能把账户证据误作历史行情。
- 行情输入校验已归属 `sources/shared/market/src/pipeline.rs`；旧 READY、monitor、session 模块不再提供第二套状态管理。Runtime 现有的 `data_ready=false` 和 `allow_new_risk=false` 门禁不能因清理而解除。

## 稳定性约束和回归

以下已经是本次冻结范围的回归条件：`binance/archive/src/archive_fetch.rs` 中 Spot/UM 归档身份测试，`sources/shared/market/src/repair.rs` 中产品隔离及分层扫描测试，`sources/binance_option/options.rs` 与 `runtime/src/data.rs` 中仅 Bar 历史回填测试，`sources/shared/nautilus/` 中产品 Instrument 区分和原生导入测试，以及 `catalog/` 既有读写测试。CI 至少需通过 workspace check、test、Clippy、Release 和 Docker build。

## 未宣称完成的工作

此前识别的共用增量合并、导入后自动重算 K 线、后台周期巡检以及实时与历史数据接续尚未完成；Spot/UM 仍是实验性只读采集，Options WebSocket 实时节点尚未接通。本次锁定不主动扩展这些能力，也不解除交易安全门禁。

已清理的代码：孤立的旧质量监控/READY 会话模块、无调用方的期权公开行权 REST 历史采集器、期权 REST 近期成交/BookTicker 快照解析及闲置 MemorySource 模拟源。历史 Git 提交仍可供审计。
