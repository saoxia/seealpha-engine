# SeeAlpha 行情 Pipeline

Pipeline 只分为 `sources/`（来源、产品类型、连接、采集及行情质量校验）和 `catalog/`（通用 Nautilus/Parquet 存储、查询及存储一致性检查）。`sources/` 按 Binance 产品类型组织；共享代码不会为每个产品重复复制。

| 位置 | 现有责任 | 当前状态 |
| --- | --- | --- |
| `sources/binance_spot/` | Spot 产品身份、Instrument ID、分层历史修复入口 | 由共享 Nautilus 进程以 `spot` 模式运行；独立于 USD-M 进程 |
| `sources/binance_futures_um/` | USD-M Futures 产品身份、聚合成交 REST 对齐与 Funding 数据 | 由共享 Nautilus 进程以 `futures_um` 模式运行；尚未支持 COIN-M |
| `sources/binance_option/` | EAPI 期权历史 K 线采集/Bar 落盘、合约识别及保留的实时消息解析 | 历史只允许 `bars`；实时 WebSocket **尚未接通** |
| `sources/shared/nautilus/` | Spot / USD-M 共用的独立 Rust NautilusTrader LiveNode 和原生历史 ZIP 导入程序 | 编译为同一二进制，但按产品启动独立进程；实验性，未通过 READY |
| `sources/binance/archive/` | 跨 Spot / USD-M 的 Binance 官方历史 ZIP 获取、来源校验与 Manifest | 现有 `seealpha-binance-market-data-source` crate |
| `sources/shared/market/` | 当前共用的公共市场模型、REST 客户端、K 线和时间校验工具；USD-M 近期成交解析由 binance_futures_um/agg_trades.rs 承担 | 现有 `seealpha-market` crate，公共 API 不变 |
| `catalog/` | 通用 Parquet 历史数据存储、读取、文件索引和缺口查询，不解析 Binance 产品 | 原生存储适配 |

## 连接生命周期

- 现货和 USD-M 目前通过 `engine/runtime/src/sources.rs` 按租户、产品分别启动原生 Nautilus 数据子进程；每个节点可以复用其内部连接订阅多个交易对。**不要求每个交易对一个 WebSocket**。
- `binance_option` 当前只存在公共 REST 与 WebSocket 消息解析函数，不表示已建立 Options WebSocket 或具备生产级实时订阅。
- `sources/shared/` 存放跨产品共用的代码，不形成一个混合 Spot / USD-M WebSocket 连接；运行时仍按产品隔离。
- 新目录不改变现有编译目标、启动参数、数据卷路径、恢复门禁及交易安全策略。

## 安全限制

Nautilus 节点仍是只读实验性采集器。REST/WS/ZIP 数据连续性、历史与实时交接、Catalog 一致性和 READY 均未通过生产验收；不得把收到行情或进程运行当作数据可用/允许开新风险。策略实时交付仍待实现。

行情数据不提交到此源码目录。参见 [数据目录约定](../../docs/architecture/DATA_LAYOUT.md)。


### USD-M Futures 历史数据自维护

`engine/pipeline/sources/binance_futures_um/repair.rs` 通过 `seealpha-market::futures_repair` 使用已有 Catalog 分层聚合和 Binance REST K 线进行 `1d → 4h → 1h → 15m → 5m → 1m` 异常定位。仅在定位到分钟缺失或 OHLCV 差异后，才通过数据任务调用官方 ZIP 下载与 Nautilus 导入；若实时节点持有写锁，则将导入保留为待处理任务。导入后 K 线重建仍需后续对接。


### Spot 历史数据自维护

`sources/binance_spot/repair.rs` 与 USD-M 复用 `sources/shared/market/src/repair.rs` 的分层比较逻辑。Data Job 动作 `repair_spot_day` 使用独立的 `spot` 产品和官方 `spot/daily/trades`（原始成交）归档；只有定位到分钟缺失或 OHLCV 差异后才发起 ZIP 下载与 Nautilus 导入。导入与实时写入互斥，导入后 K 线重算尚待完成。

### Options 历史数据约束

期权历史 `backfill` 只调用 `OptionsSource::history()` 的 `/eapi/v1/klines`，通过现有 Catalog 写入 Bars；拒绝 `exercise`、`trades`、`depth`、`mark` 等其他历史行情类型。公开行权记录不再通过行情任务写入；合约发现与账户结算证据独立处理。

## 已锁定的产品契约

[Pipeline 基线](BASELINE.md) 列出 Spot、USD-M Futures、Options 的冻结范围及对应验证。此次清理不改变上述产品链路或风险门禁；删除的只是已无调用方、与现行架构重复的旧模块。
