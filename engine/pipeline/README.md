# SeeAlpha 行情 Pipeline

Pipeline 只分为 `sources/`（来源、产品类型、连接与采集）和 `catalog/`（存储、查询及可信性检查）。`sources/` 按 Binance 产品类型组织；共享代码不会为每个产品重复复制。

| 位置 | 现有责任 | 当前状态 |
| --- | --- | --- |
| `sources/binance_spot/` | Spot 产品身份、Instrument ID 与原生数据客户端标识 | 由共享 Nautilus 进程以 `spot` 模式运行；独立于 USD-M 进程 |
| `sources/binance_futures_um/` | USD-M Futures 产品身份、聚合成交 REST 对齐与 Funding 数据 | 由共享 Nautilus 进程以 `futures_um` 模式运行；尚未支持 COIN-M |
| `sources/binance_option/` | EAPI 期权 REST、合约及公开流消息解析、行权数据 | 期权实时 WebSocket 客户端**尚未接通**，只有解析能力 |
| `sources/shared/nautilus/` | Spot / USD-M 共用的独立 Rust NautilusTrader LiveNode 和原生历史 ZIP 导入程序 | 编译为同一二进制，但按产品启动独立进程；实验性，未通过 READY |
| `sources/shared/archive/` | 跨 Spot / USD-M 的 Binance 官方历史 ZIP 获取、来源校验与 Manifest | 现有 `seealpha-binance-market-data-source` crate |
| `sources/shared/market/` | 当前共用的公共市场模型、REST 客户端、K 线和时间校验工具；USD-M 近期成交解析由 binance_futures_um/agg_trades.rs 承担 | 现有 `seealpha-market` crate，公共 API 不变 |
| `catalog/` | Parquet/SQLite 历史存储、读取、修复和派生缓存 | 当前兼容实现 |
| `catalog/integrity/` | 原 `seealpha-quality` 的数据校验、缺口与 READY 规则 | 保留现有功能；以后再按职责整理 |

## 连接生命周期

- 现货和 USD-M 目前通过 `engine/runtime/src/sources.rs` 按租户、产品分别启动原生 Nautilus 数据子进程；每个节点可以复用其内部连接订阅多个交易对。**不要求每个交易对一个 WebSocket**。
- `binance_option` 当前只存在公共 REST 与 WebSocket 消息解析函数，不表示已建立 Options WebSocket 或具备生产级实时订阅。
- `sources/shared/` 存放跨产品共用的代码，不形成一个混合 Spot / USD-M WebSocket 连接；运行时仍按产品隔离。
- 新目录不改变现有编译目标、启动参数、数据卷路径、恢复门禁及交易安全策略。

## 安全限制

Nautilus 节点仍是只读实验性采集器。REST/WS/ZIP 数据连续性、历史与实时交接、Catalog 一致性和 READY 均未通过生产验收；不得把收到行情或进程运行当作数据可用/允许开新风险。策略实时交付仍待实现。

行情数据不提交到此源码目录。参见 [数据目录约定](../../docs/architecture/DATA_LAYOUT.md)。


### USD-M Futures 历史数据自维护

`engine/pipeline/sources/binance_futures_um/repair.rs` 通过 `seealpha-market::futures_repair` 使用已有 Catalog 分层聚合和 Binance REST K 线进行 `1d → 4h → 1h → 15m → 5m → 1m` 异常定位。仅在定位到缺失分钟记录后，才通过数据任务调用官方 ZIP 下载与 Nautilus 导入；若实时节点持有写锁，则将导入保留为待处理任务。导入后 K 线重建仍需后续对接。
