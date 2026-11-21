# SeeAlpha 行情服务（pipeline）

`engine/pipeline/` 统一放置行情订阅、采集、数据模型、校验、补齐、存储接口与实时/历史消费相关代码。它是**市场数据服务的代码边界**，不承载交易所私有订单、资金账本、组合交易决策或策略 Alpha 信号。

| 位置 | 现有 Rust crate / 职责 | 当前状态 |
| --- | --- | --- |
| `nautilus/` | NautilusTrader v2.0.0rc9 Spot/USD-M 只读实时节点、Feather→Parquet 和原生历史 ZIP 导入 | 独立 Cargo 项目，被根 Workspace 排除；显式实验开关，未通过 READY |
| `sources/binance/` | Binance 官方历史 ZIP 下载及采集策略校验 | `seealpha-binance-market-data-source` |
| `sources/market/` | 公共 REST 行情、Options、Funding、Exercise 和已有市场模型 | `seealpha-market`，Spot/USD-M 通用 REST 能力仍有待去重 |
| `integrity/` | K线/成交质量、来源证明、READY 门禁及实时交付安全规则 | `seealpha-quality`；保留原 Feed 的独有安全证明和单元测试 |
| `access/catalog/` | 现有历史 Catalog 读写、缺口修复、安全删除预览和派生 SQLite K线缓存 | `seealpha-catalog`；Native Catalog 读侧替代尚待验证 |

根 Workspace 内保留四个行情 crate（已取消无真实订阅调用的 `seealpha-feed`），`nautilus/` 继续独立编译、独立进程运行。`access/` 目前只保留 Catalog 兼容入口与被 Runtime 使用的派生缓存。旧 `hub`、模拟归档、重复 K线聚合、手写指标、影子数据集和队列均没有生产消费者，已删除；这一删减不代表策略已经具备跨进程实时推送。`integrity/` 保留数据完整性、来源证明与独立的 READY 安全测试，后续仍需接入可信协调器。

## 服务关系

- `engine/runtime/src/sources.rs`：显式启动、停止和监测 Nautilus 数据子进程。
- `engine/runtime/src/data.rs`：租户授权的策略行情读侧声明；**当前还不是多个独立策略进程的实时分发实现**。
- `engine/pipeline/nautilus/src/main.rs`：真实 Binance 实时 TradeTick 订阅与原生 Streaming 配置。
- `engine/pipeline/access/catalog/`：Runtime 依赖的历史数据 API 与现存派生 SQLite 缓存；没有修改既有数据库路径或格式。
- `engine/pipeline/integrity/readiness.rs` 与 `session.rs`：来源证据与 READY 安全规则；当前不自动将实验 Nautilus 节点设置为 READY。

数据文件**不存放于** `engine/pipeline/`。本地建议仓库根级 `data/`，生产由受管挂载卷和绝对路径指定；参见 [数据目录约定](../../docs/architecture/DATA_LAYOUT.md)。

本次第二阶段清理取消没有生产调用的旧 `seealpha-feed` crate，移走唯一的 Runtime 依赖，并保留安全证明规则。除这一内部 crate 的删除外，未更改 HTTP 协议、运行中数据库路径、交易安全门禁与生产 READY 条件。**注意**：新的 Nautilus 实时订阅节点仍未向独立策略进程交付直播数据；数据完整性认证仍未接线和生产验收。
