# SeeAlpha 行情服务（pipeline）

`engine/pipeline/` 统一放置行情订阅、采集、数据模型、校验、补齐、存储接口与实时/历史消费相关代码。它是**市场数据服务的代码边界**，不承载交易所私有订单、资金账本、组合交易决策或策略 Alpha 信号。

| 位置 | 现有 Rust crate / 职责 | 当前状态 |
| --- | --- | --- |
| `nautilus/` | NautilusTrader v2.0.0rc9 Spot/USD-M 只读实时节点、Feather→Parquet 和原生历史 ZIP 导入 | 独立 Cargo 项目，被根 Workspace 排除；显式实验开关，未通过 READY |
| `sources/binance/` | Binance 官方历史 ZIP 下载及采集策略校验 | `seealpha-binance-market-data-source` |
| `sources/market/` | 公共 REST 行情、Options、Funding、Exercise 和已有市场模型 | `seealpha-market`，Spot/USD-M 通用 REST 能力仍有待去重 |
| `integrity/` | K线/成交合法性、缺口质量检查和状态监测 | `seealpha-quality` |
| `access/feed/` | 现有 Feed 会话、缓冲、聚合、门禁 | `seealpha-feed`；与 Nautilus 重叠部分尚待独立验证与精简 |
| `access/catalog/` | 现有历史 Catalog 读写、缺口修复与安全删除预览 | `seealpha-catalog`；迁向 Nautilus 原生 Catalog 的适配尚待验证 |

根 Workspace 内现有五个行情 crate **名称不变**，`nautilus/` 继续独立编译、独立进程运行。这里的 `access/` 是当前 Feed/Catalog 的物理归属，不代表额外新建一层行情分发实现；`integrity/` 目前只承载原质量 crate，未来是否迁入修复逻辑需独立审查。

## 服务关系

- `engine/runtime/src/sources.rs`：显式启动、停止和监测 Nautilus 数据子进程。
- `engine/runtime/src/data.rs`：租户授权的策略行情读侧声明；**当前还不是多个独立策略进程的实时分发实现**。
- `engine/pipeline/nautilus/src/main.rs`：真实 Binance 实时 TradeTick 订阅与原生 Streaming 配置。
- `engine/pipeline/access/catalog/`：现存 Runtime 依赖的历史数据接口；不能因重新组织目录直接删除。

数据文件**不存放于** `engine/pipeline/`。本地建议仓库根级 `data/`，生产由受管挂载卷和绝对路径指定；参见 [数据目录约定](../../docs/architecture/DATA_LAYOUT.md)。

本次重构只移动目录并同步构建、文档路径，**不变更** Rust 公共 crate 名、HTTP 协议、数据格式、账户/订单保护及生产 READY 规则。后续移除重复代码需要另行依赖审计和等价性验证。
