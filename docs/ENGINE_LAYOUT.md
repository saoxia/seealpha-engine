> **历史快照（2026-11-20，非当前目录或启动说明）。** 本文的 `engine/`、`crates/` 与 `python -m engine` 路径属于迁移前状态；当前仓库采用根目录 Rust Cargo workspace，旧 `engine/` 已退役。当前布局和启动方式请以 [README](../README.md) 与 [ARCHITECTURE](ARCHITECTURE.md) 为准。以下保留作为迁移审计记录，不应执行其中的旧命令。

# Engine 领域目录布局（2026-11-20）

当前代码在 `crates/`。本页保留迁出前的 Python 目录对照。运行结构见 [ARCHITECTURE.md](ARCHITECTURE.md)。HTTP v2 契约文件没有改动。行情门禁、滚动合成、删除快照和特征预热在 `crates/seealpha-feed`。旧 `tests/feed` 与 `tests/pipeline` 的对照见 [TEST_MAP_FEED_PIPELINE.md](TEST_MAP_FEED_PIPELINE.md)。

本次为第一阶段代码组织重构，保持对外 HTTP v2 契约、NautilusTrader 版本、SQLite/Parquet 数据目录和订单幂等语义不变。

| 路径 | 所有权与职责 |
| --- | --- |
| `engine/api/` | HTTP 路由、独立研究 API、接口输入校验 |
| `engine/market_data/control/` | 持久化 DataPolicy、运行任务协调、访问租约 |
| `engine/market_data/sources/{binance_perpetual,binance_spot,binance_options}/` | Binance 各市场的采集、计算、历史维护入口及产品专有实现 |
| `engine/market_data/sources/binance_shared/` | Binance 产品间复用的共享批量采集、逐笔、计算、历史核心 |
| `engine/market_data/collection/` | 通用分钟行情及 legacy 兼容导入和 CLI |
| `engine/market_data/calculation/` | K 线及衍生特征计算；消费持久化来源 |
| `engine/market_data/repair/` | 历史自动扫描、归档下载、核验与补齐、Rust 转换 |
| `engine/market_data/storage/` | 原始证据日志、分钟库、Nautilus Catalog 存储适配 |
| `engine/market_data/access/` | 历史/实时数据只读服务与回放隔离 |
| `engine/market_data/quality/` | 数据质量检查与服务状态观察 |
| `engine/connectors/` | 交易所协议、Nautilus 适配器；不得反向依赖消费或采集编排 |
| `engine/strategies/` | 策略包检查、发行物加载、策略配置 |
| `engine/backtesting/` | 离线回测、模拟与独立子进程执行 |
| `engine/execution/` | 组合/篮子目标、调仓净额与对账逻辑 |
| `engine/trading/` | Engine 订单、账户运行事实、幂等与恢复 |
| `engine/research/` | 研究候选、只读快照、研究评估 |
| `engine/infrastructure/` | 进程管理、任务队列等底层基础设施 |

**控制不变量**：只有经授权的持久化行情配置决定采集、计算与历史修复任务；策略、回测、交易均为只读消费者，不能通过订阅或退订改变采集开关。

**兼容与验收**：`python -m engine` 不变。旧 `python -m engine.feed.managed`、`python -m engine.feed.service`、`python -m engine.pipeline.pair_data` 和 `python -m engine.pipeline.data_management`、`python -m engine.pipeline.prepare_data`、`python -m engine.pipeline.sync_binance_batch` 和 `python -m engine.pipeline.sync_binance_file` CLI 保留薄入口；新代码应使用 `engine.market_data` 目录。历史说明保留于 `docs/implementation/`。自动化回归测试必须运行，长时间实盘/真实行情运行验收另行安排。

**数据源迁移状态（2026-11-20）**：已把现有 Binance 核心实现迁至 `sources/`，其中永续、现货有独立采集/计算/修复入口，期权采集与历史证据维护代码由其专属目录持有；共享算法留在 `sources/binance_shared/`。原模块仍以兼容别名转发至同一实现对象，不引入第二套状态。现阶段不变更物理部署拓扑、数据路径、锁或 HTTP 主契约；需通过离线 CI 与后续真实行情长时间验证。
