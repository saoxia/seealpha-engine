# Engine 领域目录布局（2026-11-20）

本次为第一阶段代码组织重构，保持对外 HTTP v2 契约、NautilusTrader 版本、SQLite/Parquet 数据目录和订单幂等语义不变。

| 路径 | 所有权与职责 |
| --- | --- |
| `engine/api/` | HTTP 路由、独立研究 API、接口输入校验 |
| `engine/market_data/control/` | 持久化 DataPolicy、运行任务协调、访问租约 |
| `engine/market_data/sources/<venue>/<product>/` | 按交易所与产品隔离的数据源入口；分阶段迁入专有采集/计算/修复实现 |
| `engine/market_data/collection/` | 当前共享采集实现和兼容入口，迁移期间继续承载旧实现 |
| `engine/market_data/calculation/` | K 线及衍生特征计算；消费持久化来源 |
| `engine/market_data/repair/` | 历史自动扫描、归档下载、核验与补齐、Rust 转换 |
| `engine/market_data/storage/` | 原始证据日志、分钟库、Nautilus Catalog 存储适配 |
| `engine/market_data/access/` | 历史/实时数据只读服务与回放隔离 |
| `engine/market_data/quality/` | 数据质量检查与服务状态观察 |
| `engine/connectors/` | 交易所协议、Nautilus 适配器；不得反向依赖消费或采集编排 |
| `engine/strategies/` | 策略包检查、发行物加载、策略配置 |
| `engine/backtesting/` | 离线回测、模拟与独立子进程执行 |
| `engine/portfolio/` | 组合/篮子目标、调仓净额与对账逻辑 |
| `engine/trading/` | Engine 订单、账户运行事实、幂等与恢复 |
| `engine/research/` | 研究候选、只读快照、研究评估 |
| `engine/infrastructure/` | 进程管理、任务队列等底层基础设施 |

**控制不变量**：只有经授权的持久化行情配置决定采集、计算与历史修复任务；策略、回测、交易均为只读消费者，不能通过订阅或退订改变采集开关。

**兼容与验收**：`python -m engine` 不变。旧 `python -m engine.feed.managed`、`python -m engine.feed.service`、`python -m engine.pipeline.pair_data` 和 `python -m engine.pipeline.data_management`、`python -m engine.pipeline.prepare_data`、`python -m engine.pipeline.sync_binance_batch` 和 `python -m engine.pipeline.sync_binance_file` CLI 保留薄入口；新代码应使用 `engine.market_data` 目录。历史说明保留于 `docs/implementation/`。自动化回归测试必须运行，长时间实盘/真实行情运行验收另行安排。

**数据源迁移第一步（2026-11-20）**：`sources/binance/{perpetual,spot,options}/collection.py` 提供产品固定的共享采集入口；`collection/managed.py` 已改为调用它们。旧 `futures_service.py`、`pair_service.py`、`options_service.py` 保留实际实现，后续才分阶段迁移内部逻辑。此阶段不创建单独部署服务、不迁移存储、不改变任务控制权限。
## Binance 源码归属（完整迁移阶段）

- `engine/market_data/sources/binance/perpetual/`：`collection.py`、`calculation.py`、`repair.py`、`funding.py`、`funding_repair.py`
- `engine/market_data/sources/binance/spot/`：`collection.py`、`calculation.py`、`repair.py`
- `engine/market_data/sources/binance/options/`：`collection.py`、`repair.py`
- `engine/market_data/sources/binance/common/`：`batch.py`、`pair_collection.py`、`calculation.py`、`history.py`、`reconcile.py`、`convert_bars.py`、`convert_trades.py`、`sync_batch.py`、`sync_file.py`

旧导入路径保留为同一 Python 模块对象的别名，必要 CLI 保留原入口，避免部署配置、测试 monkeypatch 或上下游调用即时断裂。通用管理、存储、访问和质量模块不因来源拆分而复制。此次没有拆分成三个独立服务部署。

