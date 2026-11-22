# SeeAlpha Engine 代码导航

目标：从业务需求定位模块，而不是让目录结构复制一次业务流程。

| 要找的行为 | 首选位置 | 关联边界 |
| --- | --- | --- |
| MCP 协议与研究工具 | `engine/gateway/mcp/src/research.rs` | `engine/gateway/api/src/research_v1.rs` 复用授权与任务能力；现有 `/research/mcp` 路由不变 |
| HTTP 路由、权限和版本 | `engine/gateway/api/src/lib.rs`、`src/routes/` | `parity.rs`、`derivatives.rs`、`research_v1.rs` |
| Engine 启动与运行配置 | `engine/apps/trading_engine/src/main.rs`、`engine/runtime/src/config.rs` | `gateway/api` |
| Options 领域规范化 | `engine/connection/binance-options/src/lib.rs` | 不执行签名 REST 或交易；原生 EAPI 私有适配尚未提供 |
| Binance Spot / USD-M 原生工厂 | `engine/pipeline/sources/shared/nautilus/` | Nautilus 原生客户端工厂及离线路由；实盘命令和私有证据未接通 |
| 订单登记、重复请求、恢复隔离 | `engine/runtime/src/native_handoff.rs` | `engine/ledger`；仅持久化，未启用 Nautilus 原生提交 |
| 回测协调、研究快照 | `engine/runtime/src/research.rs`、`history.rs` | `gateway/api/src/research/`、`engine/backtest` |
| 策略授权与原生策略 | `engine/strategy/src/` | `engine/runtime/src/session.rs` |
| 研究授权与操作范围校验 | `engine/gateway/api/src/research/` | `engine/gateway/api/src/research_v1.rs`、`engine/gateway/mcp/src/research.rs` |
| 目标持仓、篮子、风险裁剪 | `engine/execution/src/` | `runtime/src/basket*.rs` |
| 账户、订单和成交持久化 | `engine/ledger/src/` | `engine/runtime/src/accounts.rs` |
| Binance 实时行情入口（实验性） | `engine/pipeline/sources/shared/nautilus/src/main.rs` | 由 `engine/runtime/src/sources.rs` 显式启动，独立编译 |
| 产品采集及存储基线 | `engine/pipeline/BASELINE.md` | Spot 原始成交、USD-M 聚合成交、Options 仅历史 K 线；记录未完成的自动维护闭环 |
| Binance 官方历史 ZIP | `engine/pipeline/sources/binance/archive/src/bin/` | `engine/pipeline/sources/shared/nautilus/src/bin/import_archive.rs` 原生 Catalog 导入 |
| 行情 K 线输入校验 | `engine/pipeline/sources/shared/market/src/pipeline.rs` | Decimal 价格、OHLCV、时间对齐与连续性验证在 Source 中进行；交易风险门禁仍由 Runtime 持有 |
| 历史 Catalog | `engine/pipeline/catalog/src/` | 接收标准化 InstrumentId/BarType，仅执行 Parquet 数据读写、文件范围及存储缺口检查；产品身份/价格规则归 Source |
| 独立撮合和验证回放 | `backtest/src/` | `engine/runtime/src/verified_replay.rs` |

持久化行情文件放在 Git 忽略的根级 `data/` 或外部数据卷，见 [数据目录约定](DATA_LAYOUT.md)。

详细的一级目录所有权及 Risk/Execution 规划见 [领域布局](DOMAIN_LAYOUT.md)。

表中 `src/routes/` 等局部路径是模块内的相对路径，完整 Rust 引擎源码均在 `engine/` 下。

## 组织原则

1. 顶层保留领域归属，Cargo crate 名称不因改目录而改变。
2. `lib.rs` 只应清晰表达模块入口与必要公共 API；体量大的 HTTP handler、测试及应用用例放入职责文件。
3. 同一业务操作跨不同边界是正常的：Runtime 协调、Ledger 记录事实、Venue 对接外部协议，不能因为名字相似就合并安全事务。
4. 不将测试从行为附近移到难以定位的单一大仓库测试目录：单元测试就近存放，跨 crate 场景保留在对应 `tests/`。
5. 不改变不可见的副作用：行情消费者不启动生产者；HTTP 读取不变成下单；未知订单只通过证据对账恢复。
6. 未实现的规划模块写在文档中，不建立空的 crate 假装存在。
