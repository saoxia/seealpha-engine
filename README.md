# SeeAlpha Engine

SeeAlpha 的独立量化引擎。**默认运行进程**是 Rust 二进制 `seealpha-engine`。迁移期间已恢复旧 Python Engine 作为**独立、显式启用的兼容运行时**；Python 策略宿主仍由 Rust Engine 单独调用。

Platform 通过 HTTP v2 调用本引擎，请求和成功响应使用 `X-SeeAlpha-Engine-Contract-Version: 2`。`core/engine_contracts.py` 与 `core/trading_contracts.py` 必须与 [saoxia/SeeAlpha](https://github.com/saoxia/SeeAlpha) 逐字节一致。

## 目录

- Rust Cargo Workspace：交易业务直接位于 `market_data/`、`alpha/`、`portfolio/`、`accounting/`、`connectivity/` 和 `backtest/`；应用编排位于 `application/`，HTTP/MCP 接口位于 `interfaces/`，二进制入口位于 `apps/`。
- `python/strategy_host/`：一次请求一个进程，加载策略包并收回 `publish` 的目标持仓。
- `packages/`：示例策略。`ema_crossover` 只公布目标，不提交订单。
- `core/`：与 Platform 共享的契约模块。
- `docs/ARCHITECTURE.md`：运行路径和已退役的实盘行为。

行情读取不会隐式启动采集器。模拟会话把公开行情送进流水线，再交给策略宿主；差额记在 `venue=paper`。仓库包含部分实盘订单与账户签名查询实现，但 Rust 功能等价和交易所端到端验收尚未完成，不能据此启用实盘部署。

## 尚未迁移完成的 Python 功能

从 [`f18e48e`](https://github.com/saoxia/seealpha-engine/tree/f18e48e995becc0662df288058be6059c91c45b0) 恢复了被提前删除的旧 Python 引擎、测试及锁定依赖。**Rust 默认容器不包含旧 Python Engine**；如需使用旧能力，单独构建 `docker/engine-legacy.Dockerfile` 并使用独立端口、数据卷和状态目录。不要把两个运行时连接到同一可写账本或采集任务。

恢复步骤、风险隔离、启动方式及逐功能替换标准见 [Python Engine 恢复与迁移规则](docs/LEGACY_PYTHON_RECOVERY.md)。完整 Rust 功能等价尚未通过验收，不应删除仍承担旧功能的 Python 模块。

## Rust 集中迁移（功能等价尚未完成）

本轮已在 Rust 原生 Engine 中补齐一批账户状态、持久化行情配置/任务、策略读侧订阅、严格历史 K 线读取和已发行策略的本地回测路径，并接入独立 Binance 产品源的显式试验性管理。新数据源在完成 REST 衔接/时钟/历史证据前始终返回 `data_ready=false`，**不会**向策略提供已就绪的实盘行情保证。

Python Engine 仍作为独立兼容运行时保留。详细实现边界、未迁移功能和最终验收清单见 [Rust 集中迁移审计](docs/RUST_CUTOVER_AUDIT_20261121.md)。GitHub Actions 仍暂停，不应把当前 Rust 分支直接用于实盘。

## Rust 数据底座：对齐 Python Nautilus v2 Catalog

已增加 `market_data/catalog`，以原 Python `core/market/instruments.py`、`engine/market_data/storage/catalog.py`、`HistoricalMarketData` 的语义，直接从现有 Nautilus v2 Parquet 读取多类型历史数据；历史回测、K 线覆盖率与分页入口优先使用该目录，不再要求把已有历史数据复制到 Engine SQLite。

Rust 新增了 Parquet Bar 原子写入、源进程独立的交易所时钟、计算 Journal→Parquet 显式发布任务及全日删除预览。**现阶段数据源仍不宣称 READY**；Tick/L2/Greeks 的完整写入、交易所历史重叠修复、与 Nautilus 真实文件的互操作验收仍在迁移。如何调用、目录和质量约束详见 [Rust 数据底座迁移记录](docs/RUST_DATA_FOUNDATION_20261121.md)。

## 本地开发

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
python3 -m unittest discover -s tests -t . -p '*_test.py' -v
```

热路径对照见 [docs/BENCHMARKS.md](docs/BENCHMARKS.md)。

## 启动

```bash
cargo run --locked --release -p seealpha-engine
```

开发默认监听 `127.0.0.1:8766`，`ENGINE_AUTH_MODE=none` 时只能绑定回环地址。Platform 托管容器使用 `ENGINE_AUTH_MODE=token` 和至少 32 字符的 `ENGINE_TOKEN`。状态库是 `$ENGINE_STATE/book.sqlite`（默认 `.local/state/engine/book.sqlite`）。

公开行情不需要交易所密钥。用 Binance 现货或 USD-M 的公开 kline、成交和深度跑示例策略：

```bash
ENGINE_STATE=/tmp/seealpha-smoke ENGINE_AUTH_MODE=none \
  cargo run --locked --release -p seealpha-engine -- smoke
```

该命令只读公开接口，纸面记账，`exchange_orders` 为 0。

## 镜像

```bash
docker build -f docker/engine.Dockerfile -t seealpha-engine:dev .
```

镜像包含引擎二进制、策略宿主、示例包和两份契约文件，不包含 Platform，也不包含已退役的 Python 引擎。

## HTTP

未带令牌访问受保护路由返回 401，契约版本不是 `2` 返回 426，固定租户引擎拒绝其他租户返回 403。主要纸面入口是 `POST /tenants/{tenant_id}/accounts/{account_id}/paper/sessions`（开发租户可设 `use_sample: true`）。`GET .../backtests/latest` 在没有 Nautilus 目录回测时返回 404，正文含 `还没有回测`，供 Platform 客户端把它当成“尚无结果”。

研究签名密钥与 `ENGINE_TOKEN` 必须不同。设置 `ENGINE_RESEARCH_SIGNING_KEY` 后只挂载 `GET /research/v1/datasets`，并校验研究授权。

## License

项目自有代码采用 [Apache License 2.0](LICENSE)。示例策略包可能在目录内另有 MIT 许可证。贡献、安全报告和第三方声明见仓库中的对应文件。
