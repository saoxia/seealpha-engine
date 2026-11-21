# SeeAlpha Engine

SeeAlpha 的独立量化引擎。**默认运行进程**是 Rust 二进制 `seealpha-engine`。旧 Python Engine 的 `engine/` 业务模块已整体退役；Rust 作为唯一 Engine 服务进程。Python 策略宿主仍由 Rust Engine 作为受限子进程调用。

Platform 通过 HTTP v2 调用本引擎，请求和成功响应使用 `X-SeeAlpha-Engine-Contract-Version: 2`。`core/engine_contracts.py` 与 `core/trading_contracts.py` 必须与 [saoxia/SeeAlpha](https://github.com/saoxia/SeeAlpha) 逐字节一致。

## 目录

- Rust Cargo Workspace：交易业务直接位于 `market_data/`、`alpha/`、`portfolio/`、`accounting/`、`connectivity/` 和 `backtest/`；应用编排位于 `application/`，HTTP/MCP 接口位于 `interfaces/`，二进制入口位于 `apps/`。
- `python/strategy_host/`：一次请求一个进程，加载策略包并收回 `publish` 的目标持仓。
- `packages/`：示例策略。`ema_crossover` 只公布目标，不提交订单。
- `core/`：与 Platform 共享的契约模块。
- `docs/ARCHITECTURE.md`：运行路径和已退役的实盘行为。

行情读取不会隐式启动采集器。模拟会话把公开行情送进流水线，再交给策略宿主；差额记在 `venue=paper`。仓库包含部分实盘订单与账户签名查询实现，但 Rust 功能等价和交易所端到端验收尚未完成，不能据此启用实盘部署。

## 原生 Rust 策略模拟（开发模式）

纯 Rust EMA 示例已经可以通过现有纸面会话接口显式调用，**不需要安装 Python 或启动 Python 策略宿主**。示例请求需同时设置 `use_sample: true` 和 `native_sample: true`：

```json
{
  "symbol": "ETHUSDT",
  "interval": "1m",
  "product": "spot",
  "use_sample": true,
  "native_sample": true,
  "parameters": {
    "fast_period": 8,
    "slow_period": 21,
    "allocation": "1",
    "risk_limit": "1"
  }
}
```

发送至 `POST /tenants/{tenant_id}/accounts/{account_id}/paper/sessions`（原有认证与版本请求头不变）。该选项只对 development 租户开放，返回结果仍通过 Rust 组合净额、风险额度与模拟 Ledger；不提交真实交易所订单。未设置 `native_sample` 时，现有 Python 示例调用行为不变。

注意：**这不是整个 Engine 零 Python 迁移完成的声明**。生产策略包下发仍使用 Python 包格式和宿主；该增量提供的是一个明确隔离的纯 Rust 策略模拟路径，后续需要迁移发行协议、容器与测试。变更未完成 Cargo / Docker 实际验证前，不应部署到生产环境。

## Python 边界与退役范围

`engine/` 旧运行时、`core/market/` 旧市场模型、旧 option 投影、历史 Python Engine/feed/pipeline 测试和失效的 Legacy CI 已退役。对应的 Rust 能力位于 `market_data/`、`accounting/`、`application/`、`interfaces/` 等 Workspace 模块中，但不宣称与旧 Python 行为完全等价。旧代码仍可从 Git 历史查阅。

**仍使用 Python：** `python/strategy_host/` 和 `packages/ema_crossover/` 是受限策略执行边界；`core/engine_contracts.py` 与 `core/trading_contracts.py` 用于 Platform v2 共享契约和兼容测试；`tests/core/` 与 `tests/strategy/` 继续验证这些边界。仓库因此尚不是零 Python 依赖，不能把策略宿主删除当作 Rust 替换。历史互操作脚本因依赖已退役模块而移除，需采用原生 Rust 测试重新覆盖相关场景。

## Rust 集中迁移（功能等价尚未完成）

本轮已在 Rust 原生 Engine 中补齐一批账户状态、持久化行情配置/任务、策略读侧订阅、严格历史 K 线读取和已发行策略的本地回测路径，并接入独立 Binance 产品源的显式试验性管理。新数据源在完成 REST 衔接/时钟/历史证据前始终返回 `data_ready=false`，**不会**向策略提供已就绪的实盘行情保证。

旧 Python Engine 的 `engine/api/` 和 `engine/infrastructure/` 已删除，HTTP 由 `interfaces/api/http/` 中的 Axum 实现承担，任务持久化与状态管理由 `application/runtime/src/jobs.rs`、`application/runtime/src/data.rs`、`accounting/ledger/` 及 Rust HTTP 任务调度代码承担；旧 Python Engine 不再可执行。详细实现边界、未迁移功能和最终验收清单见 [Rust 集中迁移审计](docs/RUST_CUTOVER_AUDIT_20261121.md)。GitHub Actions 仍暂停，不应把当前 Rust 分支直接用于实盘。

## Rust 数据底座：对齐 Python Nautilus v2 Catalog

已增加 `market_data/catalog`，以原 Python `core/market/instruments.py`、`engine/market_data/storage/catalog.py`、`HistoricalMarketData` 的语义，直接从现有 Nautilus v2 Parquet 读取多类型历史数据；历史回测、K 线覆盖率与分页入口优先使用该目录，不再要求把已有历史数据复制到 Engine SQLite。

Rust 新增了 Parquet Bar 原子写入、源进程独立的交易所时钟、计算 Journal→Parquet 显式发布任务及全日删除预览。**现阶段数据源仍不宣称 READY**；Tick/L2/Greeks 的完整写入、交易所历史重叠修复、与 Nautilus 真实文件的互操作验收仍在迁移。如何调用、目录和质量约束详见 [Rust 数据底座迁移记录](docs/RUST_DATA_FOUNDATION_20261121.md)。

## 本地开发

已删除依赖退役 Engine 模块的遗留 Python Engine/feed/pipeline 测试；保留 Platform 契约与策略宿主测试，Rust Workspace 是运行时主要回归门禁。


```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
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
