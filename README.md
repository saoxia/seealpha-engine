# SeeAlpha Engine

SeeAlpha 独立量化引擎。主服务为 Rust 二进制 `seealpha-engine`。当前分支取消 Python 策略宿主和 Python 共享契约源码，仅保留 Rust 原生引擎实现。

> **不兼容变更 / 未测试**：原 Python ZIP 策略包、Python 目录策略和 NautilusTrader Python Strategy 不再执行。已有策略不会自动转换为 Rust；需要以当前支持的 Rust 内置策略清单重新下发。此分支按要求没有执行编译、测试、Docker 构建或端到端验收，不应据此判断生产就绪。真实交易所订单、衍生品和历史数据功能的完整等价性也未得到证明。

## 源码与工程结构

业务源码统一放在 [engine/](engine/)。`docker/`、`benchmarks/`、`docs/`、`.github/` 与 `engine/` 并列。Rust 测试放在各 crate 的 `src/` 或 `tests/`，不再保留空的根级 Python `tests/`。根目录 `Cargo.toml` 管理全部 14 个 Rust Workspace 成员。

### engine/ 内的业务领域与应用

- `engine/pipeline/`：完整行情服务（Nautilus 原生订阅、补充来源、数据完整性、实时和历史访问）
- `engine/strategy/`：Rust 内置策略、授权清单和目标持仓发布
- `engine/portfolio/`：目标净额和风险裁剪
- `engine/ledger/`：SQLite 账本
- `engine/connection/`：Binance Options 专属规范化接口（Spot/USD-M 原生工厂位于 `pipeline/sources/shared/nautilus/`）
- `engine/backtest/`：历史回放与回测
- `engine/runtime/`：Runtime、租户和交易会话编排
- `engine/gateway/`：对外 HTTP API、MCP 协议入口和研究授权校验（独立源码目录，同一 HTTP 服务）
- `engine/apps/`：Engine 二进制入口

`engine/risk/`（风险管理）和 `engine/execution/`（订单管理与执行）是规划领域，当前相关实现仍分布于 Portfolio、Runtime、Nautilus 与 Ledger；**尚无对应一级源码目录或独立 crate**，不会创建空模块。`benchmarks/`、`docker/`、`docs/`、`.github/` 保留工程职责。完整清单见 [一级业务领域布局](docs/architecture/DOMAIN_LAYOUT.md)。

本地持久化行情文件建议放在仓库根级 `data/`（已由 Git 忽略），生产使用外部数据卷；它们不属于 `engine/` 源码目录。详见 [数据目录约定](docs/architecture/DATA_LAYOUT.md)。

## 部署

```bash
docker build -f docker/engine.Dockerfile -t seealpha-engine:native .
docker run --rm -p 127.0.0.1:8766:8766 \
  -e ENGINE_HOST=0.0.0.0 -e ENGINE_AUTH_MODE=token \
  -e ENGINE_TOKEN='replace-with-at-least-32-character-random-token' \
  seealpha-engine:native
```

默认镜像没有 Python。已删除可选的 `docker/engine-python.Dockerfile`。

## Rust 原生 EMA

内置 Rust EMA 仅支持 `ETHUSDT`，输出目标持仓而非直接提交订单。开发租户可以通过纸面会话请求显式选择：

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

将该请求发送到 `POST /tenants/{tenant_id}/accounts/{account_id}/paper/sessions`，带入现有认证和 `X-SeeAlpha-Engine-Contract-Version: 2` 请求头。该开发示例不向交易所下单。

已发行策略现在要求原生清单字节（JSON，非 ZIP）：

```json
{"format":1,"runtime":"rust-builtin","name":"ema_crossover","version":"1","entry_point":"native:ema_crossover"}
```

清单必须通过 SHA-256 与可信发行记录匹配，同时满足租户、账户、发行用途和策略元数据一致性检查。执行路径调用预编译的 Rust EMA，**不允许**直接上传并运行任意 Rust 代码。当前并未提供将任意 Python 策略自动翻译成 Rust 的功能。

## 本地开发

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
cargo run --locked --release -p seealpha-engine
```

上面的命令仅供后续自主验证；本次改动没有执行。

状态库位于 `$ENGINE_STATE/book.sqlite`（默认 `.local/state/engine/book.sqlite`）。公开行情不需要交易所密钥。研究签名密钥必须不同于 `ENGINE_TOKEN`。旧的 Python 代码可从 Git 历史恢复。

迁移历史与功能限制参见 `docs/RUST_CUTOVER_AUDIT_20261121.md`，该文档包含历史状态记录，不代表当前功能验收结论。

## License

项目自有代码采用 [Apache License 2.0](LICENSE)。
