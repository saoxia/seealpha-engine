# SeeAlpha Engine

SeeAlpha 的独立量化引擎。进程是 Rust 二进制 `seealpha-engine`。Python 只保留策略包和加载它们的宿主。

Platform 通过 HTTP v2 调用本引擎，请求和成功响应使用 `X-SeeAlpha-Engine-Contract-Version: 2`。`core/engine_contracts.py` 与 `core/trading_contracts.py` 必须与 [saoxia/SeeAlpha](https://github.com/saoxia/SeeAlpha) 逐字节一致。

## 目录

- `crates/`：Cargo workspace。行情、流水线、组合净额、账本、策略宿主、研究授权和 HTTP 各自一箱。
- `python/strategy_host/`：一次请求一个进程，加载策略包并收回 `publish` 的目标持仓。
- `packages/`：示例策略。`ema_crossover` 只公布目标，不提交订单。
- `core/`：与 Platform 共享的契约模块。
- `docs/ARCHITECTURE.md`：运行路径和已退役的实盘行为。

行情读取不会启动采集器。模拟会话把公开行情送进流水线，再交给策略宿主；差额记在 `venue=paper`。实盘下单、连接登记和账户快照返回 409，不保存密钥，也不调用带签名的交易所接口。

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
