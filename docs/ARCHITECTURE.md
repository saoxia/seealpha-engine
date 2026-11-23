# SeeAlpha Engine 当前架构

> 本页描述当前 Rust 工作区的源码归属。历史 Python→Rust 迁移审计可能提到已经删除的模块，不代表现行能力。

## 调用链

```text
engine/apps/trading_engine       # 唯一 Engine HTTP 二进制
    └─ engine/gateway/api        # HTTP v2、Research v1（数据集/快照）
         ├─ engine/runtime      # 策略发行、账户 Ledger、只读策略预览
         │    ├─ engine/strategy
         │    └─ engine/ledger
         └─ engine/pipeline/catalog

engine/pipeline/sources          # 公共行情、归档、Nautilus 独立数据能力
engine/connection               # Binance Options 专有规范化
engine/execution                # 目标持仓、组合净额和风险裁剪
```

以上是逻辑概览，准确的编译依赖以各 crate 的 `Cargo.toml` 为准。账户事实和交易所成交必须基于可信证据；接收请求不等于实际成交。

## 研究与回测的区别

- **行情与研究数据**：Pipeline 负责历史记录、Catalog 与实时/历史数据读取；Research HTTP 保留 `GET /research/v1/datasets`。快照接口目前返回明确的 501（没有生成或下载 Parquet）。
- **研究授权**：`engine/gateway/api/src/research/` 验证签名和数据集/快照权限；可选 MCP 目前只暴露只读 `list_datasets`。
- **回测**：旧 `engine/backtest/`、模拟撮合、目标仓位历史回放、资金费/期权回测及同步/异步回测任务已经删除。Runtime 不再宣称 `backtest` 可执行，当前不存在回测服务和结果 API。
- **策略**：`engine/strategy/` 持有受控 Rust 原生策略及发行检查；`engine/runtime/src/session.rs` 只计算目标预览，不创建模拟成交。

用户授权历史数据并不意味着可以自动启动采集或伪造交易事实。未来回测从新需求和接口契约重新设计，不恢复已删除的旧实现作为兼容层。

## 目录与数据

- `engine/`：业务模块、网关、Runtime、可执行程序。
- `docker/`：容器构建与 PostgreSQL 启动。
- `benchmarks/rust/`：当前仅含 net、aggregate、klines 基准。
- `docs/`：当前结构说明与历史迁移审计。
- `data/` 或外部数据卷：持久化行情与 Catalog 文件，不属于源码；本次删除回测不清理实际历史行情数据。

详见 [一级领域布局](architecture/DOMAIN_LAYOUT.md)、[代码导航](architecture/MODULE_MAP.md) 和 [数据目录](architecture/DATA_LAYOUT.md)。

## 验证命令

```bash
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
docker build -f docker/engine.Dockerfile -t seealpha-engine:native .
```
