# SeeAlpha Engine 当前架构

> 2026-11-23：退役仅服务内置 EMA 示例的 `engine/strategy/` crate、策略下发及预览 HTTP 路由，并移除未接入实际交易的独立 `engine/execution/` 计算 crate。此前 Runtime crate、应用层 Token/租户门禁与旧 Backtest crate 已删除；历史迁移文档可能过期。

## 当前调用关系

```text
apps/trading_engine
    ├── database bootstrap / Ledger / ControlStore
    └── gateway/api (HTTP v2)
          ├── pipeline/market + catalog (行情与订阅)
          ├── ledger (真实证据与历史读侧)
          ├── orders (只读订单和成交)
          ├── derivatives (期权公共行情、资金费查询)
          └── research (只读 Catalog 索引)
```

- Apps 负责创建活跃 Store；Gateway 只持有 Ledger、ControlStore 和行情源引用，解析请求并调用领域实现。
- **无自研策略/Execution 执行链**：EMA 示例、StrategyStore、策略下发及预览路由已退役；原独立 `engine/execution/` 目标数量、净额与裁剪计算 crate 也于 2026-11-23 退役。当前没有目标组合/调仓计算服务或实盘订单发送链路。后续以 NautilusTrader 原生 OMS/Execution 为主，SeeAlpha 仅增加确有业务必要的组合、授权及成交归属功能。
- NautilusTrader Rust 项目位于 `engine/pipeline/sources/shared/nautilus/`，当前作为独立 Cargo 子项目，未接管策略执行或交易订单发送。
- 订单 POST、实盘账户和原生持仓读侧未启用；历史订单 GET 不代表 Nautilus 当前订单状态。
- `/research/v1/datasets` 是 Catalog 的只读索引；可选 `/research/mcp` 只暴露 `list_datasets`。没有研究 Grant、异步快照任务或候选回测 API。
- 旧 `engine/backtest/` 及模拟撮合、历史目标持仓回放、回测报告计算已删除。独立行情读写及缺口检查继续位于 Pipeline/Catalog。
- HTTP 不接受 `tenant_id`；`account_id` 用于区分经济证据。数据库中的历史租户列和原有用户表不会因示例代码退休而被自动清理。

## 部署与持久化

服务默认绑定 `127.0.0.1`。取消 Token 后，非 loopback 监听必须显式配置 `ENGINE_ALLOW_UNAUTHENTICATED_REMOTE=1` 并由可信反向代理或网络防火墙限制访问。不得直接发布到公网。MCP 拒绝浏览器 `Origin`，但该规则不等于身份认证。

新的 PostgreSQL schema 不再创建 `strategy_issuances` 表；这次修改没有 `DROP TABLE`、生产存储清理或数据迁移。交易前原子授权、真实成交认证和订单恢复尚未完成实盘验收。

## 验证

```bash
cargo check --workspace --all-targets --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
docker build -f docker/engine.Dockerfile -t seealpha-engine:dev .
```
