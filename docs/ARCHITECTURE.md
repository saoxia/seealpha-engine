# SeeAlpha Engine 当前架构

> 2026-11-23：已取消 `engine/runtime/` crate 和 Engine 应用层 Token / 租户门禁。以下说明当前目标调用链；历史迁移文档可能已过期。

## 调用关系

```text
apps/trading_engine
    ├── database bootstrap / Ledger / StrategyStore / ControlStore
    └── gateway/api (HTTP v2)
          ├── strategy (检查、下发、原生策略执行)
          ├── execution (目标数量与风险裁剪)
          ├── pipeline/market + catalog (行情与订阅)
          ├── ledger (真实证据与历史读侧)
```

- Apps 负责创建应用级 Store；Gateway 持有引用、解析请求、调用领域实现和返回响应。
- 只读策略预览编排位于 `gateway/api/src/strategy_preview.rs`：读取不可变策略记录，使用 Strategy 原生执行器及 Execution 的目标净额/风险裁剪；不撮合、不开仓、不写成交。
- 订单 POST、实盘账户和原生持仓读侧未启用；历史订单 GET 不代表真实 Nautilus 当前状态。
- `/research/v1/datasets` 是 Catalog 的只读索引；可选 `/research/mcp` 只暴露 `list_datasets`。没有研究 Grant、异步快照任务或候选回测 API。
- 旧 `engine/backtest/` 及模拟撮合、历史目标持仓回放、回测报告计算已经删除。历史行情读写和缺口检查仍在 Pipeline/Catalog；回测后续重新开发。
- HTTP 不接受 `tenant_id`；`account_id` 仍用于区分策略与经济证据。历史数据库表的租户列保留，当前写入使用内部兼容命名空间 `local`；这是存储遗留，不是公开租户授权功能。

## 部署边界

服务默认绑定 `127.0.0.1`。取消 Token 后，非 loopback 监听必须显式配置 `ENGINE_ALLOW_UNAUTHENTICATED_REMOTE=1` 并由可信反向代理或网络防火墙限制访问。不得直接发布到公网。MCP 拒绝浏览器 `Origin`，但该规则不等于身份认证。

此重构不迁移、删除或合并历史 PostgreSQL 租户数据；跨历史 namespace 读取必须先设计独立的安全迁移方案。交易前原子授权、真实成交认证和订单恢复尚未完成实盘验收。

## 验证

```bash
cargo check --workspace --all-targets --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
docker build -f docker/engine.Dockerfile -t seealpha-engine:dev .
```
