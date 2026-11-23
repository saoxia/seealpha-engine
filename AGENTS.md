# SeeAlpha Engine 开发规范

## 当前实现与代码入口

本仓库是 Rust Cargo workspace，默认 HTTP 服务为 `engine/apps/trading_engine` 编译出的 `seealpha-engine`。2026-11-23 已退役 `engine/strategy/` 的 EMA 示例、下发存储及预览路由；当前没有运行中的策略执行器。历史研究授权模块和 Python Engine、Python 策略宿主均不在现行运行链路。旧版 Python Engine、原 `core/` 和 `python/strategy_host/` 属于迁移历史。当前的 `engine/` 是 Rust 源码汇总目录，**不得把旧 Python 代码恢复为运行时依赖**。

当前架构见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)，代码导航见 [docs/architecture/MODULE_MAP.md](docs/architecture/MODULE_MAP.md)。如文档相互矛盾，以当前 Rust 代码和测试为实际实现依据，同时修订不一致的现行说明。

## 边界和安全

- `engine/gateway/api` 持有 HTTP v2 与 Research HTTP 路由，`engine/gateway/mcp` 持有 Research MCP 协议处理；应用入口装配 Store，Gateway 调用领域模块；行情、账本与交易所连接归属各自的领域 crate；策略运行尚未重新接入，未来优先复用 NautilusTrader 原生能力。旧回测 crate 已退役。不再存在 Runtime crate、Engine Token 认证和租户级门禁。
- 不在没有明确授权和匹配测试时更改 HTTP v2 数据接口、数据格式、持久化状态、订单幂等、真实账户安全门禁或生产部署。
- 订单必须先登记请求意图；外部请求未知结果不得盲目再次发送；只有被核验的真实成交证据能更新已成交事实；模拟成交必须与真实成交分离。
- 行情读取不得隐式启动生产者、绕过 READY 门禁，亦不得伪造缺失的 Tick/L2、资金费或期权证据；未来策略订阅和新回测实现必须遵守同样的安全边界。
- 不触碰真实账户、真实下单、生产密钥、旧数据重建、生产状态卷或数据库迁移。使用合成/离线数据验证重构。
- `engine/execution/` 原有独立 crate 已于 2026-11-23 退役，现行 Workspace 不包含自研 Execution crate；Execution 与 Risk 仅作为待重新接入的业务领域，参见 `docs/architecture/`。未来应优先接入 NautilusTrader 的原生 OMS、订单事件、账户缓存与对账能力，只补充 SeeAlpha 特有的组合决策、授权和成交归属；不得创建空实现或平行订单状态机。
- 一级领域清单与状态见 `docs/architecture/DOMAIN_LAYOUT.md`。

## 目录调整规范

1. 首先检查当前 Git 分支、Cargo workspace 和相关模块的公开类型与调用链。
2. 优先做不改变行为的文件拆分：移动处理函数及测试、修正模块引用、维持现有公开 API。不要为改善外观新增包装、兼容层和独立空 crate。
3. 使用 `git diff --find-renames` 检查移动与非预期的业务修改；相关文档/容器/CI 必须同步更新。
4. 运行并记录结果（失败不得隐瞒）：

```bash
cargo check --workspace --all-targets --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
docker build -f docker/engine.Dockerfile -t seealpha-engine:dev .
```

本仓库已有历史格式化债务，CI 中 `cargo fmt --all --check` 暂为 advisory；不能因此把本次新引入的格式问题视为通过。重构不代表旧 Python 功能已 1:1 迁移完毕，也不代表实盘或生产功能已获验收。

## Pipeline 产品基线（2026-11-22）

参见 [engine/pipeline/BASELINE.md](engine/pipeline/BASELINE.md)。Spot 仅维护原始成交与 K 线；USD-M 维护聚合成交、资金费与 K 线；Options 历史行情仅保存官方 K 线 Bar。共用的分层修复算法与 Binance ZIP 必须复用，不能重新引入 Python Feed、平行 WebSocket 数据采集器、额外的质量/READY 会话或期权逐笔历史存储。后续修改这些行为需先明确需求并通过相关现有测试。此次“锁定”是设计与 API 约束，**不等于已完成生产 READY 验收**。
