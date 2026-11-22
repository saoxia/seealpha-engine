# SeeAlpha Engine 当前架构

> 当前实现说明，基于 2026-11-21 `main` 的 Rust workspace。与此前的 Python/NautilusTrader 迁移记录不同，本文件描述**仓库中实际存在的模块**。新增目录拆分只改变源码组织，不代表行为等价或生产验收通过。

## 入口与依赖方向

以下源码路径均以仓库根目录的 `engine/` 为前缀（示意图内部省略部分共同前缀）。

```text
engine/apps/trading_engine
        |
gateway/api  (HTTP v2、Research v1/MCP-compatible routes)
        |
runtime  (租户、账户、策略、纸面会话、数据任务、订单恢复)
   |        |          |
   v        v          v
strategy portfolio   backtest
   \        |         /
    \       v        /
     ledger
           ^
           |
  pipeline/sources/shared/nautilus        pipeline/*
```

上图表示主要调用/协作关系，不是完整 Cargo 依赖图。以各 crate 的 `Cargo.toml` 为编译依赖的准确来源。

- **对外网关**：`engine/gateway/api` 负责认证、版本头、请求校验及 HTTP 响应；按 `src/routes/` 分功能维护，现有 `parity`、`derivatives` 和 `research_v1` 路由继续保留；`engine/gateway/mcp` 独立保存 MCP 协议实现，但仍由同一个 HTTP 进程挂载 `/research/mcp`。
- **应用编排**：`engine/runtime` 负责业务用例、授权范围、纸面会话、订单恢复和异步作业。订单命令位于 `orders.rs`，历史研究/回测协调位于 `research.rs`；其他模块按既有功能分文件。
- **领域能力**：`engine/strategy`、`engine/portfolio`、`engine/backtest` 分别负责策略检查/原生策略、目标持仓与风险裁剪、回放和撮合。
- **交易事实**：`engine/ledger` 持有 SQLite 状态、请求意图、成交和对账证据；`engine/pipeline/sources/shared/nautilus` 保留 NautilusTrader 原生 Binance Spot/USD-M 工厂与执行身份映射；原生订单提交及私有账户取证尚未接入 Runtime。二者不可用简单的 HTTP 传输成功代替实际成交事实。
- **行情**：`engine/pipeline/sources/shared/nautilus` 是独立编译、显式启用的 NautilusTrader Spot/USD-M 实时数据节点；`sources/shared/market` 保留公共 REST；`sources/binance_option` 持有期权 REST 与消息解析，`sources/binance_futures` 持有资金费逻辑；`sources/shared/archive` 负责官方 ZIP；`catalog/integrity` 保留数据校验与 READY 安全规则；已移除无生产订阅者的自研 Feed/Hub 并保留其独有安全规则，`catalog` 仍持有历史数据读写 API 与 Runtime 派生聚合缓存。通用 Catalog 替代尚待验收，原生节点尚未通过生产 READY 验收。
- **研究授权**：`engine/gateway/api/src/research/` 管理研究 Grant 的验签和作用域校验；用例编排仍在 `engine/runtime`，历史回放仍在 `engine/backtest`。
- **独立程序**：`engine/apps/trading_engine` 编译为 `seealpha-engine`，负责创建 Runtime、HTTP 服务以及显式 smoke 入口。

## 一级目录原则

顶层区分**业务源码**、**持久化数据**与**工程资源**：`engine/` 包含全部 Rust 业务领域、应用运行时、接口与可执行入口；`docker/`、`benchmarks/`、`docs/`、`.github/` 与 Cargo Workspace 配置保留根目录；测试位于各 Rust crate 内。

已实现业务领域：`engine/pipeline/`、`engine/strategy/`、`engine/portfolio/`、`engine/ledger/`、`engine/connection/`、`engine/backtest/`。`engine/risk/` 与 `engine/execution/` 仍是逻辑领域，没有独立空 crate。`engine/runtime/`、`engine/gateway/`、`engine/apps/` 为运行与交付层。详见 [领域布局](architecture/DOMAIN_LAYOUT.md)。

此次源码整体移动，仅同步更新 Workspace、Docker、CI、相对测试路径及当前架构说明，不改变公开 API、交易行为、真实账户门禁或持久化格式。


已开始把期权专有成交证据校验抽离至 `engine/connection/binance-options`；旧 REST crate 已删除；Spot/USD-M 当前 Runtime 只登记原生意图、不发送交易指令，账户、成交和结算私有查询明确不可用。

本地行情数据文件使用 Git 忽略的仓库根级 `data/`（生产环境使用数据卷），与 `engine/pipeline/` 的源码分离。参见 [数据目录约定](architecture/DATA_LAYOUT.md)。

## 典型流向

**行情**：授权的持久化配置 → 数据源采集/核验 → Nautilus/Catalog → 只读消费者（研究、回测、策略）。数据读取不能偷偷启动生产者，也不能将缺失的真实成交/盘口数据伪造为已验证事实。

**策略和回测**：接口 → Runtime → 已授权的 Rust 原生策略/历史数据 → 目标持仓/组合计算 → 回测或纸面持仓与账本。当前只支持受控的原生策略能力；旧 Python ZIP/NautilusTrader 策略不再直接执行。

**订单**：接口 → Runtime 请求预登记与门禁 → （待接通的）Nautilus 原生执行 → 可信成交证据和对账 → Ledger 投影。未知结果不能盲重发。当前真实交易所发送并未获得生产验收授权，旧迁移文档不得被理解为启用承诺。

## 逻辑边界但非独立 crate

- [Execution](architecture/execution.md)：订单管理目前跨 Runtime、Nautilus 和 Ledger；此前未被业务调用的独立订单状态校验 crate 已移除。
- [Risk](architecture/risk.md)：风险裁剪当前位于 Portfolio，额外门禁位于 Runtime。

不以「目录名称齐全」为目的新建空 crate。只有跨模块职责和事务边界明确后，才考虑独立 Execution/Risk crate。

## 不兼容与未验收项

此分支以 Rust 二进制为唯一 Engine HTTP Runtime，没有 Python 宿主或 Python 共享契约源文件。旧 Python 策略不会自动迁移；历史 Tick/L2、期权全生命周期、真实账户/订单恢复与跨 Platform 兼容性不能仅凭迁移后的 Rust 模块名字宣称完整。

本次整理不变更 HTTP v2、状态库/Parquet 路径、订单幂等、API token、租户隔离、数据质量门禁或交易所安全限制。运行以下命令并检查结果后再合并：

```bash
cargo check --workspace --all-targets --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
docker build -f docker/engine.Dockerfile -t seealpha-engine:dev .
```

历史过程与已经不再适用的 Python 说明请从 [文档索引](README.md) 中区分查看。
