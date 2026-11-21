# SeeAlpha Engine 开发规范

## 当前实现与代码入口

本仓库是 Rust Cargo workspace，默认 HTTP 服务为 `engine/apps/trading_engine` 编译出的 `seealpha-engine`。当前策略实现位于 `engine/strategy/`，研究授权 crate 位于 `engine/research/`，没有运行中的 Python Engine、Python 策略宿主或 Python Platform 共享契约文件。旧版 Python Engine、原 `core/` 和 `python/strategy_host/` 属于迁移历史。当前的 `engine/` 是 Rust 源码汇总目录，**不得把旧 Python 代码恢复为运行时依赖**。

当前架构见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)，代码导航见 [docs/architecture/MODULE_MAP.md](docs/architecture/MODULE_MAP.md)。如文档相互矛盾，以当前 Rust 代码和测试为实际实现依据，同时修订不一致的现行说明。

## 边界和安全

- `engine/interfaces/api/http` 持有 HTTP v2 与研究协议边界；`engine/application/runtime` 负责编排；行情、策略、组合、账本、回测与交易所连接分别归属对应领域 crate。
- 不在没有明确授权和匹配测试时更改 HTTP v2、租户隔离、数据格式、持久化状态、订单幂等、真实账户安全门禁或生产部署。
- 订单必须先登记请求意图；外部请求未知结果不得盲目再次发送；只有被核验的真实成交证据能更新已成交事实；模拟成交必须与真实成交分离。
- 行情读取、策略订阅、回测不得隐式启动生产者、绕过 READY 门禁，亦不得伪造缺失的 Tick/L2、资金费或期权证据。
- 不触碰真实账户、真实下单、生产密钥、旧数据重建、生产状态卷或数据库迁移。使用合成/离线数据验证重构。
- `engine/execution` 和 `engine/risk` 是跨现有 crate 的逻辑领域，参见 `docs/architecture/`；不存在同名独立 crate，不创建空实现。
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
