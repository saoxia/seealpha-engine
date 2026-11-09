# SeeAlpha Engine

SeeAlpha 的独立量化数据、行情、回测、模拟与交易执行内核，基于 NautilusTrader。

> **阶段一（隔离迁移）**：本仓库从 [saoxia/SeeAlpha](https://github.com/saoxia/SeeAlpha) 的 `02ba14d58c700d472584ab17494484a024a04ba1` 快照迁入 Engine 相关代码。原项目当前仍保留自己的 `engine/`；**原 Platform 尚未切换到本仓库**，不要把这次迁入视为生产部署授权。

## 职责与目录

- `engine/connectors/`：交易所协议和 NautilusTrader 适配器。
- `engine/feed/`：实时行情、连续性、重放与风险准入门禁。
- `engine/pipeline/`：历史数据、归档校验、Catalog、Rust 转换器。
- `engine/strategies/`：策略包加载、回测、组合目标协调。
- `engine/runtime.py`、`engine/store.py`：引擎运行事实、订单请求、幂等与恢复。
- `core/`：目前仍需要的轻量共享领域模型和 Engine HTTP 契约。
- `packages/`：仅用于演示和离线测试的策略包示例。
- `tests/`：从原仓库迁入的 Engine 相关测试；不包含 Platform 源码。

本仓库不含 SeeAlpha 的 `api/`、`web/`、业务数据库迁移或用户身份数据。Platform 应通过受认证、版本化 HTTP 接口调用独立部署的 Engine，不应导入本仓库的内部 Python 模块。

## 本地开发与测试

从仓库根目录执行：

```bash
uv sync --locked
uv run ruff check .
uv run pytest -q tests/engine tests/connectors tests/feed tests/pipeline tests/core
cargo test --locked --release --manifest-path engine/pipeline/rust_converter/Cargo.toml
```

部分转换/数据管道测试需要先构建 Rust 转换器：

```bash
cargo build --locked --release --manifest-path engine/pipeline/rust_converter/Cargo.toml
```

本仓库已独立命名为 `seealpha-engine`，运行依赖只声明 NautilusTrader、FastAPI/Uvicorn、Pydantic、WebSocket 和数据处理库；开发依赖仅包括 HTTPX、pytest、Ruff。原 Platform 的 `api`/`research` 依赖组不再纳入 Engine 项目。当前锁文件保留部分先前已解析但可能未使用的包记录，等待重新生成锁文件时进一步清理；实际运行安装以根依赖图为准。

## 启动

```bash
uv run python -m engine
```

开发默认只监听 `127.0.0.1:8766`。环境变量参见 [engine/README.md](engine/README.md)。仅本地开发允许默认令牌；任何共享部署必须提供新的私有 `ENGINE_TOKEN`。所有实盘、账户连接、状态卷数据迁移都需要单独批准。

也可从本仓库根目录构建容器：

```bash
docker build -f docker/engine.Dockerfile -t seealpha-engine:dev .
```

此镜像仅包含 Engine、轻量契约、示例包、锁定依赖和 Rust 转换器，不包含 SeeAlpha Platform。按租户创建/停止容器的控制面仍在原 SeeAlpha 项目。

## CI 与 Docker 构建策略

每次 PR 和 `main` 分支提交仍执行锁定依赖安装、Python Ruff/Engine 测试、Rust Release 构建和 Rust 测试。CI 的 `docker-scope` 检查很轻量，**不会在大部分普通 Python 代码修改时构建 Docker**。

下列情况会额外执行完整 Docker 构建、镜像源码范围检查、匿名令牌拒绝及租户权限 HTTP 冒烟测试：

- 修改 `docker/`、`.dockerignore`、`pyproject.toml`、`uv.lock`、`.python-version`、`rust-toolchain.toml`、`engine/pipeline/rust_converter/` 或 Engine CI 工作流；
- 推送 `v*` 版本标签（用于发布前验收，不自动推送镜像到镜像仓库）；
- 在 GitHub Actions 中手动运行 **Engine standalone CI → Run workflow**，需要复查完整镜像时使用。

如果提交只改 `engine/` 下的普通 Python 代码、`core/`、测试或文档，Docker Job 将跳过，但 Python/Rust 回归仍执行。此优化只省去重复构建，不表示新代码自动具有实盘资格。正式发布必须使用对应提交的完整 Docker 检查结果，并完成 Platform 兼容性及升级/回滚验证。

## HTTP 契约与边界

当前主契约版本：`2`，请求与成功响应使用 `X-SeeAlpha-Engine-Contract-Version: 2`。变更交易命令、错误语义或请求/响应字段前须协调 Platform 并跑跨仓库兼容测试，不能擅自降级或隐式切换到进程内执行。

交易风险检查、订单幂等、未知结果不重发、状态恢复、租户数据隔离、回放数据不准用于新风险决策等约束必须保留。任何自动化测试均不可使用真实凭据、发真实订单或修改生产状态卷。

## 当前明确未验收

跨仓库独立发布、双仓库完整 HTTP 回归、真实交易账户、长时间行情运行、生产状态卷备份恢复与正式切换尚未完成。此次提交只建立新仓库的独立源码、构建和测试基线。详见 [迁移边界](docs/STANDALONE_MIGRATION.md) 和 [Platform 跨仓库接入预演](docs/PLATFORM_INTEGRATION.md)。
