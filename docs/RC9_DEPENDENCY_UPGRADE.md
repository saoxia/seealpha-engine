# NautilusTrader RC9 依赖升级（2026-11-19）

本记录对应第一阶段：更新依赖、锁文件、开发工具链与 CI/Docker 运行环境。第二阶段的 NautilusTrader 新能力采用、交易算法和功能重构**不在本次变更范围**。

## 升级结果

| 组件 | 升级前 | 本次锁定或采用 |
| --- | --- | --- |
| NautilusTrader | 2.0.0rc8 | **2.0.0rc9** |
| Python 开发环境 | 3.14.9 | **3.15.2**（项目声明支持 >=3.13,<3.16） |
| Rust 工具链 | 1.99.1 | **1.100.0** |
| uv（Docker 与升级流程） | 0.10.0 | **0.13.9** |
| FastAPI | 0.141.1 | **0.164.0** |
| Uvicorn | 0.52.1 | **0.54.0** |
| NumPy | 2.5.4 | **2.5.5** |
| pandas | 3.1.0 | **3.1.1** |
| PyArrow | 26.0.0 | **26.0.1** |
| Pydantic | 2.14.1 | **2.14.3** |
| websockets | 17.3 | **17.4** |
| pytest / Ruff | 9.1.1 / 0.17.2 | **9.2.0 / 0.17.5** |
| Rust Arrow / Parquet | 60.0.0 | **60.1.0** |
| Rust zip | 旧锁文件版本 | **9.7.0** |

Python 依赖由 `uv lock --upgrade` 从 PyPI 解析并写入 `uv.lock`；Rust 依赖由 `cargo update` 从 crates.io 解析并写入 `Cargo.lock`。原先 `uv.lock` 的无用传递包记录随新解析结果清除。Rust Arrow 61 尚未出现在本次 crates.io 解析索引中，因此使用可安装的最新 60.x 系列，不将未验证的 61.x 写入依赖声明。

CI 使用已发布版本的 `actions/checkout@v7.0.1` 和 `astral-sh/setup-uv@v10.3.3`；Docker 基础镜像升级到 Python 3.15、Rust 1.100，保持原 Engine 功能边界。

## 离线验证

```bash
uv sync --locked
uv run ruff check .
cargo build --locked --release --manifest-path engine/pipeline/rust_converter/Cargo.toml
cargo test --locked --release --manifest-path engine/pipeline/rust_converter/Cargo.toml
uv run pytest -q tests/engine tests/connectors tests/feed tests/pipeline tests/core
docker build -f docker/engine.Dockerfile -t seealpha-engine:ci .
```

验收以本次 PR 的 GitHub Actions 运行结果为准。所有自动化验证只使用模拟/离线夹具，不连接实盘账户、不执行真实交易、不修改生产数据或状态卷；旧研究结果的经济表现不因依赖升级而自动获得再验证。

上次的 [ENGINE_UPGRADE.md](ENGINE_UPGRADE.md) 是 2026-10-26 的历史升级记录，不代表本次依赖版本。
