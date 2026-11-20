# Rust 集中迁移审计（2026-11-21）

基线：Python Runtime 切换前的 `f18e48e995becc0662df288058be6059c91c45b0`。
迁移分支：`feat/rust-full-functionality-migration-20261121`。
本页是**已写入代码与仍未完成等价的功能边界**，不是通过 CI 的声明。依照仓库维护者要求，GitHub Actions 暂时保存在 `.github/workflows-paused/`。

## 本轮已写入 Rust 的行为

| 子系统 | 本轮实现 | 不能误认为已等价的能力 |
| --- | --- | --- |
| SQLite 账本 | 账户持仓读取、请求状态读取、游标事件、按范围读取缺口；新增独立持久化数据策略、作业和策略订阅状态 | 旧版租户导入/导出、全部旧状态迁移以及交易所账户完整对账 |
| 行情控制 | `/data/control/` 配置同步、任务快照、队列、取消/人工重试、状态查询；原子保存策略，拒绝重复品种 | 全套周期数据修复、删除预览/确认、独立历史工作者调度、完整源生命周期 |
| 只读策略订阅 | 下发授权检查、数据配置声明检查、跨策略去重需求汇总、取消，读侧不启动生产者 | 生产环境策略实时消息投递、完整 journal->strategy READY 网关、所有行情类型 |
| Binance 数据源 | 独立 Rust Spot / USD-M 子进程可在明确启用后启动/停止；源清单经原生解析验证，状态一律 `DEGRADED`；Docker 包含原生二进制 | REST 交接、交易所时钟确认、断线历史补齐、期权源、生产 READY |
| 历史 K 线 | Rust HTTP 公共读入、限额分页、严格连续校验、拒绝形成中 K 线、原子不覆盖式 SQLite Catalog 写入 | 旧 Nautilus Catalog Parquet 与全周期历史索引、Tick/L2 重放、资金费与期权历史 |
| 策略回测 | 已下发策略经受限 Python 运行宿主输出目标，Rust 统一净额、量化、虚拟持仓、现金、权益曲线、结果持久化 | 任意 NautilusTrader Python Strategy 的所有回调、原生撮合、动态多账户多产品篮子、真实手续费和滑点 |
| HTTP v2 | 恢复账户持仓、请求读取、市场订阅/策略市场、数据控制、补齐任务、Catalog 读取/覆盖率/只读页/图表和策略包预检查；worker 设置持久化；持久化后台回测任务的提交/查询/取消/重试 | 与 Python 全部 HTTP 路由、异步任务边界、422 错误细节完全逐字等价 |
| 实盘安全 | 实验源默认不启动、不授权新风险；账户凭据不因数据订阅读取而使用；历史任务只读公共行情 | 未完成旧版全部实盘撤改单、期权/条件单、未知结果恢复、账户对账 |

## 新的操作边界

- Rust 主入口仍是 `cargo run --locked --release -p seealpha-engine`，默认容器仍使用 Rust 二进制。
- `ENGINE_EXPERIMENTAL_SOURCE=1` 只是允许**主动调用** `POST /tenants/{tenant_id}/data/control/source/{product}/start`；没有配置数据源根目录和清单时会拒绝。它不把行情变为 READY。
- 数据源停止为同一路径下的 `/stop`，只在本引擎进程持有该子进程时可用。源根目录默认 `$ENGINE_PROJECT/data/sources/{product}`，必须事先存在；可使用 `ENGINE_SOURCE_ROOT` 指定已有独立根目录。不要和 Python 同时写入相同 SQLite / Catalog。
- 历史补齐任务当前接受 `POST /tenants/{tenant_id}/data/control/job/start`：`{"payload":{"action":"backfill","product":"spot","symbol":"ETHUSDT","interval":"1m","start_ms":0,"end_ms":60000}}`。也接受 UTC ISO 的 `start` 和 `end` 字段；单任务上限 5000 根，不能跨缺口或覆盖冲突证据。
- 旧式日期区间 `POST /tenants/{tenant_id}/accounts/{account_id}/backtests` 以 `start/end/product/symbol/interval/starting_balance/quote_currency` 读取**本地 SQLite Catalog**和**该账户已下发策略**，然后通过 Rust 回放生成权益曲线。它不是完整 NautilusTrader Catalog 回测替身；未登记的策略和数据缺口均拒绝。
- 原恢复的 Python Engine 保留在 `engine/`，直到相同输入、相同失败路径及 Platform 端到端场景都经过验证；不会由 Rust 自动调用作为“功能兼容”旁路。

## 继续一次性迁移的阻塞项

仍需同批补齐的领域包括：NautilusTrader RC9 原生 Catalog/回测、Spot/UM/Options 历史证据和 funding、账户/订单全生命周期与专业交易控制、期权行权/结算、篮子风险阶段、源独立监督和自动补齐、Research API/MCP 全部权限与任务后台、租户导出/导入及数据目录格式兼容。缺少这些能力时，不应删去对应 Python 实现，也不能将本分支标记为 Rust 功能迁移完成。

## 最终统一验收（暂不自动运行）

恢复 `.github/workflows-paused/*.yml` 到 `.github/workflows/` 后，至少执行：

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
uv sync --locked
uv run pytest -q tests/engine tests/connectors tests/feed tests/pipeline tests/core
docker build -f docker/engine.Dockerfile -t seealpha-engine:ci .
docker build -f docker/engine-legacy.Dockerfile -t seealpha-engine:legacy-ci .
```

两份运行时应在独立临时数据根目录、离线行情夹具、无交易密钥环境中比较 HTTP、历史数据、策略目标、订单净额和异常行为。任何属于真实订单的未知执行结果都只能走交易所查询与对账，不能因为改用 Rust 而自动重试。
