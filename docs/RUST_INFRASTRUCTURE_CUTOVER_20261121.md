# infrastructure Python → Rust 目录退役（2026-11-21）

## 范围与基线

本轮按文件夹迁移并删除旧 `engine/infrastructure/`，包括：

- `engine/infrastructure/task_queue.py`：Python `TaskJournal`、`TaskCoordinator`、Worker 进程监督与任务状态管理；
- `engine/infrastructure/__init__.py`：旧 Python 包入口。

删除的是**已不再可运行的旧 Python Engine** 中的目录；不改变 `python/strategy_host/`，它仍是 Rust Engine 调用的隔离 Python 策略执行边界。旧源码可由 Git 历史查阅。

## Rust 对应实现

| Python 旧职责 | Rust 当前责任方 | 限制 |
| --- | --- | --- |
| SQLite 任务信封、租户隔离、可重启记录 | `accounting/ledger/` 的任务持久化；`application/runtime/src/jobs.rs`、`data.rs` | 不承诺迁移旧版 JSON `*.record.json` 或完整旧记录格式 |
| 回测提交、幂等 ID、状态转移和结果 | `application/runtime/src/jobs.rs` 的 `schedule_backtest_job`、`execute_backtest_job`；Rust HTTP `parity.rs` | 使用原生回测路径而非旧 Python `engine.backtesting.worker` |
| 数据作业排队、查询、取消和人工重试 | `application/runtime/src/data.rs`；`gateway/api/src/parity.rs` | 部分旧数据维护操作明确拒绝，不伪装成功 |
| Worker 设置持久化与查询 | `accounting/ledger/` 和 `gateway/api/src/lib.rs` | 当前 Rust HTTP 任务调度以单一全局 `Semaphore(1)` 串行执行；配置的 `max_workers` 不等于实际并行度 |
| 启动恢复 | `Runtime::open_book` 将未完成数据任务恢复为 interrupted；`gateway/api/src/lib.rs` 仅恢复已持久排队的研究任务 | 不自动重试执行状态未知的任务 |
| 策略隔离 | `alpha/strategy/` 的隔离宿主执行路径 | 不等同旧 Python 子进程组取消、杀进程及超时监督语义 |

## 安全边界

- 任务执行不具备对交易所实盘下单的授权；已有实盘保护不因本次删除解除。
- 任务取消只有在 Rust 状态机实际支持的条件下生效；不能声称 Python 的 `RUNNING → CANCELLING → CANCELLED` 杀进程语义已完整继承。
- 崩溃恢复遵循 fail-closed：不对执行结果未知的任务自动重发。
- 已退役 Python 模块的残留导入引用不再是运行时回退；该旧 Engine 本身已因前次删除停止运行。
- 本轮遵照逐目录退役策略，**不以 Python/Rust 的错误信息、精度处理或行为逐字节一致作为删除前置条件**。

## 验证与后续

当前通过 GitHub 连接直接完成源码修改，未在本会话取得可编译的本地仓库，因此**尚未执行 Cargo 构建、测试、Clippy 或容器端到端测试**，不可把文件提交视为验收通过。项目 GitHub Actions 当前仍暂停。

建议在可执行的环境运行：

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
```

后续独立优化：Rust Worker 并行度与 `max_workers` 一致化、运行中取消的进程生命周期监督、超时中止/孤儿进程清理、任务并发限流与排队背压、旧 JSON 记录是否需要显式一次性导入。这些不阻挡本次 Python `infrastructure` 目录退役。
