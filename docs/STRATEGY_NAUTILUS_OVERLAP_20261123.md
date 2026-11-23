# Strategy 与 NautilusTrader 能力重叠审查（2026-11-23）

基于 `main` 的当前执行路径：`apps/trading_engine -> gateway/api -> strategy`。仓库的 NautilusTrader Rust 子项目位于 `engine/pipeline/sources/shared/nautilus/`，依赖 `nautilus-*=0.68.0`，被根 `Cargo.toml` 的 `exclude` 排除；尚未承接 HTTP 策略预览。

| 当前 SeeAlpha 代码 | Nautilus 能力 | 决策 |
| --- | --- | --- |
| `native_ema.rs`：EMA 计算、交叉信号 | Nautilus 原生 EMA 指标及 Rust EmaCross 策略范例 | 有重叠但被当前 API 调用；待数据类型、预热/交叉时序和输出行为对齐后迁移 |
| `executor.rs`：自定义通用 StrategyExecutor / 实现分派 | Nautilus 原生 Strategy / Actor 生命周期与派发 | 当前只有原生 EMA 一个实际调用链；删除该抽象层，直接调用并保留授权校验 |
| `host_process.rs`、`host_policy.rs`、`protocol.rs`、`decimal.rs`、Python ZIP 检查 | 历史 Python 策略宿主已删除；现行 API 不调用这些模块 | 删除不可达的旧代码，而不是用它们重新实现 Nautilus |
| `store.rs`、`issued.rs`、`native_manifest.rs`、`native_issuance.rs` | Nautilus 策略配置/加载并不自动提供 SeeAlpha 的账户绑定、不可变发行记录及磁盘+PG 校验恢复 | 保留业务语义；后续对接 Nautilus 时可将存储与策略运行分层 |
| `Publication`、`StrategyRun` 和 HTTP preview 的目标汇总 | Nautilus 原生的策略、订单、账户模型 | 当前是只读目标持仓意图，不是订单执行/成交；保留接口，未来做显式映射 |

## 本次安全清理边界

- 清理仓库中无运行调用方的 Python 策略宿主契约和多余执行分派，更新引用，不更改 HTTP 路由、数据库/ZIP 下发协议、原生 EMA 算法和策略输出。
- **不删除整个 `engine/strategy/`**；否则 `/strategy-packages/inspect`、`/strategy-loads`、`/accounts/{account_id}/strategy-preview` 会失效。
- 不声称已切换 Nautilus 策略执行，也不声称运行了测试或 Docker 构建。

## 后续原生化验收

1. 在当前固定的 Nautilus Rust API 上实现目标持仓预览适配，**不自动提交订单**。
2. 使用相同合成 Bar 序列验证 EMA 预热、相等/穿越边界、零持仓目标、参数、Symbol / Interval 的行为差异；需要保持 `bar_index` 和 `Decimal` 输出契约。
3. 保留发行记录与 SHA/账户范围的严格校验，验证断电恢复与不可信 ZIP 的拒绝行为。
4. 验证 `cargo check --workspace --all-targets --locked`、Clippy、完整测试及 Docker 构建后，才能删除当前被依赖的 `native_ema.rs`。
