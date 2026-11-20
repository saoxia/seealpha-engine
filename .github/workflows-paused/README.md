# GitHub Actions 暂时停用（Rust 集中重写阶段）

当前按仓库维护者指示，临时停用自动 CI / GitHub Actions，以便集中完成 Rust 原生功能迁移。
**停用日期：2026-11-21。恢复日期：待 Rust 功能替换完成后确定。**

原本的三个 GitHub Actions 工作流文件被原样移动到此目录：
- `engine-ci.yml`（Rust / 策略测试、Docker）
- `legacy-python-parity.yml`（旧 Python 回归、兼容镜像）
- `rust-binance-source.yml`（原生 Binance 数据源）

GitHub 只会自动识别 `.github/workflows/` 下的 `.yml`/`.yaml` 工作流。
本目录是 `.github/workflows-paused/`，因此这里的文件不会作为工作流触发。
文件内容、版本历史与配置全部保留，方便恢复；正常 `main` 上的 Actions 工作流目录暂时没有可执行工作流。
之前已经启动的工作流可能继续结束；这次停用只保证新的提交不再由这些文件触发。

## 恢复 CI / Actions

Rust 迁移完成、代码进入整体修复与验收阶段时：

```bash
mkdir -p .github/workflows
git mv .github/workflows-paused/*.yml .github/workflows/
git add -A
git commit -m "ci: re-enable GitHub Actions after Rust migration"
git push
```

恢复时按需升级 Rust/Python 工具链和锁文件，并逐项修复 Rust Cargo、
Python 测试、Docker、API 合约、Platform 集成测试；不要因为“自动 CI 已停用”
就宣称功能迁移通过。

## 本阶段最小安全边界

- 允许开发分支中出现暂时不能编译或测试失败的代码，但不能将其标记为生产可部署。
- 禁止在未对账/备份的情况下让 Python 和 Rust 同时写同一数据卷/订单账本。
- 实盘订单、未知执行结果及账户密钥的安全约束不可因为暂停 CI 而放宽。
