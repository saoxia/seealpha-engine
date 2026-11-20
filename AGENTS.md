# SeeAlpha Engine 开发规范

## 范围与迁移约束

此仓库仅包含 Engine。技术边界：交易所连接器、行情/历史数据、Catalog、策略加载与回测、目标持仓、风控、模拟/交易执行、订单及运行事实归 Engine。用户/租户账户授权、业务数据库、策略发布审批、Platform Web/API 不属于本仓库。

阶段一将原仓库已存在的 Engine 代码迁入独立项目，保持现有模块路径与接口；可独立精简打包/构建依赖，但不授权交易逻辑重构、修改交易所行为或生产部署。原仓库 `saoxia/SeeAlpha` 不由本仓库迁移 PR 自动修改或删除。

## 冻结与安全

- 未经明确授权，不更改核心架构、NautilusTrader 版本、HTTP 主契约或主要数据流；不新增无必要的包装、兼容层、工厂。
- `engine/` 和 `core/` 不得导入 Platform 的 `api/`、`web/`、业务 ORM 或用户库。连接器不反向依赖 `engine/market_data/`、策略或 Platform。
- 严守订单请求预登记、幂等、未知结果不盲重发、风险门禁、数值精度、恢复顺序和数据完整性。历史/回放不产生实时新风险。
- 不触碰真实账户、真实下单、生产密钥、旧数据重建、生产状态卷或数据库迁移。仅使用模拟、离线及 CI 合成数据。
- 不因切仓改写冻结研究来源或历史测试证据，不掩盖测试失败。禁止为通过检查随意删除既有行为。
- `core/engine_contracts.py`、`core/trading_contracts.py` 仍为 v2 对外边界；破坏性改动必须与原 Platform 同步评审并覆盖兼容性测试。

## 必须执行的开发工作流

1. 检查当前分支/工作区与 `README.md`、`docs/ARCHITECTURE.md`，确认变更归属。
2. 最小化代码修改。策略包契约和 HTTP v2 是对外入口。
3. 同步受影响测试、文档及 Docker/CI。
4. 检查 diff，不引入 Platform 的 `api/`、`web/` 或用户库。
5. 运行 `cargo fmt --all --check`、`cargo clippy --workspace --all-targets --locked -- -D warnings`、`cargo test --workspace --locked` 和 `python3 -m unittest discover -s tests -t . -p '*_test.py' -v`。发布构建用 `cargo build --workspace --locked --release`。
6. 报告已做、未做及失败项，不把未执行的线上/长期验证描述为通过。

## 领域目录

Rust 代码按业务流程组织在根目录：`market_data/`、`alpha/`、`portfolio/`、`accounting/`、`connectivity/`、`backtest/`，应用编排在 `application/`，外部协议在 `interfaces/`，可执行程序在 `apps/`。`python/strategy_host/` 只负责加载策略包。`packages/` 是示例策略。`core/engine_contracts.py` 与 `core/trading_contracts.py` 仍是 v2 契约，必须与 Platform 逐字节一致。实盘签名下单已关闭：对应 HTTP 路由返回 409，不保存密钥。模拟成交只写入 `venue=paper`。
