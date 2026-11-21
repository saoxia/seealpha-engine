# SeeAlpha Engine 一级领域布局（方案 B）

本文件描述**当前代码归属与长期领域边界**。规划不等于实现；真正功能状态以源码、Cargo Workspace 和验证结果为准。

## 已落地的业务领域

| 目录 | 职责与实施状态 |
| --- | --- |
| `market_data/` | 行情采集、数据质量、历史 Catalog、Feed、数据处理 |
| `strategy/` | 策略授权、原生策略、目标持仓发布；由 `alpha/strategy/` 原样迁入，Cargo 包名保持 `seealpha-strategy` |
| `portfolio/` | 多策略组合、目标持仓、资金分配、净额与篮子；当前也包含部分风险逻辑 |
| `accounting/` | 账户、订单意图、成交事实、对账证据与账本持久化 |
| `connectivity/` | 交易所协议与外部请求/状态适配 |
| `backtest/` | 历史回放、模拟撮合、验证性回测 |
| `research/` | 研究签名授权与作用域校验；由 `application/research/` 原样迁入，Cargo 包名保持 `seealpha-research` |

## 规划中的业务领域（当前无同名源码目录）

| 规划目录 | 目标职责 | 当前归属 |
| --- | --- | --- |
| `risk/` | 风险预算、敞口、组合限制、动态风控与订单准入 | `portfolio/core/`、`application/runtime/`、`connectivity/venue/` |
| `execution/` | 订单决策、生命周期、撤改、幂等、未知结果恢复与执行优化 | `application/runtime/`、`connectivity/venue/`、`accounting/ledger/` |

不为了目录完整而创建空目录、空 crate 或兼容包装。独立前必须先确认安全事务边界与回归测试。参见 [Risk](risk.md)、[Execution](execution.md)。

## 应用和对外边界

- `application/`：用例编排、租户/账户/会话管理、研究任务、订单恢复
- `interfaces/`：HTTP v2、Research v1/MCP-compatible 接口与输入输出、认证
- `apps/`：Engine 主程序和其他可执行入口

## 工程设施与资产

- `tests/`：跨模块与端到端测试；crate 单测和集成测试仍就近放置
- `benchmarks/`：性能基准
- `docker/`：现有 Docker 构建和部署，沿用名称
- `docs/`：现行架构与历史审计；历史迁移快照不批量重写
- `.github/`：CI 与仓库工作流
- `packages/`：已有示例策略资产，部分仍使用历史 Python 协议，**不等于当前 Rust Runtime 可执行**
- `scripts/`：将来有自动化脚本再创建，目前不创建空目录

## 维护不变量

1. 目录调整不变更 crate 名、Rust 公开 API、HTTP 契约、持久化格式、订单幂等或租户隔离。
2. 真实订单 ACK 不等于成交；只有独立核验的真实成交证据才能更新账本事实。
3. 行情读取或回测不得隐式启动生产者、伪造 READY 或缺失的行情证据。
4. 领域边界不等于同步调用顺序；重构时保留正确的事务和失败恢复语义。
