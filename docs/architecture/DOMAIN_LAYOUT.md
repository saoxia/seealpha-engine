# SeeAlpha Engine 一级领域布局（方案 B）

本文件描述**当前代码归属与长期领域边界**。Rust 引擎源码统一位于仓库的 `engine/`，部署、文档、测试和基准测试等工程文件与其并列。规划不等于实现；真正功能状态以源码、Cargo Workspace 和验证结果为准。

## 已落地的业务领域

| 目录 | 职责与实施状态 |
| --- | --- |
| `engine/pipeline/` | 整个行情服务：Nautilus 实时节点、补充数据源、数据完整性验证、Catalog 访问 |
| `engine/strategy/` | 策略授权、原生策略、目标持仓发布；由 `alpha/strategy/` 原样迁入，Cargo 包名保持 `seealpha-strategy` |
| `engine/portfolio/` | 多策略组合、目标持仓、资金分配、净额与篮子；当前也包含部分风险逻辑 |
| `engine/ledger/` | 账户、订单意图、成交事实、对账证据与账本持久化 |
| `engine/connection/` | 交易所私有订单、账户协议与状态适配；当前仅 Binance，行情由 `engine/pipeline/` 持有 |
| `engine/backtest/` | 历史回放、模拟撮合、验证性回测 |

### Binance Options 专属代码边界（分步迁移）

`engine/connection/binance/` 已删除。`engine/connection/binance-options/` 保留 Options 领域规范化，但没有签名私有通信通路；Spot / USD-M 使用 NautilusTrader 原生数据节点和执行工厂离线配置，Runtime 仍只登记订单。

## 规划中的业务领域（当前无同名源码目录）

| 规划目录 | 目标职责 | 当前归属 |
| --- | --- | --- |
| `engine/risk/` | 风险预算、敞口、组合限制、动态风控与订单准入 | `engine/portfolio/`、`engine/runtime/`、`engine/pipeline/nautilus/` |
| `engine/execution/` | 订单决策、生命周期、撤改、幂等、未知结果恢复与执行优化 | `engine/runtime/`、`engine/connection/binance/`、`engine/ledger/` |

不为了目录完整而创建空目录、空 crate 或兼容包装。独立前必须先确认安全事务边界与回归测试。参见 [Risk](risk.md)、[Execution](execution.md)。

## 应用和对外边界

- `engine/runtime/`：用例编排、租户/账户/会话管理、研究任务、订单恢复
- `engine/gateway/`：对外协议入口；`api/` 为 HTTP API crate，内部 `src/research/` 负责研究签名授权与范围校验；`mcp/` 为复用现有 HTTP 服务的 MCP 协议模块，不增加独立进程或监听端口
- `engine/apps/`：Engine 主程序和其他可执行入口

## 持久化数据目录

仓库根级 `data/` 用于运行时产生的 Feather、Parquet Catalog 和历史归档；仅 `data/README.md` 作为目录说明纳入 Git，实际数据文件被 `.gitignore`、`.dockerignore` 排除，不是 Rust Workspace 成员。生产环境可挂载独立数据卷，路径由部署参数指定。此次目录调整不移动、删除或修改已有生产数据。参见 [数据目录约定](DATA_LAYOUT.md)。

## 工程设施与资产

本节目录均在仓库根目录，与 `engine/` 并列。根目录 `Cargo.toml` 和 `Cargo.lock` 管理 Rust Workspace，`engine/pipeline/nautilus` 作为独立 Cargo 项目被明确排除，`benchmarks/rust` 仍属于 Workspace。


- 测试：crate 单测、集成测试均就近放置；不再保留只有 Python `__init__.py` 的根目录 `tests/`
- `benchmarks/`：性能基准
- `docker/`：现有 Docker 构建和部署，沿用名称
- `docs/`：现行架构与历史审计；历史迁移快照不批量重写
- `.github/`：CI 与仓库工作流
- `packages/`：Rust 原生策略清单示例；旧 Python ZIP 示例已退役
- `scripts/`：将来有自动化脚本再创建，目前不创建空目录

## 维护不变量

1. 目录调整不变更 crate 名、Rust 公开 API、HTTP 契约、持久化格式、订单幂等或租户隔离。
2. 真实订单 ACK 不等于成交；只有独立核验的真实成交证据才能更新账本事实。
3. 行情读取或回测不得隐式启动生产者、伪造 READY 或缺失的行情证据。
4. 领域边界不等于同步调用顺序；重构时保留正确的事务和失败恢复语义。
