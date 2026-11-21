# Python / Rust HTTP 路由与契约差异矩阵

> 基线：`main` 提交 `83a19d857bca0ec16faf1a731ccb9835081672ab`（2026-11-21）；本表由 Python 六个路由源文件及 Rust 四个路由源文件的装饰器与 `.route(...)` 声明逐条扫描后，结合实现语义复核。它是入口覆盖矩阵，**不是**逐字段行为已通过验收的声明。

Python 基线 **101** 条 method/path；Rust 现分支找到 **87** 条声明（包含 Rust 原生新增路由）。Python 同名路由中的 GET/POST 按不同 method 计数；MCP 位于 `POST /research/mcp`，未纳入 Python decorators 数。所有下面的“已路由”均不等于功能完全等价。

## 统一安全与契约边界

| 类型 | Python 基线 | Rust 分支 | 结论 |
|---|---|---|---|
| 管理鉴权 | `ENGINE_AUTH_MODE=token` 时 Bearer 不符 401 | 全局 gate 校验，Research/MCP 不接受管理 Token | 401 独立测试新增；管理路径不被 Research Grant 绕过 |
| 契约版本 | 管理路由提供 v2 请求时不匹配 426；响应头为 `X-SeeAlpha-Engine-Contract-Version: 2` | 保留管理 v2；Research HTTP 必须同时提供 v2 与 Research v1，缺失/不符 426；MCP 使用自己的协议头 | Research 错误响应也返回双契约头 |
| 租户边界 | 固定租户不匹配 403；Research HMAC Grant 必须绑定开发租户与活跃状态 | 固定租户 gate 403；独立 Research 签名、有效期、租户用途、租户活跃状态二次检查 | 不接受请求体 tenant_id/purpose 代替 Grant |
| 请求验证 | Pydantic V2 字段类型/长度、extra=forbid，422 | 研究请求用 serde `deny_unknown_fields` + 业务校验 422；管理各路由仍存在 400/422 差异 | 未宣称所有 422 响应体逐字一致 |
| 异步任务 | TaskCoordinator：PENDING/RUNNING/SUCCEEDED/FAILED/CANCELLED/INTERRUPTED | Rust 队列为 queued/running/completed/failed/cancelled/interrupted；已登记、状态可读，取消仅 queued；研究任务绑定 Grant `task_id` | 未完成 Python 重试/工作者资源配额完全等价 |
| 快照 | 持久队列写入不可变 Parquet + SHA-256，202，按任务下载 | Runtime 当前只保存原请求文本摘要，未创建 Parquet；HTTP 改为明确 501 | 需 Runtime 新增真正快照任务接口 |
| MCP | 按 grant.ops 过滤工具与执行权限，JSON-RPC | 只宣告真正可执行的 datasets、回测提交/读取/取消；无虚构回测或假取消 | 仅开发租户、默认关闭、拒绝 Origin、工具仍二次验权 |

## 全量 Python 路由逐条对照

### 租户/管理（12）

| Method | Path | Python 请求 | Python 成功响应 | Rust 状态 | 核心差异 / 失败语义 |
|---|---|---|---|---|---|
| GET | `/health` | `Query / path` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| PUT | `/tenants/{tenant_id}` | `TenantBody` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/runnable` | `RunnableBody` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/export` | `Query / path` | 导出 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/drop` | `无体或对象` | 删除租户状态 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/import` | `ImportBody` | 导入状态 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/market/strategy-subscriptions` | `Query / path` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/workers/{worker_id}` | `Query / path` | JSON 对象/列表 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/workers/{worker_id}/stop` | `无体或对象` | JSON 对象/列表 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/strategy-packages/inspect` | `PackageInspectBody` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/strategy-loads` | `StrategyLoadBody` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/strategy-loads` | `Query / path` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |

### 账户/交易/回测（36）

| Method | Path | Python 请求 | Python 成功响应 | Rust 状态 | 核心差异 / 失败语义 |
|---|---|---|---|---|---|
| POST | `/tenants/{tenant_id}/accounts/{account_id}/connections` | `Credentials` | JSON 对象/列表 | **已路由（受限）** | 凭据不落库；网关不可用时 409；公共 401/403/426；422/404/409 按实现复核 |
| DELETE | `/tenants/{tenant_id}/accounts/{account_id}/connections` | `Query / path` | JSON 对象/列表 | **已路由（受限）** | 凭据不落库；网关不可用时 409；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/accounts/{account_id}/health` | `Query / path` | JSON 对象/列表 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/snapshots` | `Credentials` | 账户快照 | **已路由（受限）** | 凭据不落库；网关不可用时 409；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/trading/state` | `TradingStateBody` | JSON 对象/列表 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/spot/trading/state` | `SpotTradingStateBody` | JSON 对象/列表 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/spot/orders` | `SpotCommandBody` | 委托/受理状态（非成交） | **已路由（受限）** | 实盘写入关闭/须预登记，不能当作完整 Python 交易行为等价；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/orders` | `CommandBody` | 委托/受理状态（非成交） | **已路由（受限）** | 实盘写入关闭/须预登记，不能当作完整 Python 交易行为等价；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/trading/controls` | `ProfessionalCommandBody` | JSON 对象/列表 | **已路由（受限）** | 实盘写入关闭/须预登记，不能当作完整 Python 交易行为等价；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/accounts/{account_id}/orders` | `Query / path` | 委托/受理状态（非成交） | **已路由（受限）** | 实盘写入关闭/须预登记，不能当作完整 Python 交易行为等价；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/accounts/{account_id}/fills` | `Query / path` | 真实 Fill 证据 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/accounts/{account_id}/positions` | `Query / path` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/accounts/{account_id}/gaps` | `Query / path` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/gaps` | `GapBody` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/accounts/{account_id}/requests/{request_id}` | `Query / path` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/requests/{request_id}/reconcile` | `ReconcileBody` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/requests/{request_id}/ledger-sync` | `无体或对象` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/accounts/{account_id}/events` | `Query / path` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/accounts/{account_id}/market/subscriptions` | `Query / path` | 订阅状态 / 配置 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/market/subscriptions` | `SubscriptionBody` | 订阅状态 / 配置 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/strategy-market/subscriptions` | `StrategyMarketBody` | 订阅状态 / 配置 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| DELETE | `/tenants/{tenant_id}/accounts/{account_id}/strategy-market/subscriptions/{issuance_id}` | `Query / path` | 订阅状态 / 配置 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/market/workers/note` | `WorkerNote` | JSON 对象/列表 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/market/workers` | `WorkerBody` | JSON 对象/列表 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/strategy-preview` | `PreviewBody` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/backtests` | `BacktestBody` | 回测报告 | **已路由（差异）** | Rust 原生语义和历史证据与 Python/Nautilus 不完全等价；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/accounts/{account_id}/backtests/latest` | `Query / path` | 最新持久结果 / 404 | **已路由（差异）** | Rust 原生语义和历史证据与 Python/Nautilus 不完全等价；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/generic-portfolio-backtests` | `GenericPortfolioBacktestBody` | JSON 对象/列表 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/accounts/{account_id}/generic-portfolio-backtests/latest` | `Query / path` | 最新持久结果 / 404 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/issued-spot-perp-backtests` | `IssuedSpotPerpBacktestBody` | JSON 对象/列表 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/accounts/{account_id}/issued-spot-perp-backtests/latest` | `Query / path` | 最新持久结果 / 404 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/carry-backtests` | `CarryBacktestBody` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/accounts/{account_id}/carry-backtests/latest` | `Query / path` | 最新持久结果 / 404 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/protected-option-backtests` | `ProtectedOptionBacktestBody` | JSON 对象/列表 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/accounts/{account_id}/protected-option-backtests/latest` | `Query / path` | 最新持久结果 / 404 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/accounts/{account_id}/valuation` | `Credentials` | JSON 对象/列表 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |

### 交易所查询（13）

| Method | Path | Python 请求 | Python 成功响应 | Rust 状态 | 核心差异 / 失败语义 |
|---|---|---|---|---|---|
| POST | `/tenants/{tenant_id}/venue/catalog` | `PublicRequest` | 行情/账户公开或签名查询 | **已路由（差异待测）** | 本轮未宣称逐字段请求/响应完全一致；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/venue/spot/klines` | `SpotKlinesRequest` | 行情/账户公开或签名查询 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/venue/spot/trades` | `SpotTradesRequest` | 行情/账户公开或签名查询 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/venue/tickers` | `PublicRequest` | 行情/账户公开或签名查询 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/venue/asset-price` | `PriceRequest` | 行情/账户公开或签名查询 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/venue/asset-account` | `AccountRequest` | 行情/账户公开或签名查询 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/venue/income` | `HistoryRequest` | 行情/账户公开或签名查询 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/venue/operation-history` | `OperationRequest` | 行情/账户公开或签名查询 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/venue/options/time` | `PublicRequest` | 行情/账户公开或签名查询 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/venue/options/positions` | `OptionsRequest` | 行情/账户公开或签名查询 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/venue/options/open-orders` | `OptionsRequest` | 行情/账户公开或签名查询 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/venue/options/order-history` | `OptionsRequest` | 行情/账户公开或签名查询 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/venue/options/pages` | `OptionsRequest` | 历史数据 / 图表 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |

### 历史行情（7）

| Method | Path | Python 请求 | Python 成功响应 | Rust 状态 | 核心差异 / 失败语义 |
|---|---|---|---|---|---|
| POST | `/tenants/{tenant_id}/data/funding` | `FundingQuery` | JSON 对象/列表 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/datasets` | `Inventory` | JSON 对象/列表 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/latest` | `Inventory` | 最新持久结果 / 404 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/coverage` | `Range` | 历史覆盖 / 缺口 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/prepare-plan` | `Range` | 历史覆盖 / 缺口 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/page` | `Page` | 历史数据 / 图表 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/chart` | `Chart` | 历史数据 / 图表 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |

### 行情控制（15）

| Method | Path | Python 请求 | Python 成功响应 | Rust 状态 | 核心差异 / 失败语义 |
|---|---|---|---|---|---|
| POST | `/tenants/{tenant_id}/data/control/storage` | `无体或对象` | 数据控制/任务响应 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/control/sync` | `PolicySet` | 数据控制/任务响应 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/control/subscriptions` | `SubscriptionOptions` | 订阅状态 / 配置 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/control/services` | `无体或对象` | 数据控制/任务响应 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/control/snapshot` | `TaskSnapshot` | 数据控制/任务响应 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/control/job` | `IdBody` | 数据控制/任务响应 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/control/job/start` | `JobPayload` | 数据控制/任务响应 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/control/job/retry` | `IdBody` | 数据控制/任务响应 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/control/job/cancel` | `IdBody` | 数据控制/任务响应 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| GET | `/tenants/{tenant_id}/data/control/exports/{identity}` | `Query / path` | 数据控制/任务响应 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/control/delete-plan` | `DataDeletePlanRequest` | 数据控制/任务响应 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/control/pair-delete-plan` | `PairDeletePlanRequest` | 数据控制/任务响应 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/control/configuration-plan` | `PolicyBody` | 数据控制/任务响应 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/control/feed-status` | `无体或对象` | 数据控制/任务响应 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |
| POST | `/tenants/{tenant_id}/data/control/health` | `无体或对象` | 数据控制/任务响应 | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；公共 401/403/426；422/404/409 按实现复核 |

### 异步任务（9）

| Method | Path | Python 请求 | Python 成功响应 | Rust 状态 | 核心差异 / 失败语义 |
|---|---|---|---|---|---|
| POST | `/tenants/{tenant_id}/data/control/jobs` | `无体或对象` | 任务收据/状态 | **已路由（差异）** | Python Catalog/Journal 语义、422 细节、分页/质量须继续互操作核对；401/403/426 前置门禁，404 未知任务，409 状态冲突，422 需对照 |
| POST | `/tenants/{tenant_id}/jobs` | `SubmitJob` | 任务收据/状态 | **已路由（差异）** | 持久任务可查/取消/显式重试；Rust 状态及超时调度未完全对齐；401/403/426 前置门禁，404 未知任务，409 状态冲突，422 需对照 |
| GET | `/tenants/{tenant_id}/jobs` | `Query / path` | items | **已路由（差异）** | 持久任务可查/取消/显式重试；Rust 状态及超时调度未完全对齐；401/403/426 前置门禁，404 未知任务，409 状态冲突，422 需对照 |
| GET | `/tenants/{tenant_id}/jobs/worker-settings` | `Query / path` | worker 配置 | **已路由（差异）** | 持久任务可查/取消/显式重试；Rust 状态及超时调度未完全对齐；401/403/426 前置门禁，404 未知任务，409 状态冲突，422 需对照 |
| PUT | `/tenants/{tenant_id}/jobs/worker-settings` | `WorkerSettingsUpdate` | worker 配置 | **已路由（差异）** | 持久任务可查/取消/显式重试；Rust 状态及超时调度未完全对齐；401/403/426 前置门禁，404 未知任务，409 状态冲突，422 需对照 |
| GET | `/tenants/{tenant_id}/jobs/{identity}` | `Query / path` | 任务状态 | **已路由（差异）** | 持久任务可查/取消/显式重试；Rust 状态及超时调度未完全对齐；401/403/426 前置门禁，404 未知任务，409 状态冲突，422 需对照 |
| GET | `/tenants/{tenant_id}/jobs/{identity}/result` | `Query / path` | 任务结果 | **已路由（差异）** | 持久任务可查/取消/显式重试；Rust 状态及超时调度未完全对齐；401/403/426 前置门禁，404 未知任务，409 状态冲突，422 需对照 |
| POST | `/tenants/{tenant_id}/jobs/{identity}/cancel` | `无体或对象` | 任务收据/状态 | **已路由（差异）** | 持久任务可查/取消/显式重试；Rust 状态及超时调度未完全对齐；401/403/426 前置门禁，404 未知任务，409 状态冲突，422 需对照 |
| POST | `/tenants/{tenant_id}/jobs/{identity}/retry` | `无体或对象` | 任务收据/状态 | **已路由（差异）** | 持久任务可查/取消/显式重试；Rust 状态及超时调度未完全对齐；401/403/426 前置门禁，404 未知任务，409 状态冲突，422 需对照 |

### Research v1（9）

| Method | Path | Python 请求 | Python 成功响应 | Rust 状态 | 核心差异 / 失败语义 |
|---|---|---|---|---|---|
| GET | `/research/v1/datasets` | `Query / path` | datasets + 覆盖/质量 | **已接入（限定）** | 只列实际 Nautilus Catalog 库存 + grant 数据集白名单；未挂载时报 409；401 无效签名、403 越权、426 版本不符、422 非法参数 |
| POST | `/research/v1/snapshots` | `SnapshotRequest` | snapshot_id、status | **显式 501** | 不返回伪造 snapshot_id/哈希；待真实 Parquet 导出、所有权、下载 API；401 无效签名、403 越权、426 版本不符、422 非法参数 |
| GET | `/research/v1/snapshots/{snapshot_id}` | `Query / path` | 元数据 / Parquet | **显式 501** | 不返回伪造 snapshot_id/哈希；待真实 Parquet 导出、所有权、下载 API；401 无效签名、403 越权、426 版本不符、422 非法参数 |
| GET | `/research/v1/snapshots/{snapshot_id}/content` | `Query / path` | 元数据 / Parquet | **显式 501** | 不返回伪造 snapshot_id/哈希；待真实 Parquet 导出、所有权、下载 API；401 无效签名、403 越权、426 版本不符、422 非法参数 |
| POST | `/research/v1/backtests` | `BacktestRequest` | backtest_id、status / result | **已接入（受限）** | 已下发开发策略、单品种、最多 5000 根闭合 Bar；任务所有权校验；401 无效签名、403 越权、426 版本不符、422 非法参数 |
| POST | `/research/v1/candidate-backtests` | `CandidateBacktestRequest` | backtest_id、status / result | **显式 501** | 候选包验证与 worker 接口未实现；无虚构任务；401 无效签名、403 越权、426 版本不符、422 非法参数 |
| POST | `/research/v1/python-candidate-backtests` | `PythonCandidateBacktestRequest` | backtest_id、status / result | **缺失** | 缺少对应 HTTP 路由；需核对底层 Runtime/Domain 能力；401 无效签名、403 越权、426 版本不符、422 非法参数 |
| GET | `/research/v1/backtests/{backtest_id}` | `Query / path` | backtest_id、status / result | **已接入（受限）** | 已下发开发策略、单品种、最多 5000 根闭合 Bar；任务所有权校验；401 无效签名、403 越权、426 版本不符、422 非法参数 |
| POST | `/research/v1/backtests/{backtest_id}/cancel` | `无体或对象` | backtest_id、status / result | **已接入（受限）** | 已下发开发策略、单品种、最多 5000 根闭合 Bar；任务所有权校验；401 无效签名、403 越权、426 版本不符、422 非法参数 |

## Rust 新增而非 Python 基线的入口

- `POST /tenants/{tenant_id}/accounts/{account_id}/paper/sessions`
- `POST /tenants/{tenant_id}/data/catalog/bars`
- `POST /tenants/{tenant_id}/accounts/{account_id}/candidate-backtests`
- `DELETE /tenants/{tenant_id}/accounts/{account_id}/candidate-backtests`
- `GET /tenants/{tenant_id}/accounts/{account_id}/candidate-backtests/latest`
- `POST /tenants/{tenant_id}/accounts/{account_id}/requests/{request_id}/fills/sync`
- `POST /tenants/{tenant_id}/data/control/source/{product}/start`
- `POST /tenants/{tenant_id}/data/control/source/{product}/stop`
- `GET /tenants/{tenant_id}/data/control/source/{product}`
- `POST /tenants/{tenant_id}/data/catalog/events`
- `POST /tenants/{tenant_id}/data/catalog/inventory`
- `GET /tenants/{tenant_id}/data/catalog/bars`
- `POST /tenants/{tenant_id}/data/options/exercises`
- `POST /tenants/{tenant_id}/venue/options/contracts`
- `POST /tenants/{tenant_id}/venue/options/klines`
- `POST /research/mcp`

## 阻塞与跨 crate 接口契约（**不修改 Runtime**）

1. **Research Snapshot**：Runtime 应提供 `enqueue_snapshot(tenant, task_id, dataset_id, [start,end), format, idempotency)`、`snapshot_status(tenant, task_id, snapshot_id)` 与 `open_verified_snapshot(tenant, task_id, snapshot_id)`；创建任务前验证 Catalog 文件版本/快照范围，持久化状态、行数、字节数、源版本与 SHA-256；下载需基于实际文件重新验证且严格同任务授权。现有 `create_research_snapshot` 的正文哈希不可充作 Parquet 校验证据。
2. **受限/自由候选策略**：需要可复用的候选包静态解析、路径/ZIP/hash/manifest/AST 校验与不可信 Python 独立隔离 Worker，向 HTTP 返回确实已经登记的队列 ID 与结果；绝不能复用 `run_candidate_backtest` 的演示收益当真实异步结果。Python candidate 的开发开关目前不在 Rust `Config`，因此保持未挂载。
3. **Research 任务元数据**：当前 HTTP 通过 Rust `data_jobs.payload._research` 保存 task_id、dataset_id、strategy_ref；正式实现建议在 Runtime 以不可变任务归属与授权关联字段查询，不允许用户通过普通任务 API 冒充 Research 记录；结果读取须保持归属/范围检查、重启后状态可追踪。
4. **HTTP v2 其他缺口**：租户导出/导入/销毁、账户健康和交易状态查询、专门的 market workers、通用组合/跨市场回测、控制层导出及配对删除/健康检查、交易所公共与签名查询等，应由对应 Runtime / Venue / Ledger crate 提供明确只读或有预登记保证的接口后再暴露，不实现固定空数组或隐式实盘操作。

## 本分支测试与验证状态

- 新增 Rust Axum `oneshot` 离线合成测试，覆盖 401/403/426、Research 双 Header、422、任务归属与取消、MCP 按 Grant 过滤工具、快照拒绝伪成功。测试源码位于 `src/research_v1.rs`。
- 本会话执行环境没有 `cargo` / `rustc`，且仓库 `.github/workflows-paused/` 的 CI 未运行；因此**Cargo fmt/clippy/test/workspace 均未实际执行**，不标记通过。该分支需要 CI 或可用 Rust 环境运行后才能宣称通过。
- 未执行外网数据下载、真实交易、生产签名请求或真实研究任务。
