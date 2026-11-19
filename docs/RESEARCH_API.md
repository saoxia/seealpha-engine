# Engine Research HTTP v1（开发阶段）

> 本接口位于独立 `saoxia/seealpha-engine`，是对已有 Engine HTTP v2 的**新增、默认关闭**的研究服务。不能视为生产环境已启用或平台/Harness 已完成联调。

## 启用条件

仅在**固定开发租户**的 Engine 容器上配置独立 `ENGINE_RESEARCH_SIGNING_KEY`（至少 32 字符、不得与 `ENGINE_TOKEN` 相同）时，才挂载 `/research/v1/*`。

同时要求：
- `ENGINE_TENANT_ID` 已固定，`ENGINE_TENANT_PURPOSE=development`。
- Engine 持久化租户状态为 `active` 且用途为 `development`。
- 请求附带 **`X-SeeAlpha-Engine-Contract-Version: 2`** 和 **`X-SeeAlpha-Research-Contract-Version: 1`**。
- Bearer 为 Platform 在授权后签发的短时任务级 HMAC grant，包含 `version`、`audience`、`tenant_id`、`task_id`、`ops`、`datasets`、`strategies`、`not_before`、`expires_at`，有效期最多一小时。

Engine 只提供 `engine.research_auth.sign_research_grant()` 作为可信 Platform 后端未来可调用/移植的合同示例，**不提供任何远程签发端点**。当前 Platform 尚未实现颁发授权的集成，也未设计实时撤销，因此这一阶段不能用于不可信多租户生产环境。签名密钥不能放入 Harness Shell、浏览器、工作区或研究文件。所有研究路由由独立 ASGI app 处理，不能使用管理令牌调用，研究 grant 也不能使用管理 API、策略发布或交易接口。

## 研究接口

| 方法 | 路由 | 行为 |
| --- | --- | --- |
| GET | `/research/v1/datasets` | 返回被当前 grant 数据集白名单允许的 Catalog 库存，附实际覆盖与质量标记 |
| POST | `/research/v1/snapshots` | 在 Engine 端排队生成只读 Parquet 快照，返回 `snapshot_id`（202） |
| GET | `/research/v1/snapshots/{id}` | 返回当前状态、快照来源、行数、字节数、SHA-256 |
| GET | `/research/v1/snapshots/{id}/content` | 同源下载校验后不可变的 Parquet |
| POST | `/research/v1/backtests` | 将受限开发回测映射到原 `TaskCoordinator` 队列（202） |
| GET | `/research/v1/backtests/{id}` | 返回本研究任务名下的队列状态，成功时返回已记录的回测结果 |
| POST | `/research/v1/backtests/{id}/cancel` | 调用原回测队列取消；**正在运行时仍可能返回 409** |

研究 grant 的操作枚举：`datasets:read`、`snapshots:create`、`snapshots:read`、`backtests:submit`、`backtests:read`、`backtests:cancel`。每次请求 Engine 都检验过期、租户、操作、数据集/策略范围；请求体中的 `tenant_id` 或 `purpose` 不能代替这些校验。

### 数据快照

数据集 ID 为 `product:symbol:level`，例如 `futures_um:ETHUSDT:1m`。来源为既有 Catalog 的库存及 Arrow 读取路径，不触发行情下载、回填或重建。按 [start, end) UTC 切片；每份最多 31 天、300 万行、128 MiB，单 Engine 最多排队 2 份、同时执行 1 份；大历史数据可按日期拆分多个研究快照。

创建时先持久记录待执行任务，在后台单独执行有界 Parquet 导出；完成后记录大小、行数和 SHA-256。运行失败则记录 `failed`，Engine 重新启动时尚未完成的记录变为 `interrupted`，不会自动重复生成。下载需要该原始任务授权，跨任务和跨数据集访问拒绝。

现阶段快照主要证明**数据访问机制**，并非严格的历史数据版本冻结证明：质量标记来自原 Catalog，不能把 `unverified` 或 `partial_recent_trades` 表述为全量/官方逐笔完整。物理文件完整性、数据集版本指纹、长时间任务恢复与强沙箱要进一步加固；这些在联调前不可省略。

### 开发回测的当前兼容范围

为避免重新实现 NautilusTrader 回测，暂时只支持现有 Engine 的开发租户队列：

- `strategy_ref` 必须是 `account:<account_id>`，表示**已经由 Platform 下发给该开发账户**的策略组合，不接受任意 Python 路径或生成代码。
- `dataset_refs` 当前恰好 1 个，格式 `futures_um:<symbol>:1m` 或 `futures_um:<symbol>:5m`，须与已下发策略的 symbol 一致。
- `purpose=development`、`execution=simulation` 固定；`parameters={}`；其它动态参数/多品种/期权回测尚需 Engine 契约升级。现有 366 天上限继续保留。
- 相同租户和研究任务的相同 `idempotency_key` 经有范围的哈希映射到旧队列幂等键。回测查询/取消会核验任务归属；跨任务不能因猜出 job ID 就读取结果。
- 当前队列运行中的回测不支持安全强制取消，仍返回 409；不伪造取消成功。

这限制的是**受控回测执行路径**，不是 Harness 在沙箱里探索算法、编写代码或分析授权数据的能力。


## 受限 AI 候选策略回测（首期实施）

`POST /research/v1/candidate-backtests` 已为显式授权的**开发研究任务**增加独立操作 `candidates:submit`。原 `/v1/backtests` 仍只回测已下发策略，互不改变。

候选包 ZIP（最多 64 KiB）**严格仅有** `seealpha-package.json` 与 `strategy.py`；前者限定 `format/name/title/version/entry_point=strategy:CANDIDATE`，后者**只允许**一条 Python 字面量赋值：

```python
CANDIDATE = {
    "kind": "ema_cross",
    "symbol": "ETHUSDT",
    "fast_period": 8,
    "slow_period": 21,
    "allocation": "1",
    "risk_limit": "1",
}
```

允许的 `kind`：`ema_cross`、`ema_revert`、`sma_cross`、`sma_revert`。仅一个 `futures_um:symbol:1m|5m`、固定开发目的、模拟模式、账户作用域、短时 Research Grant。Engine 在调用 `TaskCoordinator` 前重新解包、静态解析 Python AST 并校验 SHA-256；**绝不 import/exec/eval AI 源码**。Worker 复用 NautilusTrader、现有有界任务队列/取消/超时/幂等机制，但只创建 Engine 自己维护的 `CandidateSignal`。候选回测不写入已下发策略清单，也不自动进入生产。

请求包含 `candidate_id`（32 位小写十六进制）、`package_hash`（SHA-256）、`archive`（base64）、`strategy_ref`、`dataset_refs`、`start/end`、`starting_balance`、`quote_currency` 和 `idempotency_key`。查询结果复用 `GET /v1/backtests/{id}`，完成时返回关联的候选 ID/哈希/规范化规则与回测结果。相同幂等键不同规则会拒绝；研究任务不能读取其他任务回测。

**重要限制**：本期支持 AI 生成的**受限策略描述代码**，并不支持任意 Python 策略类或第三方依赖。现有回测 Worker 的子进程**不是完整的恶意代码沙箱**；只有受信任的 Engine 代码会被执行。要允许自由 Python，仍需后续独立最小权限容器、禁网、只读数据、独立 uid、资源限制和拒绝越权的实测验收。这里没有声称支持任意 Python。

## 与 Harness 的接入状态

现选定调用链是 Harness → Platform 任务级代理 → Engine Research API。Platform 已实现开发租户授权、短期能力与模拟回测命令，Harness 已提供受限 Python/CLI Client。无须部署 Gateway/MCP；Agent 不掌握 Engine 管理令牌或 Research Grant。

仍需真实开发容器、真实数据、AI 模型多轮策略研究的跨仓库端到端验收。Harness 自由 Shell 目前可读取自己的受限任务能力，不能视作不可信多租户的强隔离。
