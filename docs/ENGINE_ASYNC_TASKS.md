# Tenant Engine统一异步任务契约

## 范围

Engine Docker 为每个租户维护独立的任务记录。业务 API 通过 HTTP 向 Engine
提交任务、查询进度、取消和获取结果，不在 HTTP 连接内等待回测结束。

- `POST /tenants/{tenant_id}/jobs`：`kind=backtest|data`，返回 202 任务快照；
  可以提供 `idempotency_key`（1-128 个安全字符，作用域为租户及任务类型），重复请求返回同一 ID，
  同键不同参数返回 409。
- `GET /tenants/{tenant_id}/jobs`：返回 `items`，合并回测和既有行情任务。
- `GET /tenants/{tenant_id}/jobs/{id}`：统一任务快照。
- `GET /tenants/{tenant_id}/jobs/{id}/result`：成功后返回任务结果；未成功返回 409。
- `POST /tenants/{tenant_id}/jobs/{id}/cancel`：取消排队中及运行中的回测 Worker；
  运行中先返回 `CANCELLING`，子进程实际退出后才成为 `CANCELLED`；数据任务继续使用原进程取消器。
- `POST /tenants/{tenant_id}/jobs/{id}/retry`：显式重试失败、取消、中断的回测，
  并保留已有数据任务仅允许历史补齐重试的安全约束。

业务后台提供：
`POST /api/tenants/{tenant_id}/accounts/{account_id}/backtests/async` 及
`GET /api/tenants/{tenant_id}/jobs`、`.../jobs/{id}`、`.../result`、
`.../cancel`、`.../retry`，要求管理员登录及租户授权。

## 生命周期

统一状态：`PENDING → RUNNING → SUCCEEDED|FAILED`；
`PENDING → CANCELLED`；`RUNNING → CANCELLING → CANCELLED`。
API 返回 `CANCELLING` 仅代表取消请求已登记，**不是确认进程已退出**；
只有等待 Worker 进程被回收后才报告 `CANCELLED`。
Engine 意外重启时未完成的 `PENDING/RUNNING/CANCELLING` 标记为
`INTERRUPTED`，**不会自动执行第二次**。经用户明确调用 retry 后才生成新任务 ID。

历史行情任务保留独立的子进程执行器、日志、超时处理和删除快照校验。
统一 API 将状态投影为 `RUNNING/SUCCEEDED/FAILED/CANCELLED/INTERRUPTED/UNKNOWN`；
`UNKNOWN` 表示数据任务所有权未确认，不会擅自重试。

回测任务的参数、状态、幂等键和创建时间存于 Engine 原有的
`book.sqlite`（WAL）的 `backtest_tasks` 表，使用租户范围内的唯一幂等键约束。
大体量结果仍原子写入 `tasks/<tenant_id>/<task_id>.result.json`，
只经专门结果接口读取，不暴露任意文件路径。

首次启动时，旧的 `*.record.json` 经身份核验后导入 SQLite，并在持久化
成功后重命名为 `*.record.json.migrated` 保留审计副本；之后不再重复导入。
恢复时仅将未确认完成的 `PENDING/RUNNING/CANCELLING` 标记为 `INTERRUPTED`，
不得自动重复执行。迁移前应备份状态卷，正式部署仍需独立验收升级与回滚。
每个 Engine 最多等待 16 个回测，另有最多 `ENGINE_BACKTEST_MAX_WORKERS` 个
**独立 Python 子进程**并行执行，默认 1，允许 1–8；子进程复用已有 Engine
Docker 镜像和本地 Catalog，不会为每次回测创建新 Docker 容器。
`ENGINE_BACKTEST_TIMEOUT_SECONDS` 默认 3600 秒，范围 1–86400 秒。
新增 `GET/PUT /tenants/{tenant_id}/jobs/worker-settings`，必须经过 Engine Token 认证；
业务管理后台再经管理员会话转发。PUT 只接受 `max_workers` 和
`timeout_seconds` 两个整数字段。首次没有持久化设置时使用以上环境默认值；
保存后写入该 Engine 数据卷的 `book.sqlite`，**优先于环境默认值且重启保留**。
提高 Worker 数量可即时启用新调度线程；降低数量不会强行终止已运行任务，
只限制随后新启动的任务；修改执行超时仅影响新 Worker，不追溯已有任务。
达到运行时限会终止子进程组并将任务标为 `FAILED`，可由用户显式重试。
子进程在独立会话中运行，取消优先发 SIGTERM，短暂宽限后升级为 SIGKILL。
日志保存于私有 `tasks/<tenant_id>/<task_id>.worker.log`；父进程核对
子进程退出码和结果归属后才持久化成功状态。

## 限制与安全边界

- 正在运行的回测可通过现有取消接口终止对应 Worker 进程组，**不会终止
  Engine HTTP 主进程**。进程尚未确认退出时状态保持 `CANCELLING` 或
  `INTERRUPTED`，绝不虚报 `CANCELLED`。
- 回测任务仅允许开发租户且须已下发策略；正式租户不能提交研究回测。
- 数据任务继续遵守原有删除前预览/快照确认；统一接口不会绕过这些约束。
- 当前 Engine 以每租户单进程 Docker 部署为前提；如果未来一个租户启动多个
  Engine 写入者，需要再引入跨进程队列租约。
- 任务存储与参数不包含交易所凭证；Worker 仅继承白名单环境变量，不获得
  `ENGINE_TOKEN`、研究授权或交易所环境凭据；未知异常不会作为公开错误详情返回。
- 子进程隔离不是不可信 Python 代码的完整安全沙箱；Worker 仍可看到其
  Docker 内的挂载目录，运行未知代码须另行使用更强的容器/OS 沙箱。
  最大 Worker **数量**不等于逐 Worker 内存或 CPU 配额；应结合 Docker 的资源限制。
- 策略包动态验证、AI Harness、SSE 日志流不在本次范围内；
  后续可直接按同一任务契约增加 `kind=strategy_validation`。

## 操作示例

```json
{
  "kind": "backtest",
  "idempotency_key": "ema-2024-test-01",
  "parameters": {
    "account_id": "12",
    "start": "2024-01-01",
    "end": "2024-02-01",
    "interval": "1m",
    "starting_balance": "1000",
    "quote_currency": "USDT"
  }
}
```

正常返回 `202`、任务 ID 和 `PENDING/RUNNING` 状态。
随后轮询查询状态；`SUCCEEDED` 后请求专门结果接口。
