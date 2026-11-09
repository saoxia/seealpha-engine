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
- `POST /tenants/{tenant_id}/jobs/{id}/cancel`：取消排队中的回测，或交给数据任务原有进程取消器。
- `POST /tenants/{tenant_id}/jobs/{id}/retry`：显式重试失败、取消、中断的回测，
  并保留已有数据任务仅允许历史补齐重试的安全约束。

业务后台提供：
`POST /api/tenants/{tenant_id}/accounts/{account_id}/backtests/async` 及
`GET /api/tenants/{tenant_id}/jobs`、`.../jobs/{id}`、`.../result`、
`.../cancel`、`.../retry`，要求管理员登录及租户授权。

## 生命周期

统一状态：`PENDING → RUNNING → SUCCEEDED|FAILED`；
`PENDING → CANCELLED`；Engine 意外重启时未完成的回测标记为
`INTERRUPTED`，**不会自动执行第二次**。经用户明确调用 retry 后才生成新任务 ID。

历史行情任务保留独立的子进程执行器、日志、超时处理和删除快照校验。
统一 API 将状态投影为 `RUNNING/SUCCEEDED/FAILED/CANCELLED/INTERRUPTED/UNKNOWN`；
`UNKNOWN` 表示数据任务所有权未确认，不会擅自重试。

回测任务的参数、任务状态、结果分别存于该 Engine 状态卷的
`tasks/<tenant_id>/<task_id>.record.json` 和 `*.result.json`；
原子写入，不暴露任意文件路径。大体量结果只经专门结果接口读取。
每个 Engine 最多排队 16 个回测，仅一个回测 worker 同时执行。
这保留 Engine 本地 Catalog 和会话，避免长连接/重复容器初始化。

## 限制与安全边界

- **运行中的回测尚无法安全强制终止**：取消返回 409，不虚报已取消。
  未来需要受限执行子进程及强制超时机制，本轮不冒险终止 Engine 主进程。
- 回测任务仅允许开发租户且须已下发策略；正式租户不能提交研究回测。
- 数据任务继续遵守原有删除前预览/快照确认；统一接口不会绕过这些约束。
- 当前 Engine 以每租户单进程 Docker 部署为前提；如果未来一个租户启动多个
  Engine 写入者，需要再引入跨进程队列租约。
- 任务存储与参数不包含交易所凭证；未知异常不会作为公开错误详情返回。
- 策略包动态验证、AI Harness、SSE 日志流及强制超时不在本次范围内；
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
