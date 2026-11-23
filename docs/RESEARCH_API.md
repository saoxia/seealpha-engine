# Engine Research HTTP v1（当前能力）

Research 与 Engine 管理 API 使用同一 HTTP 服务，但采用独立的短期签名 Research Grant。设置 `ENGINE_RESEARCH_SIGNING_KEY`（至少 32 字符且不能复用管理令牌）并绑定 `ENGINE_TENANT_ID` 后才会挂载 `/research/v1/*`。

## 当前路由

| 请求 | 路径 | 当前行为 |
| --- | --- | --- |
| GET | `/research/v1/datasets` | 按 Grant 的 `datasets:read` 白名单列出真实 Catalog 库存；未挂载 Catalog 时明确失败 |
| POST | `/research/v1/snapshots` | `snapshots:create` 权限校验与参数校验后明确返回 501；未创建任务/文件 |
| GET | `/research/v1/snapshots/{snapshot_id}` | `snapshots:read` 权限校验后明确返回 501 |
| GET | `/research/v1/snapshots/{snapshot_id}/content` | `snapshots:read` 权限校验后明确返回 501 |

允许的 Research Grant 操作只有 `datasets:read`、`snapshots:create`、`snapshots:read`。HTTP 请求需提交 `X-SeeAlpha-Engine-Contract-Version: 2` 和 `X-SeeAlpha-Research-Contract-Version: 1`。Research Bearer Grant 不能代替 Engine 管理令牌，二者权限互不通用。

## 回测能力

已移除历史回测、资金费/期权组合回测、研究候选回测和 Python 候选回测的实现及入口。当前 **没有** `/research/v1/backtests*`、候选回测路由、回测任务队列或回测结果读取能力。独立的 Catalog 与 Research 数据集发现并非回测执行能力。未来重新设计回测时不沿用这些已删除的契约。

可选 MCP 入口仅提供 `list_datasets`；参见 [MCP_RESEARCH.md](MCP_RESEARCH.md)。
