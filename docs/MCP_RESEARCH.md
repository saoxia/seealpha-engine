# Engine 原生 MCP Research 接口（可选）

SeeAlpha Engine 在现有独立 `/research/v1/*` HTTP Research API 之外，可以**显式启用**
一个 JSON-only、无会话的 **MCP Streamable HTTP** 适配层。其用途是供已获任务授权的
AI 研究客户端发现和调用 Engine 研究工具，**不是交易管理 API，也不替代 Platform**。

## 默认关闭与部署约束

- 默认 `ENGINE_RESEARCH_MCP_ENABLED=false`。启用时需同时配置独立的
  `ENGINE_RESEARCH_SIGNING_KEY`（至少 32 字符，不可复用 Engine 管理令牌）、
  `ENGINE_TENANT_ID` 和 `ENGINE_TENANT_PURPOSE=development`；
  还需该租户在 Engine 运行状态中实际处于 `active`。
- 入口为同一 Engine 端口下的 **`POST /research/mcp`**。
  标准协议为 `2025-11-25` 的 Streamable HTTP，支持 JSON-RPC 单响应；
  不开放独立 SSE GET 流、MCP session、stdio 或自动服务发现。
  `GET /research/mcp` 和 `DELETE /research/mcp` 返回 405。
- MCP 请求需要 `Authorization: Bearer <short-lived-research-grant>`，
  由可信 Platform 在验证用户、租户、任务、数据集和策略权限后签发。
  Engine 每次独立验签、检查任务/租户/用途/Grant TTL，并在调用工具时再次执行
  原 Research API 的具体权限检查。
- 原来的 HTTP API 仍使用 `X-SeeAlpha-Engine-Contract-Version: 2` 和
  `X-SeeAlpha-Research-Contract-Version: 1`；MCP 通过自己的
  `MCP-Protocol-Version` 协商，不要求重复发送两个业务 HTTP 版本头。
- 仅受信任服务间内网访问，拒绝带任何 `Origin` 头的浏览器请求；
  没有配置 CORS。容器/宿主机网络须限制可访问方，必要时配置可信 TLS 终止。
  **不可将研究 Grant 注入不可信 Agent Shell 或浏览器。**

Docker 镜像继续默认只暴露既有端口 `8766`。新增开关不会自动修改既有
租户容器、凭据或环境文件；需要在显式审核并重建隔离开发容器后才可启用。
生产租户不能开启 MCP。不存在 MCP 到生产下单、交易所凭据或通用 Engine 管理 API
的工具映射。

## 可调用工具

工具列表按请求中 Research Grant 的 `ops` 权限过滤。工具调用继续检查
数据集、策略和任务归属，不因发现工具而扩大权限。

| MCP 工具 | Research Grant 操作 | 原有处理路径 |
| --- | --- | --- |
| `list_datasets` | `datasets:read` | `GET /v1/datasets` |
| `create_snapshot` | `snapshots:create` | `POST /v1/snapshots` |
| `get_snapshot` | `snapshots:read` | `GET /v1/snapshots/{id}` |
| `submit_backtest` | `backtests:submit` | `POST /v1/backtests` |
| `get_backtest` | `backtests:read` | `GET /v1/backtests/{id}` |
| `cancel_backtest` | `backtests:cancel` | `POST /v1/backtests/{id}/cancel` |
| `submit_candidate_backtest` | `candidates:submit` | `POST /v1/candidate-backtests` |
| `submit_python_candidate_backtest` | `python_candidates:submit` | `POST /v1/python-candidate-backtests` |

最后一项仅在另行启用 `ENGINE_RESEARCH_PYTHON_ENABLED=true` 时可发现。
它会调用**已有的开发回测 Worker**，但不提供恶意 Python 的强沙箱，
不可对不可信租户开放。受限候选策略仍进行原有 AST 和包哈希静态校验。

任务提交只返回排队收据；查询仍需显式轮询，未知结果不自动重试。
大文件 Parquet **不通过 MCP 返回 base64**，仍使用已有受保护的
`GET /research/v1/snapshots/{id}/content`，由可信客户端按 SHA-256
及长度验证。MCP 不提供任意本地路径、URL、Shell 执行或文件读取工具。

## 协议调用示例

以下示例中的 `$RESEARCH_GRANT` 必须由可信 Platform 为开发租户和当前任务签发，
**不要**把令牌写入 Agent Prompt、命令历史、日志或模型上下文。

```bash
curl --fail-with-body http://127.0.0.1:8766/research/mcp \
  -H "Authorization: Bearer $RESEARCH_GRANT" \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2025-11-25' \
  --data '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"trusted-client","version":"1"}}}'
```

初始化后，使用同样的身份与协议头发送
`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`，然后以
`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_datasets","arguments":{}}}`
调用工具。服务器返回 `structuredContent` 和兼容 MCP 文本客户端的 JSON 文本
`content`；业务执行错误以 `isError=true` 返回，不暴露原始内部错误消息。

## 与 Harness 的关系

当前 `saoxia/seealpha-harness` 默认使用
**Harness → Platform 任务研究命令代理 → Engine HTTP**，
Harness Worker 只得到 Platform 的任务级 Capability，**没有 Engine Research Grant**。
本次仅为 Engine 增加可选原生 MCP，不会静默绕开 Platform/给 Harness 发 Engine Grant，
也不改变其默认 CLI。

如果后续 Harness 改用 MCP，应优先由 Platform 暴露任务级 MCP 代理，
或由可信服务向隔离的 MCP Client 授权，并确保 Engine Grant 不可被自由 Shell
读取。需要独立验证端到端调用、取消、重试、网络失效、身份归属和资源预算，
才能完成 Harness 的生产调用链切换。

自动化测试使用合成 Runtime/TaskCoordinator 和 TestClient，不启动交易所、
真实模型、生产数据、实盘账户或容器。真实 MCP SDK 客户端联调及 Docker 验收另行执行。

相关文档：[Research API](RESEARCH_API.md)、
[SeeAlpha Platform AI 研究命令](https://github.com/saoxia/SeeAlpha/blob/main/docs/AI_RESEARCH_COMMANDS.md)、
[MCP Streamable HTTP 规范](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)。
