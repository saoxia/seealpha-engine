# Engine Research MCP（可选）

该入口是 `engine/gateway/api` 同一 HTTP 进程中的 JSON-only、无会话 MCP 适配。默认关闭；显式配置 `ENGINE_RESEARCH_MCP_ENABLED=true`、独立的 `ENGINE_RESEARCH_SIGNING_KEY` 和绑定租户 `ENGINE_TENANT_ID` 后，挂载 `POST /research/mcp`。

客户端必须提供由可信后端签发的短期 `Authorization: Bearer <research-grant>`，以及 JSON-RPC 请求。请求会校验任务范围、租户绑定和 Grant 有效期；拒绝携带 `Origin` 头的浏览器请求。该入口不会签发 Grant，也不能向交易所提交订单。

## 当前工具

| 工具 | Grant 操作 | 行为 |
| --- | --- | --- |
| `list_datasets` | `datasets:read` | 列出当前任务获准查看的真实 Catalog 数据集 |

当前没有快照创建工具，也没有提交、查询、取消回测或候选回测的工具。

## 示例

```bash
curl --fail-with-body http://127.0.0.1:8766/research/mcp \
  -H "Authorization: Bearer $RESEARCH_GRANT" \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2025-11-25' \
  --data '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"trusted-client","version":"1"}}}'
```

同一授权下可调用 `tools/list` 和 `tools/call`（工具名 `list_datasets`）。MCP 不具备独立进程隔离，Research Grant 不应暴露给不可信 Shell 或浏览器。完整 HTTP 数据集接口见 [RESEARCH_API.md](RESEARCH_API.md)。
