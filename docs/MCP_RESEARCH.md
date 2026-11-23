# Engine 可选 Research MCP

设 `ENGINE_RESEARCH_MCP_ENABLED=true` 启用 `POST /research/mcp`；默认关闭。只有一个工具：`list_datasets`，使用 Catalog 只读库存，不创建任务、不提交订单、不启动行情采集。

适配器实现 JSON-only Streamable HTTP 单次 JSON-RPC 响应，MCP 协议版本 `2025-11-25`。拒绝携带浏览器 `Origin` 的请求，限制 Content-Type、Accept、请求体大小和协议版本；这不是认证机制。

不再存在签名 Research Grant、`ENGINE_TENANT_ID` 或 `ENGINE_RESEARCH_SIGNING_KEY`。接口只适合可信的本机或受控网络，**不要直接暴露公网**。

```bash
curl http://127.0.0.1:8766/research/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'MCP-Protocol-Version: 2025-11-25' \
  --data '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

`GET /research/v1/datasets` 可用于不需要 MCP 协议的客户端。
