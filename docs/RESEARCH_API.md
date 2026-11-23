# Engine Research HTTP v1（当前接口）

`GET /research/v1/datasets`：列出已挂载 Catalog 中可确认的库存，返回 `datasets`，每个记录补充 `product:symbol:level` 格式的 `id`。无 Catalog 时返回明确错误，不虚构数据集；不会启动采集任务。

Research 授权 Grant（签名、过期、任务/租户及操作白名单）与 `ENGINE_RESEARCH_SIGNING_KEY` 已取消。该只读请求无需 Token。未实现的 Parquet 快照端点、候选回测/异步任务端点不挂载，不暗示已支持。

服务默认监听 loopback；如显式允许非 loopback 监听，必须由运维在外部控制访问，不可公开暴露。

可选 `POST /research/mcp` 提供同一只读查询，详见 [MCP Research](MCP_RESEARCH.md)。
