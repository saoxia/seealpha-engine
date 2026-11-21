# External interfaces

- [api/http](api/http/) 是当前活跃 HTTP v2/Research v1 服务。
- `api/http/src/lib.rs` 维护路由总表、鉴权中间件、错误/DTO 类型和入口；`api/http/src/routes/` 根据 system、strategies、orders、market_data、accounts、backtests 拆分现有处理函数。
- `parity.rs`、`derivatives.rs`、`research_v1.rs` 保留特殊接口实现。
- 独立 MCP、WebSocket、gRPC 服务并未实现；部分 MCP-compatible 路由仍挂载在现有 HTTP 服务。

维持外部契约不变，业务编排仍归属 `runtime`。
