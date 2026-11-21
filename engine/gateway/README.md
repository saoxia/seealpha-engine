# Gateway / 对外协议入口

- [`api/`](api/)：`seealpha-http` crate，负责 HTTP v2、Research v1、路由、认证边界和已有 HTTP 监听入口。
- [`mcp/`](mcp/)：Research MCP 协议处理与工具分发源码，和 API 分属独立目录；复用 API 研究授权与 Runtime 任务，不重复实现业务逻辑。

当前 `POST /research/mcp` 仍由同一个 HTTP 服务注册，需显式启用 `ENGINE_RESEARCH_MCP_ENABLED`。`mcp/` 不是独立 Cargo crate、进程或端口，也不应被描述为一个已部署的独立服务。未来如需独立服务，须另外设计身份、安全和生命周期边界。
