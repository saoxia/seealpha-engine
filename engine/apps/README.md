# Process entrypoints

- [trading_engine](trading_engine/)：当前唯一的 Engine HTTP 二进制程序 `seealpha-engine`，包含明确的 smoke 子命令。
- Binance 数据源的独立维护进程入口在 [pipeline/sources/shared/archive/src/bin](../pipeline/sources/shared/archive/src/bin/)；对应 crate 的 `Cargo.toml` 显式列出 binary name。
- `backtest_cli`、`api_server`、`mcp_server` 仅为可能的未来拆分方向，目前没有对应可执行目标。

服务当前路由仍由 `gateway/api` 负责；不要假定 MCP、WebSocket 或 gRPC 已部署独立服务。
