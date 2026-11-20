# Application entrypoints

Executable targets belong under `apps/`, while protocol implementations live under `interfaces/` and shared orchestration under `application/`.

- `trading_engine/`: existing `seealpha-engine` binary in `crates/seealpha-engine`; keep its binary name and startup options
- `backtest_cli/`: future dedicated CLI, no implemented binary implied
- `api_server/`: optional separate HTTP process; currently part of the engine binary
- `mcp_server/`: optional separate MCP process; not yet implemented

These are **planned locations**, not runnable packages. The current workspace and deployment Dockerfile are unchanged until a tested path relocation is available.
