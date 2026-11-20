# Process entrypoints

- [trading_engine](trading_engine/): active `seealpha-engine` Rust executable and HTTP service entrypoint
- `backtest_cli/`, `api_server/`, `mcp_server/`: potential separate binaries, not yet implemented

The existing binary name `seealpha-engine`, startup flags, port and HTTP v2 contract are unchanged.
