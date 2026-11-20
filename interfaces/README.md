# Interfaces

External protocol adapters belong here, separate from trading logic and process bootstrapping.

- `api/`: REST/HTTP v2 (currently implemented in `crates/seealpha-http`)
- `mcp/`: Model Context Protocol adapter (do not assume a standalone MCP server already exists)
- `websocket/`: streaming adapter (future, only when supported)
- `grpc/`: RPC adapter (future, only when supported)

Current research MCP route lives in the HTTP crate; preserve its existing auth/contract gates. Interface adapters should call shared application services instead of duplicating portfolio, ledger, risk or execution rules. Migration status: **target topology, current code remains in the original Cargo crates**.
