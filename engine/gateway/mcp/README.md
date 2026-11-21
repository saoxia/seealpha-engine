# Research MCP gateway

`src/research.rs` owns the MCP protocol handler and tool dispatch. It is a separate directory from `../api/`, but the handler is compiled as a module of `seealpha-http` using a Rust `#[path]` declaration so that it can reuse the current research authorization and task operations without duplicating them.

`gateway/api` mounts the existing `POST /research/mcp` route only when `ENGINE_RESEARCH_MCP_ENABLED=true`. This is not an independent Cargo crate, service process, listener, or new security boundary. HTTP Research v1, grant verification, startup and middleware remain unchanged; see [`docs/MCP_RESEARCH.md`](../../../docs/MCP_RESEARCH.md).
