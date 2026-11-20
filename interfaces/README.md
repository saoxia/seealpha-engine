# External interfaces

- [api/http](api/http/): the existing REST HTTP v2 server and request validation; this is the active interface
- `mcp/`: a proposed future dedicated protocol boundary; existing research MCP endpoints currently remain implemented in `api/http`
- `websocket/`, `grpc/`: potential future adapters, not implemented by this migration

Do not present unimplemented protocols as working services. Shared business orchestration belongs to `application/runtime`.
