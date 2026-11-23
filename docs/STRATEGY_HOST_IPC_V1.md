> **历史设计文档（2026-11-23）**：Python strategy_host 已从当前仓库移除；本文协议没有运行中的实现，不是当前 Engine API。当前只支持 Rust 内置策略下发与只读预览。请以 `docs/ARCHITECTURE.md`、`engine/strategy/README.md` 和当前源码为准。

# Strategy Host IPC v1 (local stdio)

The Rust Engine is the sole trading runtime. Python is an **optional** strategy execution adapter, not an order router. This document specifies the bounded **local stdio JSON** boundary. It does not define a remote service, arbitrary package loading or a production Rust package issuance mechanism.

## Request

One JSON object is sent on the host's standard input, with a required `protocol_version: 1` for Rust callers. Existing direct legacy Python host callers that omit it remain temporarily supported.

```json
{
  "protocol_version": 1,
  "op": "run",
  "package_dir": "/app/packages/ema_crossover",
  "archive_path": null,
  "parameters": {"fast_period": 8, "slow_period": 21},
  "symbol": "ETHUSDT",
  "interval": "1m",
  "bars": [{"open_time_ms": 0, "open": "100", "high": "100", "low": "100", "close": "100", "volume": "1"}]
}
```

Exactly one of `package_dir` and `archive_path` must be set. `prepare` has no bars; `run` uses at most 5,000 strictly increasing bars. Prices, quantities and weights are decimal **strings**, never JSON floating point. The strategy can publish only target holdings, never orders.

## Response

```json
{
  "protocol_version": 1,
  "ok": true,
  "prepared": {"symbol": "ETHUSDT", "allocation": "1", "risk_limit": "1"},
  "publications": [{"symbol": "ETHUSDT", "bar_index": 0, "quantity": "1"}]
}
```

A publication has exactly one of `quantity` or `weight`; a weight is in [-1,1]. Failure responses use `{"protocol_version":1,"ok":false,"error":"..." }`. Extra fields, unknown versions and missing versions are rejected by Rust before accepting target holdings. Unsupported host request versions fail closed. A legacy Python-only caller that omits the request version can still receive the old unversioned response, but **Rust never accepts unversioned responses**.

## Trust and safety

The protocol version is not an authorization credential. Before any issued Python strategy runs, the runtime verifies the trusted Ledger issuance and requested package bytes/parameters, and `run_issued_host` independently checks the immutable archive SHA-256, metadata, tenant and account identity. Directory strategies do not gain privileges from tenant types; issued archives still require immutable authorization. Host subprocess I/O, timeout and memory bounds still apply. On unsupported protocol versions, no target is forwarded to portfolio/risk/ledger.

This is the first incremental envelope for future adapters; changing the wire schema requires a new version and migration/compatibility tests. Live exchange orders are not exposed to the strategy adapter.
