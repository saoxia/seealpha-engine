# Application services

This directory is the **target boundary** for shared use-case orchestration, not a second trading runtime.

Intended call path: `interfaces/{api,mcp,...}` → `application` → domain services.

Existing implementation: `crates/seealpha-runtime` owns application orchestration today. Do not duplicate trading state, bypass admission gates, or move order reconciliation without integration tests. Migration status: **planned, not yet relocated**.

Typical use cases: start/stop strategy, run paper session, query positions, start backtest, reconcile order by verified evidence. All entry points must share authorization, idempotency and audit behavior.
