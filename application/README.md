# Application services

The active Rust application orchestration crate is [runtime](runtime/).

Interfaces call shared application operations rather than duplicating trading logic.
Current runtime responsibilities include paper sessions, account state, verified order reconciliation, strategy submission and data jobs. The runtime is **not** a production live-trading approval.

Desired call direction: `interfaces/*` → `application/runtime` → domain crates. The current HTTP adapter is not yet fully refactored to thin use-case handlers; that is a separate behavioral refactor, not part of this directory relocation.

Keep tenant authorization, idempotency, risk limits, evidence-first reconciliation and audit behavior consistent across all protocols.
