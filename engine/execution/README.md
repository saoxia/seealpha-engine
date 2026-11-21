# SeeAlpha Execution domain

This crate owns **venue-neutral** order-state and quantity invariants:
valid identities, monotonic cumulative fills, terminal immutability and
fail-closed error handling. `VerifiedOrderState` is a single canonical
implementation, moved from the old Binance connector without changing
the state transition rules.

This crate **does not** implement a venue order client, the NautilusTrader OMS,
order routing, execution events, a durable intent journal, or a fill ledger.
Those responsibilities remain with their existing owners until proven
end-to-end native replacement.

Migration: `engine/runtime/src/fills.rs` uses this crate directly. The
old `seealpha_venue::lifecycle` path temporarily re-exports the types for
still-dependent modules; no duplicate implementation remains.

## Durable admission independent of Binance

`src/admission.rs` owns the venue-neutral **exactly-once-attempt** contract:
`Admission::Fresh/Seen`, `IntentJournal<Record, Error>`, recorded outcome,
and `dispatch_once` (admit → one send → verify ACK → persist observation).
Duplicate IDs never send. Timeouts, invalid ACKs and persistence failure all
remain unconfirmed; no blind retries are allowed. The journal must commit
immutable identity before granting `Fresh`, including after restart.

This does **not** recreate a NautilusTrader order router or WebSocket client.
The existing Binance-specific `PreparedWrite` still prepares exchange
paths/parameters and validates ACK fields; it now calls the single
`seealpha-execution::admission::dispatch_once` implementation. Runtime Ledger
still owns the journal's transaction and no real trading path is enabled.

