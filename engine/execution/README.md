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
