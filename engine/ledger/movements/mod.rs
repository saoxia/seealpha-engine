//! Movements — authenticated external economic events and their evidence.
//!
//! These inputs do not form a double-entry cash ledger. A native order ACK or
//! unverified execution report cannot create a verified exchange movement.
mod account_events;
mod verified_fills;
mod settlements;
