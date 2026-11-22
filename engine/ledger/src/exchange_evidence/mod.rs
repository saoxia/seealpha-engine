//! Authenticated account and fill evidence, kept separate from raw Nautilus events.
//! Exchange cash settlement must not be inferred from market observations.
mod account_events;
mod account_snapshots;
mod verified_fills;
mod settlements;
pub use account_snapshots::AccountSnapshot;
