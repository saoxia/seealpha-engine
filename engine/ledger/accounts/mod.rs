//! Accounts — account state observations and snapshot provenance.
//!
//! Nautilus Account / AccountState remains the preferred runtime account model.
//! The persisted exchange snapshot is evidence, not a second balance engine.
mod account_snapshots;
pub use account_snapshots::AccountSnapshot;
