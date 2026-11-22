//! SeeAlpha target, virtual order and execution coordination.
//! This crate does not submit venue orders or authenticate exchange data.
pub mod target;
pub mod order;
pub mod execution;

// Preserve existing public import paths while reorganizing implementation.
pub use target::*;
pub use order::{coordinator, coordinator_snapshot};
pub use execution::{basket, journal, order_evidence, phase_gate, risk, settlement};
