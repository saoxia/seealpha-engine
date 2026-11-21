//! SeeAlpha execution-domain invariants; no transport, credentials or venues.
//! This module is intentionally kept distinct from NautilusTrader execution
//! clients: it owns only SeeAlpha's fail-closed business lifecycle assertions.
pub mod lifecycle;
pub mod admission;

use thiserror::Error;

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum OrderStateError {
    #[error("交易所订单状态或数量尚未可靠确认。")]
    Unconfirmed,
}
