//! Exchange order lifecycle validation, independent of transport and product.
use crate::OrderStateError;
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderPhase {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
    Unknown,
}

impl OrderPhase {
    pub fn parse(status: &str) -> Result<Self, OrderStateError> {
        match status {
            "NEW" | "ACCEPTED" => Ok(Self::New),
            "PARTIALLY_FILLED" => Ok(Self::PartiallyFilled),
            "FILLED" => Ok(Self::Filled),
            "CANCELED" | "CANCELLED" => Ok(Self::Canceled),
            "REJECTED" => Ok(Self::Rejected),
            "EXPIRED" | "EXPIRED_IN_MATCH" => Ok(Self::Expired),
            "UNKNOWN" => Ok(Self::Unknown),
            _ => Err(OrderStateError::Unconfirmed),
        }
    }
    pub fn terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Canceled | Self::Rejected | Self::Expired
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedOrderState {
    pub client_order_id: String,
    pub exchange_order_id: String,
    pub phase: OrderPhase,
    pub filled_quantity: Decimal,
    pub original_quantity: Decimal,
}

fn amount(value: &Value) -> Result<Decimal, OrderStateError> {
    let raw = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_u64().map(|x| x.to_string()))
        .ok_or(OrderStateError::Unconfirmed)?;
    Decimal::from_str(&raw).map_err(|_| OrderStateError::Unconfirmed)
}
impl VerifiedOrderState {
    /// No fabricated FILLED state: exchange identities and cumulative quantities
    /// must agree with the actual response.
    pub fn from_exchange(order: &Value) -> Result<Self, OrderStateError> {
        let client_order_id = order
            .get("client_order_id")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .ok_or(OrderStateError::Unconfirmed)?
            .to_owned();
        let exchange_order_id = order
            .get("order_id")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .ok_or(OrderStateError::Unconfirmed)?
            .to_owned();
        let phase = OrderPhase::parse(
            order
                .get("status")
                .and_then(Value::as_str)
                .ok_or(OrderStateError::Unconfirmed)?,
        )?;
        let filled_quantity = amount(order.get("filled").ok_or(OrderStateError::Unconfirmed)?)?;
        let original_quantity = amount(order.get("quantity").ok_or(OrderStateError::Unconfirmed)?)?;
        if original_quantity <= Decimal::ZERO
            || filled_quantity < Decimal::ZERO
            || filled_quantity > original_quantity
            || (phase == OrderPhase::Filled && filled_quantity != original_quantity)
            || (phase == OrderPhase::New && !filled_quantity.is_zero())
            || (phase == OrderPhase::PartiallyFilled
                && (filled_quantity.is_zero() || filled_quantity == original_quantity))
        {
            return Err(OrderStateError::Unconfirmed);
        }
        Ok(Self {
            client_order_id,
            exchange_order_id,
            phase,
            filled_quantity,
            original_quantity,
        })
    }

    /// A stale websocket update cannot move the order backwards. A terminal
    /// order cannot become open or change the terminal reason. An exchange
    /// correction requires explicit investigation instead of automatic overwrite.
    pub fn advance(&self, newer: &Self) -> Result<(), OrderStateError> {
        if self.client_order_id != newer.client_order_id
            || self.exchange_order_id != newer.exchange_order_id
            || self.original_quantity != newer.original_quantity
            || newer.filled_quantity < self.filled_quantity
        {
            return Err(OrderStateError::Unconfirmed);
        }
        if self.phase == OrderPhase::Unknown || newer == self {
            return Ok(());
        }
        if newer.phase == OrderPhase::Unknown || self.phase.terminal() {
            return Err(OrderStateError::Unconfirmed);
        }
        if newer.phase == OrderPhase::New && self.phase != OrderPhase::New {
            return Err(OrderStateError::Unconfirmed);
        }
        if newer.phase == OrderPhase::PartiallyFilled
            && newer.filled_quantity <= self.filled_quantity
        {
            return Err(OrderStateError::Unconfirmed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn order(status: &str, filled: &str) -> VerifiedOrderState {
        VerifiedOrderState::from_exchange(&json!({
            "client_order_id":"client","order_id":"123","status":status,
            "quantity":"2","filled":filled,
        }))
        .unwrap()
    }
    #[test]
    fn partial_fill_can_finish_or_cancel_but_not_regress() {
        let partial = order("PARTIALLY_FILLED", "1");
        assert!(partial.advance(&order("FILLED", "2")).is_ok());
        assert!(partial.advance(&order("CANCELED", "1")).is_ok());
        assert!(partial.advance(&order("NEW", "0")).is_err());
        assert!(
            order("FILLED", "2")
                .advance(&order("CANCELED", "2"))
                .is_err()
        );
    }
    #[test]
    fn inconsistent_exchange_state_fails_closed() {
        assert!(
            VerifiedOrderState::from_exchange(&json!({
            "client_order_id":"c","order_id":"1","status":"FILLED",
            "quantity":"2","filled":"1"}))
            .is_err()
        );
        assert!(
            VerifiedOrderState::from_exchange(&json!({
            "client_order_id":"c","order_id":"1","status":"NEW",
            "quantity":"2","filled":"1"}))
            .is_err()
        );
    }
}
