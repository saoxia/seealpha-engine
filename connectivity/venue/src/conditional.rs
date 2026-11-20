//! USD-M Algo Service conditional orders. These are NOT regular futures
//! orders; triggering an algo generates a separate actualOrderId.
use crate::{Credentials, VenueClient, VenueError, check_credentials, venue_symbol};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UmConditionalIntent {
    pub symbol: String,
    pub client_algo_id: String,
    pub side: String,
    pub order_type: String,
    pub quantity: Option<String>,
    pub trigger_price: Option<String>,
    pub limit_price: Option<String>,
    pub activate_price: Option<String>,
    pub callback_rate: Option<String>,
    pub position_side: String,
    pub working_type: String,
    pub time_in_force: String,
    pub reduce_only: bool,
    pub close_position: bool,
}

fn positive(raw: &str) -> Result<Decimal, VenueError> {
    let value = Decimal::from_str(raw).map_err(|_| VenueError::Unconfirmed)?;
    if value <= Decimal::ZERO {
        return Err(VenueError::Unconfirmed);
    }
    Ok(value)
}
fn valid_id(raw: &str) -> bool {
    !raw.is_empty()
        && raw.len() <= 36
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'/' | b':'))
}
impl UmConditionalIntent {
    pub fn signed_pairs(&self) -> Result<Vec<(String, String)>, VenueError> {
        let symbol = venue_symbol(&self.symbol)?;
        if !valid_id(&self.client_algo_id)
            || !matches!(self.side.as_str(), "BUY" | "SELL")
            || !matches!(self.position_side.as_str(), "BOTH" | "LONG" | "SHORT")
            || !matches!(self.working_type.as_str(), "MARK_PRICE" | "CONTRACT_PRICE")
            || !matches!(self.time_in_force.as_str(), "GTC" | "IOC" | "FOK" | "GTX")
        {
            return Err(VenueError::Unconfirmed);
        }
        let trailing = self.order_type == "TRAILING_STOP_MARKET";
        let limit = matches!(self.order_type.as_str(), "STOP" | "TAKE_PROFIT");
        if !trailing
            && !limit
            && !matches!(
                self.order_type.as_str(),
                "STOP_MARKET" | "TAKE_PROFIT_MARKET"
            )
        {
            return Err(VenueError::Unconfirmed);
        }
        if self.close_position {
            if !matches!(
                self.order_type.as_str(),
                "STOP_MARKET" | "TAKE_PROFIT_MARKET"
            ) || self.quantity.is_some()
                || self.reduce_only
            {
                return Err(VenueError::Unconfirmed);
            }
        } else {
            positive(self.quantity.as_deref().ok_or(VenueError::Unconfirmed)?)?;
        }
        if self.position_side != "BOTH" && self.reduce_only {
            return Err(VenueError::Unconfirmed);
        }
        if self.close_position
            && self.position_side != "BOTH"
            && ((self.side == "BUY" && self.position_side == "LONG")
                || (self.side == "SELL" && self.position_side == "SHORT"))
        {
            return Err(VenueError::Unconfirmed);
        }
        if trailing {
            if self.trigger_price.is_some() || self.limit_price.is_some() {
                return Err(VenueError::Unconfirmed);
            }
            let rate = positive(
                self.callback_rate
                    .as_deref()
                    .ok_or(VenueError::Unconfirmed)?,
            )?;
            if rate < Decimal::new(1, 1) || rate > Decimal::new(20, 0) {
                return Err(VenueError::Unconfirmed);
            }
            if let Some(raw) = &self.activate_price {
                positive(raw)?;
            }
        } else {
            positive(
                self.trigger_price
                    .as_deref()
                    .ok_or(VenueError::Unconfirmed)?,
            )?;
            if self.callback_rate.is_some() || self.activate_price.is_some() {
                return Err(VenueError::Unconfirmed);
            }
            if limit {
                positive(self.limit_price.as_deref().ok_or(VenueError::Unconfirmed)?)?;
            } else if self.limit_price.is_some() {
                return Err(VenueError::Unconfirmed);
            }
        }
        let mut params = vec![
            ("algoType".into(), "CONDITIONAL".into()),
            ("symbol".into(), symbol),
            ("side".into(), self.side.clone()),
            ("type".into(), self.order_type.clone()),
            ("clientAlgoId".into(), self.client_algo_id.clone()),
            ("positionSide".into(), self.position_side.clone()),
            ("workingType".into(), self.working_type.clone()),
        ];
        if let Some(raw) = &self.quantity {
            params.push(("quantity".into(), raw.clone()));
        }
        if let Some(raw) = &self.trigger_price {
            params.push(("triggerPrice".into(), raw.clone()));
        }
        if let Some(raw) = &self.limit_price {
            params.push(("price".into(), raw.clone()));
        }
        if let Some(raw) = &self.activate_price {
            params.push(("activatePrice".into(), raw.clone()));
        }
        if let Some(raw) = &self.callback_rate {
            params.push(("callbackRate".into(), raw.clone()));
        }
        if limit {
            params.push(("timeInForce".into(), self.time_in_force.clone()));
        }
        if self.close_position {
            params.push(("closePosition".into(), "true".into()));
        } else if self.position_side == "BOTH" && self.reduce_only {
            params.push(("reduceOnly".into(), "true".into()));
        }
        Ok(params)
    }
}

pub fn validated_algo_view(value: &Value, id: &str, symbol: &str) -> Result<Value, VenueError> {
    if value.get("clientAlgoId").and_then(Value::as_str) != Some(id)
        || value.get("symbol").and_then(Value::as_str) != Some(symbol)
        || value.get("algoType").and_then(Value::as_str) != Some("CONDITIONAL")
        || value.get("algoId").and_then(Value::as_u64).is_none()
        || value.get("algoStatus").and_then(Value::as_str).is_none()
    {
        return Err(VenueError::Unconfirmed);
    }
    Ok(json!({
        "client_order_id":id,
        "algo_id":value.get("algoId"),
        "symbol":symbol,
        "status":value.get("algoStatus"),
        "order_type":value.get("orderType"),
        "actual_order_id":value.get("actualOrderId"),
        "source":"signed_exchange_algo_service",
        "raw":value,
    }))
}
impl VenueClient {
    /// Call only after the Runtime persisted its request ID. Never retry a POST
    /// whose outcome is unknown; reconcile with query_um_conditional.
    pub fn place_um_conditional(
        &self,
        credentials: &Credentials,
        intent: &UmConditionalIntent,
    ) -> Result<Value, VenueError> {
        check_credentials(credentials)?;
        let pairs = intent.signed_pairs()?;
        let value = self.signed(credentials, "POST", "/fapi/v1/algoOrder", &pairs)?;
        validated_algo_view(
            &value,
            &intent.client_algo_id,
            &venue_symbol(&intent.symbol)?,
        )
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn base() -> UmConditionalIntent {
        UmConditionalIntent {
            symbol: "ETHUSDT-PERP".into(),
            client_algo_id: "SA123".into(),
            side: "SELL".into(),
            order_type: "STOP_MARKET".into(),
            quantity: Some("0.1".into()),
            trigger_price: Some("1999.5".into()),
            limit_price: None,
            activate_price: None,
            callback_rate: None,
            position_side: "BOTH".into(),
            working_type: "MARK_PRICE".into(),
            time_in_force: "GTC".into(),
            reduce_only: true,
            close_position: false,
        }
    }
    #[test]
    fn stop_market_routes_to_algo_service_and_uses_precise_strings() {
        let p = base().signed_pairs().unwrap();
        assert!(p.contains(&("algoType".into(), "CONDITIONAL".into())));
        assert!(p.contains(&("triggerPrice".into(), "1999.5".into())));
        assert!(!p.iter().any(|(k, _)| k == "price"));
    }
    #[test]
    fn trailing_rate_and_hedge_reduce_only_fail_closed() {
        let mut x = base();
        x.position_side = "SHORT".into();
        assert!(x.signed_pairs().is_err());
        x.position_side = "BOTH".into();
        x.order_type = "TRAILING_STOP_MARKET".into();
        x.trigger_price = None;
        x.callback_rate = Some("0.05".into());
        assert!(x.signed_pairs().is_err());
        x.callback_rate = Some("1".into());
        assert!(x.signed_pairs().is_ok());
    }
}
