//! Spot OCO order list lifecycle. Both child client IDs are stable and
//! derived from the persisted list ID; a network timeout never authorizes
//! a second POST without querying the original list.
use crate::{Credentials, VenueClient, VenueError, check_credentials, venue_symbol};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotOcoIntent {
    pub symbol: String,
    pub side: String,
    pub quantity: String,
    pub list_client_order_id: String,
    pub above_type: String,
    pub above_price: Option<String>,
    pub above_stop_price: Option<String>,
    pub below_type: String,
    pub below_price: Option<String>,
    pub below_stop_price: Option<String>,
    pub above_time_in_force: Option<String>,
    pub below_time_in_force: Option<String>,
}

fn positive(raw: &str) -> Result<(), VenueError> {
    let x = Decimal::from_str(raw).map_err(|_| VenueError::Unconfirmed)?;
    if x <= Decimal::ZERO {
        return Err(VenueError::Unconfirmed);
    }
    Ok(())
}
fn valid_client_id(raw: &str) -> bool {
    !raw.is_empty()
        && raw.len() <= 36
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':' | b'/'))
}
fn child_id(list: &str, suffix: &str) -> String {
    let digest = Sha256::digest(list.as_bytes());
    let mut out = String::from("SA");
    for byte in digest.iter().take(14) {
        out.push_str(&format!("{byte:02x}"));
    }
    out.push_str(suffix);
    out
}
fn validate_leg(
    kind: &str,
    price: Option<&str>,
    stop: Option<&str>,
    time_in_force: Option<&str>,
) -> Result<(), VenueError> {
    let has_price = matches!(
        kind,
        "LIMIT_MAKER" | "TAKE_PROFIT_LIMIT" | "STOP_LOSS_LIMIT"
    );
    let has_stop = matches!(
        kind,
        "TAKE_PROFIT" | "TAKE_PROFIT_LIMIT" | "STOP_LOSS" | "STOP_LOSS_LIMIT"
    );
    if has_price != price.is_some() || has_stop != stop.is_some() {
        return Err(VenueError::Unconfirmed);
    }
    if let Some(x) = price {
        positive(x)?;
    }
    if let Some(x) = stop {
        positive(x)?;
    }
    if matches!(kind, "TAKE_PROFIT_LIMIT" | "STOP_LOSS_LIMIT") {
        if !matches!(time_in_force, Some("GTC" | "IOC" | "FOK")) {
            return Err(VenueError::Unconfirmed);
        }
    } else if time_in_force.is_some() {
        return Err(VenueError::Unconfirmed);
    }
    Ok(())
}
impl SpotOcoIntent {
    pub fn signed_pairs(&self) -> Result<Vec<(String, String)>, VenueError> {
        let symbol = venue_symbol(&self.symbol)?;
        if symbol.contains('-')
            || !valid_client_id(&self.list_client_order_id)
            || !matches!(self.side.as_str(), "BUY" | "SELL")
        {
            return Err(VenueError::Unconfirmed);
        }
        positive(&self.quantity)?;
        let stop = |t: &str| matches!(t, "STOP_LOSS" | "STOP_LOSS_LIMIT");
        let take = |t: &str| matches!(t, "LIMIT_MAKER" | "TAKE_PROFIT" | "TAKE_PROFIT_LIMIT");
        if !((stop(&self.above_type) && take(&self.below_type))
            || (take(&self.above_type) && stop(&self.below_type)))
        {
            return Err(VenueError::Unconfirmed);
        }
        validate_leg(
            &self.above_type,
            self.above_price.as_deref(),
            self.above_stop_price.as_deref(),
            self.above_time_in_force.as_deref(),
        )?;
        validate_leg(
            &self.below_type,
            self.below_price.as_deref(),
            self.below_stop_price.as_deref(),
            self.below_time_in_force.as_deref(),
        )?;
        let mut pairs = vec![
            ("symbol".into(), symbol),
            ("side".into(), self.side.clone()),
            ("quantity".into(), self.quantity.clone()),
            (
                "listClientOrderId".into(),
                self.list_client_order_id.clone(),
            ),
            (
                "aboveClientOrderId".into(),
                child_id(&self.list_client_order_id, "A"),
            ),
            (
                "belowClientOrderId".into(),
                child_id(&self.list_client_order_id, "B"),
            ),
            ("aboveType".into(), self.above_type.clone()),
            ("belowType".into(), self.below_type.clone()),
            ("newOrderRespType".into(), "RESULT".into()),
        ];
        for (key, val) in [
            ("abovePrice", &self.above_price),
            ("aboveStopPrice", &self.above_stop_price),
            ("belowPrice", &self.below_price),
            ("belowStopPrice", &self.below_stop_price),
            ("aboveTimeInForce", &self.above_time_in_force),
            ("belowTimeInForce", &self.below_time_in_force),
        ] {
            if let Some(val) = val {
                pairs.push((key.into(), val.clone()));
            }
        }
        Ok(pairs)
    }
}

pub fn validated_spot_oco_view(
    value: &Value,
    list_id: &str,
    symbol: &str,
) -> Result<Value, VenueError> {
    if value.get("listClientOrderId").and_then(Value::as_str) != Some(list_id)
        || value.get("symbol").and_then(Value::as_str) != Some(symbol)
        || value.get("contingencyType").and_then(Value::as_str) != Some("OCO")
    {
        return Err(VenueError::Unconfirmed);
    }
    let orders = value
        .get("orders")
        .and_then(Value::as_array)
        .ok_or(VenueError::Unconfirmed)?;
    if orders.len() != 2 {
        return Err(VenueError::Unconfirmed);
    }
    let mut ids = HashSet::new();
    let mut clients = HashSet::new();
    for row in orders {
        if row.get("symbol").and_then(Value::as_str) != Some(symbol) {
            return Err(VenueError::Unconfirmed);
        }
        let order_id = row
            .get("orderId")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
            })
            .ok_or(VenueError::Unconfirmed)?;
        let client = row
            .get("clientOrderId")
            .and_then(Value::as_str)
            .filter(|x| !x.is_empty())
            .ok_or(VenueError::Unconfirmed)?;
        if !ids.insert(order_id) || !clients.insert(client) {
            return Err(VenueError::Unconfirmed);
        }
    }
    let status = value
        .get("listOrderStatus")
        .and_then(Value::as_str)
        .ok_or(VenueError::Unconfirmed)?;
    if !matches!(status, "EXECUTING" | "ALL_DONE" | "REJECT") {
        return Err(VenueError::Unconfirmed);
    }
    Ok(json!({
        "client_order_id":list_id,"symbol":symbol,
        "order_list_id":value.get("orderListId"),
        "list_status":status,"status":status,
        "orders":orders,"source":"signed_exchange_order_list",
        "raw":value,
    }))
}
impl VenueClient {
    pub fn place_spot_oco(
        &self,
        credentials: &Credentials,
        intent: &SpotOcoIntent,
    ) -> Result<Value, VenueError> {
        check_credentials(credentials)?;
        let pairs = intent.signed_pairs()?;
        let value = self.signed(credentials, "POST", "/api/v3/orderList/oco", &pairs)?;
        validated_spot_oco_view(
            &value,
            &intent.list_client_order_id,
            &venue_symbol(&intent.symbol)?,
        )
    }

    pub fn query_spot_oco(
        &self,
        credentials: &Credentials,
        symbol: &str,
        list_id: &str,
    ) -> Result<Value, VenueError> {
        check_credentials(credentials)?;
        if !valid_client_id(list_id) {
            return Err(VenueError::Unconfirmed);
        }
        let symbol = venue_symbol(symbol)?;
        let value = self.signed(
            credentials,
            "GET",
            "/api/v3/orderList",
            &[("origClientOrderId".into(), list_id.into())],
        )?;
        validated_spot_oco_view(&value, list_id, &symbol)
    }

    pub fn cancel_spot_oco(
        &self,
        credentials: &Credentials,
        symbol: &str,
        list_id: &str,
    ) -> Result<Value, VenueError> {
        check_credentials(credentials)?;
        if !valid_client_id(list_id) {
            return Err(VenueError::Unconfirmed);
        }
        let symbol = venue_symbol(symbol)?;
        let value = self.signed(
            credentials,
            "DELETE",
            "/api/v3/orderList",
            &[
                ("symbol".into(), symbol.clone()),
                ("listClientOrderId".into(), list_id.into()),
            ],
        )?;
        if value.get("listClientOrderId").and_then(Value::as_str) != Some(list_id) {
            return Err(VenueError::Unconfirmed);
        }
        self.query_spot_oco(credentials, &symbol, list_id)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn oco_deterministic_child_ids_and_precise_prices() {
        let base = SpotOcoIntent {
            symbol: "ETHUSDT".into(),
            side: "SELL".into(),
            quantity: "0.1".into(),
            list_client_order_id: "SA123".into(),
            above_type: "LIMIT_MAKER".into(),
            above_price: Some("2500".into()),
            above_stop_price: None,
            above_time_in_force: None,
            below_type: "STOP_LOSS_LIMIT".into(),
            below_price: Some("1900".into()),
            below_stop_price: Some("1950".into()),
            below_time_in_force: Some("GTC".into()),
        };
        let params = base.signed_pairs().unwrap();
        assert!(params.contains(&("belowStopPrice".into(), "1950".into())));
        let a = params
            .iter()
            .find(|(k, _)| k == "aboveClientOrderId")
            .unwrap()
            .1
            .clone();
        let b = params
            .iter()
            .find(|(k, _)| k == "belowClientOrderId")
            .unwrap()
            .1
            .clone();
        assert_ne!(a, b);
        assert_eq!(base.signed_pairs().unwrap(), params);
    }
    #[test]
    fn incomplete_oco_response_must_remain_unknown() {
        assert!(
            validated_spot_oco_view(
                &json!({"listClientOrderId":"a",
            "symbol":"ETHUSDT","contingencyType":"OCO",
            "listOrderStatus":"EXECUTING","orders":[]}),
                "a",
                "ETHUSDT"
            )
            .is_err()
        );
    }
}
