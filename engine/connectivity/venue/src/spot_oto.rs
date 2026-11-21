//! Binance Spot OTO/OTOCO: pending legs are exchange-managed.
//! An order-list ACK never proves a child order executed.
use crate::order_guard::{IntentJournal, PreparedWrite, WriteAction};
use crate::{Credentials, Product, VenueClient, VenueError, check_credentials, venue_symbol};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, str::FromStr};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpotLeg {
    pub kind: String,
    pub price: Option<String>,
    pub stop_price: Option<String>,
    pub time_in_force: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pending {
    Oto {
        side: String,
        quantity: String,
        leg: SpotLeg,
    },
    Otoco {
        side: String,
        quantity: String,
        above: SpotLeg,
        below: SpotLeg,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpotTriggerList {
    pub symbol: String,
    pub list_client_order_id: String,
    pub working_side: String,
    pub working_type: String,
    pub working_price: String,
    pub working_quantity: String,
    pub working_time_in_force: Option<String>,
    pub pending: Pending,
}
fn positive(s: &str) -> Result<(), VenueError> {
    let d = Decimal::from_str(s).map_err(|_| VenueError::Unconfirmed)?;
    if d <= Decimal::ZERO {
        Err(VenueError::Unconfirmed)
    } else {
        Ok(())
    }
}
fn side(s: &str) -> bool {
    matches!(s, "BUY" | "SELL")
}
fn id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 36
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b':' | b'/'))
}
fn child(parent: &str, suffix: &str) -> String {
    let digest = Sha256::digest(parent.as_bytes());
    let mut id = String::from("SA");
    for byte in digest.iter().take(14) {
        id.push_str(&format!("{byte:02x}"));
    }
    id.push_str(suffix);
    id
}
fn valid_leg(leg: &SpotLeg, market: bool) -> Result<(), VenueError> {
    let limit = matches!(
        leg.kind.as_str(),
        "LIMIT" | "LIMIT_MAKER" | "STOP_LOSS_LIMIT" | "TAKE_PROFIT_LIMIT"
    );
    let stop = matches!(
        leg.kind.as_str(),
        "STOP_LOSS" | "STOP_LOSS_LIMIT" | "TAKE_PROFIT" | "TAKE_PROFIT_LIMIT"
    );
    if !(limit || stop || (market && leg.kind == "MARKET"))
        || limit != leg.price.is_some()
        || stop != leg.stop_price.is_some()
    {
        return Err(VenueError::Unconfirmed);
    }
    if let Some(p) = &leg.price {
        positive(p)?;
    }
    if let Some(p) = &leg.stop_price {
        positive(p)?;
    }
    let need_tif = matches!(
        leg.kind.as_str(),
        "LIMIT" | "STOP_LOSS_LIMIT" | "TAKE_PROFIT_LIMIT"
    );
    if need_tif {
        if !matches!(leg.time_in_force.as_deref(), Some("GTC" | "IOC" | "FOK")) {
            return Err(VenueError::Unconfirmed);
        }
    } else if leg.time_in_force.is_some() {
        return Err(VenueError::Unconfirmed);
    }
    Ok(())
}
fn add_leg(prefix: &str, leg: &SpotLeg, params: &mut Vec<(String, String)>) {
    params.push((format!("{prefix}Type"), leg.kind.clone()));
    for (part, val) in [
        ("Price", &leg.price),
        ("StopPrice", &leg.stop_price),
        ("TimeInForce", &leg.time_in_force),
    ] {
        if let Some(v) = val {
            params.push((format!("{prefix}{part}"), v.clone()));
        }
    }
}
impl SpotTriggerList {
    pub fn action(&self) -> WriteAction {
        match &self.pending {
            Pending::Oto { .. } => WriteAction::NewOto,
            Pending::Otoco { .. } => WriteAction::NewOtoco,
        }
    }
    pub fn expected_children(&self) -> Vec<String> {
        let suffixes: &[&str] = match &self.pending {
            Pending::Oto { .. } => &["W", "P"],
            Pending::Otoco { .. } => &["W", "A", "B"],
        };
        suffixes
            .iter()
            .map(|x| child(&self.list_client_order_id, x))
            .collect()
    }
    pub fn signed_pairs(&self) -> Result<Vec<(String, String)>, VenueError> {
        let symbol = venue_symbol(&self.symbol)?;
        if symbol.contains('-')
            || !id(&self.list_client_order_id)
            || !side(&self.working_side)
            || !matches!(self.working_type.as_str(), "LIMIT" | "LIMIT_MAKER")
        {
            return Err(VenueError::Unconfirmed);
        }
        positive(&self.working_price)?;
        positive(&self.working_quantity)?;
        if self.working_type == "LIMIT" {
            if !matches!(
                self.working_time_in_force.as_deref(),
                Some("GTC" | "IOC" | "FOK")
            ) {
                return Err(VenueError::Unconfirmed);
            }
        } else if self.working_time_in_force.is_some() {
            return Err(VenueError::Unconfirmed);
        }
        let ids = self.expected_children();
        let mut params = vec![
            ("symbol".into(), symbol),
            (
                "listClientOrderId".into(),
                self.list_client_order_id.clone(),
            ),
            ("workingClientOrderId".into(), ids[0].clone()),
            ("workingSide".into(), self.working_side.clone()),
            ("workingType".into(), self.working_type.clone()),
            ("workingPrice".into(), self.working_price.clone()),
            ("workingQuantity".into(), self.working_quantity.clone()),
            ("newOrderRespType".into(), "RESULT".into()),
        ];
        if let Some(tif) = &self.working_time_in_force {
            params.push(("workingTimeInForce".into(), tif.clone()));
        }
        match &self.pending {
            Pending::Oto {
                side,
                quantity,
                leg,
            } => {
                if !side_ok(side, quantity) {
                    return Err(VenueError::Unconfirmed);
                }
                valid_leg(leg, true)?;
                params.extend([
                    ("pendingClientOrderId".into(), ids[1].clone()),
                    ("pendingSide".into(), side.clone()),
                    ("pendingQuantity".into(), quantity.clone()),
                ]);
                add_leg("pending", leg, &mut params);
            }
            Pending::Otoco {
                side,
                quantity,
                above,
                below,
            } => {
                if !side_ok(side, quantity) {
                    return Err(VenueError::Unconfirmed);
                }
                valid_leg(above, false)?;
                valid_leg(below, false)?;
                let stop = |t: &str| matches!(t, "STOP_LOSS" | "STOP_LOSS_LIMIT");
                let take =
                    |t: &str| matches!(t, "LIMIT_MAKER" | "TAKE_PROFIT" | "TAKE_PROFIT_LIMIT");
                if !((stop(&above.kind) && take(&below.kind))
                    || (take(&above.kind) && stop(&below.kind)))
                {
                    return Err(VenueError::Unconfirmed);
                }
                params.extend([
                    ("pendingAboveClientOrderId".into(), ids[1].clone()),
                    ("pendingBelowClientOrderId".into(), ids[2].clone()),
                    ("pendingSide".into(), side.clone()),
                    ("pendingQuantity".into(), quantity.clone()),
                ]);
                add_leg("pendingAbove", above, &mut params);
                add_leg("pendingBelow", below, &mut params);
            }
        }
        Ok(params)
    }
}
fn side_ok(s: &str, qty: &str) -> bool {
    side(s) && positive(qty).is_ok()
}
pub fn validated_spot_trigger_list(
    v: &Value,
    intent: &SpotTriggerList,
) -> Result<Value, VenueError> {
    let symbol = venue_symbol(&intent.symbol)?;
    let kind = match &intent.pending {
        Pending::Oto { .. } => "OTO",
        Pending::Otoco { .. } => "OTOCO",
    };
    if v["listClientOrderId"].as_str() != Some(intent.list_client_order_id.as_str())
        || v["symbol"].as_str() != Some(symbol.as_str())
        || v["contingencyType"].as_str() != Some(kind)
        || v["orderListId"].as_u64().is_none()
    {
        return Err(VenueError::Unconfirmed);
    }
    let status = v["listOrderStatus"]
        .as_str()
        .ok_or(VenueError::Unconfirmed)?;
    if !matches!(status, "EXECUTING" | "ALL_DONE" | "REJECT") {
        return Err(VenueError::Unconfirmed);
    }
    let rows = v["orders"].as_array().ok_or(VenueError::Unconfirmed)?;
    let mut children = BTreeSet::new();
    let mut order_ids = BTreeSet::new();
    for row in rows {
        let client = row["clientOrderId"]
            .as_str()
            .filter(|x| !x.is_empty())
            .ok_or(VenueError::Unconfirmed)?;
        let order_id = row["orderId"]
            .as_u64()
            .map(|v| v.to_string())
            .or_else(|| row["orderId"].as_str().map(str::to_owned))
            .ok_or(VenueError::Unconfirmed)?;
        if row["symbol"].as_str() != Some(symbol.as_str())
            || !children.insert(client.to_string())
            || !order_ids.insert(order_id)
        {
            return Err(VenueError::Unconfirmed);
        }
    }
    if children
        != intent
            .expected_children()
            .into_iter()
            .collect::<BTreeSet<_>>()
    {
        return Err(VenueError::Unconfirmed);
    }
    Ok(json!({"list_client_order_id":intent.list_client_order_id,
        "symbol":symbol,"order_list_id":v["orderListId"],"status":status,
        "orders":rows,"source":"signed_exchange_order_list","raw":v}))
}
impl VenueClient {
    pub fn place_spot_trigger_list<J: IntentJournal>(
        &self,
        credentials: &Credentials,
        journal: &mut J,
        account: &str,
        request: &str,
        intent: &SpotTriggerList,
    ) -> Result<Value, VenueError> {
        check_credentials(credentials)?;
        let mut op = PreparedWrite::new(
            account,
            request,
            Product::Spot,
            intent.action(),
            &intent.symbol,
            &intent.list_client_order_id,
            intent.signed_pairs()?,
        )?;
        let reply = op.dispatch_live(journal, self, credentials)?;
        validated_spot_trigger_list(&reply, intent)
    }
    /// Only a signed GET; NEVER use a missing result to authorize a new POST.
    pub fn query_spot_trigger_list(
        &self,
        credentials: &Credentials,
        intent: &SpotTriggerList,
    ) -> Result<Value, VenueError> {
        check_credentials(credentials)?;
        let v = self.signed(
            credentials,
            "GET",
            "/api/v3/orderList",
            &[(
                "origClientOrderId".into(),
                intent.list_client_order_id.clone(),
            )],
        )?;
        validated_spot_trigger_list(&v, intent)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn sample() -> SpotTriggerList {
        SpotTriggerList {
            symbol: "ETHUSDT".into(),
            list_client_order_id: "id123".into(),
            working_side: "BUY".into(),
            working_type: "LIMIT".into(),
            working_price: "1950.01".into(),
            working_quantity: "0.125".into(),
            working_time_in_force: Some("GTC".into()),
            pending: Pending::Oto {
                side: "SELL".into(),
                quantity: "0.125".into(),
                leg: SpotLeg {
                    kind: "LIMIT".into(),
                    price: Some("2050.02".into()),
                    stop_price: None,
                    time_in_force: Some("GTC".into()),
                },
            },
        }
    }
    #[test]
    fn oto_client_ids_are_stable_and_no_float_precision_is_lost() {
        let order = sample();
        let params = order.signed_pairs().unwrap();
        assert!(params.contains(&("workingPrice".into(), "1950.01".into())));
        assert!(params.contains(&("pendingPrice".into(), "2050.02".into())));
        assert_eq!(order.expected_children().len(), 2);
        assert_ne!(order.expected_children()[0], order.expected_children()[1]);
    }
    #[test]
    fn otoco_rejects_incomplete_child_evidence() {
        let mut order = sample();
        order.pending = Pending::Otoco {
            side: "SELL".into(),
            quantity: "0.125".into(),
            above: SpotLeg {
                kind: "LIMIT_MAKER".into(),
                price: Some("2200".into()),
                stop_price: None,
                time_in_force: None,
            },
            below: SpotLeg {
                kind: "STOP_LOSS_LIMIT".into(),
                price: Some("1800".into()),
                stop_price: Some("1850".into()),
                time_in_force: Some("GTC".into()),
            },
        };
        assert!(order.signed_pairs().is_ok());
        let ids = order.expected_children();
        let good = json!({"listClientOrderId":"id123","symbol":"ETHUSDT",
            "contingencyType":"OTOCO","orderListId":10,"listOrderStatus":"EXECUTING",
            "orders":ids.iter().enumerate().map(|(i,id)|json!({
                "symbol":"ETHUSDT","clientOrderId":id,"orderId":i+1})).collect::<Vec<_>>()});
        assert!(validated_spot_trigger_list(&good, &order).is_ok());
        assert!(validated_spot_trigger_list(&json!({"orders":[]}), &order).is_err());
    }
}
