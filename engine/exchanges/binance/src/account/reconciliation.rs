//! Read-only multi-product account evidence and USD-M conditional-order reconciliation.
//! No method in this module submits a new order.
use crate::{Credentials, Product, VenueClient, VenueError, check_credentials, venue_symbol};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountDifference {
    pub key: String,
    pub expected: Decimal,
    pub observed: Decimal,
}

fn decimal(value: &Value) -> Result<Decimal, VenueError> {
    let repr = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| {
            if value.is_number() {
                Some(value.to_string())
            } else {
                None
            }
        })
        .ok_or(VenueError::Unconfirmed)?;
    Decimal::from_str(&repr).map_err(|_| VenueError::Unconfirmed)
}

fn field<'a>(item: &'a Value, alternatives: &[&str]) -> Option<&'a Value> {
    alternatives.iter().find_map(|key| item.get(*key))
}

/// A missing field, duplicate key or malformed amount invalidates the *whole*
/// snapshot. Returning a partial map could silently conceal a missing position.
pub fn position_quantities(
    product: Product,
    snapshot: &Value,
) -> Result<BTreeMap<String, Decimal>, VenueError> {
    let positions = snapshot
        .get("positions")
        .and_then(Value::as_array)
        .ok_or(VenueError::Unconfirmed)?;
    let mut result = BTreeMap::new();
    for item in positions {
        let symbol = field(item, &["symbol"])
            .and_then(Value::as_str)
            .ok_or(VenueError::Unconfirmed)?;
        let raw = match product {
            Product::FuturesUm => field(item, &["positionAmt"]),
            Product::Option => field(item, &["quantity", "positionAmt"]),
            Product::Spot => return Err(VenueError::Symbol),
        }
        .ok_or(VenueError::Unconfirmed)?;
        let amount = decimal(raw)?;
        let side = field(item, &["positionSide"])
            .and_then(Value::as_str)
            .unwrap_or("BOTH");
        let key = format!("{symbol}:{side}");
        if result.insert(key, amount).is_some() {
            return Err(VenueError::Unconfirmed);
        }
    }
    Ok(result)
}

/// Compare two complete exchange-authored position snapshots. Both inputs must
/// use the same venue product and account context; callers enforce that scope.
pub fn reconcile_positions(
    product: Product,
    expected: &Value,
    observed: &Value,
) -> Result<Vec<AccountDifference>, VenueError> {
    let left = position_quantities(product, expected)?;
    let right = position_quantities(product, observed)?;
    let mut result = Vec::new();
    for (key, amount) in &left {
        let actual = right.get(key).copied().unwrap_or(Decimal::ZERO);
        if *amount != actual {
            result.push(AccountDifference {
                key: key.clone(),
                expected: *amount,
                observed: actual,
            });
        }
    }
    for (key, amount) in &right {
        if !left.contains_key(key) && !amount.is_zero() {
            result.push(AccountDifference {
                key: key.clone(),
                expected: Decimal::ZERO,
                observed: *amount,
            });
        }
    }
    Ok(result)
}

/// Asset differences must preserve product-specific wallets. Spot free/locked,
/// USD-M wallet and options balances are intentionally not netted together.
pub fn asset_quantities(
    product: Product,
    snapshot: &Value,
) -> Result<BTreeMap<String, Decimal>, VenueError> {
    let assets = snapshot
        .get("balances")
        .and_then(Value::as_array)
        .ok_or(VenueError::Unconfirmed)?;
    let mut result = BTreeMap::new();
    for item in assets {
        let asset = field(item, &["asset"])
            .and_then(Value::as_str)
            .ok_or(VenueError::Unconfirmed)?;
        let amount = match product {
            Product::Spot => {
                decimal(field(item, &["free"]).ok_or(VenueError::Unconfirmed)?)?
                    + decimal(field(item, &["locked"]).ok_or(VenueError::Unconfirmed)?)?
            }
            Product::FuturesUm => {
                decimal(field(item, &["walletBalance", "balance"]).ok_or(VenueError::Unconfirmed)?)?
            }
            Product::Option => decimal(
                field(item, &["marginBalance", "balance", "walletBalance"])
                    .ok_or(VenueError::Unconfirmed)?,
            )?,
        };
        if result.insert(asset.to_owned(), amount).is_some() {
            return Err(VenueError::Unconfirmed);
        }
    }
    Ok(result)
}

pub fn reconcile_assets(
    product: Product,
    expected: &Value,
    observed: &Value,
) -> Result<Vec<AccountDifference>, VenueError> {
    let left = asset_quantities(product, expected)?;
    let right = asset_quantities(product, observed)?;
    let mut result = Vec::new();
    for (key, amount) in &left {
        let actual = right.get(key).copied().unwrap_or(Decimal::ZERO);
        if *amount != actual {
            result.push(AccountDifference {
                key: key.clone(),
                expected: *amount,
                observed: actual,
            });
        }
    }
    for (key, amount) in &right {
        if !left.contains_key(key) && !amount.is_zero() {
            result.push(AccountDifference {
                key: key.clone(),
                expected: Decimal::ZERO,
                observed: *amount,
            });
        }
    }
    Ok(result)
}

impl VenueClient {
    /// Product-specific signed snapshots. No account field is fabricated from
    /// another market; options positions require their own signed endpoint.
    pub fn account_snapshot_product(
        &self,
        credentials: &Credentials,
        product: Product,
    ) -> Result<Value, VenueError> {
        check_credentials(credentials)?;
        match product {
            Product::FuturesUm => self.account_snapshot(credentials),
            Product::Spot => {
                let value = self.signed(credentials, "GET", "/api/v3/account", &[])?;
                let balances = value
                    .get("balances")
                    .and_then(Value::as_array)
                    .ok_or(VenueError::Unconfirmed)?;
                Ok(json!({"product":"spot","balances":balances,"positions":[],
                    "observed_at":value.get("updateTime").cloned().unwrap_or(Value::Null)}))
            }
            Product::Option => {
                let account = self.signed(credentials, "GET", "/eapi/v1/account", &[])?;
                let positions = self.signed(credentials, "GET", "/eapi/v1/position", &[])?;
                let balances = account
                    .as_array()
                    .or_else(|| account.get("balances").and_then(Value::as_array))
                    .or_else(|| account.get("asset").and_then(Value::as_array))
                    .ok_or(VenueError::Unconfirmed)?;
                let positions = positions.as_array().ok_or(VenueError::Unconfirmed)?;
                Ok(
                    json!({"product":"options","balances":balances,"positions":positions,
                    "observed_at":Value::Null}),
                )
            }
        }
    }

    /// USD-M conditional orders use Binance Algo Service, not /fapi/v1/order.
    /// A missing or malformed exchange identity is never treated as confirmed.
    pub fn query_um_conditional(
        &self,
        credentials: &Credentials,
        symbol: &str,
        client_algo_id: &str,
    ) -> Result<Value, VenueError> {
        check_credentials(credentials)?;
        if client_algo_id.is_empty()
            || client_algo_id.len() > 36
            || !client_algo_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        {
            return Err(VenueError::Rejected("条件单客户编号无效。".into()));
        }
        let symbol = venue_symbol(symbol)?;
        let value = self.signed(
            credentials,
            "GET",
            "/fapi/v1/algoOrder",
            &[("clientAlgoId".into(), client_algo_id.into())],
        )?;
        if value.get("clientAlgoId").and_then(Value::as_str) != Some(client_algo_id)
            || value.get("symbol").and_then(Value::as_str) != Some(symbol.as_str())
        {
            return Err(VenueError::Unconfirmed);
        }
        Ok(value)
    }

    /// Explicit cancellation only. An ambiguous response remains unconfirmed
    /// until the caller queries by clientAlgoId; never resubmit automatically.
    pub fn cancel_um_conditional(
        &self,
        credentials: &Credentials,
        symbol: &str,
        client_algo_id: &str,
    ) -> Result<Value, VenueError> {
        check_credentials(credentials)?;
        if client_algo_id.is_empty()
            || client_algo_id.len() > 36
            || !client_algo_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        {
            return Err(VenueError::Rejected("条件单客户编号无效。".into()));
        }
        let symbol = venue_symbol(symbol)?;
        let value = self.signed(
            credentials,
            "DELETE",
            "/fapi/v1/algoOrder",
            &[("clientAlgoId".into(), client_algo_id.into())],
        )?;
        // Binance DELETE /algoOrder returns code/msg and clientAlgoId,
        // not necessarily a symbol or final algo status. Confirm by GET.
        if value.get("clientAlgoId").and_then(Value::as_str) != Some(client_algo_id) {
            return Err(VenueError::Unconfirmed);
        }
        let queried = self.query_um_conditional(credentials, &symbol, client_algo_id)?;
        if queried.get("algoStatus").and_then(Value::as_str) != Some("CANCELED") {
            return Err(VenueError::Unconfirmed);
        }
        crate::conditional::validated_algo_view(&queried, client_algo_id, &symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn detects_missing_and_changed_hedge_mode_positions() {
        let left = json!({"positions":[
            {"symbol":"ETHUSDT","positionSide":"LONG","positionAmt":"2.00"},
            {"symbol":"ETHUSDT","positionSide":"SHORT","positionAmt":"-1"}]});
        let right = json!({"positions":[
            {"symbol":"ETHUSDT","positionSide":"LONG","positionAmt":"1.5"}]});
        let diffs = reconcile_positions(Product::FuturesUm, &left, &right).unwrap();
        assert_eq!(diffs.len(), 2);
        assert_eq!(diffs[0].key, "ETHUSDT:LONG");
        assert_eq!(diffs[1].observed, Decimal::ZERO);
    }
    #[test]
    fn duplicate_or_malformed_exchange_evidence_fails_closed() {
        let duplicate = json!({"positions":[
            {"symbol":"ETHUSDT","positionAmt":"1"},
            {"symbol":"ETHUSDT","positionAmt":"2"}]});
        assert_eq!(
            position_quantities(Product::FuturesUm, &duplicate),
            Err(VenueError::Unconfirmed)
        );
        let bad = json!({"balances":[{"asset":"USDT","free":"invalid","locked":"0"}]});
        assert_eq!(
            asset_quantities(Product::Spot, &bad),
            Err(VenueError::Unconfirmed)
        );
    }
    #[test]
    fn spot_assets_include_locked_and_do_not_use_floating_point() {
        let left = json!({"balances":[{"asset":"USDT","free":"0.1","locked":"0.2"}]});
        let right = json!({"balances":[{"asset":"USDT","free":"0.30","locked":"0"}]});
        assert!(
            reconcile_assets(Product::Spot, &left, &right)
                .unwrap()
                .is_empty()
        );
    }
}
