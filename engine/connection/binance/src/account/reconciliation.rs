//! Read-only account evidence and USD-M conditional-order reconciliation.
//! Pure Spot/USD-M account comparison lives in seealpha-portfolio now.
use crate::{Credentials, Product, VenueClient, VenueError, check_credentials, venue_symbol};
use seealpha_portfolio::account_evidence::{self, AccountProduct};
pub use seealpha_portfolio::account_evidence::AccountDifference;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn account_product(product: Product) -> Result<AccountProduct, VenueError> {
    match product {
        Product::Spot => Ok(AccountProduct::Spot),
        Product::FuturesUm => Ok(AccountProduct::FuturesUm),
        Product::Option => Err(VenueError::Unconfirmed),
    }
}

/// Compatibility API. The common parser now belongs to Portfolio rather than
/// the legacy REST adapter; Options still uses its dedicated EAPI parser.
pub fn position_quantities(
    product: Product,
    snapshot: &Value,
) -> Result<BTreeMap<String, Decimal>, VenueError> {
    if product == Product::Option {
        return seealpha_binance_options::account::option_position_quantities(snapshot)
            .map_err(|_| VenueError::Unconfirmed);
    }
    account_evidence::position_quantities(account_product(product)?, snapshot)
        .map_err(|_| VenueError::Unconfirmed)
}

pub fn reconcile_positions(
    product: Product,
    expected: &Value,
    observed: &Value,
) -> Result<Vec<AccountDifference>, VenueError> {
    if product == Product::Option {
        // Preserve dedicated Options handling, not Spot/USD-M coercion.
        return Ok(compare(position_quantities(product, expected)?, position_quantities(product, observed)?));
    }
    account_evidence::reconcile_positions(account_product(product)?, expected, observed)
        .map_err(|_| VenueError::Unconfirmed)
}

pub fn asset_quantities(
    product: Product,
    snapshot: &Value,
) -> Result<BTreeMap<String, Decimal>, VenueError> {
    if product == Product::Option {
        return seealpha_binance_options::account::option_asset_quantities(snapshot)
            .map_err(|_| VenueError::Unconfirmed);
    }
    account_evidence::asset_quantities(account_product(product)?, snapshot)
        .map_err(|_| VenueError::Unconfirmed)
}

pub fn reconcile_assets(
    product: Product,
    expected: &Value,
    observed: &Value,
) -> Result<Vec<AccountDifference>, VenueError> {
    if product == Product::Option {
        return Ok(compare(asset_quantities(product, expected)?, asset_quantities(product, observed)?));
    }
    account_evidence::reconcile_assets(account_product(product)?, expected, observed)
        .map_err(|_| VenueError::Unconfirmed)
}

// Options compatibility until its dedicated snapshots are migrated to their
// own Runtime account service; no second Binance Spot/USD-M parsing logic.
fn compare(
    left: BTreeMap<String, Decimal>,
    right: BTreeMap<String, Decimal>,
) -> Vec<AccountDifference> {
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
    result
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
                seealpha_binance_options::account::collect_options_account_snapshot(|path| {
                    self.signed(credentials, "GET", path, &[])
                })
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
    fn option_reconciliation_uses_dedicated_parser_and_preserves_old_errors() {
        let left = json!({"positions":[
            {"symbol":"BTC-271231-90000-C","quantity":"1.25"}
        ]});
        let right = json!({"positions":[
            {"symbol":"BTC-271231-90000-C","quantity":"1.00"}
        ]});
        let diffs = reconcile_positions(Product::Option, &left, &right).unwrap();
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].expected, Decimal::new(125, 2));
        assert_eq!(diffs[0].observed, Decimal::ONE);
        let bad = json!({"balances":[{"asset":"USDT","marginBalance":"invalid"}]});
        assert_eq!(
            asset_quantities(Product::Option, &bad),
            Err(VenueError::Unconfirmed)
        );
    }
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
