//! Transport-free reconciliation of Spot balances and USD-M hedge positions.
//! Input snapshots must be authenticated and scoped by the calling adapter.
//! Comparing two JSON objects is never proof of account ownership or freshness.
use rust_decimal::Decimal;
use serde_json::Value;
use std::{collections::BTreeMap, str::FromStr};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountProduct {
    Spot,
    FuturesUm,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountDifference {
    pub key: String,
    pub expected: Decimal,
    pub observed: Decimal,
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum AccountEvidenceError {
    #[error("invalid or incomplete account evidence")]
    Invalid,
}

fn decimal(value: &Value) -> Result<Decimal, AccountEvidenceError> {
    let raw = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.is_number().then(|| value.to_string()))
        .ok_or(AccountEvidenceError::Invalid)?;
    Decimal::from_str(&raw).map_err(|_| AccountEvidenceError::Invalid)
}

pub fn position_quantities(
    product: AccountProduct,
    snapshot: &Value,
) -> Result<BTreeMap<String, Decimal>, AccountEvidenceError> {
    let positions = snapshot.get("positions")
        .and_then(Value::as_array)
        .ok_or(AccountEvidenceError::Invalid)?;
    let mut output = BTreeMap::new();
    for item in positions {
        let symbol = item.get("symbol").and_then(Value::as_str)
            .ok_or(AccountEvidenceError::Invalid)?;
        if product != AccountProduct::FuturesUm || symbol.is_empty() {
            return Err(AccountEvidenceError::Invalid);
        }
        let amount = decimal(item.get("positionAmt").ok_or(AccountEvidenceError::Invalid)?)?;
        let side = item.get("positionSide").and_then(Value::as_str).unwrap_or("BOTH");
        // No implicit netting of LONG and SHORT position identities.
        if !matches!(side, "BOTH" | "LONG" | "SHORT") {
            return Err(AccountEvidenceError::Invalid);
        }
        let key = format!("{symbol}:{side}");
        if output.insert(key, amount).is_some() {
            return Err(AccountEvidenceError::Invalid);
        }
    }
    Ok(output)
}

pub fn asset_quantities(
    product: AccountProduct,
    snapshot: &Value,
) -> Result<BTreeMap<String, Decimal>, AccountEvidenceError> {
    let balances = snapshot.get("balances")
        .and_then(Value::as_array)
        .ok_or(AccountEvidenceError::Invalid)?;
    let mut output = BTreeMap::new();
    for item in balances {
        let asset = item.get("asset").and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or(AccountEvidenceError::Invalid)?;
        let amount = match product {
            AccountProduct::Spot => decimal(
                item.get("free").ok_or(AccountEvidenceError::Invalid)?
            )?.checked_add(decimal(
                item.get("locked").ok_or(AccountEvidenceError::Invalid)?
            )?).ok_or(AccountEvidenceError::Invalid)?,
            AccountProduct::FuturesUm => {
                let raw = item.get("walletBalance")
                    .or_else(|| item.get("balance"))
                    .ok_or(AccountEvidenceError::Invalid)?;
                decimal(raw)?
            }
        };
        if output.insert(asset.to_owned(), amount).is_some() {
            return Err(AccountEvidenceError::Invalid);
        }
    }
    Ok(output)
}

fn compare(
    left: BTreeMap<String, Decimal>,
    right: BTreeMap<String, Decimal>,
) -> Vec<AccountDifference> {
    let mut differences = Vec::new();
    for (key, expected) in &left {
        let observed = right.get(key).copied().unwrap_or(Decimal::ZERO);
        if *expected != observed {
            differences.push(AccountDifference {
                key: key.clone(), expected: *expected, observed,
            });
        }
    }
    for (key, observed) in &right {
        if !left.contains_key(key) && !observed.is_zero() {
            differences.push(AccountDifference {
                key: key.clone(), expected: Decimal::ZERO, observed: *observed,
            });
        }
    }
    differences
}

pub fn reconcile_positions(
    product: AccountProduct,
    expected: &Value,
    observed: &Value,
) -> Result<Vec<AccountDifference>, AccountEvidenceError> {
    Ok(compare(
        position_quantities(product, expected)?,
        position_quantities(product, observed)?,
    ))
}

pub fn reconcile_assets(
    product: AccountProduct,
    expected: &Value,
    observed: &Value,
) -> Result<Vec<AccountDifference>, AccountEvidenceError> {
    Ok(compare(
        asset_quantities(product, expected)?,
        asset_quantities(product, observed)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn spot_balance_uses_exact_decimal_including_locked() {
        let left = json!({"balances":[{"asset":"USDT","free":"0.1","locked":"0.2"}]});
        let right = json!({"balances":[{"asset":"USDT","free":"0.30","locked":"0"}]});
        assert!(reconcile_assets(AccountProduct::Spot, &left, &right).unwrap().is_empty());
    }

    #[test]
    fn hedge_sides_are_distinct_and_missing_side_is_visible() {
        let left = json!({"positions":[
            {"symbol":"ETHUSDT","positionSide":"LONG","positionAmt":"2.00"},
            {"symbol":"ETHUSDT","positionSide":"SHORT","positionAmt":"-1"}]});
        let right = json!({"positions":[
            {"symbol":"ETHUSDT","positionSide":"LONG","positionAmt":"1.5"}]});
        let differences = reconcile_positions(AccountProduct::FuturesUm, &left, &right).unwrap();
        assert_eq!(differences.len(), 2);
        assert_eq!(differences[0].key, "ETHUSDT:LONG");
        assert_eq!(differences[1].observed, Decimal::ZERO);
    }

    #[test]
    fn invalid_duplicate_or_incomplete_evidence_rejects_whole_snapshot() {
        let duplicates = json!({"positions":[
            {"symbol":"ETHUSDT","positionAmt":"1"},
            {"symbol":"ETHUSDT","positionAmt":"2"}]});
        assert!(position_quantities(AccountProduct::FuturesUm, &duplicates).is_err());
        assert!(position_quantities(
            AccountProduct::FuturesUm,
            &json!({"positions":[{"symbol":"ETHUSDT","positionSide":"UNKNOWN","positionAmt":"1"}]})
        ).is_err());
        assert!(asset_quantities(
            AccountProduct::Spot,
            &json!({"balances":[{"asset":"USDT","free":"invalid","locked":"0"}]})
        ).is_err());
        assert!(asset_quantities(AccountProduct::Spot, &json!({})).is_err());
    }
}
