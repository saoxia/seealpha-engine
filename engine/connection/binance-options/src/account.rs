//! Canonicalize Binance Options signed read-only account/position responses.
//! Inputs are the two *separate* signed EAPI GET responses. No balances or
//! positions are inferred from order acknowledgements or another market.
use serde_json::{Value, json};
use rust_decimal::Decimal;
use std::{collections::BTreeMap, str::FromStr};

use crate::OptionsEvidenceError;

pub const EAPI_ACCOUNT_PATH: &str = "/eapi/v1/marginAccount";
pub const EAPI_POSITION_PATH: &str = "/eapi/v1/position";

/// Read the two independent Options account resources through the caller's
/// signed GET transport. This crate never signs, sends writes or stores keys.
/// Failure of either read stops the sequence and cannot yield a partial snapshot.
pub fn collect_options_account_snapshot<E, F>(
    mut signed_get: F,
) -> Result<Value, E>
where
    E: From<OptionsEvidenceError>,
    F: FnMut(&'static str) -> Result<Value, E>,
{
    let account = signed_get(EAPI_ACCOUNT_PATH)?;
    let positions = signed_get(EAPI_POSITION_PATH)?;
    normalize_options_account_snapshot(&account, &positions).map_err(E::from)
}

/// Normalize the signed exchange response into a validated account view.
///
/// This validates container shape and does not claim the snapshot is current,
/// complete, or an authorization to open positions.
pub fn normalize_options_account_snapshot(
    account: &Value,
    positions: &Value,
) -> Result<Value, OptionsEvidenceError> {
    // Official marginAccount response uses an "asset" array.
    let balances = account
        .get("asset")
        .and_then(Value::as_array)
        .ok_or(OptionsEvidenceError::Unconfirmed)?;
    let positions = positions
        .as_array()
        .ok_or(OptionsEvidenceError::Unconfirmed)?;
    Ok(json!({
        "product": "options",
        "balances": balances,
        "positions": positions,
        "observed_at": Value::Null
    }))
}


/// Parse Options positions under the exchange's own contract and position-side
/// identities. Missing/duplicate evidence invalidates the whole snapshot.
pub fn option_position_quantities(
    snapshot: &Value,
) -> Result<BTreeMap<String, Decimal>, OptionsEvidenceError> {
    let positions = snapshot
        .get("positions")
        .and_then(Value::as_array)
        .ok_or(OptionsEvidenceError::Unconfirmed)?;
    let mut result = BTreeMap::new();
    for item in positions {
        let symbol = field(item, &["symbol"])
            .and_then(Value::as_str)
            .ok_or(OptionsEvidenceError::Unconfirmed)?;
        let raw = field(item, &["quantity", "positionAmt"])
            .ok_or(OptionsEvidenceError::Unconfirmed)?;
        let amount = decimal(raw)?;
        let side = item.get("side")
            .and_then(Value::as_str)
            .filter(|v| matches!(*v, "LONG" | "SHORT"))
            .ok_or(OptionsEvidenceError::Unconfirmed)?;
        // Options quantities are signed; do not infer a futures-style BOTH.
        if (side == "LONG" && amount < Decimal::ZERO)
            || (side == "SHORT" && amount > Decimal::ZERO)
        {
            return Err(OptionsEvidenceError::Unconfirmed);
        }
        let key = format!("{symbol}:{side}");
        if result.insert(key, amount).is_some() {
            return Err(OptionsEvidenceError::Unconfirmed);
        }
    }
    Ok(result)
}

/// Parse Options margin balances without merging them into Spot/UM wallets.
pub fn option_asset_quantities(
    snapshot: &Value,
) -> Result<BTreeMap<String, Decimal>, OptionsEvidenceError> {
    let balances = snapshot
        .get("balances")
        .and_then(Value::as_array)
        .ok_or(OptionsEvidenceError::Unconfirmed)?;
    let mut result = BTreeMap::new();
    for item in balances {
        let asset = field(item, &["asset"])
            .and_then(Value::as_str)
            .ok_or(OptionsEvidenceError::Unconfirmed)?;
        let amount = decimal(
            field(item, &["marginBalance", "balance", "walletBalance"])
                .ok_or(OptionsEvidenceError::Unconfirmed)?,
        )?;
        if result.insert(asset.to_owned(), amount).is_some() {
            return Err(OptionsEvidenceError::Unconfirmed);
        }
    }
    Ok(result)
}

fn decimal(value: &Value) -> Result<Decimal, OptionsEvidenceError> {
    let repr = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.is_number().then(|| value.to_string()))
        .ok_or(OptionsEvidenceError::Unconfirmed)?;
    Decimal::from_str(&repr).map_err(|_| OptionsEvidenceError::Unconfirmed)
}

fn field<'a>(item: &'a Value, alternatives: &[&str]) -> Option<&'a Value> {
    alternatives.iter().find_map(|key| item.get(*key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_account_reads_follow_eapi_order_and_preserve_snapshot() {
        let mut paths = Vec::new();
        let result = collect_options_account_snapshot::<OptionsEvidenceError, _>(|path| {
            paths.push(path);
            match path {
                EAPI_ACCOUNT_PATH => Ok(json!({"asset":[{"asset":"USDT","marginBalance":"1.25"}]})),
                EAPI_POSITION_PATH => Ok(json!([{"symbol":"BTC-271231-90000-C","side":"LONG","quantity":"2"}])),
                _ => panic!("unexpected route"),
            }
        }).unwrap();
        assert_eq!(paths, [EAPI_ACCOUNT_PATH, EAPI_POSITION_PATH]);
        assert_eq!(result["product"], "options");
        assert_eq!(result["balances"][0]["marginBalance"], "1.25");
        assert_eq!(result["positions"][0]["quantity"], "2");
        assert!(result["observed_at"].is_null());
    }

    #[test]
    fn failed_first_read_never_queries_positions_or_returns_partial_evidence() {
        let mut calls = 0;
        let result = collect_options_account_snapshot::<OptionsEvidenceError, _>(|_| {
            calls += 1;
            Err(OptionsEvidenceError::Unconfirmed)
        });
        assert_eq!(result, Err(OptionsEvidenceError::Unconfirmed));
        assert_eq!(calls, 1);
    }

    #[test]
    fn failed_second_read_and_invalid_body_fail_closed() {
        let mut calls = 0;
        let result = collect_options_account_snapshot::<OptionsEvidenceError, _>(|_| {
            calls += 1;
            if calls == 1 { Ok(json!([])) } else { Err(OptionsEvidenceError::Unconfirmed) }
        });
        assert_eq!(result, Err(OptionsEvidenceError::Unconfirmed));
        assert_eq!(calls, 2);
        let malformed = collect_options_account_snapshot::<OptionsEvidenceError, _>(|path| {
            if path == EAPI_ACCOUNT_PATH { Ok(json!({})) } else { Ok(json!([])) }
        });
        assert_eq!(malformed, Err(OptionsEvidenceError::Unconfirmed));
    }

    #[test]
    fn preserves_options_position_sides_and_quantity_fallbacks() {
        let snapshot = json!({"positions":[
            {"symbol":"BTC-271231-90000-C","side":"LONG","quantity":"1.250"},
            {"symbol":"BTC-271231-90000-C","side":"SHORT","positionAmt":"-0.50"}
        ]});
        let amounts = option_position_quantities(&snapshot).unwrap();
        assert_eq!(amounts["BTC-271231-90000-C:LONG"], Decimal::new(1250, 3));
        assert_eq!(amounts["BTC-271231-90000-C:SHORT"], Decimal::new(-50, 2));
    }

    #[test]
    fn options_balances_use_margin_priority_and_preserve_small_values() {
        let snapshot = json!({"balances":[
            {"asset":"USDT","marginBalance":"0.00000001","balance":"1234"},
            {"asset":"BNB","walletBalance":"-1.25"}
        ]});
        let balances = option_asset_quantities(&snapshot).unwrap();
        assert_eq!(balances["USDT"], Decimal::new(1, 8));
        assert_eq!(balances["BNB"], Decimal::new(-125, 2));
    }

    #[test]
    fn options_duplicate_or_malformed_account_evidence_fails_closed() {
        let bad_positions = json!({"positions":[
            {"symbol":"ETH-271231-1000-P","side":"LONG","quantity":"1"},
            {"symbol":"ETH-271231-1000-P","side":"LONG","quantity":"2"}
        ]});
        assert_eq!(option_position_quantities(&bad_positions), Err(OptionsEvidenceError::Unconfirmed));
        assert_eq!(
            option_position_quantities(&json!({"positions":[{"symbol":"X","side":"LONG","quantity":"NaN"}]})),
            Err(OptionsEvidenceError::Unconfirmed)
        );
        assert_eq!(
            option_asset_quantities(&json!({"balances":[
                {"asset":"USDT","marginBalance":"1"},
                {"asset":"USDT","marginBalance":"2"}
            ]})),
            Err(OptionsEvidenceError::Unconfirmed)
        );
        assert_eq!(
            option_asset_quantities(&json!({"balances":[{"asset":"USDT"}]})),
            Err(OptionsEvidenceError::Unconfirmed)
        );
    }

    #[test]
    fn invalid_position_directions_fail_closed() {
        for position in [
            json!({"symbol":"BTC-271231-90000-C","quantity":"1"}),
            json!({"symbol":"BTC-271231-90000-C","positionSide":"LONG","quantity":"1"}),
            json!({"symbol":"BTC-271231-90000-C","side":"BOTH","quantity":"1"}),
            json!({"symbol":"BTC-271231-90000-C","side":"SHORT","quantity":"1"}),
            json!({"symbol":"BTC-271231-90000-C","side":"LONG","quantity":"-1"}),
        ] {
            assert_eq!(
                option_position_quantities(&json!({"positions":[position]})),
                Err(OptionsEvidenceError::Unconfirmed)
            );
        }
    }

    #[test]
    fn accepts_official_signed_margin_account_envelope() {
        let positions = json!([{"symbol":"BTC-271231-90000-P","quantity":"1.00"}]);
        let account = json!({"asset":[{"asset":"USDT","marginBalance":"0.20"}]});
        let result = normalize_options_account_snapshot(&account, &positions).unwrap();
        assert_eq!(result["balances"][0]["marginBalance"], "0.20");
        assert_eq!(result["positions"], positions);
        assert!(result["observed_at"].is_null());
        assert_eq!(
            normalize_options_account_snapshot(
                &json!({"balances":[{"asset":"USDT","marginBalance":"0.20"}]}),
                &positions
            ),
            Err(OptionsEvidenceError::Unconfirmed)
        );
    }

    #[test]
    fn rejects_missing_account_or_position_collections() {
        assert_eq!(
            normalize_options_account_snapshot(&json!({}), &json!([])),
            Err(OptionsEvidenceError::Unconfirmed)
        );
        assert_eq!(
            normalize_options_account_snapshot(&json!([]), &json!({})),
            Err(OptionsEvidenceError::Unconfirmed)
        );
        assert_eq!(
            normalize_options_account_snapshot(
                &json!({"balances":"not-an-array"}), &json!([])
            ),
            Err(OptionsEvidenceError::Unconfirmed)
        );
    }
}
