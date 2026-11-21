//! Canonicalize Binance Options signed read-only account/position responses.
//! Inputs are the two *separate* signed EAPI GET responses. No balances or
//! positions are inferred from order acknowledgements or another market.
use serde_json::{Value, json};

use crate::OptionsEvidenceError;

/// Preserve the exact historical JSON contract consumed by Runtime/Ledger:
/// "product", "balances", "positions", "observed_at".
///
/// This validates container shape and does not claim the snapshot is current,
/// complete, or an authorization to open positions.
pub fn normalize_options_account_snapshot(
    account: &Value,
    positions: &Value,
) -> Result<Value, OptionsEvidenceError> {
    let balances = account
        .as_array()
        .or_else(|| account.get("balances").and_then(Value::as_array))
        .or_else(|| account.get("asset").and_then(Value::as_array))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_both_supported_signed_account_envelopes_without_revaluation() {
        let positions = json!([{"symbol":"BTC-271231-90000-P","quantity":"1.00"}]);
        for account in [
            json!([{"asset":"USDT","marginBalance":"0.20"}]),
            json!({"balances":[{"asset":"USDT","marginBalance":"0.20"}]}),
            json!({"asset":[{"asset":"USDT","marginBalance":"0.20"}]})
        ] {
            let result = normalize_options_account_snapshot(&account, &positions).unwrap();
            assert_eq!(result["product"], "options");
            assert_eq!(result["balances"][0]["marginBalance"], "0.20");
            assert_eq!(result["positions"], positions);
            assert!(result["observed_at"].is_null());
        }
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
