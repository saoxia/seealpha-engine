//! Signed Binance Options exercise evidence normalization.
//! Exercise records prove exchange outcomes, not arbitrary calculated wallet changes.
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::{collections::HashSet, str::FromStr};

use crate::OptionsEvidenceError;

fn parse_option_amount(value: &Value) -> Result<Decimal, OptionsEvidenceError> {
    let text = value.as_str().ok_or(OptionsEvidenceError::Unconfirmed)?;
    Decimal::from_str(text).map_err(|_| OptionsEvidenceError::Unconfirmed)
}

pub fn normalize_option_exercises(rows: &Value, symbol: &str) -> Result<Vec<Value>, OptionsEvidenceError> {
    let rows = rows.as_array().ok_or(OptionsEvidenceError::Unconfirmed)?;
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for row in rows {
        if row.get("symbol").and_then(Value::as_str) != Some(symbol) {
            return Err(OptionsEvidenceError::Unconfirmed);
        }
        // The exchange exercise record is evidence of an outcome, not an
        // instruction to generate an additional cash movement.
        let exercise_id = row
            .get("id")
            .or_else(|| row.get("exerciseId"))
            .and_then(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .or_else(|| v.as_u64().map(|n| n.to_string()))
            })
            .filter(|v| !v.is_empty())
            .ok_or(OptionsEvidenceError::Unconfirmed)?;
        for field in ["exercisePrice", "quantity", "amount", "fee"] {
            parse_option_amount(row.get(field).ok_or(OptionsEvidenceError::Unconfirmed)?)?;
        }
        if row
            .get("createDate")
            .and_then(Value::as_i64)
            .is_none_or(|t| t <= 0)
        {
            return Err(OptionsEvidenceError::Unconfirmed);
        }
        if !seen.insert(exercise_id.clone()) {
            return Err(OptionsEvidenceError::Unconfirmed);
        }
        result.push(json!({"product":"options","symbol":symbol,
            "exercise_id":exercise_id,"source":"binance_signed_exercise_record",
            "raw":row,"account_exercise_confirmed":true}));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_record() -> Value {
        json!({
            "symbol": "BTC-271231-90000-C",
            "id": 77,
            "exercisePrice": "90000.00000001",
            "quantity": "0.250",
            "amount": "-0.00000001",
            "fee": "-0.01",
            "createDate": 1700000000123i64
        })
    }

    #[test]
    fn signed_exercise_keeps_identity_and_raw_precision_without_cash_inference() {
        let raw = json!([valid_record()]);
        let result = normalize_option_exercises(&raw, "BTC-271231-90000-C").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["exercise_id"], "77");
        assert_eq!(result[0]["raw"]["amount"], "-0.00000001");
        assert_eq!(result[0]["raw"]["fee"], "-0.01");
        assert_eq!(result[0]["account_exercise_confirmed"], true);
        assert!(result[0].get("wallet_delta").is_none());
    }

    #[test]
    fn invalid_duplicate_wrong_symbol_and_missing_id_fail_closed() {
        let row = valid_record();
        assert_eq!(
            normalize_option_exercises(&json!([row, valid_record()]), "BTC-271231-90000-C"),
            Err(OptionsEvidenceError::Unconfirmed)
        );
        assert_eq!(
            normalize_option_exercises(&json!([valid_record()]), "BTC-271231-90000-P"),
            Err(OptionsEvidenceError::Unconfirmed)
        );
        assert_eq!(
            normalize_option_exercises(&json!([{"symbol":"BTC-271231-90000-C","amount":"1"}]),"BTC-271231-90000-C"),
            Err(OptionsEvidenceError::Unconfirmed)
        );
        let mut malformed = valid_record();
        malformed["quantity"] = json!("NaN");
        assert_eq!(
            normalize_option_exercises(&json!([malformed]),"BTC-271231-90000-C"),
            Err(OptionsEvidenceError::Unconfirmed)
        );
    }
}
