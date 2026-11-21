//! Signed, bounded settlement evidence readers. These methods never post
//! assumed cash movements; callers must reconcile external transaction IDs.
use crate::{Credentials, VenueClient, VenueError, check_credentials, venue_symbol};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::str::FromStr;

fn parse_amount(value: &Value) -> Result<Decimal, VenueError> {
    let text = value.as_str().ok_or(VenueError::Unconfirmed)?;
    Decimal::from_str(text).map_err(|_| VenueError::Unconfirmed)
}
fn transaction_id(row: &Value) -> Result<String, VenueError> {
    let id = row.get("tranId").ok_or(VenueError::Unconfirmed)?;
    let id = id
        .as_str()
        .map(str::to_owned)
        .or_else(|| id.as_u64().map(|v| v.to_string()))
        .ok_or(VenueError::Unconfirmed)?;
    if id.is_empty() {
        return Err(VenueError::Unconfirmed);
    }
    Ok(id)
}

pub fn normalize_um_income(rows: &Value, symbol: &str) -> Result<Vec<Value>, VenueError> {
    let rows = rows.as_array().ok_or(VenueError::Unconfirmed)?;
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for row in rows {
        if row["incomeType"] != "FUNDING_FEE" || row["symbol"].as_str() != Some(symbol) {
            return Err(VenueError::Unconfirmed);
        }
        let id = transaction_id(row)?;
        let amount = parse_amount(row.get("income").ok_or(VenueError::Unconfirmed)?)?;
        let asset = row
            .get("asset")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .ok_or(VenueError::Unconfirmed)?;
        let time = row
            .get("time")
            .and_then(Value::as_i64)
            .filter(|t| *t > 0)
            .ok_or(VenueError::Unconfirmed)?;
        if !seen.insert(id.clone()) {
            return Err(VenueError::Unconfirmed);
        }
        result.push(
            json!({"product":"futures_um","symbol":symbol,"income_type":"FUNDING_FEE",
            "transaction_id":id,"income":amount.to_string(),"asset":asset,
            "time_ms":time,"source":"binance_signed_income"}),
        );
    }
    Ok(result)
}

pub fn normalize_option_exercises(rows: &Value, symbol: &str) -> Result<Vec<Value>, VenueError> {
    let rows = rows.as_array().ok_or(VenueError::Unconfirmed)?;
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for row in rows {
        if row.get("symbol").and_then(Value::as_str) != Some(symbol) {
            return Err(VenueError::Unconfirmed);
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
            .ok_or(VenueError::Unconfirmed)?;
        for field in ["exercisePrice", "quantity", "amount", "fee"] {
            parse_amount(row.get(field).ok_or(VenueError::Unconfirmed)?)?;
        }
        if row
            .get("createDate")
            .and_then(Value::as_i64)
            .is_none_or(|t| t <= 0)
        {
            return Err(VenueError::Unconfirmed);
        }
        if !seen.insert(exercise_id.clone()) {
            return Err(VenueError::Unconfirmed);
        }
        result.push(json!({"product":"options","symbol":symbol,
            "exercise_id":exercise_id,"source":"binance_signed_exercise_record",
            "raw":row,"account_exercise_confirmed":true}));
    }
    Ok(result)
}
impl VenueClient {
    /// Conservative, explicitly time-bounded funding collection. A full page
    /// is treated as incomplete instead of silently dropping later records.
    pub fn um_funding_income(
        &self,
        credentials: &Credentials,
        symbol: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Value>, VenueError> {
        check_credentials(credentials)?;
        if start_ms <= 0 || end_ms <= start_ms || end_ms - start_ms > 7 * 86_400_000 {
            return Err(VenueError::Rejected("资金费对账时间窗口无效。".into()));
        }
        let symbol = venue_symbol(symbol)?;
        let value = self.signed(
            credentials,
            "GET",
            "/fapi/v1/income",
            &[
                ("symbol".into(), symbol.clone()),
                ("incomeType".into(), "FUNDING_FEE".into()),
                ("startTime".into(), start_ms.to_string()),
                ("endTime".into(), (end_ms - 1).to_string()),
                ("limit".into(), "1000".into()),
            ],
        )?;
        let rows = value.as_array().ok_or(VenueError::Unconfirmed)?;
        if rows.len() >= 1000 {
            return Err(VenueError::Unconfirmed);
        }
        let normalized = normalize_um_income(&value, &symbol)?;
        if normalized.iter().any(|row| {
            row["time_ms"]
                .as_i64()
                .is_none_or(|time| time < start_ms || time >= end_ms)
        }) {
            return Err(VenueError::Unconfirmed);
        }
        Ok(normalized)
    }

    /// A signed exercise record is authoritative only as a record, not a
    /// complete settlement ledger. Do not infer wallet delta from option math.
    pub fn option_exercise_records(
        &self,
        credentials: &Credentials,
        symbol: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Value>, VenueError> {
        check_credentials(credentials)?;
        if start_ms <= 0 || end_ms <= start_ms || end_ms - start_ms > 7 * 86_400_000 {
            return Err(VenueError::Rejected("行权对账时间窗口无效。".into()));
        }
        let symbol = venue_symbol(symbol)?;
        let value = self.signed(
            credentials,
            "GET",
            "/eapi/v1/exerciseRecord",
            &[
                ("symbol".into(), symbol.clone()),
                ("startTime".into(), start_ms.to_string()),
                ("endTime".into(), (end_ms - 1).to_string()),
                ("limit".into(), "1000".into()),
            ],
        )?;
        let rows = value.as_array().ok_or(VenueError::Unconfirmed)?;
        if rows.len() >= 1000 {
            return Err(VenueError::Unconfirmed);
        }
        let normalized = normalize_option_exercises(&value, &symbol)?;
        if normalized.iter().any(|r| {
            r["raw"]["createDate"]
                .as_i64()
                .is_none_or(|t| t < start_ms || t >= end_ms)
        }) {
            return Err(VenueError::Unconfirmed);
        }
        Ok(normalized)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn signed_income_keeps_negative_cash_flow_and_unique_ids() {
        let row = json!([{"symbol":"ETHUSDT","incomeType":"FUNDING_FEE",
            "income":"-0.00000001","asset":"USDT","time":1234,"tranId":91}]);
        let norm = normalize_um_income(&row, "ETHUSDT").unwrap();
        assert_eq!(norm[0]["income"], "-0.00000001");
        assert!(normalize_um_income(&json!([row[0], row[0]]), "ETHUSDT").is_err());
    }
    #[test]
    fn public_exercise_without_exchange_identity_is_not_settlement() {
        let row = json!([{"symbol":"BTC-271231-90000-C","amount":"1"}]);
        assert!(normalize_option_exercises(&row, "BTC-271231-90000-C").is_err());
    }
}
