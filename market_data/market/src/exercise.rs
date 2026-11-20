//! Public, exchange-authored option exercise observations.
//! This module records settlement reference evidence, not an account's own
//! exercise amount or cash payout. Payouts need signed account statements,
//! contract unit and quote currency before they can become ledger fills.
use crate::{MarketError, options::parse_option_symbol};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const HISTORY: &str = "https://eapi.binance.com/eapi/v1/exerciseHistory";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExerciseObservation {
    pub symbol: String,
    pub expiry_time_ms: i64,
    pub strike: String,
    pub settlement_price: String,
    pub outcome: String,
}
fn parse_decimal(v: &Value) -> Result<Decimal, MarketError> {
    let text = v.as_str().ok_or(MarketError::Decimal)?;
    text.parse::<Decimal>().map_err(|_| MarketError::Decimal)
}
pub fn parse_exercise_history(
    value: &Value,
    underlying: &str,
    start: i64,
    end: i64,
) -> Result<Vec<ExerciseObservation>, MarketError> {
    let rows = value.as_array().ok_or(MarketError::Payload)?;
    if rows.len() > 100 {
        return Err(MarketError::Payload);
    }
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let symbol = row["symbol"].as_str().ok_or(MarketError::Payload)?;
        let identity = parse_option_symbol(symbol)?;
        let matching_underlying = format!("{}USDT", identity.underlying);
        if matching_underlying != underlying {
            return Err(MarketError::Symbol);
        }
        let expiry = row["expiryDate"].as_i64().ok_or(MarketError::Time)?;
        if !(start..end).contains(&expiry) {
            return Err(MarketError::Time);
        }
        let strike = parse_decimal(&row["strikePrice"])?;
        let settlement = parse_decimal(&row["realStrikePrice"])?;
        if strike <= Decimal::ZERO || settlement <= Decimal::ZERO || strike != identity.strike {
            return Err(MarketError::Payload);
        }
        let outcome = row["strikeResult"].as_str().ok_or(MarketError::Payload)?;
        if !matches!(
            outcome,
            "REALISTIC_VALUE_STRICKEN" | "EXTRINSIC_VALUE_EXPIRED"
        ) {
            return Err(MarketError::Payload);
        }
        result.push(ExerciseObservation {
            symbol: symbol.to_string(),
            expiry_time_ms: expiry,
            strike: strike.to_string(),
            settlement_price: settlement.to_string(),
            outcome: outcome.to_string(),
        });
    }
    result.sort_by(|a, b| {
        a.expiry_time_ms
            .cmp(&b.expiry_time_ms)
            .then(a.symbol.cmp(&b.symbol))
    });
    if result
        .windows(2)
        .any(|w| w[0].symbol == w[1].symbol && w[0].expiry_time_ms == w[1].expiry_time_ms)
    {
        return Err(MarketError::Payload);
    }
    Ok(result)
}

#[derive(Clone)]
pub struct ExerciseSource {
    http: reqwest::Client,
}
impl ExerciseSource {
    pub fn new() -> Result<Self, MarketError> {
        let http = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(12))
            .user_agent("seealpha-public-options-exercise/0.1")
            .build()
            .map_err(|_| MarketError::Network)?;
        Ok(Self { http })
    }
    pub async fn history(
        &self,
        underlying: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<ExerciseObservation>, MarketError> {
        if !(underlying.ends_with("USDT")
            && underlying.len() >= 5
            && underlying
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()))
        {
            return Err(MarketError::Symbol);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_millis()).ok())
            .ok_or(MarketError::Time)?;
        if start < 0 || end <= start || end > now || end - start > 90 * 86_400_000 {
            return Err(MarketError::Time);
        }
        // Binance's public exercise-history maximum page size is 100. We
        // reject a full page rather than pretending it is complete.
        let response = self
            .http
            .get(HISTORY)
            .query(&[
                ("underlying", underlying.to_string()),
                ("startTime", start.to_string()),
                ("endTime", (end - 1).to_string()),
                ("limit", "100".to_string()),
            ])
            .send()
            .await
            .map_err(|_| MarketError::Network)?;
        if !response.status().is_success() {
            return Err(MarketError::Status(response.status().as_u16()));
        }
        let body = response
            .json::<Value>()
            .await
            .map_err(|_| MarketError::Payload)?;
        if body.as_array().is_some_and(|rows| rows.len() == 100) {
            return Err(MarketError::Payload);
        }
        parse_exercise_history(&body, underlying, start, end)
    }
}
pub fn as_evidence(observation: &ExerciseObservation) -> Value {
    json!({"symbol":observation.symbol,"expiry_time_ms":observation.expiry_time_ms,
        "strike":observation.strike,"settlement_price":observation.settlement_price,
        "outcome":observation.outcome,"source":"binance_public_exercise_history",
        "account_exercise_confirmed":false})
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn public_exercise_is_not_a_confirmed_account_payout() {
        let source = json!([{"symbol":"BTC-271231-90000-P","expiryDate":1830000000000i64,
            "strikePrice":"90000","realStrikePrice":"82000",
            "strikeResult":"REALISTIC_VALUE_STRICKEN"}]);
        let records =
            parse_exercise_history(&source, "BTCUSDT", 1800000000000, 1900000000000).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            as_evidence(&records[0])["account_exercise_confirmed"],
            false
        );
        assert!(parse_exercise_history(&source, "ETHUSDT", 1800000000000, 1900000000000).is_err());
    }
}
