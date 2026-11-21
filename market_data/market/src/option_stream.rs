//! Stateless EAPI optionTrade stream normalization: never creates a fill.
use crate::{
    MarketError,
    options::{OptionTrade, parse_option_symbol},
};
use rust_decimal::Decimal;
use serde_json::Value;

/// Normalize a Binance public optionTrade event. The maker flag determines
/// the aggressor's side; timestamp remains exchange milliseconds.
pub fn parse_option_trade_event(
    value: &Value,
    expected_symbol: &str,
) -> Result<OptionTrade, MarketError> {
    parse_option_symbol(expected_symbol)?;
    if value["e"].as_str() != Some("trade")
        || value["s"].as_str() != Some(expected_symbol)
        || !matches!(value["X"].as_str(), Some("MARKET" | "BLOCK"))
    {
        return Err(MarketError::Symbol);
    }
    let maker = value["m"].as_bool().ok_or(MarketError::Payload)?;
    let id = value["t"].as_i64().ok_or(MarketError::Payload)?;
    let timestamp = value["T"].as_i64().ok_or(MarketError::Time)?;
    // Legacy option parser rejects lossy floating-point wire numbers.
    fn precise(value: &Value) -> Result<Decimal, MarketError> {
        match value {
            Value::String(text) => text.parse::<Decimal>().map_err(|_| MarketError::Decimal),
            Value::Number(number) if number.is_i64() || number.is_u64() => number
                .to_string()
                .parse::<Decimal>()
                .map_err(|_| MarketError::Decimal),
            _ => Err(MarketError::Decimal),
        }
    }
    let price = precise(&value["p"])?;
    let quantity = precise(&value["q"])?;
    if id < 0 || timestamp < 0 || price <= Decimal::ZERO || quantity <= Decimal::ZERO {
        return Err(MarketError::Trade);
    }
    Ok(OptionTrade {
        symbol: expected_symbol.to_owned(),
        trade_id: id,
        timestamp_ms: timestamp,
        price,
        quantity,
        aggressor_side: if maker { -1 } else { 1 },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    const SYMBOL: &str = "BTC-261225-100000-C";

    #[test]
    fn normalize_public_trade_and_maker_semantics() {
        let buy = json!({"e":"trade","s":SYMBOL,"X":"MARKET","m":false,"t":7,"T":12345,"p":"42.10","q":"0.01"});
        let result = parse_option_trade_event(&buy, SYMBOL).unwrap();
        assert_eq!(result.aggressor_side, 1);
        assert_eq!(result.price.to_string(), "42.10");
        let mut sell = buy;
        sell["m"] = json!(true);
        sell["X"] = json!("BLOCK");
        assert_eq!(
            parse_option_trade_event(&sell, SYMBOL)
                .unwrap()
                .aggressor_side,
            -1
        );
    }

    #[test]
    fn rejects_wrong_identity_float_boolean_and_negative_amount() {
        let good = json!({"e":"trade","s":SYMBOL,"X":"MARKET","m":false,"t":7,"T":12345,"p":"42.10","q":"0.01"});
        for (field, invalid) in [
            ("s", json!("OTHER")),
            ("X", json!("UNKNOWN")),
            ("m", json!(0)),
            ("t", json!(true)),
            ("T", json!(-1)),
            ("p", json!(2.5)),
            ("q", json!("0")),
        ] {
            let mut row = good.clone();
            row[field] = invalid;
            assert!(parse_option_trade_event(&row, SYMBOL).is_err(), "{field}");
        }
    }
}
