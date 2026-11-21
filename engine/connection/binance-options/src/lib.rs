//! Binance Options signed trade-evidence normalization.
//!
//! This crate owns Options-only evidence decoding. It never sends orders,
//! signs requests, stores credentials, or treats an order ACK as a fill.
//! An incomplete or inconsistent payload fails closed.
pub mod account;
pub mod trades_page;
pub mod settlement;
pub use settlement::normalize_option_exercises;
pub mod order;
pub use order::{OptionsOrderFields, build_options_order_pairs};
pub use account::normalize_options_account_snapshot;

use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OptionsEvidenceError {
    Unconfirmed,
}

pub fn parse_trade_id(value: &Value) -> Result<u64, OptionsEvidenceError> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|v| v.parse::<u64>().ok()))
        .ok_or(OptionsEvidenceError::Unconfirmed)
}
/// Parse exchange identifiers without lossy floating-point conversions.
pub(crate) fn parse_order_id_text(value: &str) -> Result<u64, OptionsEvidenceError> {
    value.parse::<u64>().ok().filter(|id| *id > 0)
        .ok_or(OptionsEvidenceError::Unconfirmed)
}

pub(crate) fn parse_order_id_value(value: &Value) -> Result<u64, OptionsEvidenceError> {
    parse_trade_id(value).and_then(|id| {
        (id > 0).then_some(id).ok_or(OptionsEvidenceError::Unconfirmed)
    })
}

pub(crate) fn parse_decimal_str(value: &str) -> Result<Decimal, OptionsEvidenceError> {
    Decimal::from_str(value).map_err(|_| OptionsEvidenceError::Unconfirmed)
}

fn decimal<'a>(row: &'a Value, key: &str) -> Result<(&'a str, Decimal), OptionsEvidenceError> {
    let raw = row
        .get(key)
        .and_then(Value::as_str)
        .ok_or(OptionsEvidenceError::Unconfirmed)?;
    let amount = parse_decimal_str(raw)?;
    Ok((raw, amount))
}
pub fn normalize_option_trades(
    symbol: &str,
    order_id: &str,
    rows: &[Value],
) -> Result<Vec<Value>, OptionsEvidenceError> {
    let expected_order = parse_order_id_text(order_id)?;
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for row in rows {
        if row.get("symbol").and_then(Value::as_str) != Some(symbol) {
            return Err(OptionsEvidenceError::Unconfirmed);
        }
        let observed_order = parse_order_id_value(
            row.get("orderId").ok_or(OptionsEvidenceError::Unconfirmed)?
        )?;
        if observed_order != expected_order {
            continue;
        }
        let id = parse_trade_id(row.get("id").ok_or(OptionsEvidenceError::Unconfirmed)?)?;
        let trade_id = parse_trade_id(row.get("tradeId").ok_or(OptionsEvidenceError::Unconfirmed)?)?;
        if !seen.insert(id) {
            return Err(OptionsEvidenceError::Unconfirmed);
        }
        let (price_raw, price) = decimal(row, "price")?;
        let (quantity_raw, quantity) = decimal(row, "quantity")?;
        let (fee_raw, fee) = decimal(row, "fee")?;
        let (realized_raw, _) = decimal(row, "realizedProfit")?;
        if price <= Decimal::ZERO || quantity <= Decimal::ZERO {
            return Err(OptionsEvidenceError::Unconfirmed);
        }
        let side = row
            .get("side")
            .and_then(Value::as_str)
            .filter(|x| matches!(*x, "BUY" | "SELL"))
            .ok_or(OptionsEvidenceError::Unconfirmed)?;
        let quote_asset = row
            .get("quoteAsset")
            .and_then(Value::as_str)
            .filter(|x| !x.is_empty())
            .ok_or(OptionsEvidenceError::Unconfirmed)?;
        let time = row
            .get("time")
            .and_then(Value::as_i64)
            .filter(|t| *t > 0)
            .ok_or(OptionsEvidenceError::Unconfirmed)?;
        out.push(json!({
            "trade_id":format!("binance:options:{symbol}:{id}"),
            "venue_trade_id":trade_id.to_string(),
            "venue_execution_id":id.to_string(),
            "venue_order_id":order_id,
            "symbol":symbol,"side":side,
            "quantity":quantity_raw,"price":price_raw,
            "commission":fee.abs().to_string(),
            "fee_signed":fee_raw,
            "commission_asset":quote_asset,
            "realized_pnl":realized_raw,"trade_time_ms":time,
            "source":"signed_exchange_user_trades"
        }));
    }
    Ok(out)
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn option_trades_keep_real_execution_and_signed_fee() {
        let rows = vec![json!({"id":200,"tradeId":21,"orderId":10,
            "symbol":"BTC-271231-90000-C","price":"5","quantity":"2",
            "fee":"-0.25","realizedProfit":"0","side":"BUY",
            "quoteAsset":"USDT","time":100})];
        let fills = normalize_option_trades("BTC-271231-90000-C", "10", &rows).unwrap();
        assert_eq!(fills[0]["quantity"], "2");
        assert_eq!(fills[0]["fee_signed"], "-0.25");
        assert_eq!(fills[0]["commission"], "0.25");
        assert!(
            normalize_option_trades("BTC-271231-90000-C", "11", &rows)
                .unwrap()
                .is_empty()
        );
        assert!(
            normalize_option_trades(
                "BTC-271231-90000-C",
                "10",
                &[rows[0].clone(), rows[0].clone()]
            )
            .is_err()
        );
        let mut malformed = rows[0].clone();
        malformed["orderId"] = json!(false);
        assert_eq!(
            normalize_option_trades("BTC-271231-90000-C", "10", &[malformed]),
            Err(OptionsEvidenceError::Unconfirmed)
        );
        assert_eq!(parse_order_id_value(&json!("42")).unwrap(), 42);
        assert!(parse_order_id_value(&json!(0)).is_err());
        assert!(parse_order_id_value(&json!(1.5)).is_err());
    }
}
