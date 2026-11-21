//! Binance Options signed trade-evidence normalization.
//!
//! This crate owns Options-only evidence decoding. It never sends orders,
//! signs requests, stores credentials, or treats an order ACK as a fill.
//! An incomplete or inconsistent payload fails closed.
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
fn decimal<'a>(row: &'a Value, key: &str) -> Result<(&'a str, Decimal), OptionsEvidenceError> {
    let raw = row
        .get(key)
        .and_then(Value::as_str)
        .ok_or(OptionsEvidenceError::Unconfirmed)?;
    let amount = Decimal::from_str(raw).map_err(|_| OptionsEvidenceError::Unconfirmed)?;
    Ok((raw, amount))
}
pub fn normalize_option_trades(
    symbol: &str,
    order_id: &str,
    rows: &[Value],
) -> Result<Vec<Value>, OptionsEvidenceError> {
    let expected_order = order_id
        .parse::<u64>()
        .map_err(|_| OptionsEvidenceError::Unconfirmed)?;
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for row in rows {
        if row.get("symbol").and_then(Value::as_str) != Some(symbol) {
            return Err(OptionsEvidenceError::Unconfirmed);
        }
        if parse_trade_id(row.get("orderId").ok_or(OptionsEvidenceError::Unconfirmed)?) != Ok(expected_order) {
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
    }
}
