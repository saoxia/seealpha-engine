//! Signed Binance Options trade evidence. No order may be marked filled
//! from its ACK; only userTrades with exchange IDs can create ledger fills.
use crate::{Credentials, VenueClient, VenueError, check_credentials, venue_symbol};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::str::FromStr;

fn digits(value: &Value) -> Result<u64, VenueError> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|v| v.parse::<u64>().ok()))
        .ok_or(VenueError::Unconfirmed)
}
fn decimal<'a>(row: &'a Value, key: &str) -> Result<(&'a str, Decimal), VenueError> {
    let raw = row
        .get(key)
        .and_then(Value::as_str)
        .ok_or(VenueError::Unconfirmed)?;
    let amount = Decimal::from_str(raw).map_err(|_| VenueError::Unconfirmed)?;
    Ok((raw, amount))
}
pub fn normalize_option_trades(
    symbol: &str,
    order_id: &str,
    rows: &[Value],
) -> Result<Vec<Value>, VenueError> {
    let expected_order = order_id
        .parse::<u64>()
        .map_err(|_| VenueError::Unconfirmed)?;
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for row in rows {
        if row.get("symbol").and_then(Value::as_str) != Some(symbol) {
            return Err(VenueError::Unconfirmed);
        }
        if digits(row.get("orderId").ok_or(VenueError::Unconfirmed)?) != Ok(expected_order) {
            continue;
        }
        let id = digits(row.get("id").ok_or(VenueError::Unconfirmed)?)?;
        let trade_id = digits(row.get("tradeId").ok_or(VenueError::Unconfirmed)?)?;
        if !seen.insert(id) {
            return Err(VenueError::Unconfirmed);
        }
        let (price_raw, price) = decimal(row, "price")?;
        let (quantity_raw, quantity) = decimal(row, "quantity")?;
        let (fee_raw, fee) = decimal(row, "fee")?;
        let (realized_raw, _) = decimal(row, "realizedProfit")?;
        if price <= Decimal::ZERO || quantity <= Decimal::ZERO {
            return Err(VenueError::Unconfirmed);
        }
        let side = row
            .get("side")
            .and_then(Value::as_str)
            .filter(|x| matches!(*x, "BUY" | "SELL"))
            .ok_or(VenueError::Unconfirmed)?;
        let quote_asset = row
            .get("quoteAsset")
            .and_then(Value::as_str)
            .filter(|x| !x.is_empty())
            .ok_or(VenueError::Unconfirmed)?;
        let time = row
            .get("time")
            .and_then(Value::as_i64)
            .filter(|t| *t > 0)
            .ok_or(VenueError::Unconfirmed)?;
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

impl VenueClient {
    /// Read oldest-to-newest with bounded pagination, never silently treat a
    /// saturated or truncated page as a complete trade history.
    pub fn option_order_trades(
        &self,
        credentials: &Credentials,
        symbol: &str,
        order_id: &str,
    ) -> Result<Vec<Value>, VenueError> {
        check_credentials(credentials)?;
        let canonical = venue_symbol(symbol)?;
        let _ = order_id
            .parse::<u64>()
            .map_err(|_| VenueError::Unconfirmed)?;
        let mut from_id = 0u64;
        let mut all = Vec::new();
        let mut seen = BTreeSet::new();
        for _ in 0..32 {
            let reply = self.signed(
                credentials,
                "GET",
                "/eapi/v1/userTrades",
                &[
                    ("symbol".into(), canonical.clone()),
                    ("fromId".into(), from_id.to_string()),
                    ("limit".into(), "1000".into()),
                ],
            )?;
            let rows = reply.as_array().ok_or(VenueError::Unconfirmed)?;
            let mut greatest = None;
            for row in rows {
                let id = digits(row.get("id").ok_or(VenueError::Unconfirmed)?)?;
                if id < from_id || !seen.insert(id) {
                    return Err(VenueError::Unconfirmed);
                }
                greatest = Some(greatest.map_or(id, |old: u64| old.max(id)));
                all.push(row.clone());
            }
            if rows.len() < 1000 {
                return normalize_option_trades(&canonical, order_id, &all);
            }
            from_id = greatest
                .ok_or(VenueError::Unconfirmed)?
                .checked_add(1)
                .ok_or(VenueError::Unconfirmed)?;
        }
        Err(VenueError::Unconfirmed)
    }
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
