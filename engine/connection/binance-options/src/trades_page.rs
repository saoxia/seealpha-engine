//! Bounded signed Binance Options user-trades pagination protocol.
//! The caller supplies a signed, read-only GET callback. No HTTP client,
//! secrets, execution commands, account mutation or retries live here.
use serde_json::Value;
use std::collections::BTreeSet;

use crate::{OptionsEvidenceError, normalize_option_trades, parse_trade_id};

pub const EAPI_USER_TRADES_PATH: &str = "/eapi/v1/userTrades";
const PAGE_LIMIT: usize = 1000;
const MAX_PAGES: usize = 32;

/// Construct only the options-specific GET fields; the caller owns signing.
pub fn option_user_trades_page_params(symbol: &str, from_id: u64) -> Vec<(String, String)> {
    vec![
        ("symbol".into(), symbol.into()),
        ("fromId".into(), from_id.to_string()),
        ("limit".into(), PAGE_LIMIT.to_string()),
    ]
}

/// Traverse consecutive exchange-authored pages before accepting any fills.
/// Each page callback MUST return the signed GET response for the given cursor.
/// An incomplete 32nd saturated page, duplicate IDs or regression fails closed.
pub fn collect_option_order_trades<E, F>(
    symbol: &str,
    order_id: &str,
    mut signed_read: F,
) -> Result<Vec<Value>, E>
where
    E: From<OptionsEvidenceError>,
    F: FnMut(u64) -> Result<Value, E>,
{
    let invalid = || E::from(OptionsEvidenceError::Unconfirmed);
    let _ = order_id.parse::<u64>().map_err(|_| invalid())?;
    let mut from_id = 0u64;
    let mut all = Vec::new();
    let mut seen = BTreeSet::new();

    for _ in 0..MAX_PAGES {
        let reply = signed_read(from_id)?;
        let rows = reply.as_array().ok_or_else(invalid)?;
        if rows.len() > PAGE_LIMIT {
            return Err(invalid());
        }
        let mut greatest = None;
        for row in rows {
            let id = parse_trade_id(row.get("id").ok_or_else(invalid)?)
                .map_err(E::from)?;
            if id < from_id || !seen.insert(id) {
                return Err(invalid());
            }
            greatest = Some(greatest.map_or(id, |old: u64| old.max(id)));
            all.push(row.clone());
        }
        if rows.len() < PAGE_LIMIT {
            return normalize_option_trades(symbol, order_id, &all).map_err(E::from);
        }
        from_id = greatest
            .ok_or_else(invalid)?
            .checked_add(1)
            .ok_or_else(invalid)?;
    }
    Err(invalid())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(id: u64) -> Value {
        json!({
            "id": id, "tradeId": id, "orderId": 9,
            "symbol": "BTC-271231-90000-C",
            "price": "12.500", "quantity": "0.125",
            "fee": "-0.001", "realizedProfit": "0",
            "side": "BUY", "quoteAsset": "USDT", "time": 1700000000000i64,
        })
    }

    #[test]
    fn keeps_wire_contract_and_returns_only_verified_trades() {
        assert_eq!(EAPI_USER_TRADES_PATH, "/eapi/v1/userTrades");
        assert_eq!(
            option_user_trades_page_params("BTC-271231-90000-C", 17),
            vec![
                ("symbol".into(), "BTC-271231-90000-C".into()),
                ("fromId".into(), "17".into()),
                ("limit".into(), "1000".into()),
            ]
        );
        let result = collect_option_order_trades::<OptionsEvidenceError, _>(
            "BTC-271231-90000-C", "9",
            |cursor| { assert_eq!(cursor, 0); Ok(json!([row(77)])) },
        ).unwrap();
        assert_eq!(result[0]["trade_id"], "binance:options:BTC-271231-90000-C:77");
        assert_eq!(result[0]["fee_signed"], "-0.001");
    }

    #[test]
    fn a_full_page_requires_a_verified_next_page() {
        let page = Value::Array((1..=1000).map(row).collect());
        let mut cursors = Vec::new();
        let fills = collect_option_order_trades::<OptionsEvidenceError, _>(
            "BTC-271231-90000-C", "9",
            |cursor| {
                cursors.push(cursor);
                Ok(if cursor == 0 { page.clone() } else { json!([row(1001)]) })
            },
        ).unwrap();
        assert_eq!(cursors, vec![0, 1001]);
        assert_eq!(fills.len(), 1001);
        assert_eq!(fills[1000]["venue_execution_id"], "1001");
    }

    #[test]
    fn duplicate_trade_ids_and_invalid_order_ids_fail_without_retries() {
        let duplicate = collect_option_order_trades::<OptionsEvidenceError, _>(
            "BTC-271231-90000-C", "9",
            |_| Ok(json!([row(7), row(7)])),
        );
        assert_eq!(duplicate, Err(OptionsEvidenceError::Unconfirmed));
        let invalid = collect_option_order_trades::<OptionsEvidenceError, _>(
            "BTC-271231-90000-C", "not-an-id",
            |_| -> Result<Value, OptionsEvidenceError> {
                panic!("invalid order ID must prevent any exchange request");
            },
        );
        assert_eq!(invalid, Err(OptionsEvidenceError::Unconfirmed));
    }
}
