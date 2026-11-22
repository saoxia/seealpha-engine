//! Binance USD-M compressed/aggregate trade REST evidence.
//!
//! The native Nautilus USD-M live stream and the daily Binance ZIP importer
//! use aggTrade/aggTrades IDs. Do not silently mix these with /fapi/v1/trades
//! raw execution IDs: they are different data granularities.
use crate::{MarketError, PublicTrade};
use rust_decimal::Decimal;
use serde_json::Value;

pub(crate) const AGG_TRADES_URL: &str = "https://fapi.binance.com/fapi/v1/aggTrades";

/// Parse one bounded, recent REST page. This is overlap evidence only:
/// successful parsing does not certify a historical interval, even when
/// aggregate IDs appear consecutive within the page.
pub(crate) fn parse_recent_agg_trades(value: &Value) -> Result<Vec<PublicTrade>, MarketError> {
    let rows = value.as_array().ok_or(MarketError::Payload)?;
    if rows.len() > 1000 {
        return Err(MarketError::Payload);
    }

    let mut observations = Vec::with_capacity(rows.len());
    let mut previous_aggregate_id: Option<i64> = None;
    for row in rows {
        let object = row.as_object().ok_or(MarketError::Payload)?;
        let required_i64 = |key| {
            object
                .get(key)
                .and_then(Value::as_i64)
                .ok_or(MarketError::Payload)
        };
        let aggregate_id = required_i64("a")?;
        let first_raw_id = required_i64("f")?;
        let last_raw_id = required_i64("l")?;
        let timestamp_ms = required_i64("T")?;
        // The exchange encodes price and quantity as decimal strings.
        // Reject JSON floats instead of accepting precision-losing casts.
        let decimal = |key| -> Result<Decimal, MarketError> {
            object
                .get(key)
                .and_then(Value::as_str)
                .ok_or(MarketError::Decimal)?
                .parse::<Decimal>()
                .map_err(|_| MarketError::Decimal)
        };
        let price = decimal("p")?;
        let quantity = decimal("q")?;
        let buyer_maker = object
            .get("m")
            .and_then(Value::as_bool)
            .ok_or(MarketError::Payload)?;

        if aggregate_id < 0
            || first_raw_id < 0
            || last_raw_id < first_raw_id
            || timestamp_ms < 0
            || price <= Decimal::ZERO
            || quantity <= Decimal::ZERO
            || previous_aggregate_id.is_some_and(|id| aggregate_id <= id)
        {
            return Err(MarketError::Trade);
        }
        previous_aggregate_id = Some(aggregate_id);
        observations.push(PublicTrade {
            id: aggregate_id,
            timestamp_ms,
            price,
            quantity,
            buyer_maker,
        });
    }
    Ok(observations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn aggregate_trade_has_same_id_domain_as_ws_and_official_zip() {
        let payload = json!([
            {"a":26129,"p":"0.01633102","q":"4.70443515","f":27781,"l":27781,
             "T":1498793709153_i64,"m":true},
            {"a":26130,"p":"0.01633102","q":"1.5","f":27782,"l":27784,
             "T":1498793709154_i64,"m":false}
        ]);
        let result = parse_recent_agg_trades(&payload).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, 26129);
        assert_eq!(result[0].quantity.to_string(), "4.70443515");
        assert!(result[0].buyer_maker);
        assert_eq!(result[1].id, 26130);
    }

    #[test]
    fn raw_futures_trades_are_not_compatible_aggregate_trade_evidence() {
        let raw = json!([{"id":28457,"price":"4.00000100","qty":"12",
                          "time":1499865549590_i64,"isBuyerMaker":true}]);
        assert!(parse_recent_agg_trades(&raw).is_err());
    }

    #[test]
    fn rejects_invalid_aggregate_identity_ranges_and_precision() {
        let valid = json!({"a":10,"p":"100.01","q":"0.3","f":20,"l":21,
                           "T":12345,"m":false});
        assert!(parse_recent_agg_trades(&json!([valid.clone(), valid.clone()])).is_err());

        for (field, replacement) in [
            ("a", json!(-1)),
            ("f", json!(-1)),
            ("l", json!(19)),
            ("T", json!(-1)),
            ("p", json!(0)),
            ("p", json!(100.01)),
            ("q", json!("0")),
            ("q", json!(0.3)),
            ("m", json!(0)),
        ] {
            let mut row = valid.clone();
            row[field] = replacement;
            assert!(
                parse_recent_agg_trades(&json!([row])).is_err(),
                "malformed field {field}"
            );
        }

        let earlier = json!({"a":9,"p":"100.01","q":"0.3","f":18,"l":19,
                             "T":12344,"m":false});
        assert!(parse_recent_agg_trades(&json!([valid, earlier])).is_err());
    }

    #[test]
    fn bounded_recent_page_cannot_prove_full_history() {
        // Empty recent queries are valid observations, not proof of a gap-free day.
        assert!(parse_recent_agg_trades(&json!([])).unwrap().is_empty());
        let input = json!([{"a":10,"p":"101","q":"1","f":20,"l":20,
                            "T":12345,"m":false},
                           {"a":12,"p":"101","q":"1","f":25,"l":26,
                            "T":12346,"m":false}]);
        let result = parse_recent_agg_trades(&input).unwrap();
        assert_eq!(result.len(), 2); // missing IDs must be reconciled elsewhere
        let audit = crate::pipeline::audit_trade_ids(&result).unwrap();
        assert!(audit.suspected_gap);
    }
}
