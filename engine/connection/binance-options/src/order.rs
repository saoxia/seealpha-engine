//! Binance Options EAPI order parameter construction.
//!
//! This module only builds and validates deterministic wire parameters.
//! The legacy VenueClient still owns the durable intent guard, signing,
//! transport, and confirmation policy. Do not submit from this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionsOrderFieldError {
    MissingLimitPrice,
    UnsupportedOrderType,
    InvalidTimeInForce,
}

impl OptionsOrderFieldError {
    pub const fn message(self) -> &'static str {
        match self {
            Self::MissingLimitPrice => "限价单需要有效价格。",
            Self::UnsupportedOrderType => "期权订单仅支持已核实的 LIMIT 类型。",
            Self::InvalidTimeInForce => "期权订单有效期无效。",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OptionsOrderFields<'a> {
    pub symbol: &'a str,
    pub side: &'a str,
    pub order_type: &'a str,
    pub quantity: &'a str,
    pub price: Option<&'a str>,
    pub client_order_id: &'a str,
    pub reduce_only: bool,
    pub time_in_force: &'a str,
}

/// Keep exact prior field order and values for signed EAPI requests.
/// This is not an order-admission decision: persistent admission stays in Venue.
pub fn build_options_order_pairs(
    fields: &OptionsOrderFields<'_>,
) -> Result<Vec<(String, String)>, OptionsOrderFieldError> {
    // Preserve the previous LIMIT-price precedence before unsupported order type.
    if fields.order_type.eq_ignore_ascii_case("limit") && fields.price.is_none() {
        return Err(OptionsOrderFieldError::MissingLimitPrice);
    }
    if !fields.order_type.eq_ignore_ascii_case("limit") {
        return Err(OptionsOrderFieldError::UnsupportedOrderType);
    }
    if !matches!(
        fields.time_in_force.to_ascii_uppercase().as_str(),
        "GTC" | "IOC" | "FOK" | "GTX"
    ) {
        return Err(OptionsOrderFieldError::InvalidTimeInForce);
    }

    Ok(vec![
        ("symbol".into(), fields.symbol.into()),
        ("side".into(), fields.side.to_ascii_uppercase()),
        ("type".into(), fields.order_type.to_ascii_uppercase()),
        ("quantity".into(), fields.quantity.into()),
        ("clientOrderId".into(), fields.client_order_id.into()),
        ("price".into(), fields.price.unwrap_or_default().into()),
        ("timeInForce".into(), fields.time_in_force.into()),
        (
            "reduceOnly".into(),
            if fields.reduce_only { "true" } else { "false" }.into(),
        ),
        ("postOnly".into(), "false".into()),
    ])
}

/// The only EAPI order resource supported by the current Options order path.
/// Transport and request signing remain with the existing VenueClient.
pub const EAPI_ORDER_PATH: &str = "/eapi/v1/order";

/// Binance Options uses `clientOrderId` for signed order GET/DELETE.
/// Standard Spot / USD-M endpoints instead use `origClientOrderId`.
/// Return exactly the original ordered payload; do not mutate or send it.
pub fn options_lookup_pairs(symbol: &str, client_order_id: &str) -> Vec<(String, String)> {
    vec![
        ("symbol".to_owned(), symbol.to_owned()),
        ("clientOrderId".to_owned(), client_order_id.to_owned()),
    ]
}


/// Validate the exchange-authored identity before a signed Options order GET
/// can be treated as evidence. Presentation fallbacks are not authoritative.
pub fn validate_options_order_reply(
    reply: &serde_json::Value,
    symbol: &str,
    client_order_id: &str,
) -> Result<(), crate::OptionsEvidenceError> {
    if reply.get("clientOrderId").and_then(serde_json::Value::as_str)
        != Some(client_order_id)
        || reply.get("symbol").and_then(serde_json::Value::as_str) != Some(symbol)
        || reply.get("orderId").is_none_or(serde_json::Value::is_null)
    {
        return Err(crate::OptionsEvidenceError::Unconfirmed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_order_reply_must_bind_symbol_client_and_exchange_order_id() {
        use serde_json::json;
        let symbol = "BTC-271231-90000-C";
        let client = "SA123";
        let valid = json!({"symbol":symbol,"clientOrderId":client,"orderId":42,"status":"NEW"});
        assert_eq!(validate_options_order_reply(&valid,symbol,client),Ok(()));
        for invalid in [
            json!({"symbol":"ETH-271231-90000-C","clientOrderId":client,"orderId":42}),
            json!({"symbol":symbol,"clientOrderId":"different","orderId":42}),
            json!({"symbol":symbol,"clientOrderId":client}),
            json!({"symbol":symbol,"clientOrderId":client,"orderId":null}),
        ] {
            assert_eq!(
                validate_options_order_reply(&invalid,symbol,client),
                Err(crate::OptionsEvidenceError::Unconfirmed)
            );
        }
    }


    fn fields<'a>(price: Option<&'a str>, order_type: &'a str, time_in_force: &'a str) -> OptionsOrderFields<'a> {
        OptionsOrderFields {
            symbol: "BTC-271231-90000-C",
            side: "buy",
            order_type,
            quantity: "0.25",
            price,
            client_order_id: "SA123",
            reduce_only: true,
            time_in_force,
        }
    }

    #[test]
    fn query_and_cancel_keep_eapi_path_and_client_order_id_wire_field() {
        assert_eq!(EAPI_ORDER_PATH, "/eapi/v1/order");
        assert_eq!(
            options_lookup_pairs("BTC-271231-90000-C", "SA-123"),
            vec![
                ("symbol".to_owned(), "BTC-271231-90000-C".to_owned()),
                ("clientOrderId".to_owned(), "SA-123".to_owned()),
            ]
        );
    }

    #[test]
    fn preserves_exact_signed_parameter_sequence_and_values() {
        let got = build_options_order_pairs(&fields(Some("12.00"), "limit", "gtx")).unwrap();
        let expected = vec![
            ("symbol".into(), "BTC-271231-90000-C".into()),
            ("side".into(), "BUY".into()),
            ("type".into(), "LIMIT".into()),
            ("quantity".into(), "0.25".into()),
            ("clientOrderId".into(), "SA123".into()),
            ("price".into(), "12.00".into()),
            ("timeInForce".into(), "gtx".into()),
            ("reduceOnly".into(), "true".into()),
            ("postOnly".into(), "false".into()),
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn retains_original_failure_reasons_and_order_of_checks() {
        assert_eq!(
            build_options_order_pairs(&fields(None, "LIMIT", "DAY")),
            Err(OptionsOrderFieldError::MissingLimitPrice)
        );
        assert_eq!(
            build_options_order_pairs(&fields(None, "MARKET", "GTC")),
            Err(OptionsOrderFieldError::UnsupportedOrderType)
        );
        assert_eq!(
            build_options_order_pairs(&fields(Some("1"), "LIMIT", "DAY")),
            Err(OptionsOrderFieldError::InvalidTimeInForce)
        );
    }

    #[test]
    fn supports_only_existing_option_tif_values_without_normalizing_wire_text() {
        for tif in ["GTC", "IOC", "FOK", "GTX"] {
            let got = build_options_order_pairs(&fields(Some("1"), "LIMIT", tif)).unwrap();
            assert_eq!(got[6].1, tif);
        }
    }
}
