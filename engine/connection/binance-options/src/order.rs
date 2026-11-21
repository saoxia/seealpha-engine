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

#[cfg(test)]
mod tests {
    use super::*;

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
