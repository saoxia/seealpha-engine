//! Private account-event normalization. Public market TradeTicks are not account fills.
//! This layer translates venue-specific evidence into Ledger's canonical contract.
//! HTTP signing, pagination/watermark checks, and Nautilus event ownership remain
//! the responsibilities of authenticated connectors and the native runtime.
use rust_decimal::Decimal;
use serde_json::Value;
use seealpha_ledger::{AssetEffect, MovementInput};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct AccountScope<'a> {
    pub tenant: &'a str,
    pub account: &'a str,
    pub environment: &'a str,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizeError {
    Invalid(&'static str),
}
impl std::fmt::Display for NormalizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for NormalizeError {}

fn field<'a>(value: &'a Value, name: &'static str) -> Result<&'a str, NormalizeError> {
    value.get(name).and_then(Value::as_str).filter(|s| !s.is_empty())
        .ok_or(NormalizeError::Invalid(name))
}
fn number(value: &Value, name: &'static str) -> Result<Decimal, NormalizeError> {
    Decimal::from_str(field(value,name)?).map_err(|_| NormalizeError::Invalid(name))
}
fn ms(value: &Value, name: &'static str) -> Result<i64, NormalizeError> {
    value.get(name).and_then(Value::as_i64).filter(|t| *t > 0)
        .ok_or(NormalizeError::Invalid(name))
}
fn base(scope: &AccountScope<'_>, source_type: &str, source_id: String,
        kind: &str, occurred_at_ms: i64, instrument: Option<String>,
        native_event_id: Option<String>, raw_payload: Value, effects: Vec<AssetEffect>) -> MovementInput {
    MovementInput {
        tenant_id: scope.tenant.into(), account_id: scope.account.into(),
        environment: scope.environment.into(), venue: "BINANCE".into(),
        source_type: source_type.into(), source_id,
        event_type: kind.into(), occurred_at_ms,
        observed_at_ms: scope.observed_at_ms, instrument_id: instrument,
        native_event_id, raw_payload, effects,
    }
}

/// Convert the already retrieved signed USD-M income record into a pending
/// canonical event. This does NOT authenticate the API response or post cash.
pub fn binance_usdm_funding(
    scope: &AccountScope<'_>, record: &Value,
) -> Result<MovementInput, NormalizeError> {
    if record["source"] != "binance_signed_income"
        || record["income_type"] != "FUNDING_FEE"
        || record["product"] != "futures_um" {
        return Err(NormalizeError::Invalid("funding provenance"));
    }
    let source_id = field(record,"transaction_id")?.to_owned();
    let symbol = field(record,"symbol")?;
    let asset = field(record,"asset")?;
    let income = number(record,"income")?;
    let occurred = ms(record,"time_ms")?;
    Ok(base(scope,"usdm_income_funding",source_id,"FUNDING",occurred,
        Some(format!("{symbol}-PERP.BINANCE")),None,record.clone(),
        vec![AssetEffect {asset:asset.into(),delta:income,role:"cash".into()}]))
}

/// Trade facts are distinct from public market TradeTicks. A verified native
/// OrderFilled retains its own lifecycle; this record only points to that ID.
/// No balances or fees are inferred from price * quantity.
pub fn binance_account_fill(
    scope: &AccountScope<'_>, record: &Value, native_event_id: &str,
) -> Result<MovementInput, NormalizeError> {
    if record["source"] != "signed_exchange_user_trades" || native_event_id.is_empty() {
        return Err(NormalizeError::Invalid("fill provenance/native event"));
    }
    let trade_id = field(record,"trade_id")?;
    if !trade_id.starts_with("binance:") {
        return Err(NormalizeError::Invalid("trade_id"));
    }
    let symbol = field(record,"symbol")?;
    let product = field(record,"product")?;
    let instrument = match product {
        "spot" => format!("{symbol}.BINANCE"),
        "futures_um" => format!("{symbol}-PERP.BINANCE"),
        _ => return Err(NormalizeError::Invalid("product")),
    };
    let quantity = number(record,"quantity")?;
    let price = number(record,"price")?;
    if quantity <= Decimal::ZERO || price <= Decimal::ZERO {
        return Err(NormalizeError::Invalid("price/quantity"));
    }
    let occurred = ms(record,"time_ms")?;
    Ok(base(scope,"account_fill",trade_id.into(),"TRADE",occurred,
        Some(instrument),Some(native_event_id.into()),record.clone(),vec![]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn scope() -> AccountScope<'static> {
        AccountScope { tenant:"tenant",account:"account",environment:"sandbox",observed_at_ms:100 }
    }
    #[test]
    fn funding_maps_exact_negative_decimal() {
        let input = binance_usdm_funding(&scope(),&json!({
            "source":"binance_signed_income","product":"futures_um",
            "income_type":"FUNDING_FEE","symbol":"ETHUSDT","asset":"USDT",
            "income":"-0.00000001","transaction_id":"123","time_ms":50
        })).unwrap();
        assert_eq!(input.effects[0].delta.to_string(),"-0.00000001");
        assert_eq!(input.instrument_id.as_deref(),Some("ETHUSDT-PERP.BINANCE"));
    }
    #[test]
    fn public_funding_and_unverified_fills_rejected() {
        assert!(binance_usdm_funding(&scope(),&json!({
            "source":"public_funding_rate","product":"futures_um","income_type":"FUNDING_FEE",
            "symbol":"ETHUSDT","asset":"USDT","income":"5","transaction_id":"1","time_ms":50
        })).is_err());
        assert!(binance_account_fill(&scope(),&json!({
            "source":"order_ack","trade_id":"binance:123","symbol":"ETHUSDT",
            "product":"spot","quantity":"1","price":"100","time_ms":50
        }),"evt-1").is_err());
    }
}
