//! Durable SeeAlpha preflight for the future Nautilus-native execution path.
//! This module is not a transport, a native submitter, or live-trading approval.
//! A new execution path must additionally enforce portfolio/risk reservation
//! and native lifecycle acceptance before consuming a fresh registration.
use seealpha_ledger::{IntentStart, Ledger, LedgerError};
use serde_json::json;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeIntentDecision {
    /// First durable registration; NOT sufficient to send a real order.
    StagedFirstAttempt,
    /// The exact same intent was already registered: GET/reconcile only.
    ReconcileOnly,
}

pub(crate) struct NativeIntent<'a> {
    pub tenant: &'a str,
    pub account: &'a str,
    pub environment: &'a str,
    pub request_id: &'a str,
    pub client_order_id: &'a str,
    pub product: &'a str,
    pub symbol: &'a str,
    pub side: &'a str,
    pub order_type: &'a str,
    pub quantity: &'a str,
    pub price: &'a str,
    pub position_mode: &'a str,
}

/// Bind the complete native instruction identity to a durable Ledger intent.
/// This intentionally accepts only simple LIMIT single-leg Spot/USD-M orders.
/// Native dispatch is NOT invoked: risk authorization and an atomic pre-native
/// send cutover gate are still missing in the production Runtime.
pub(crate) fn stage_native_intent(
    ledger: &Ledger,
    intent: &NativeIntent<'_>,
) -> Result<NativeIntentDecision, LedgerError> {
    let invalid = || LedgerError::Conflict("invalid native execution intent".into());
    if !matches!(intent.environment, "sandbox" | "production")
        || !matches!(intent.product, "spot" | "futures_um")
        || intent.order_type != "LIMIT"
        || !matches!(intent.side, "BUY" | "SELL")
        || !valid_id(intent.account, 40)
        || !valid_id(intent.request_id, 128)
        || !valid_id(intent.client_order_id, 128)
        || !valid_symbol(intent.product, intent.symbol)
        || !valid_mode(intent.product, intent.position_mode)
        || !positive_decimal(intent.quantity)
        || !positive_decimal(intent.price)
    {
        return Err(invalid());
    }

    // Canonical ordered JSON encoding and SHA-256 bind all fields that affect
    // the native order; any changed mode/amount/side for an existing ID fails.
    let envelope = json!({
        "action": "NEW",
        "environment": intent.environment,
        "client_order_id": intent.client_order_id,
        "product": intent.product,
        "symbol": intent.symbol,
        "side": intent.side,
        "order_type": intent.order_type,
        "quantity": intent.quantity,
        "price": intent.price,
        "position_mode": intent.position_mode,
        "execution_backend": "nautilus_binance_native",
    });
    let digest = Sha256::digest(envelope.to_string().as_bytes());
    let fingerprint = format!("{digest:x}");
    match ledger.preregister_order_intent(
        intent.tenant,
        intent.account,
        intent.environment,
        intent.request_id,
        intent.client_order_id,
        &fingerprint,
        &envelope,
    )? {
        IntentStart::Fresh => Ok(NativeIntentDecision::StagedFirstAttempt),
        IntentStart::ReconcileOnly => Ok(NativeIntentDecision::ReconcileOnly),
    }
}

fn valid_id(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn valid_symbol(product: &str, symbol: &str) -> bool {
    let bare = if product == "futures_um" {
        symbol.strip_suffix("-PERP").unwrap_or(symbol)
    } else {
        symbol
    };
    (5..=30).contains(&bare.len())
        && bare.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        && (product != "spot" || symbol == bare)
}

fn valid_mode(product: &str, mode: &str) -> bool {
    match product {
        "spot" => mode == "spot",
        "futures_um" => matches!(mode, "one_way" | "hedge"),
        _ => false,
    }
}

fn positive_decimal(raw: &str) -> bool {
    raw.parse::<rust_decimal::Decimal>()
        .is_ok_and(|v| v > rust_decimal::Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent<'a>() -> NativeIntent<'a> {
        NativeIntent {
            tenant: "tenant1", account: "account1", environment: "sandbox",
            request_id: "request1", client_order_id: "client1",
            product: "futures_um", symbol: "BTCUSDT-PERP", side: "BUY",
            order_type: "LIMIT", quantity: "0.25", price: "2000.50",
            position_mode: "hedge",
        }
    }

    fn ledger() -> Ledger {
        let ledger = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        ledger.ensure_tenant("tenant1", "production", "active").unwrap();
        ledger
    }

    #[test]
    fn first_registration_is_durable_and_replay_is_reconcile_only() {
        let ledger = ledger();
        assert_eq!(
            stage_native_intent(&ledger, &intent()).unwrap(),
            NativeIntentDecision::StagedFirstAttempt
        );
        assert_eq!(
            stage_native_intent(&ledger, &intent()).unwrap(),
            NativeIntentDecision::ReconcileOnly
        );
        let recovered = ledger.registered_order_intent("tenant1", "account1", "request1")
            .unwrap().unwrap();
        assert_eq!(recovered["execution_backend"], "nautilus_binance_native");
        assert_eq!(recovered["position_mode"], "hedge");
        assert_eq!(recovered["quantity"], "0.25");
        assert!(ledger.order_recovery_page("tenant1", 0, 10).unwrap()[0].reconciliation_only);
    }

    #[test]
    fn native_intent_restart_never_reissues_first_attempt() {
        let path = std::env::temp_dir().join(format!(
            "seealpha-native-handoff-{}-{:?}.sqlite",
            std::process::id(),
            std::thread::current().id(),
        ));
        let _ = std::fs::remove_file(&path);
        {
            let ledger = Ledger::open(&path).unwrap();
            ledger.ensure_tenant("tenant1", "production", "active").unwrap();
            assert_eq!(
                stage_native_intent(&ledger, &intent()).unwrap(),
                NativeIntentDecision::StagedFirstAttempt
            );
        }
        {
            let recovered = Ledger::open(&path).unwrap();
            assert_eq!(
                stage_native_intent(&recovered, &intent()).unwrap(),
                NativeIntentDecision::ReconcileOnly
            );
            let page = recovered.order_recovery_page("tenant1", 0, 10).unwrap();
            assert_eq!(page.len(), 1);
            assert!(page[0].reconciliation_only);
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn conflicting_native_instruction_never_gets_another_fresh_attempt() {
        let ledger = ledger();
        stage_native_intent(&ledger, &intent()).unwrap();
        for changed in [
            NativeIntent { quantity: "0.26", ..intent() },
            NativeIntent { side: "SELL", ..intent() },
            NativeIntent { position_mode: "one_way", ..intent() },
            NativeIntent { price: "2001", ..intent() },
        ] {
            assert!(stage_native_intent(&ledger, &changed).is_err());
        }
        let other_request = NativeIntent { request_id: "request2", ..intent() };
        assert!(stage_native_intent(&ledger, &other_request).is_err());
    }

    #[test]
    fn rejects_unverified_native_route_and_non_single_leg_orders() {
        let ledger = ledger();
        for invalid in [
            NativeIntent { product: "options", ..intent() },
            NativeIntent { position_mode: "unknown", ..intent() },
            NativeIntent { symbol: "BTC/USDT", ..intent() },
            NativeIntent { order_type: "STOP_MARKET", ..intent() },
            NativeIntent { order_type: "OCO", ..intent() },
            NativeIntent { quantity: "0", ..intent() },
            NativeIntent { price: "-1", ..intent() },
            NativeIntent { environment: "paper", ..intent() },
        ] {
            assert!(stage_native_intent(&ledger, &invalid).is_err());
        }
        assert!(ledger.order_recovery_page("tenant1", 0, 10).unwrap().is_empty());
    }
}
