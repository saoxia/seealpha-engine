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


/// Explicit native-backend Runtime entrypoint. This is an *outbox staging*
/// operation, not a send path. It never instantiates VenueClient or Nautilus.
/// The old REST sender cannot be a fallback for this backend.
pub(crate) fn stage_native_order_command(
    runtime: &crate::Runtime,
    command: &crate::orders::OrderCommand,
) -> Result<serde_json::Value, crate::RuntimeError> {
    use crate::{RuntimeError, map_ledger};
    use serde_json::Value;

    if command.action != "submit"
        || command.values["environment"].as_str() != Some(command.environment.as_str())
    {
        return Err(RuntimeError::Unprocessable("原生订单操作或账户环境不匹配。".into()));
    }
    let values = command.values.as_object().ok_or_else(|| {
        RuntimeError::Unprocessable("原生订单参数必须为对象。".into())
    })?;
    // Never silently discard meaningful trading fields when converting to
    // a native LIMIT instruction (particularly contingency/reduce-only).
    const ALLOWED: &[&str] = &[
        "execution_backend", "environment", "request_id", "product", "symbol",
        "side", "order_type", "quantity", "price", "position_mode", "time_in_force",
    ];
    if values.keys().any(|key| !ALLOWED.contains(&key.as_str()))
        || command.values["execution_backend"].as_str() != Some("nautilus_binance_native")
        || command.values["time_in_force"].as_str() != Some("GTC")
    {
        return Err(RuntimeError::Unprocessable(
            "原生执行仅支持已验证的单腿 GTC LIMIT 参数，禁止隐式降级。".into(),
        ));
    }
    let required = |key: &str| -> Result<&str, RuntimeError> {
        command.values[key].as_str().ok_or_else(|| {
            RuntimeError::Unprocessable(format!("原生订单缺少有效字段：{key}"))
        })
    };
    let request_id = required("request_id")?;
    // Use the same deterministic client identity as legacy Runtime. The
    // exact 36-character request ID is already an established API contract.
    if request_id.len() != 36
        || !request_id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-')
    {
        return Err(RuntimeError::Unprocessable("请求编号无效。".into()));
    }
    let client_order_id = format!("SA{}", request_id.replace('-', ""));
    let native = NativeIntent {
        tenant: &command.tenant_id,
        account: &command.account_id,
        environment: &command.environment,
        request_id,
        client_order_id: &client_order_id,
        product: required("product")?,
        symbol: required("symbol")?,
        side: required("side")?,
        order_type: required("order_type")?,
        quantity: required("quantity")?,
        price: required("price")?,
        position_mode: required("position_mode")?,
    };
    let decision = stage_native_intent(&runtime.ledger, &native).map_err(map_ledger)?;
    // A durable first registration is still not an authorization to issue
    // the Nautilus native command. No send-capable native node is connected.
    Ok(serde_json::json!({
        "execution_backend": "nautilus_binance_native",
        "request_id": request_id,
        "client_order_id": client_order_id,
        "environment": command.environment,
        "product": native.product,
        "symbol": native.symbol,
        "response": {
            "request_id": request_id,
            "client_order_id": client_order_id,
            "status": "unknown",
            "order": Value::Null,
            "message": "原生执行意图已持久化；执行端尚未接线，禁止发送。请仅通过已核实的交易所证据对账。",
        },
        "replay": decision == NativeIntentDecision::ReconcileOnly,
        "reconciliation_only": true,
        "ledger_synced": false,
    }))
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
    fn runtime_native_backend_stages_without_old_venue_or_sending() {
        use crate::{Config, Runtime, orders::OrderCommand};
        let root = std::env::temp_dir().join(format!(
            "seealpha-native-route-{}-{:?}", std::process::id(), std::thread::current().id(),
        ));
        let _ = std::fs::remove_dir_all(&root);
        let config = Config {
            host: "127.0.0.1".into(), port: 0,
            auth_mode: crate::AuthMode::None, token: String::new(),
            state_dir: root.join("state"), project: root.clone(),
            tenant_id: None, tenant_purpose: None,
            research_signing_key: None, research_mcp_enabled: false,
            venue_base: Some("http://127.0.0.1:18767".into()),
            python: std::path::PathBuf::from("python3"),
        };
        let runtime = Runtime::open(&config).unwrap();
        runtime.ensure_tenant("tenant1", "production", "active").unwrap();
        let base = serde_json::json!({
            "execution_backend": "nautilus_binance_native",
            "request_id": "00000000-0000-0000-0000-000000000001",
            "environment": "sandbox", "product": "futures_um",
            "symbol": "BTCUSDT-PERP", "side": "BUY", "order_type": "LIMIT",
            "quantity": "0.25", "price": "2000.50",
            "position_mode": "hedge", "time_in_force": "GTC",
        });
        let command = |values| OrderCommand {
            tenant_id: "tenant1".into(), account_id: "account1".into(),
            environment: "sandbox".into(), api_key: "private-key".into(),
            secret: "private-secret".into(), action: "submit".into(),
            values, venue_base: "http://127.0.0.1:18767".into(),
        };
        let first = runtime.execute_order(command(base.clone())).unwrap();
        assert_eq!(first["execution_backend"], "nautilus_binance_native");
        assert_eq!(first["response"]["status"], "unknown");
        assert_eq!(first["replay"], false);
        assert_eq!(first["response"]["order"], serde_json::Value::Null);
        // Native pending intent must not fall through the legacy signed-REST
        // reconciliation path (and must never treat it as a settled fill).
        assert!(runtime.reconcile_order(
            "tenant1", "account1", "00000000-0000-0000-0000-000000000001",
            "sandbox", "private-key", "private-secret", "http://127.0.0.1:18767", "",
        ).is_err());
        let mut external = command(base.clone());
        external.venue_base = "https://fapi.binance.com".into();
        assert!(runtime.execute_order(external).is_err());
        let second = runtime.execute_order(command(base.clone())).unwrap();
        assert_eq!(second["replay"], true);
        let stored = runtime.ledger.registered_order_intent(
            "tenant1", "account1", "00000000-0000-0000-0000-000000000001",
        ).unwrap().unwrap();
        assert_eq!(stored["execution_backend"], "nautilus_binance_native");
        assert_eq!(runtime.ledger.order_recovery_page("tenant1", 0, 10).unwrap().len(), 1);
        assert!(runtime.ledger.fills("tenant1", "account1", "sandbox").unwrap().is_empty());
        let mut changed = base.clone();
        changed["quantity"] = serde_json::json!("0.30");
        assert!(runtime.execute_order(command(changed)).is_err());
        let mut unsupported = base.clone();
        unsupported["request_id"] = serde_json::json!("00000000-0000-0000-0000-000000000002");
        unsupported["reduce_only"] = serde_json::json!(true);
        assert!(runtime.execute_order(command(unsupported)).is_err());
        // The explicit native backend must never be routed to the legacy Venue,
        // even when given a loopback fixture.
        let mut blob = Vec::new();
        for entry in std::fs::read_dir(&config.state_dir).unwrap() {
            let entry = entry.unwrap();
            if let Ok(bytes) = std::fs::read(entry.path()) {
                blob.extend(bytes);
            }
        }
        assert!(!blob.windows(14).any(|w| w == b"private-secret"));
        assert!(!blob.windows(11).any(|w| w == b"private-key"));
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn staged_native_order_can_store_unverified_reports_without_creating_fills() {
        use seealpha_ledger::UnverifiedNativeOrderEvent;
        let ledger = ledger();
        assert_eq!(
            stage_native_intent(&ledger, &intent()).unwrap(),
            NativeIntentDecision::StagedFirstAttempt
        );
        let report = UnverifiedNativeOrderEvent {
            tenant:"tenant1", account:"account1", environment:"sandbox",
            request_id:"request1", client_order_id:"client1",
            product:"futures_um", symbol:"BTCUSDT-PERP",
            event_id:"native-event-001", event_type:"FillReport",
            observed_at_ns:100000,
        };
        assert!(ledger.quarantine_native_order_event(report).unwrap());
        assert!(!ledger.quarantine_native_order_event(report).unwrap());
        assert_eq!(ledger.quarantined_native_order_events(
            "tenant1","account1","sandbox","client1",
        ).unwrap().len(),1);
        assert!(ledger.fills("tenant1","account1","sandbox").unwrap().is_empty());
        assert!(ledger.latest_order_observation(
            "tenant1","account1","sandbox","client1",
        ).unwrap().is_none());
        assert_eq!(
            stage_native_intent(&ledger, &intent()).unwrap(),
            NativeIntentDecision::ReconcileOnly
        );
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
