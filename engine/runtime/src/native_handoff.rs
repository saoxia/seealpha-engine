//! Durable SeeAlpha preflight for the future Nautilus-native execution path.
//! This module is not a transport, a native submitter, or live-trading approval.
//! A new execution path must additionally enforce portfolio/risk reservation
//! and native lifecycle acceptance before consuming a fresh registration.
use crate::state_store::{IntentStart, StateStore};
use seealpha_ledger::LedgerError;
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
    ledger: &StateStore,
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
    register_native_envelope(ledger, intent, &envelope)
}

fn register_native_envelope(
    store: &StateStore,
    intent: &NativeIntent<'_>,
    envelope: &serde_json::Value,
) -> Result<NativeIntentDecision, LedgerError> {
    let digest = Sha256::digest(envelope.to_string().as_bytes());
    let fingerprint = format!("{digest:x}");
    match store.preregister_order_intent(
        intent.tenant, intent.account, intent.environment,
        intent.request_id, intent.client_order_id, &fingerprint, envelope,
    )? {
        IntentStart::Fresh => Ok(NativeIntentDecision::StagedFirstAttempt),
        IntentStart::ReconcileOnly => Ok(NativeIntentDecision::ReconcileOnly),
    }
}


/// Typed Target -> immutable Nautilus intent handoff. This does not submit an
/// order or grant permission to send. The caller must be an internal Nautilus
/// adapter which has independently reconciled account positions and open orders.
#[allow(dead_code)] // Not attached to live Nautilus until trusted cache bridge exists.
#[derive(Clone, Copy)]
pub(crate) struct TargetIntent<'a> {
    pub tenant: &'a str,
    pub account: &'a str,
    pub environment: &'a str,
    pub request_id: &'a str,
    pub client_order_id: &'a str,
    pub product: &'a str,
    pub symbol: &'a str,
    pub position_mode: &'a str,
    pub price: rust_decimal::Decimal,
    pub targets: &'a [seealpha_execution::MemberTarget],
    pub position: rust_decimal::Decimal,
    pub increment: rust_decimal::Decimal,
    pub max_abs_target: rust_decimal::Decimal,
    pub position_reconciled: bool,
    pub orders_reconciled: bool,
}

/// None = targets already satisfied (or no strategy publications), so no
/// outbox registration is made. A replay of the same immutable adjustment is
/// reconcile-only. The decision is NEVER authorization to invoke Nautilus.
#[allow(dead_code)]
pub(crate) fn stage_target_adjustment(
    store: &StateStore,
    input: &TargetIntent<'_>,
) -> Result<Option<NativeIntentDecision>, LedgerError> {
    use seealpha_execution::target::plan::plan_adjustment;
    if input.price <= rust_decimal::Decimal::ZERO {
        return Err(LedgerError::Conflict("invalid target limit price".into()));
    }
    let adjustment = plan_adjustment(
        input.targets,
        input.position,
        input.increment,
        input.max_abs_target,
        input.position_reconciled,
        input.orders_reconciled,
    ).map_err(|error| LedgerError::Conflict(format!("unsafe target adjustment: {error}")))?;
    let Some(adjustment) = adjustment else { return Ok(None); };
    let side = if adjustment.signed_quantity > rust_decimal::Decimal::ZERO {
        "BUY"
    } else {
        "SELL"
    };
    let quantity = adjustment.signed_quantity.abs().to_string();
    let price = input.price.to_string();
    let intent = NativeIntent {
        tenant: input.tenant,
        account: input.account,
        environment: input.environment,
        request_id: input.request_id,
        client_order_id: input.client_order_id,
        product: input.product,
        symbol: input.symbol,
        side,
        order_type: "LIMIT",
        quantity: &quantity,
        price: &price,
        position_mode: input.position_mode,
    };
    // The exact strategy composition and preflight position are part of the
    // immutable order request, not just the net venue quantity. Sort by member
    // ID for deterministic fingerprints independent of input iteration order.
    // These are attribution *claims*, never authenticated venue fills.
    let mut publications: Vec<_> = input.targets.iter()
        .map(|target| (target.member_id.as_str(), target.signed_quantity.to_string(), target.risk_exit))
        .collect();
    publications.sort_unstable_by(|left, right| left.0.cmp(right.0));
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
        "origin": "target_netting",
        "target_publications": publications,
        "net_target": adjustment.net_target.to_string(),
        "planning_position": input.position.to_string(),
        "planning_increment": input.increment.to_string(),
        "planning_max_abs_target": input.max_abs_target.to_string(),
    });
    // Avoid trusting the order's calculated total without the same validation
    // used by the explicit native instruction path.
    if !matches!(intent.environment, "sandbox" | "production")
        || !matches!(intent.product, "spot" | "futures_um")
        || !valid_id(intent.account, 40)
        || !valid_id(intent.request_id, 128)
        || !valid_id(intent.client_order_id, 128)
        || !valid_symbol(intent.product, intent.symbol)
        || !valid_mode(intent.product, intent.position_mode)
        || !positive_decimal(intent.quantity)
        || !positive_decimal(intent.price)
    {
        return Err(LedgerError::Conflict("invalid target native intent".into()));
    }
    register_native_envelope(store, &intent, &envelope).map(Some)
}


/// Read-only audit/recovery trace. This joins SeeAlpha's immutable target
/// attribution with the native client-order identity and any *already stored*
/// exchange observation. It neither queries an exchange nor accepts new
/// observations; in particular, an ACK is never promoted to a verified fill.
pub(crate) fn load_native_intent_trace(
    store: &StateStore,
    tenant: &str,
    account: &str,
    environment: &str,
    request_id: &str,
) -> Result<serde_json::Value, LedgerError> {
    if !valid_id(tenant, 128) || !valid_id(account, 128)
        || !valid_id(request_id, 128)
        || !matches!(environment, "sandbox" | "production")
    {
        return Err(LedgerError::Conflict("invalid native intent trace scope".into()));
    }
    let intent = store.registered_order_intent(tenant, account, request_id)?
        .ok_or_else(|| LedgerError::Conflict("native order intent not found".into()))?;
    let client = intent["client_order_id"].as_str()
        .ok_or_else(|| LedgerError::Conflict("missing client order identity".into()))?;
    if intent["execution_backend"] != "nautilus_binance_native"
        || intent["environment"] != environment
        || !valid_id(client, 128)
    {
        return Err(LedgerError::Conflict("native intent scope or backend mismatch".into()));
    }
    let observation = store.latest_order_observation(
        tenant, account, environment, client,
    )?;
    Ok(json!({
        "tenant_id": tenant,
        "account_id": account,
        "environment": environment,
        "request_id": request_id,
        "client_order_id": client,
        "intent": intent,
        "latest_order_observation": observation,
        "observation_present": observation.is_some(),
        // A status report is not a verified fill, funding transaction or
        // complete account reconciliation. These flags are deliberately false.
        "fill_verified_by_trace": false,
        "account_reconciled_by_trace": false,
        "send_authorized": false,
    }))
}

impl crate::Runtime {
    /// Recovery inspection only. Does not send, retry or invent fills.
    pub fn native_execution_trace(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        request_id: &str,
    ) -> Result<serde_json::Value, crate::RuntimeError> {
        use crate::map_ledger;
        self.require_active(tenant)?;
        load_native_intent_trace(
            &self.state_store, tenant, account, environment, request_id,
        ).map_err(map_ledger)
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
    let decision = stage_native_intent(&runtime.state_store, &native).map_err(map_ledger)?;
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

    fn ledger() -> StateStore {
        let ledger = StateStore::open(std::path::Path::new(":memory:")).unwrap();
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
        let stored = runtime.state_store.registered_order_intent(
            "tenant1", "account1", "00000000-0000-0000-0000-000000000001",
        ).unwrap().unwrap();
        assert_eq!(stored["execution_backend"], "nautilus_binance_native");
        assert_eq!(runtime.state_store.order_recovery_page("tenant1", 0, 10).unwrap().len(), 1);
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
    fn staged_native_intent_does_not_create_order_or_fill_evidence() {
        // Nautilus owns native event recording. Intent staging alone must not
        // synthesize an accepted order, a FillReport or an authenticated fill.
        let ledger = ledger();
        assert_eq!(
            stage_native_intent(&ledger, &intent()).unwrap(),
            NativeIntentDecision::StagedFirstAttempt
        );
        assert!(ledger.fills("tenant1", "account1", "sandbox").unwrap().is_empty());
        assert!(ledger.latest_order_observation(
            "tenant1", "account1", "sandbox", "client1"
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
            let ledger = StateStore::open(&path).unwrap();
            ledger.ensure_tenant("tenant1", "production", "active").unwrap();
            assert_eq!(
                stage_native_intent(&ledger, &intent()).unwrap(),
                NativeIntentDecision::StagedFirstAttempt
            );
        }
        {
            let recovered = StateStore::open(&path).unwrap();
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
    #[test]
    fn targets_stage_one_immutable_native_intent_without_sending() {
        use seealpha_execution::MemberTarget;
        let store = ledger();
        let targets = [
            MemberTarget { member_id: "long".into(), signed_quantity: rust_decimal::Decimal::from(6), risk_exit: false },
            MemberTarget { member_id: "short".into(), signed_quantity: rust_decimal::Decimal::from(-2), risk_exit: false },
        ];
        let request = TargetIntent {
            tenant: "tenant1", account: "account1", environment: "sandbox",
            request_id: "target-1", client_order_id: "target-client-1",
            product: "futures_um", symbol: "BTCUSDT-PERP",
            position_mode: "one_way", price: rust_decimal::Decimal::from(2000),
            targets: &targets, position: rust_decimal::Decimal::ONE,
            increment: rust_decimal::Decimal::ONE,
            max_abs_target: rust_decimal::Decimal::from(10),
            position_reconciled: true, orders_reconciled: true,
        };
        assert_eq!(
            stage_target_adjustment(&store, &request).unwrap(),
            Some(NativeIntentDecision::StagedFirstAttempt)
        );
        let stored = store.registered_order_intent("tenant1", "account1", "target-1").unwrap().unwrap();
        assert_eq!(stored["side"], "BUY");
        assert_eq!(stored["quantity"], "3");
        assert_eq!(stored["execution_backend"], "nautilus_binance_native");
        assert_eq!(
            stage_target_adjustment(&store, &request).unwrap(),
            Some(NativeIntentDecision::ReconcileOnly)
        );
        let changed = TargetIntent { position: rust_decimal::Decimal::ZERO, ..request };
        assert!(stage_target_adjustment(&store, &changed).is_err());
        assert!(store.fills("tenant1", "account1", "sandbox").unwrap().is_empty());
    }

    #[test]
    fn trace_joins_registered_target_identity_without_fabricating_execution() {
        use seealpha_execution::MemberTarget;
        let store = ledger();
        let targets = [MemberTarget {
            member_id: "alpha".into(),
            signed_quantity: rust_decimal::Decimal::from(2),
            risk_exit: false,
        }];
        let input = TargetIntent {
            tenant: "tenant1", account: "account1", environment: "sandbox",
            request_id: "trace-request", client_order_id: "native-trace-1",
            product: "spot", symbol: "BTCUSDT", position_mode: "spot",
            price: rust_decimal::Decimal::from(2000), targets: &targets,
            position: rust_decimal::Decimal::ZERO,
            increment: rust_decimal::Decimal::ONE,
            max_abs_target: rust_decimal::Decimal::from(10),
            position_reconciled: true, orders_reconciled: true,
        };
        stage_target_adjustment(&store, &input).unwrap();
        let trace = load_native_intent_trace(
            &store, "tenant1", "account1", "sandbox", "trace-request",
        ).unwrap();
        assert_eq!(trace["intent"]["origin"], "target_netting");
        assert_eq!(trace["intent"]["target_publications"][0][0], "alpha");
        assert_eq!(trace["client_order_id"], "native-trace-1");
        assert_eq!(trace["observation_present"], false);
        assert_eq!(trace["fill_verified_by_trace"], false);
        assert_eq!(trace["send_authorized"], false);
        assert!(load_native_intent_trace(
            &store, "tenant1", "account1", "production", "trace-request",
        ).is_err());
        assert!(load_native_intent_trace(
            &store, "tenant1", "other", "sandbox", "trace-request",
        ).is_err());
    }

    #[test]
    fn strategy_composition_is_immutable_even_with_identical_net_order() {
        use seealpha_execution::MemberTarget;
        let store = ledger();
        let first = [
            MemberTarget { member_id: "alpha".into(), signed_quantity: rust_decimal::Decimal::from(4), risk_exit: false },
            MemberTarget { member_id: "beta".into(), signed_quantity: rust_decimal::Decimal::from(-1), risk_exit: false },
        ];
        let reversed = [first[1].clone(), first[0].clone()];
        let second = [
            MemberTarget { member_id: "alpha".into(), signed_quantity: rust_decimal::Decimal::from(5), risk_exit: false },
            MemberTarget { member_id: "beta".into(), signed_quantity: rust_decimal::Decimal::from(-2), risk_exit: false },
        ];
        let input = TargetIntent {
            tenant: "tenant1", account: "account1", environment: "sandbox",
            request_id: "attribution-1", client_order_id: "native-attribution-1",
            product: "futures_um", symbol: "BTCUSDT-PERP", position_mode: "one_way",
            price: rust_decimal::Decimal::from(2000),
            targets: &first, position: rust_decimal::Decimal::ZERO,
            increment: rust_decimal::Decimal::ONE,
            max_abs_target: rust_decimal::Decimal::from(10),
            position_reconciled: true, orders_reconciled: true,
        };
        assert_eq!(stage_target_adjustment(&store, &input).unwrap(), Some(NativeIntentDecision::StagedFirstAttempt));
        let original = store.registered_order_intent("tenant1", "account1", "attribution-1").unwrap().unwrap();
        assert_eq!(original["quantity"], "3");
        assert_eq!(original["origin"], "target_netting");
        assert_eq!(original["target_publications"].as_array().unwrap().len(), 2);
        assert_eq!(stage_target_adjustment(&store, &TargetIntent { targets: &reversed, ..input }).unwrap(),
            Some(NativeIntentDecision::ReconcileOnly));
        assert!(stage_target_adjustment(&store, &TargetIntent { targets: &second, ..input }).is_err());
        let altered_risk = [
            MemberTarget { risk_exit: true, ..first[0].clone() },
            first[1].clone(),
        ];
        assert!(stage_target_adjustment(&store, &TargetIntent { targets: &altered_risk, ..input }).is_err());
        assert_eq!(store.registered_order_intent("tenant1", "account1", "attribution-1").unwrap().unwrap(), original);
    }

    #[test]
    fn target_handoff_requires_reconciled_nautilus_inputs_and_nonzero_delta() {
        use seealpha_execution::MemberTarget;
        let store = ledger();
        let targets = [MemberTarget {
            member_id: "alpha".into(),
            signed_quantity: rust_decimal::Decimal::from(2), risk_exit: false,
        }];
        let base = TargetIntent {
            tenant: "tenant1", account: "account1", environment: "sandbox",
            request_id: "target-2", client_order_id: "target-client-2",
            product: "spot", symbol: "BTCUSDT", position_mode: "spot",
            price: rust_decimal::Decimal::from(2000),
            targets: &targets, position: rust_decimal::from_str_exact("2").unwrap(),
            increment: rust_decimal::Decimal::ONE,
            max_abs_target: rust_decimal::Decimal::from(10),
            position_reconciled: true, orders_reconciled: true,
        };
        assert_eq!(stage_target_adjustment(&store, &base).unwrap(), None);
        let pending = TargetIntent {
            position: rust_decimal::Decimal::ZERO,
            orders_reconciled: false,
            ..base
        };
        assert!(stage_target_adjustment(&store, &pending).is_err());
        assert!(store.registered_order_intent("tenant1", "account1", "target-2").unwrap().is_none());
    }

}
