//! Research-only native basket runtime adapter with authenticated tenant scope
//! and transactional checkpoint persistence. No venue transport is exposed.
use crate::{Runtime, RuntimeError, map_ledger};
use seealpha_portfolio::{
    basket::{BasketError, Checkpoint, LegId},
    journal::{StoredCheckpoint, decode_checkpoint, encode_checkpoint},
};
impl Runtime {
    /// Reconcile an unresolved basket from persisted Ledger observations and
    /// an authenticated account snapshot, never from an order ACK or a
    /// client-provided position map. No venue POST or fallback is possible.
    ///
    /// Missing order observations are UNKNOWN (and freeze the phase). Any
    /// malformed or cross-scoped exchange evidence fails closed before
    /// portfolio state is updated.
    #[allow(clippy::too_many_arguments)]
    pub fn reconcile_basket_from_ledger(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        basket: &str,
        expected_revision: &str,
        expected_account_revision: i64,
        now_ns: u64,
        max_age_ns: u64,
    ) -> Result<StoredCheckpoint, RuntimeError> {
        use rust_decimal::Decimal;
        use seealpha_portfolio::{
            basket::Product,
            order_evidence::{OrderEvidence, VenueOrderStatus, bind_phase_orders},
        };
        use std::collections::BTreeMap;

        if !matches!(environment, "production" | "sandbox")
            || expected_account_revision <= 0
            || now_ns == 0
            || max_age_ns == 0
        {
            return Err(RuntimeError::Conflict("账户环境或对账时钟无效。".into()));
        }
        let stored = self
            .recover_basket_checkpoint(tenant, account, basket)?
            .ok_or_else(|| RuntimeError::NotFound("basket checkpoint missing".into()))?;
        if stored.revision != expected_revision {
            return Err(RuntimeError::Conflict("stale basket revision".into()));
        }
        let manifest = bind_phase_orders(&stored.checkpoint).map_err(basket_error)?;
        let snapshot = self
            .ledger
            .account_snapshot_for_risk(
                tenant, account, environment, expected_account_revision,
                now_ns, max_age_ns,
            )
            .map_err(map_ledger)?;
        let reported = snapshot.state["positions"]
            .as_object()
            .ok_or_else(|| RuntimeError::Conflict("账户持仓证据缺失。".into()))?;
        let mut positions = BTreeMap::new();
        for leg in stored.checkpoint.positions.keys() {
            let product = match &leg.product {
                Product::Spot => "spot",
                Product::FuturesUm => "futures_um",
                Product::Option => "options",
            };
            let name = format!("{product}:{}", leg.symbol);
            let amount = reported
                .get(&name)
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<Decimal>().ok())
                .ok_or_else(|| {
                    RuntimeError::Conflict("账户快照缺少 Basket 腿或 Decimal 持仓无效。".into())
                })?;
            positions.insert(leg.clone(), amount);
        }
        let raw_fills = self
            .ledger
            .fills(tenant, account, environment)
            .map_err(map_ledger)?;
        let verified_fills: Vec<serde_json::Value> = raw_fills
            .iter()
            .map(|raw| {
                serde_json::from_str(raw)
                    .map_err(|_| RuntimeError::Conflict("已保存成交证据损坏，拒绝对账。".into()))
            })
            .collect::<Result<_, _>>()?;
        let mut evidence = Vec::with_capacity(manifest.orders.len());
        for bound in &manifest.orders {
            let row = self
                .ledger
                .latest_order_observation(tenant, account, environment, &bound.client_order_id)
                .map_err(map_ledger)?;
            let (venue_id, execution_id, filled_quantity, status, as_of_ns) = if let Some(row) = row
            {
                if row["environment"].as_str() != Some(environment)
                    || row["client_order_id"].as_str() != Some(bound.client_order_id.as_str())
                    || !matches!(
                        row["source"].as_str(),
                        Some("signed_exchange_order_query" | "authenticated_user_stream")
                    )
                {
                    return Err(RuntimeError::Conflict(
                        "订单观察证据的身份或来源不匹配。".into(),
                    ));
                }
                let status = match row["status"].as_str() {
                    Some("FILLED") => VenueOrderStatus::Filled,
                    Some("PARTIALLY_FILLED") => VenueOrderStatus::PartiallyFilled,
                    Some("CANCELED") => VenueOrderStatus::Canceled,
                    Some("REJECTED") => VenueOrderStatus::Rejected,
                    Some("EXPIRED") => VenueOrderStatus::Expired,
                    Some("NEW" | "ACCEPTED" | "TRIGGERED") => VenueOrderStatus::New,
                    _ => VenueOrderStatus::Unknown,
                };
                let timestamp = row["observed_at_ns"]
                    .as_u64()
                    .filter(|v| *v > 0)
                    .ok_or_else(|| RuntimeError::Conflict("订单观察时间无效。".into()))?;
                let venue_id = row["venue_order_id"].as_str().unwrap_or("").to_owned();
                let ids = row["trade_ids"].as_array();
                let mut cumulative = Decimal::ZERO;
                if let Some(ids) = ids {
                    if ids.is_empty()
                        || ids.len() >= 1000
                        || !matches!(
                            status,
                            VenueOrderStatus::Filled
                                | VenueOrderStatus::PartiallyFilled
                                | VenueOrderStatus::Canceled
                                | VenueOrderStatus::Expired
                        )
                    {
                        return Err(RuntimeError::Conflict(
                            "订单状态和成交 ID 证据不一致。".into(),
                        ));
                    }
                    let mut unique = std::collections::HashSet::new();
                    for id in ids {
                        let id = id.as_str().filter(|id| unique.insert(*id)).ok_or_else(|| {
                            RuntimeError::Conflict("重复或无效的交易所成交 ID。".into())
                        })?;
                        let fill = verified_fills
                            .iter()
                            .find(|fill| {
                                fill["trade_id"].as_str() == Some(id)
                                    && fill["venue_order_id"].as_str() == Some(venue_id.as_str())
                                    && fill["source"].as_str()
                                        == Some("signed_exchange_user_trades")
                            })
                            .ok_or_else(|| {
                                RuntimeError::Conflict("成交 ID 没有可信的交易所成交记录。".into())
                            })?;
                        let quantity = fill["quantity"]
                            .as_str()
                            .and_then(|s| s.parse::<Decimal>().ok())
                            .filter(|q| *q > Decimal::ZERO)
                            .ok_or_else(|| RuntimeError::Conflict("交易所成交数量无效。".into()))?;
                        cumulative = cumulative.checked_add(quantity).ok_or_else(|| {
                            RuntimeError::Conflict("交易所累计成交数量溢出。".into())
                        })?;
                    }
                }
                if matches!(
                    status,
                    VenueOrderStatus::Filled | VenueOrderStatus::PartiallyFilled
                ) && (ids.is_none() || cumulative <= Decimal::ZERO)
                {
                    return Err(RuntimeError::Conflict("成交状态缺少真实成交明细。".into()));
                }
                // Portfolio requires alphanumeric execution identities.
                // This timestamp is observation provenance, not a trade ID.
                (
                    venue_id,
                    timestamp.to_string(),
                    cumulative,
                    status,
                    timestamp,
                )
            } else {
                (
                    String::new(),
                    String::new(),
                    Decimal::ZERO,
                    VenueOrderStatus::Unknown,
                    now_ns,
                )
            };
            evidence.push(OrderEvidence {
                leg: bound.leg.clone(),
                client_order_id: bound.client_order_id.clone(),
                venue_order_id: venue_id,
                execution_id,
                filled_quantity,
                status,
                as_of_ns,
            });
        }
        self.reconcile_basket_evidence(
            tenant,
            account,
            basket,
            expected_revision,
            &manifest,
            &evidence,
            &positions,
            ts_ns,
            now_ns,
            max_age_ns,
        )
    }

    fn check_basket_scope(
        &self,
        tenant: &str,
        account: &str,
        checkpoint: &Checkpoint,
    ) -> Result<(), RuntimeError> {
        self.require_active(tenant)?;
        if tenant.is_empty()
            || account.is_empty()
            || checkpoint.plan.executable
            || checkpoint.plan.targets.is_empty()
            || checkpoint
                .plan
                .targets
                .keys()
                .any(|leg: &LegId| leg.tenant != tenant || leg.account != account)
        {
            return Err(RuntimeError::Conflict(
                "basket scope or execution authority invalid".into(),
            ));
        }
        Ok(())
    }
    /// Creates a journal before any action can be considered.
    pub fn create_basket_checkpoint(
        &self,
        tenant: &str,
        account: &str,
        basket: &str,
        checkpoint: &Checkpoint,
    ) -> Result<StoredCheckpoint, RuntimeError> {
        self.save_basket_checkpoint(tenant, account, basket, checkpoint, None)
    }
    /// Transactional compare-and-swap. Every write is replay-verified.
    pub fn save_basket_checkpoint(
        &self,
        tenant: &str,
        account: &str,
        basket: &str,
        checkpoint: &Checkpoint,
        expected_revision: Option<&str>,
    ) -> Result<StoredCheckpoint, RuntimeError> {
        self.check_basket_scope(tenant, account, checkpoint)?;
        let encoded = encode_checkpoint(basket, checkpoint).map_err(basket_error)?;
        self.ledger
            .save_basket_journal(
                tenant,
                account,
                basket,
                &encoded.json,
                &encoded.revision,
                expected_revision,
            )
            .map_err(map_ledger)?;
        decode_checkpoint(basket, &encoded.json).map_err(basket_error)
    }
    /// Recovery never resubmits an unknown in-flight request.
    pub fn recover_basket_checkpoint(
        &self,
        tenant: &str,
        account: &str,
        basket: &str,
    ) -> Result<Option<StoredCheckpoint>, RuntimeError> {
        self.require_active(tenant)?;
        let Some(record) = self
            .ledger
            .load_basket_journal(tenant, account, basket)
            .map_err(map_ledger)?
        else {
            return Ok(None);
        };
        let restored = decode_checkpoint(basket, &record.body_json).map_err(basket_error)?;
        if restored.revision != record.revision {
            return Err(RuntimeError::Conflict(
                "basket journal revision mismatch".into(),
            ));
        }
        self.check_basket_scope(tenant, account, &restored.checkpoint)?;
        Ok(Some(restored))
    }
    /// Persist phase 'begin' before handing any binding to an authenticated adapter.
    pub fn register_basket_phase(
        &self,
        tenant: &str,
        account: &str,
        basket: &str,
        expected_revision: &str,
        request_id: &str,
    ) -> Result<StoredCheckpoint, RuntimeError> {
        let mut stored = self
            .recover_basket_checkpoint(tenant, account, basket)?
            .ok_or_else(|| RuntimeError::NotFound("basket checkpoint missing".into()))?;
        if stored.revision != expected_revision {
            return Err(RuntimeError::Conflict("stale basket revision".into()));
        }
        stored.checkpoint.begin(request_id).map_err(basket_error)?;
        self.save_basket_checkpoint(
            tenant,
            account,
            basket,
            &stored.checkpoint,
            Some(expected_revision),
        )
    }
    /// Apply only source-verified order evidence; unknown statuses freeze.
    // Retain explicit v2 migration inputs until equivalent contract coverage is proven.
    #[allow(clippy::too_many_arguments)]
    pub fn reconcile_basket_evidence(
        &self,
        tenant: &str,
        account: &str,
        basket: &str,
        expected_revision: &str,
        manifest: &seealpha_portfolio::order_evidence::BoundPhase,
        orders: &[seealpha_portfolio::order_evidence::OrderEvidence],
        positions: &std::collections::BTreeMap<LegId, rust_decimal::Decimal>,
        position_ts: u64,
        now_ns: u64,
        max_age_ns: u64,
    ) -> Result<StoredCheckpoint, RuntimeError> {
        let stored = self
            .recover_basket_checkpoint(tenant, account, basket)?
            .ok_or_else(|| RuntimeError::NotFound("basket checkpoint missing".into()))?;
        if stored.revision != expected_revision {
            return Err(RuntimeError::Conflict("stale basket revision".into()));
        }
        let updated = seealpha_portfolio::order_evidence::reconcile_order_evidence(
            stored.checkpoint,
            manifest,
            orders,
            positions,
            position_ts,
            now_ns,
            max_age_ns,
        )
        .map_err(basket_error)?;
        self.save_basket_checkpoint(tenant, account, basket, &updated, Some(expected_revision))
    }
    // Retain explicit v2 migration inputs until equivalent contract coverage is proven.
    #[allow(clippy::too_many_arguments)]
    pub fn monitor_basket_risk(
        &self,
        tenant: &str,
        account: &str,
        basket: &str,
        expected_revision: &str,
        positions: &std::collections::BTreeMap<LegId, rust_decimal::Decimal>,
        evidence: &std::collections::BTreeMap<LegId, seealpha_portfolio::basket::Evidence>,
        limits: seealpha_portfolio::risk::RiskLimits,
        pending_since_ns: Option<u64>,
    ) -> Result<seealpha_portfolio::risk::RiskObservation, RuntimeError> {
        let stored = self
            .recover_basket_checkpoint(tenant, account, basket)?
            .ok_or_else(|| RuntimeError::NotFound("basket checkpoint missing".into()))?;
        if stored.revision != expected_revision {
            return Err(RuntimeError::Conflict("stale basket revision".into()));
        }
        let observation = seealpha_portfolio::risk::observe_phase(
            stored.checkpoint,
            positions,
            evidence,
            limits,
            pending_since_ns,
        )
        .map_err(basket_error)?;
        if observation.frozen {
            self.save_basket_checkpoint(
                tenant,
                account,
                basket,
                &observation.checkpoint,
                Some(expected_revision),
            )?;
        }
        Ok(observation)
    }
}
fn basket_error(error: BasketError) -> RuntimeError {
    RuntimeError::Conflict(error.to_string())
}

#[cfg(test)]
mod verified_ledger_basket_tests {
    use super::*;
    use crate::config::{AuthMode, Config};
    use rust_decimal::Decimal;
    use seealpha_portfolio::{
        basket::{LegId, Phase, Plan, Product, State},
        order_evidence::bind_phase_orders,
    };
    use serde_json::json;

    fn runtime(name: &str) -> (Runtime, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "seealpha-reconcile-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id(),
        ));
        let _ = std::fs::remove_dir_all(&path);
        let config = Config {
            host: "127.0.0.1".into(),
            port: 8766,
            auth_mode: AuthMode::None,
            token: String::new(),
            state_dir: path.clone(),
            project: path.clone(),
            tenant_id: None,
            tenant_purpose: Some("production".into()),
            research_signing_key: None,
            research_mcp_enabled: false,
            venue_base: None,
            python: "python3".into(),
        };
        (Runtime::open(&config).unwrap(), path)
    }
    fn stage(runtime: &Runtime, position: &str) -> (String, String) {
        runtime.ensure_tenant("t", "production", "active").unwrap();
        let leg = LegId {
            tenant: "t".into(),
            account: "a".into(),
            product: Product::Spot,
            symbol: "ETHUSDT".into(),
        };
        let qty = Decimal::ONE;
        let plan = Plan {
            targets: [(leg.clone(), qty)].into(),
            phases: vec![Phase {
                name: "buy".into(),
                changes: [(leg.clone(), qty)].into(),
            }],
            gross_notional: Decimal::from(100),
            executable: false,
        };
        let mut checkpoint = Checkpoint::new(plan, [(leg, Decimal::ZERO)].into()).unwrap();
        checkpoint.begin("request1").unwrap();
        let issued = bind_phase_orders(&checkpoint).unwrap();
        let revision = runtime
            .create_basket_checkpoint("t", "a", "basket1", &checkpoint)
            .unwrap()
            .revision;
        runtime
            .ledger
            .save_account_snapshot(
                "t",
                "a",
                "production",
                None,
                "exchange1",
                1,
                "signed_exchange_account_query",
                &json!({"balances":{"USDT":"1000"},"positions":{"spot:ETHUSDT":position}}),
            )
            .unwrap();
        (revision, issued.orders[0].client_order_id.clone())
    }
    #[test]
    fn missing_order_observation_freezes_without_fabricating_a_fill() {
        let (runtime, path) = runtime("unknown");
        let (revision, _) = stage(&runtime, "0");
        let result = runtime
            .reconcile_basket_from_ledger(
                "t",
                "a",
                "production",
                "basket1",
                &revision,
                1,
                1_000_000,
                1_000_000,
            )
            .unwrap();
        assert_eq!(result.checkpoint.state, State::Frozen);
        assert!(result.checkpoint.fills.is_empty());
        assert!(
            runtime
                .ledger
                .fills("t", "a", "production")
                .unwrap()
                .is_empty()
        );
        drop(runtime);
        let _ = std::fs::remove_dir_all(path);
    }
    #[test]
    fn authenticated_trade_and_signed_position_confirm_complete_phase() {
        let (runtime, path) = runtime("filled");
        let (revision, client) = stage(&runtime, "1");
        let intent = json!({"action":"NEW","environment":"production",
            "client_order_id":client,"symbol":"ETHUSDT","side":"BUY","quantity":"1"});
        runtime
            .ledger
            .preregister_order_intent(
                "t",
                "a",
                "production",
                "request1",
                &client,
                &"a".repeat(64),
                &intent,
            )
            .unwrap();
        let trade = json!({"trade_id":"binance:spot:ETHUSDT:901",
            "venue_order_id":"42","quantity":"1","price":"100",
            "source":"signed_exchange_user_trades"});
        runtime
            .ledger
            .import_verified_fills("t", "a", "production", "42", &[trade])
            .unwrap();
        let observed = json!({"environment":"production","client_order_id":client,
            "status":"FILLED","source":"signed_exchange_order_query",
            "observed_at_ns":1_000_000,"venue_order_id":"42",
            "trade_ids":["binance:spot:ETHUSDT:901"]});
        runtime
            .ledger
            .append_order_observation("t", "a", "production", &client, "obs1", &observed)
            .unwrap();
        let result = runtime
            .reconcile_basket_from_ledger(
                "t",
                "a",
                "production",
                "basket1",
                &revision,
                1,
                1_000_000,
                1_000_000,
            )
            .unwrap();
        assert_eq!(result.checkpoint.state, State::Completed);
        assert_eq!(
            result.checkpoint.positions.values().next(),
            Some(&Decimal::ONE)
        );
        drop(runtime);
        let _ = std::fs::remove_dir_all(path);
    }
}
