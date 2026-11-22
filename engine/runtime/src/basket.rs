//! Research-only native basket runtime adapter with authenticated tenant scope
//! and transactional checkpoint persistence. No venue transport is exposed.
use crate::{Runtime, RuntimeError, map_ledger};
use seealpha_portfolio::{
    basket::{BasketError, Checkpoint, LegId},
    journal::{StoredCheckpoint, decode_checkpoint, encode_checkpoint},
};
impl Runtime {
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

