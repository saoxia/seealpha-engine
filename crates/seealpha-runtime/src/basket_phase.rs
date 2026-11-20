//! Persist a cross-leg risk decision and an unresolved basket phase before
//! any potential Venue side effect. This module never sends an order.
use crate::{map_ledger, Runtime, RuntimeError};
use rust_decimal::Decimal;
use seealpha_portfolio::{
    basket::{Evidence, LegId},
    journal::{decode_checkpoint, encode_checkpoint},
    phase_gate::{gate_next_phase, PhaseGateLimits},
};
use serde_json::{json, Value};
use std::collections::BTreeMap;

impl Runtime {
    /// Read the trusted, replay-verified basket checkpoint, validate all legs,
    /// then CAS-persist AwaitingConfirmation; no VENUE permission is returned.
    pub fn reserve_basket_phase(
        &self,
        tenant: &str,
        account: &str,
        basket_id: &str,
        expected_revision: &str,
        request_id: &str,
        positions: &BTreeMap<LegId, Decimal>,
        evidence: &BTreeMap<LegId, Evidence>,
        limits: PhaseGateLimits,
        hedge_pairs: &[(LegId, LegId)],
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        let stored = self.ledger.load_basket_journal(tenant, account, basket_id)
            .map_err(map_ledger)?
            .ok_or_else(|| RuntimeError::NotFound("篮子持久化记录不存在。".into()))?;
        if stored.revision != expected_revision {
            return Err(RuntimeError::Conflict("篮子版本过期，请先恢复状态。".into()));
        }
        let decoded = decode_checkpoint(basket_id, &stored.body_json)
            .map_err(|e| RuntimeError::Conflict(e.to_string()))?;
        if decoded.revision != stored.revision {
            return Err(RuntimeError::Conflict("篮子持久化校验失败。".into()));
        }
        if decoded.checkpoint.plan.targets.keys().any(|leg|
            leg.tenant != tenant || leg.account != account
        ) || positions.keys().any(|leg|
            leg.tenant != tenant || leg.account != account
        ) {
            return Err(RuntimeError::Conflict("篮子仓位与租户账户作用域不一致。".into()));
        }
        // Upstream must supply independently authenticated account positions
        // and fresh source timestamps. This method does not invent either.
        let gate = gate_next_phase(
            &decoded.checkpoint, positions, evidence, limits, hedge_pairs,
        ).map_err(|e| RuntimeError::Conflict(e.to_string()))?;
        if !gate.admitted || gate.executable {
            return Err(RuntimeError::Conflict(
                gate.block_reason.unwrap_or_else(|| "篮子阶段风险门禁拒绝。".into()),
            ));
        }
        let mut checkpoint = decoded.checkpoint;
        checkpoint.begin(request_id)
            .map_err(|e| RuntimeError::Conflict(e.to_string()))?;
        let encoded = encode_checkpoint(basket_id, &checkpoint)
            .map_err(|e| RuntimeError::Conflict(e.to_string()))?;
        let saved = self.ledger.save_basket_journal(
            tenant, account, basket_id, &encoded.json, &encoded.revision,
            Some(expected_revision),
        ).map_err(map_ledger)?;
        Ok(json!({
            "basket_id": basket_id,
            "revision": saved.revision,
            "request_id": request_id,
            "status": "awaiting_confirmation",
            "worst_case_gross": gate.worst_case_gross.to_string(),
            "worst_case_leg_drift": gate.worst_case_leg_drift.map(|v|v.to_string()),
            "executable": false,
            "exchange_orders": 0,
        }))
    }
}
