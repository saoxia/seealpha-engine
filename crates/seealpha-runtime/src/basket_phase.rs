//! Persist a cross-leg risk decision and an unresolved basket phase before
//! any potential Venue side effect. This module never sends an order.
use crate::{map_ledger, Runtime, RuntimeError};
use rust_decimal::Decimal;
use seealpha_portfolio::{
    basket::{Evidence, LegId, Product},
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
        environment: &str,
        expected_account_revision: i64,
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
        // Bind supplied positions to one immutable, fresh, CAS-versioned
        // authoritative account observation. Caller-provided positions alone
        // are never an authority for a multi-leg admission.
        if !matches!(environment, "production" | "sandbox" | "paper") {
            return Err(RuntimeError::Conflict("账户环境无效。".into()));
        }
        let account_state = self.ledger.account_snapshot(tenant, account, environment)
            .map_err(map_ledger)?
            .ok_or_else(|| RuntimeError::Conflict("缺少已核验账户仓位快照。".into()))?;
        if account_state.revision != expected_account_revision
            || (environment == "paper" && account_state.source != "paper_simulator")
            || (environment != "paper"
                && account_state.source != "signed_exchange_account_query")
        {
            return Err(RuntimeError::Conflict("账户快照来源或版本不可信。".into()));
        }
        let snapshot_ns = u64::try_from(account_state.observed_at_ms)
            .ok().and_then(|v| v.checked_mul(1_000_000))
            .ok_or_else(|| RuntimeError::Conflict("账户快照时间溢出。".into()))?;
        if snapshot_ns > limits.now_ns
            || limits.now_ns - snapshot_ns > limits.max_age_ns
        {
            return Err(RuntimeError::Conflict("账户快照已过期，禁止篮子阶段登记。".into()));
        }
        let reported = account_state.state["positions"].as_object()
            .ok_or_else(|| RuntimeError::Conflict("账户仓位证据缺失。".into()))?;
        for (leg, desired) in positions {
            let product = match &leg.product {
                Product::Spot => "spot",
                Product::FuturesUm => "futures_um",
                Product::Option => "options",
            };
            let key = format!("{product}:{}", leg.symbol);
            let amount = reported.get(&key).and_then(Value::as_str)
                .ok_or_else(|| RuntimeError::Conflict("账户快照未覆盖篮子全部腿。".into()))?
                .parse::<Decimal>()
                .map_err(|_| RuntimeError::Conflict("账户仓位精度或数值无效。".into()))?;
            if amount != *desired {
                return Err(RuntimeError::Conflict("账户快照与篮子持仓不一致。".into()));
            }
        }
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
