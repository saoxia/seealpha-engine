//! Explicit evidence-only replay adapter.
//!
//! The legacy bar-level worker is not automatically upgraded to verified
//! execution. Only a caller that has independently certified exact historical
//! coverage may use this path. The strategy host remains external to this API.
use crate::{Runtime, RuntimeError, map_ledger};
use seealpha_backtest::timeline::verified::{Contract, Event, Policy, Strategy, replay};
use serde_json::{Value, json};

/// Evidence certification is produced by the source/catalog coordinator,
/// never inferred from a bar close or from a successful network request.
pub struct VerifiedHistoryWindow<'a> {
    pub dataset_id: &'a str,
    pub provenance_sha256: &'a str,
    pub start_ms: i64,
    pub end_ms: i64,
    pub contiguous: bool,
}

impl Runtime {
    /// Offline-only evidence replay. Does not authorize any exchange operation.
    /// The bar-only HTTP worker intentionally remains a separate model.
    pub fn run_verified_history<S: Strategy>(
        &self,
        tenant: &str,
        account: &str,
        evidence: VerifiedHistoryWindow<'_>,
        contracts: &[Contract],
        events: &[Event],
        policy: Policy,
        strategy: &mut S,
    ) -> Result<Value, RuntimeError> {
        let tenant_state = self.require_active(tenant)?;
        if tenant_state.purpose != "development" {
            return Err(RuntimeError::Conflict(
                "证据驱动历史回测只允许开发租户。".into(),
            ));
        }
        if account.is_empty()
            || evidence.dataset_id.is_empty()
            || !evidence.contiguous
            || evidence.start_ms < 0
            || evidence.end_ms <= evidence.start_ms
            || evidence.provenance_sha256.len() != 64
            || !evidence
                .provenance_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
            || contracts.is_empty()
            || events.is_empty()
            || events
                .iter()
                .any(|e| e.time_ms() < evidence.start_ms || e.time_ms() >= evidence.end_ms)
            || !events.iter().any(|e| matches!(e, Event::Quote(_)))
        {
            return Err(RuntimeError::Conflict(
                "历史成交证据、不可变数据指纹或覆盖窗口不足。".into(),
            ));
        }
        let report = replay(contracts, events, policy, strategy).map_err(RuntimeError::Conflict)?;
        let positions = report
            .positions
            .iter()
            .map(|(id, amount)| (id.clone(), Value::String(amount.to_string())))
            .collect::<serde_json::Map<String, Value>>();
        let result = json!({
            "execution_model": "quote_evidence_only",
            "history_source": evidence.dataset_id,
            "provenance_sha256": evidence.provenance_sha256,
            "start_ms": evidence.start_ms,
            "end_ms": evidence.end_ms,
            "cash": report.cash.to_string(),
            "equity": report.equity.to_string(),
            "positions": positions,
            "fill_count": report.fills.len(),
            "settlement_count": report.settlements.len(),
            "pending_order_count": report.pending.len(),
            "rejected": report.rejected,
            "funding": report.funding.to_string(),
            "commissions": report.commissions.to_string(),
            "exchange_orders": 0,
            "venue": "offline_verified_replay",
            "data_ready_for_live": false,
        });
        self.ledger
            .save_backtest(tenant, account, "verified_evidence", &result.to_string())
            .map_err(map_ledger)?;
        Ok(result)
    }
}
