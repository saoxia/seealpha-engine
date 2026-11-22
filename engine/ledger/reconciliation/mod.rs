//! Reconciliation — read-only integrity checks of SeeAlpha evidence attribution.
//! Nautilus remains authoritative for live orders, positions, and account state.
//! This is NOT a certification of complete exchange transaction history.
use crate::{Ledger, LedgerError};
use seealpha_database::params;
use serde_json::{Value, json};

impl Ledger {
    /// Audit recorded evidence attribution within one tenant/account/environment.
    /// A nonzero unallocated count means attribution is incomplete, not a
    /// detected cash discrepancy. No account balance is inferred here.
    pub fn reconcile_account_attribution(
        &self, tenant: &str, account: &str, environment: &str,
    ) -> Result<Value, LedgerError> {
        if tenant.is_empty() || account.is_empty()
            || !matches!(environment,"production" | "sandbox") {
            return Err(LedgerError::Conflict("invalid reconciliation scope".into()));
        }
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT e.id, COALESCE(SUM(a.basis_points),0)
             FROM account_evidence_events e
             LEFT JOIN account_event_allocations a
               ON a.event_id=e.id AND a.tenant_id=e.tenant_id
              AND a.account_id=e.account_id AND a.environment=e.environment
             WHERE e.tenant_id=$1 AND e.account_id=$2 AND e.environment=$3
             GROUP BY e.id ORDER BY e.id"
        ).map_err(|e| LedgerError::Database(e.to_string()))?;
        let rows = stmt.query_map(params![tenant,account,environment],
            |r| Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?))
        ).map_err(|e| LedgerError::Database(e.to_string()))?;
        let mut events=0_i64;
        let mut fully_allocated=0_i64;
        let mut partially_allocated=0_i64;
        let mut unattributed=0_i64;
        let mut invalid=0_i64;
        for row in rows {
            let (_event, share)=row.map_err(|e| LedgerError::Database(e.to_string()))?;
            events+=1;
            if share==10_000 { fully_allocated+=1; }
            else if share==0 { unattributed+=1; }
            else if (1..10_000).contains(&share) { partially_allocated+=1; }
            else { invalid+=1; }
        }
        Ok(json!({
            "tenant_id":tenant,"account_id":account,"environment":environment,
            "verified_evidence_events":events,
            "fully_allocated_events":fully_allocated,
            "partially_allocated_events":partially_allocated,
            "unattributed_events":unattributed,
            "invalid_allocations":invalid,
            "attribution_integrity_ok":invalid==0,
            "history_completeness_verified":false,
            "exchange_balance_reconciled":false,
            "position_reconciled":false
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn report_does_not_certify_exchange_or_infer_cash() {
        let db = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        let first = db.reconcile_account_attribution("t","a","sandbox").unwrap();
        assert_eq!(first["verified_evidence_events"],0);
        assert_eq!(first["history_completeness_verified"],false);
        assert_eq!(first["exchange_balance_reconciled"],false);
        assert!(db.reconcile_account_attribution("t","a","paper").is_err());
        let signed = json!({"product":"futures_um","symbol":"ETHUSDT","income_type":"FUNDING_FEE",
            "income":"1","asset":"USDT","time_ms":10,"transaction_id":"abc",
            "source":"binance_signed_income"});
        db.import_account_settlements("t","a","sandbox","futures_um",&[signed]).unwrap();
        let event=db.record_account_evidence_event("t","a","sandbox","FUNDING_FEE","abc").unwrap();
        let before=db.reconcile_account_attribution("t","a","sandbox").unwrap();
        assert_eq!(before["unattributed_events"],1);
        db.attribute_account_event("t","a","sandbox",event,"strategy-1",10000).unwrap();
        let after=db.reconcile_account_attribution("t","a","sandbox").unwrap();
        assert_eq!(after["fully_allocated_events"],1);
        assert_eq!(after["exchange_balance_reconciled"],false);
        assert_eq!(db.reconcile_account_attribution("other","a","sandbox").unwrap()["verified_evidence_events"],0);
    }
}
