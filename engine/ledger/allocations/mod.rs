//! Allocations — immutable strategy attribution of verified economic events.
//! Allocations are percentages of evidence, NOT synthetic exchange fills,
//! cash postings, positions, or permission to trade.
use crate::{Ledger, LedgerError, now_ms};
use seealpha_database::{OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};

fn bad() -> LedgerError {
    LedgerError::Conflict("allocation requires existing scoped evidence and available share".into())
}
fn sql(e: seealpha_database::Error) -> LedgerError {
    LedgerError::Database(e.to_string())
}
impl Ledger {
    /// Attribute basis points (1..=10000) of an already verified account event
    /// to a strategy. Repeating an identical attribution is safe; any rewrite
    /// is rejected. Cross-account references and over-allocation fail closed.
    pub fn attribute_account_event(
        &self, tenant: &str, account: &str, environment: &str,
        event_id: i64, strategy_id: &str, basis_points: i64,
    ) -> Result<(), LedgerError> {
        if tenant.is_empty() || account.is_empty() || strategy_id.is_empty()
            || strategy_id.len() > 128 || event_id <= 0
            || !(1..=10_000).contains(&basis_points)
            || !matches!(environment, "production" | "sandbox")
        { return Err(bad()); }
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(sql)?;
        let exists: Option<i64> = tx.query_row(
            "SELECT id FROM account_evidence_events WHERE id=$1 AND tenant_id=$2 AND account_id=$3 AND environment=$4",
            params![event_id,tenant,account,environment], |r| r.get(0)
        ).optional().map_err(sql)?;
        if exists.is_none() { return Err(bad()); }
        let previous: Option<i64> = tx.query_row(
            "SELECT basis_points FROM account_event_allocations WHERE tenant_id=$1 AND account_id=$2 AND environment=$3 AND event_id=$4 AND strategy_id=$5",
            params![tenant,account,environment,event_id,strategy_id], |r| r.get(0)
        ).optional().map_err(sql)?;
        if let Some(old) = previous {
            if old != basis_points { return Err(bad()); }
            return Ok(());
        }
        let used: i64 = tx.query_row(
            "SELECT COALESCE(SUM(basis_points),0) FROM account_event_allocations WHERE tenant_id=$1 AND account_id=$2 AND environment=$3 AND event_id=$4",
            params![tenant,account,environment,event_id], |r| r.get(0)
        ).map_err(sql)?;
        if used.checked_add(basis_points).is_none_or(|v| v > 10_000) {
            return Err(bad());
        }
        tx.execute(
            "INSERT INTO account_event_allocations (tenant_id,account_id,environment,event_id,strategy_id,basis_points,recorded_at_ms) VALUES ($1,$2,$3,$4,$5,$6,$7)",
            params![tenant,account,environment,event_id,strategy_id,basis_points,now_ms()]
        ).map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(())
    }

    /// Read-only attribution. Unattributed share remains explicitly visible;
    /// the absence of attribution does not imply a funding discrepancy.
    pub fn account_event_allocations(
        &self, tenant: &str, account: &str, environment: &str, event_id: i64
    ) -> Result<Value, LedgerError> {
        if !matches!(environment, "production" | "sandbox") || event_id <= 0 { return Err(bad()); }
        let conn = self.lock()?;
        let exists: Option<i64> = conn.query_row(
            "SELECT id FROM account_evidence_events WHERE id=$1 AND tenant_id=$2 AND account_id=$3 AND environment=$4",
            params![event_id,tenant,account,environment], |r| r.get(0)
        ).optional().map_err(sql)?;
        if exists.is_none() { return Err(bad()); }
        let mut stmt = conn.prepare(
            "SELECT strategy_id,basis_points FROM account_event_allocations WHERE tenant_id=$1 AND account_id=$2 AND environment=$3 AND event_id=$4 ORDER BY strategy_id"
        ).map_err(sql)?;
        let rows = stmt.query_map(params![tenant,account,environment,event_id],
            |r| Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?))
        ).map_err(sql)?;
        let mut allocations = Vec::new();
        let mut total = 0_i64;
        for row in rows {
            let (strategy, share) = row.map_err(sql)?;
            total += share;
            allocations.push(json!({"strategy_id":strategy,"basis_points":share}));
        }
        Ok(json!({"event_id":event_id,"allocations":allocations,
            "allocated_basis_points":total,"unallocated_basis_points":10_000-total}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scoped_immutable_attribution_of_verified_funding() {
        let db = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        let evidence = json!({"product":"futures_um","symbol":"ETHUSDT",
            "income_type":"FUNDING_FEE","income":"-0.1","asset":"USDT",
            "time_ms":10,"transaction_id":"funding-1","source":"binance_signed_income"});
        db.import_account_settlements("t","a","sandbox","futures_um",&[evidence]).unwrap();
        let event = db.record_account_evidence_event("t","a","sandbox","FUNDING_FEE","funding-1").unwrap();
        assert!(db.attribute_account_event("other","a","sandbox",event,"s1",5000).is_err());
        assert!(db.attribute_account_event("t","a","production",event,"s1",5000).is_err());
        db.attribute_account_event("t","a","sandbox",event,"s1",6000).unwrap();
        db.attribute_account_event("t","a","sandbox",event,"s1",6000).unwrap();
        assert!(db.attribute_account_event("t","a","sandbox",event,"s1",5000).is_err());
        assert!(db.attribute_account_event("t","a","sandbox",event,"s2",5000).is_err());
        db.attribute_account_event("t","a","sandbox",event,"s2",4000).unwrap();
        let result = db.account_event_allocations("t","a","sandbox",event).unwrap();
        assert_eq!(result["allocated_basis_points"],10000);
        assert_eq!(result["unallocated_basis_points"],0);
        assert!(db.account_event_allocations("other","a","sandbox",event).is_err());
    }
}
