//! V2 append-only economic facts. Independent from legacy evidence projections.
//! Nautilus owns order/position/account lifecycles; this layer stores source evidence
//! and canonical external facts, including references to native event identities.
use crate::{Ledger, LedgerError};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use rust_decimal::Decimal;
use serde_json::{json, Value};

pub(crate) const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS movement_raw_events_v2 (
 tenant_id TEXT NOT NULL, account_id TEXT NOT NULL, environment TEXT NOT NULL,
 venue TEXT NOT NULL, source_type TEXT NOT NULL, source_id TEXT NOT NULL,
 payload_json TEXT NOT NULL, observed_at_ms INTEGER NOT NULL,
 PRIMARY KEY (tenant_id,account_id,environment,venue,source_type,source_id)
);
CREATE TABLE IF NOT EXISTS economic_movements_v2 (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 tenant_id TEXT NOT NULL, account_id TEXT NOT NULL, environment TEXT NOT NULL,
 venue TEXT NOT NULL, source_type TEXT NOT NULL, source_id TEXT NOT NULL,
 event_type TEXT NOT NULL, occurred_at_ms INTEGER NOT NULL,
 instrument_id TEXT, native_event_id TEXT,
 movement_json TEXT NOT NULL,
 UNIQUE(tenant_id,account_id,environment,venue,source_type,source_id),
 FOREIGN KEY(tenant_id,account_id,environment,venue,source_type,source_id)
 REFERENCES movement_raw_events_v2(tenant_id,account_id,environment,venue,source_type,source_id)
);
CREATE INDEX IF NOT EXISTS economic_movements_page_v2
 ON economic_movements_v2(tenant_id,account_id,environment,id);
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetEffect {
    pub asset: String,
    /// Signed decimal string is persisted exactly; no f64 conversion.
    pub delta: Decimal,
    /// cash, asset, fee, collateral; an empty set is valid for fill-only facts.
    pub role: String,
}

#[derive(Debug, Clone)]
pub struct MovementInput {
    pub tenant_id: String,
    pub account_id: String,
    pub environment: String,
    pub venue: String,
    pub source_type: String,
    pub source_id: String,
    pub event_type: String,
    pub occurred_at_ms: i64,
    pub observed_at_ms: i64,
    pub instrument_id: Option<String>,
    /// Native Nautilus event identity, not a locally reconstructed OrderFilled.
    pub native_event_id: Option<String>,
    pub raw_payload: Value,
    pub effects: Vec<AssetEffect>,
}

fn invalid(reason: &str) -> LedgerError { LedgerError::Conflict(reason.into()) }
fn db(error: rusqlite::Error) -> LedgerError { LedgerError::Sqlite(error.to_string()) }

impl MovementInput {
    fn validate(&self) -> Result<(), LedgerError> {
        for value in [&self.tenant_id, &self.account_id, &self.venue,
                      &self.source_type, &self.source_id, &self.event_type] {
            if value.trim().is_empty() || value.len() > 256 {
                return Err(invalid("movement identity is empty or too long"));
            }
        }
        if !matches!(self.environment.as_str(), "production" | "sandbox" | "paper")
            || self.occurred_at_ms <= 0 || self.observed_at_ms <= 0
            || !self.raw_payload.is_object()
            || self.instrument_id.as_ref().is_some_and(|v| v.trim().is_empty())
            || self.native_event_id.as_ref().is_some_and(|v| v.trim().is_empty())
        {
            return Err(invalid("invalid economic movement scope or evidence"));
        }
        if self.effects.len() > 32 { return Err(invalid("too many asset effects")); }
        for effect in &self.effects {
            if effect.asset.trim().is_empty() || effect.asset.len() > 64
                || !matches!(effect.role.as_str(), "cash" | "asset" | "fee" | "collateral")
            { return Err(invalid("invalid asset effect")); }
        }
        Ok(())
    }
    fn canonical_json(&self) -> String {
        let effects: Vec<Value> = self.effects.iter().map(|e| json!({
            "asset":e.asset, "delta":e.delta.to_string(), "role":e.role
        })).collect();
        json!({
            "schema_version":2, "venue":self.venue, "source_type":self.source_type,
            "source_id":self.source_id, "event_type":self.event_type,
            "occurred_at_ms":self.occurred_at_ms, "instrument_id":self.instrument_id,
            "native_event_id":self.native_event_id, "effects":effects,
            "verification":"pending"
        }).to_string()
    }
}

impl Ledger {
    /// Atomic raw + normalized write. Inputs remain unverified until an authorized
    /// source adapter and reconciliation path have established provenance.
    /// Returns stable row ID on exact replay; rejects conflicting reuse of identity.
    pub fn ingest_economic_movement_v2(&self, input: &MovementInput) -> Result<i64, LedgerError> {
        input.validate()?;
        let raw = input.raw_payload.to_string();
        let normalized = input.canonical_json();
        let mut connection = self.lock()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(db)?;
        let key = params![input.tenant_id,input.account_id,input.environment,
                          input.venue,input.source_type,input.source_id];
        let prior_raw: Option<String> = tx.query_row(
            "SELECT payload_json FROM movement_raw_events_v2 WHERE tenant_id=?1 AND account_id=?2
             AND environment=?3 AND venue=?4 AND source_type=?5 AND source_id=?6",
            key, |r| r.get(0)).optional().map_err(db)?;
        if prior_raw.as_deref().is_some_and(|v| v != raw) {
            return Err(invalid("conflicting original source event"));
        }
        if prior_raw.is_none() {
            tx.execute("INSERT INTO movement_raw_events_v2
                (tenant_id,account_id,environment,venue,source_type,source_id,payload_json,observed_at_ms)
                VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![input.tenant_id,input.account_id,input.environment,input.venue,
                    input.source_type,input.source_id,raw,input.observed_at_ms]).map_err(db)?;
        }
        let prior: Option<(i64,String)> = tx.query_row(
            "SELECT id,movement_json FROM economic_movements_v2 WHERE tenant_id=?1 AND account_id=?2
             AND environment=?3 AND venue=?4 AND source_type=?5 AND source_id=?6",
            params![input.tenant_id,input.account_id,input.environment,input.venue,input.source_type,input.source_id],
            |r| Ok((r.get(0)?,r.get(1)?))).optional().map_err(db)?;
        if let Some((id, old)) = prior {
            if old != normalized { return Err(invalid("conflicting normalized movement")); }
            tx.commit().map_err(db)?;
            return Ok(id);
        }
        tx.execute("INSERT INTO economic_movements_v2
            (tenant_id,account_id,environment,venue,source_type,source_id,event_type,
             occurred_at_ms,instrument_id,native_event_id,movement_json)
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![input.tenant_id,input.account_id,input.environment,input.venue,input.source_type,
                input.source_id,input.event_type,input.occurred_at_ms,input.instrument_id,
                input.native_event_id,normalized]).map_err(db)?;
        let id = tx.last_insert_rowid();
        tx.commit().map_err(db)?;
        Ok(id)
    }

    pub fn economic_movement_page_v2(
        &self, tenant: &str, account: &str, environment: &str, after: i64, limit: i64
    ) -> Result<Vec<(i64, Value)>, LedgerError> {
        if tenant.is_empty() || account.is_empty() || after < 0 || limit < 1
            || !matches!(environment,"production"|"sandbox"|"paper")
        { return Err(invalid("invalid movement query")); }
        let connection = self.lock()?;
        let mut stmt = connection.prepare(
            "SELECT id,movement_json FROM economic_movements_v2 WHERE tenant_id=?1
             AND account_id=?2 AND environment=?3 AND id>?4 ORDER BY id LIMIT ?5"
        ).map_err(db)?;
        let rows = stmt.query_map(params![tenant,account,environment,after,limit.min(1000)],
            |row| Ok((row.get::<_,i64>(0)?,row.get::<_,String>(1)?))).map_err(db)?;
        rows.map(|row| {
            let (id, raw) = row.map_err(db)?;
            let value = serde_json::from_str(&raw)
                .map_err(|_| invalid("stored normalized event is invalid"))?;
            Ok((id,value))
        }).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample() -> MovementInput {
        MovementInput {
            tenant_id:"t".into(), account_id:"a".into(), environment:"sandbox".into(),
            venue:"BINANCE".into(),source_type:"signed_funding".into(), source_id:"42".into(),
            event_type:"FUNDING".into(),occurred_at_ms:123,observed_at_ms:124,
            instrument_id:Some("ETHUSDT-PERP.BINANCE".into()),
            native_event_id:None,raw_payload:json!({"transaction_id":"42","income":"-0.01"}),
            effects:vec![AssetEffect { asset:"USDT".into(),delta:"-0.01".parse().unwrap(),
                role:"cash".into() }],
        }
    }
    #[test]
    fn replay_scope_and_conflict() {
        let ledger = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        let item = sample();
        let id = ledger.ingest_economic_movement_v2(&item).unwrap();
        assert_eq!(ledger.ingest_economic_movement_v2(&item).unwrap(),id);
        assert_eq!(ledger.economic_movement_page_v2("t","a","sandbox",0,10).unwrap().len(),1);
        assert!(ledger.economic_movement_page_v2("t","a","production",0,10).unwrap().is_empty());
        let mut changed = item.clone();
        changed.effects[0].delta = "0.01".parse().unwrap();
        assert!(ledger.ingest_economic_movement_v2(&changed).is_err());
        changed = item.clone();
        changed.raw_payload["income"] = json!("0.01");
        assert!(ledger.ingest_economic_movement_v2(&changed).is_err());
        assert_eq!(ledger.economic_movement_page_v2("t","a","sandbox",0,10).unwrap().len(),1);
    }
}
