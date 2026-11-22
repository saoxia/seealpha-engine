//! Application-owned durable state: research outputs and Portfolio checkpoints.
//! This is not a balance, position, or exchange fill ledger. Uses the same
//! PostgreSQL schema initialized by Runtime before stores are opened.
mod registry;
pub use registry::Tenant;
mod research;
mod paper;
mod order_intents;
mod order_reconciliation_tasks;
mod order_requests;
mod events;
pub use order_requests::StoredOrder;
pub use order_intents::{IntentStart, OrderRecoveryItem};
pub use order_reconciliation_tasks::ReconciliationTask;
pub(crate) use paper::{SessionStart, SessionWrite};

use seealpha_database::Connection;
use seealpha_ledger::LedgerError;
use std::{path::Path, sync::{Mutex, MutexGuard}};

pub struct StateStore { connection: Mutex<Connection> }

impl StateStore {
    pub fn open(path: &Path) -> Result<Self, LedgerError> {
        let connection = Connection::open(path)
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        // Tests use standalone :memory: fixtures; production schema creation is
        // owned by Runtime::open, called before this store is constructed.
        #[cfg(test)]
        seealpha_database::bootstrap_engine_schema(&connection)
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        Ok(Self { connection: Mutex::new(connection) })
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>, LedgerError> {
        self.connection.lock()
            .map_err(|_| LedgerError::Database("application state mutex poisoned".into()))
    }
}

pub(super) fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)
        .map(|v| v.as_millis()).unwrap_or(0)).unwrap_or(0)
}

#[cfg(test)]
impl StateStore {
    fn request(&self, tenant: &str, account: &str, request: &str) -> Result<Option<serde_json::Value>, LedgerError> {
        self.get_order_request(tenant,account,request)?.map(|s| serde_json::from_str(&s).map_err(|e| LedgerError::Database(e.to_string()))).transpose()
    }
    fn fills(&self, tenant: &str, account: &str, env: &str) -> Result<Vec<String>, LedgerError> {
        use seealpha_database::params;
        let conn=self.lock()?;
        let mut stmt=conn.prepare("SELECT fill_json FROM fills WHERE tenant_id=$1 AND account_id=$2 AND environment=$3").map_err(|e| LedgerError::Database(e.to_string()))?;
        let rows=stmt.query_map(params![tenant,account,env], |r| r.get::<_,String>(0)).map_err(|e| LedgerError::Database(e.to_string()))?;
        rows.collect::<Result<Vec<_>,_>>().map_err(|e| LedgerError::Database(e.to_string()))
    }
    // Test-only authenticated fill fixtures. Production ingestion stays in Ledger.
    fn import_verified_fills(&self, tenant: &str, account: &str, env: &str, order_id: &str, rows: &[serde_json::Value]) -> Result<usize, LedgerError> {
        use seealpha_database::params;
        let mut conn=self.lock()?;
        let tx=conn.transaction_with_behavior(seealpha_database::TransactionBehavior::Immediate).map_err(|e| LedgerError::Database(e.to_string()))?;
        for row in rows {
            let trade_id=row["trade_id"].as_str().ok_or_else(|| LedgerError::Conflict("missing test trade id".into()))?;
            if !trade_id.starts_with("binance:") || row["venue_order_id"].as_str()!=Some(order_id)
                || row["source"].as_str()!=Some("signed_exchange_user_trades") {
                return Err(LedgerError::Conflict("invalid test fill".into()));
            }
            tx.execute("INSERT INTO fills(tenant_id,account_id,environment,fill_id,fill_json,updated_at) VALUES ($1,$2,$3,$4,$5,0) ON CONFLICT(tenant_id,account_id,environment,fill_id) DO NOTHING",
                params![tenant,account,env,trade_id,row.to_string()]).map_err(|e| LedgerError::Database(e.to_string()))?;
        }
        tx.commit().map_err(|e| LedgerError::Database(e.to_string()))?;
        Ok(rows.len())
    }

}
