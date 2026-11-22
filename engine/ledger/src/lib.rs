//! PostgreSQL evidence book for tenants and verified trading facts.
// Native Nautilus owns account/position state; this crate only persists
// SeeAlpha-specific movement evidence, attribution and reconciliation data.
// Keep src/lib.rs as the Rust crate root.
#[path = "../accounts/mod.rs"]
mod accounts;
#[path = "../movements/mod.rs"]
mod movements;
#[path = "../allocations/mod.rs"]
mod allocations;
#[path = "../reconciliation/mod.rs"]
mod reconciliation;

// Existing execution, portfolio and compatibility APIs remain unchanged.
// They are not subdomains of Ledger/Funds and are not migrated in this PR.
mod order_intents;
mod order_reconciliation_tasks;
mod records;
pub use order_reconciliation_tasks::ReconciliationTask;
pub use order_intents::{IntentStart, OrderRecoveryItem};
mod parity;
use seealpha_database::{Connection, OptionalExtension, params};
use std::path::Path;
use thiserror::Error;

const SCHEMA: &str = include_str!("../../../docker/seealpha-postgres-schema.sql");

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("状态库不可用。{0}")]
    Database(String),
    #[error("{0}")]
    Conflict(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tenant {
    pub tenant_id: String,
    pub purpose: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct StoredOrder {
    pub order_json: String,
}

pub struct Ledger {
    connection: std::sync::Mutex<Connection>,
}

impl Ledger {
    pub fn open(path: &Path) -> Result<Self, LedgerError> {
        let connection = Connection::open(path)
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        connection.execute_batch(SCHEMA)
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        Ok(Self {
            connection: std::sync::Mutex::new(connection),
        })
    }

    /// Enumerate active tenant scopes for safe recovery of never-started jobs.
    /// This does not grant a new execution or exchange order permission.
    pub fn active_tenant_ids(&self) -> Result<Vec<String>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare("SELECT tenant_id FROM tenants WHERE status='active' ORDER BY tenant_id")
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        let rows = statement
            .query_map(&[], |row| row.get::<_, String>(0))
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        let tenants = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        Ok(tenants)
    }

    pub fn ensure_tenant(
        &self,
        tenant_id: &str,
        purpose: &str,
        status: &str,
    ) -> Result<Tenant, LedgerError> {
        if !matches!(purpose, "production" | "development")
            || !matches!(status, "active" | "archived")
        {
            return Err(LedgerError::Conflict("租户类型或状态无效。".into()));
        }
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO tenants (tenant_id, purpose, status) VALUES ($1, $2, $3)
                 ON CONFLICT(tenant_id) DO UPDATE SET purpose=excluded.purpose, status=excluded.status",
                params![tenant_id, purpose, status],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        self.tenant_with(&connection, tenant_id)?
            .ok_or_else(|| LedgerError::Database("missing row".into()))
    }

    pub fn tenant(&self, tenant_id: &str) -> Result<Option<Tenant>, LedgerError> {
        let connection = self.lock()?;
        self.tenant_with(&connection, tenant_id)
    }

    pub fn orders(
        &self,
        tenant_id: &str,
        account_id: &str,
        environment: &str,
    ) -> Result<Vec<StoredOrder>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT order_json FROM orders
                 WHERE tenant_id=$1 AND account_id=$2 AND environment=$3
                 ORDER BY updated_at, client_order_id",
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        let rows = statement
            .query_map(params![tenant_id, account_id, environment], |row| {
                Ok(StoredOrder {
                    order_json: row.get(0)?,
                })
            })
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Database(error.to_string()))
    }

    fn tenant_with(
        &self,
        connection: &Connection,
        tenant_id: &str,
    ) -> Result<Option<Tenant>, LedgerError> {
        connection
            .query_row(
                "SELECT tenant_id, purpose, status FROM tenants WHERE tenant_id=$1",
                params![tenant_id],
                |row| {
                    Ok(Tenant {
                        tenant_id: row.get(0)?,
                        purpose: row.get(1)?,
                        status: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(|error| LedgerError::Database(error.to_string()))
    }

    pub(crate) fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, LedgerError> {
        self.connection
            .lock()
            .map_err(|error| LedgerError::Database(error.to_string()))
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0),
    )
    .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn retired_native_event_table_is_not_recreated_or_destroyed() {
        let path = std::env::temp_dir().join(format!(
            "seealpha-retired-events-{}-{}", std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        let ledger = Ledger::open(&path).unwrap();
        let conn = ledger.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM information_schema.tables
             WHERE table_schema=current_schema() AND table_name='native_order_event_quarantine'",
            &[], |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 0);
        conn.execute_batch(
            "CREATE TABLE native_order_event_quarantine (event_id TEXT PRIMARY KEY, event_json TEXT NOT NULL);
             INSERT INTO native_order_event_quarantine VALUES ('old-1', '{\"evidence_status\":\"unverified_native_event\"}');"
        ).unwrap();
        drop(conn);
        drop(ledger);
        let reopened = Ledger::open(&path).unwrap();
        let conn = reopened.lock().unwrap();
        let original: String = conn.query_row(
            "SELECT event_json FROM native_order_event_quarantine WHERE event_id='old-1'",
            &[], |row| row.get(0)
        ).unwrap();
        assert_eq!(original, r#"{"evidence_status":"unverified_native_event"}"#);
    }


}
