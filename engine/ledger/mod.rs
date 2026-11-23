//! PostgreSQL evidence book for verified trading facts.
//! Nautilus owns account and position state; Ledger owns verified economic evidence.
#[path = "movements/mod.rs"]
mod movements;
#[path = "allocations/mod.rs"]
mod allocations;
#[path = "reconciliation/mod.rs"]
mod reconciliation;

use seealpha_database::Connection;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("状态库不可用。{0}")]
    Database(String),
    #[error("{0}")]
    Conflict(String),
}

pub struct Ledger {
    connection: std::sync::Mutex<Connection>,
}

impl Ledger {
    pub fn open(path: &Path) -> Result<Self, LedgerError> {
        let connection = Connection::open(path)
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        // Standalone Ledger unit fixtures need the shared schema; production
        // bootstrap belongs to the application entry point before opening stores.
        #[cfg(test)]
        seealpha_database::bootstrap_engine_schema(&connection)
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        Ok(Self {
            connection: std::sync::Mutex::new(connection),
        })
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
