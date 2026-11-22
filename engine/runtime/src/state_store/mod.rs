//! Application-owned durable state: research outputs and Portfolio checkpoints.
//! This is not a balance, position, or exchange fill ledger. Uses the same
//! PostgreSQL schema already initialized by Ledger::open during Runtime::open.
mod basket_journal;
mod coordinator_state;
mod research;
mod paper;
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
        // owned by Ledger::open, called before this store is constructed.
        #[cfg(test)]
        connection.execute_batch(include_str!("../../../../docker/seealpha-postgres-schema.sql"))
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
