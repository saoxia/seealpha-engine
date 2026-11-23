//! Tenant-scoped application state; verified economic facts remain in Ledger.
mod registry;
pub use registry::Tenant;

use seealpha_database::Connection;
use seealpha_ledger::LedgerError;
use std::{path::Path, sync::{Mutex, MutexGuard}};

pub struct StateStore { connection: Mutex<Connection> }

impl StateStore {
    pub fn open(path: &Path) -> Result<Self, LedgerError> {
        let connection = Connection::open(path)
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        // Standalone test fixtures; Runtime::open bootstraps the production schema.
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
