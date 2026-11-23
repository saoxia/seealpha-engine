//! PostgreSQL-backed minimal synchronous SQL access for SeeAlpha state stores.
//! Retains the business-facing row/transaction API while replacing the storage engine.
pub use postgres;

/// Initialize the shared application schema independently of a particular store.
/// Ledger and Runtime use the same database namespace; no tables are dropped.
pub fn bootstrap_engine_schema(connection: &Connection) -> Result<(), Error> {
    connection.execute_batch(include_str!("../../../docker/seealpha-postgres-schema.sql"))
}
use postgres::{Client, NoTls, types::{FromSql, ToSql}};
use std::{fmt, hash::{Hash, Hasher}, path::Path, sync::Mutex};

#[derive(Debug)]
pub enum Error {
    Postgres(postgres::Error),
    NotFound,
    InvalidRow(String),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Postgres(e) => write!(f, "{e}"),
            Self::NotFound => write!(f, "database row not found"),
            Self::InvalidRow(e) => write!(f, "{e}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<postgres::Error> for Error {
    fn from(error: postgres::Error) -> Self { Self::Postgres(error) }
}

#[macro_export]
macro_rules! params {
    ($($value:expr),* $(,)?) => {
        &[$(&$value as &(dyn $crate::postgres::types::ToSql + Sync)),*]
    };
}

pub trait OptionalExtension<T> {
    fn optional(self) -> Result<Option<T>, Error>;
}
impl<T> OptionalExtension<T> for Result<T, Error> {
    fn optional(self) -> Result<Option<T>, Error> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub enum TransactionBehavior { Immediate }

pub struct Row(postgres::Row);
impl Row {
    pub fn get<I, T>(&self, index: I) -> Result<T, Error>
    where
        I: TryInto<usize>,
        T: for<'a> FromSql<'a>,
    {
        let index = index.try_into().map_err(|_| Error::InvalidRow("invalid column index".into()))?;
        self.0.try_get(index).map_err(Error::from)
    }
}

pub fn schema_for_path(path: &Path) -> String {
    if path.to_str() == Some(":memory:") {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        return format!("seealpha_fixture_memory_{}_{}",
            std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed));
    }
    // Every test's temporary state directory is isolated, but its Ledger
    // and Control stores must share the same schema across restarts.
    if path.starts_with(std::env::temp_dir()) {
        let scoped = if matches!(path.file_name().and_then(|n| n.to_str()),
            Some("postgres-state" | "control-state")) {
            path.parent().unwrap_or(path)
        } else {
            path
        };
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        scoped.hash(&mut hash);
        return format!("seealpha_fixture_{:016x}", hash.finish());
    }
    // Normal application state (including container restarts) shares one schema.
    "seealpha".into()
}

pub struct Connection { client: Mutex<Option<Client>>, schema: String }
impl Drop for Connection {
    fn drop(&mut self) {
        // The blocking postgres client calls an internal runtime during Drop.
        // Destroy it outside Tokio's async executor, just like DB queries.
        if let Ok(mut guard) = self.client.lock()
            && let Some(client) = guard.take()
        {
            let _ = std::thread::spawn(move || drop(client)).join();
        }
    }
}
impl Connection {
    // The synchronous postgres crate creates an internal Tokio runtime.
    // All database I/O runs off the async HTTP executor to avoid nesting runtimes.
    fn with_client<T, F>(&self, run: F) -> Result<T, Error>
    where T: Send, F: FnOnce(&mut Client) -> Result<T, Error> + Send {
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let mut client = self.client.lock().map_err(|_| Error::InvalidRow("postgres mutex poisoned".into()))?;
                let client = client.as_mut().ok_or_else(|| Error::InvalidRow("postgres client closed".into()))?;
                run(client)
            }).join().map_err(|_| Error::InvalidRow("postgres worker panicked".into()))?
        })
    }

    pub fn open(path: &Path) -> Result<Self, Error> {
        let url = std::env::var("SEEALPHA_DATABASE_URL")
            .map_err(|_| Error::InvalidRow("SEEALPHA_DATABASE_URL is required".into()))?;
        let mut client = std::thread::spawn(move || Client::connect(&url, NoTls))
            .join().map_err(|_| Error::InvalidRow("postgres connect worker panicked".into()))??;
        let schema = schema_for_path(path);
        // Lock bootstrap to prevent concurrent CREATE SCHEMA catalog races.
        let ddl = format!(
            "BEGIN; SELECT pg_advisory_xact_lock(732114683); CREATE SCHEMA IF NOT EXISTS {schema}; COMMIT; SET search_path TO {schema}, public;"
        );
        std::thread::scope(|scope| {
            scope.spawn(|| client.batch_execute(&ddl)).join()
                .map_err(|_| Error::InvalidRow("postgres schema worker panicked".into()))?
                .map_err(Error::from)
        })?;
        Ok(Self { client: Mutex::new(Some(client)), schema })
    }

    pub fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        let sql = sql.replace("CREATE SCHEMA IF NOT EXISTS seealpha;", &format!(
            "CREATE SCHEMA IF NOT EXISTS {};", self.schema
        )).replace("SET LOCAL search_path TO seealpha, public;", &format!(
            "SET LOCAL search_path TO {}, public;", self.schema
        )).replace("BEGIN;\n", "BEGIN;\nSELECT pg_advisory_xact_lock(732114683);\n");
        self.with_client(move |client| { client.batch_execute(&sql)?; Ok(()) })
    }

    pub fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<usize, Error> {
        let n = self.with_client(|client| Ok(client.execute(sql, params)?))?;
        usize::try_from(n).map_err(|_| Error::InvalidRow("row count overflow".into()))
    }

    pub fn query_row<T, F>(&self, sql: &str, params: &[&(dyn ToSql + Sync)], f: F) -> Result<T, Error>
    where F: FnOnce(&Row) -> Result<T, Error> {
        let row = self.with_client(|client| Ok(client.query_opt(sql, params)?))?;
        match row {
            Some(row) => f(&Row(row)),
            None => Err(Error::NotFound),
        }
    }

    pub fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<postgres::Row>, Error> {
        self.with_client(|client| Ok(client.query(sql, params)?))
    }
    pub fn query_opt(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<postgres::Row>, Error> {
        self.with_client(|client| Ok(client.query_opt(sql, params)?))
    }
    pub fn query_one(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<postgres::Row, Error> {
        self.with_client(|client| Ok(client.query_one(sql, params)?))
    }
    pub fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.transaction_with_behavior(TransactionBehavior::Immediate)
    }
    pub fn prepare<'a>(&'a self, sql: &str) -> Result<Statement<'a>, Error> {
        Ok(Statement { conn: self, sql: sql.into() })
    }

    pub fn transaction_with_behavior(&mut self, _: TransactionBehavior) -> Result<Transaction<'_>, Error> {
        self.execute_batch("BEGIN; SELECT pg_advisory_xact_lock(732114682);")?;
        Ok(Transaction { conn: self, committed: false })
    }
}

pub struct Statement<'a> { conn: &'a Connection, sql: String }
impl Statement<'_> {
    pub fn query_map<T, F>(&mut self, params: &[&(dyn ToSql + Sync)], mut f: F) -> Result<std::vec::IntoIter<Result<T, Error>>, Error>
    where F: FnMut(&Row) -> Result<T, Error> {
        let rows = self.conn.with_client(|client| Ok(client.query(&self.sql, params)?))?;
        Ok(rows.into_iter().map(|row| f(&Row(row))).collect::<Vec<_>>().into_iter())
    }
}

pub struct Transaction<'a> { conn: &'a Connection, committed: bool }
impl std::ops::Deref for Transaction<'_> {
    type Target = Connection;
    fn deref(&self) -> &Self::Target { self.conn }
}
impl Transaction<'_> {
    pub fn query_one(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<postgres::Row, Error> {
        self.conn.query_one(sql, params)
    }
    pub fn query_opt(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<postgres::Row>, Error> {
        self.conn.query_opt(sql, params)
    }
    pub fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<usize, Error> {
        self.conn.execute(sql, params)
    }
    pub fn query_row<T, F>(&self, sql: &str, params: &[&(dyn ToSql + Sync)], f: F) -> Result<T, Error>
    where F: FnOnce(&Row) -> Result<T, Error> {
        self.conn.query_row(sql, params, f)
    }
    pub fn prepare(&self, sql: &str) -> Result<Statement<'_>, Error> {
        self.conn.prepare(sql)
    }
    pub fn last_identity(&self) -> Result<i64, Error> {
        self.query_row("SELECT currval(pg_get_serial_sequence('account_evidence_events','id'))",
            &[], |row| row.get(0))
    }
    pub fn commit(mut self) -> Result<(), Error> {
        self.conn.execute_batch("COMMIT")?;
        self.committed = true;
        Ok(())
    }
}
impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}


#[cfg(test)]
mod schema_contract_tests {
    #[test]
    fn bootstrap_creates_only_active_application_tables() {
        let schema = include_str!("../../../docker/seealpha-postgres-schema.sql");
        for table in [
            "portfolio_coordinator_states", "basket_journals",
            "order_reconciliation_tasks", "order_requests", "order_request_views",
            "positions", "paper_positions", "paper_sessions",
        ] {
            assert!(
                !schema.contains(&format!("CREATE TABLE IF NOT EXISTS {table} (")),
                "retired table {table} must not be recreated",
            );
        }
        // All application tables are account-, product-, or instrument-scoped.
        for table in [
            "order_intents", "order_observations", "orders", "fills",
            "account_evidence_events",
            "account_event_allocations", "account_settlements",
            "pipeline_subscriptions",
        ] {
            let prefix = format!("CREATE TABLE IF NOT EXISTS {table} (");
            let section = schema.split_once(&prefix).unwrap().1.split_once(");").unwrap().0;
            assert!(!section.contains("tenant_id"), "{table} still stores tenant data");
        }
        for table in [
            "order_intents", "order_observations", "orders", "fills",
            "account_evidence_events",
            "account_event_allocations", "account_settlements",
            "pipeline_subscriptions",
        ] {
            assert!(
                schema.contains(&format!("CREATE TABLE IF NOT EXISTS {table} (")),
                "live table {table} must remain",
            );
        }
    }
}
