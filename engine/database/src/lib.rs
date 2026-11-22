//! PostgreSQL-backed minimal synchronous SQL access for SeeAlpha state stores.
//! Retains the business-facing row/transaction API while replacing the storage engine.
pub use postgres;
use postgres::{Client, NoTls, types::{FromSql, ToSql}};
use std::{cell::RefCell, fmt, path::Path};

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

pub struct Connection { client: RefCell<Client> }
impl Connection {
    /// The original path argument remains for existing callers; only PostgreSQL is used.
    pub fn open(_path: &Path) -> Result<Self, Error> {
        let url = std::env::var("SEEALPHA_DATABASE_URL")
            .map_err(|_| Error::InvalidRow("SEEALPHA_DATABASE_URL is required".into()))?;
        let mut client = Client::connect(&url, NoTls)?;
        client.batch_execute("CREATE SCHEMA IF NOT EXISTS seealpha; SET search_path TO seealpha, public;")?;
        Ok(Self { client: RefCell::new(client) })
    }

    pub fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        self.client.borrow_mut().batch_execute(sql)?;
        Ok(())
    }

    pub fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<usize, Error> {
        let n = self.client.borrow_mut().execute(sql, params)?;
        usize::try_from(n).map_err(|_| Error::InvalidRow("row count overflow".into()))
    }

    pub fn query_row<T, F>(&self, sql: &str, params: &[&(dyn ToSql + Sync)], f: F) -> Result<T, Error>
    where F: FnOnce(&Row) -> Result<T, Error> {
        let row = self.client.borrow_mut().query_opt(sql, params)?;
        match row {
            Some(row) => f(&Row(row)),
            None => Err(Error::NotFound),
        }
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
        let rows = self.conn.client.borrow_mut().query(&self.sql, params)?;
        Ok(rows.into_iter().map(|row| f(&Row(row))).collect::<Vec<_>>().into_iter())
    }
}

pub struct Transaction<'a> { conn: &'a Connection, committed: bool }
impl Transaction<'_> {
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
    pub fn last_insert_rowid(&self) -> i64 {
        self.query_row("SELECT currval(pg_get_serial_sequence('seealpha.account_evidence_events','id'))",
            &[], |row| row.get(0)).unwrap_or(0)
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
