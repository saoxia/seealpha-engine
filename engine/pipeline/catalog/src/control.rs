//! PostgreSQL handle for Pipeline subscription desired state.
//! Execution and retry are owned by product workers; there is no Runtime job queue.
use seealpha_database::Connection;
use std::{path::Path,sync::Mutex,time::{SystemTime,UNIX_EPOCH}};
use thiserror::Error;

#[derive(Debug,Error)]
pub enum ControlError {
    #[error("控制状态库不可用：{0}")] Database(String),
    #[error("{0}")] Conflict(String),
}
pub(crate) fn db_error(error:seealpha_database::Error)->ControlError{
    ControlError::Database(error.to_string())
}
pub(crate) fn now_ms()->i64{
    SystemTime::now().duration_since(UNIX_EPOCH).ok()
        .and_then(|d|i64::try_from(d.as_millis()).ok()).unwrap_or(0)
}
pub struct ControlStore {conn:Mutex<Connection>}
const SCHEMA:&str=include_str!("../../../../docker/seealpha-postgres-schema.sql");
impl ControlStore {
    pub fn open(path:&Path)->Result<Self,ControlError>{
        let conn=Connection::open(path).map_err(db_error)?;
        conn.execute_batch(SCHEMA).map_err(db_error)?;
        Ok(Self{conn:Mutex::new(conn)})
    }
    pub(crate) fn lock(&self)->Result<std::sync::MutexGuard<'_,Connection>,ControlError>{
        self.conn.lock().map_err(|_|ControlError::Conflict("控制状态锁不可用".into()))
    }
}
