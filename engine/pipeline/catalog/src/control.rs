//! Durable non-financial Pipeline control-plane state.
use seealpha_database::{Connection,OptionalExtension,TransactionBehavior,params};
use serde_json::{Value,json};
use std::{path::Path,sync::Mutex,time::{SystemTime,UNIX_EPOCH}};
use thiserror::Error;
#[derive(Debug,Error)]
pub enum ControlError {
    #[error("控制状态库不可用：{0}")] Database(String),
    #[error("{0}")] Conflict(String),
}
pub(crate) fn db_error(e:seealpha_database::Error)->ControlError{ControlError::Database(e.to_string())}
pub(crate) fn now_ms()->i64{SystemTime::now().duration_since(UNIX_EPOCH).ok().and_then(|d|i64::try_from(d.as_millis()).ok()).unwrap_or(0)}
fn parse(s:&str)->Result<Value,ControlError>{serde_json::from_str(s).map_err(|_|ControlError::Conflict("控制记录 JSON 损坏".into()))}
pub struct ControlStore { conn:Mutex<Connection> }
const SCHEMA: &str = include_str!("../../../../docker/seealpha-postgres-schema.sql");
impl ControlStore {
    /// Current control state only: no legacy book import or migration tracking.
    pub fn open(path: &Path) -> Result<Self, ControlError> {
        let connection = Connection::open(path).map_err(db_error)?;
        connection.execute_batch(SCHEMA).map_err(db_error)?;
        Ok(Self { conn: Mutex::new(connection) })
    }
    pub(crate) fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, ControlError> {
        self.conn.lock().map_err(|_| ControlError::Conflict("控制状态锁不可用".into()))
    }
}

impl ControlStore {
    /// Atomic replacement of the tenant's declared producer policies.
    /// Existing readers and order state are untouched.
    pub fn replace_policies(&self, tenant_id: &str, policies: &[Value]) -> Result<(), ControlError> {
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        tx.execute(
            "DELETE FROM data_policies WHERE tenant_id=$1",
            params![tenant_id],
        )
        .map_err(db_error)?;
        for policy in policies {
            let product = policy["product"].as_str().unwrap_or("");
            let symbol = policy["symbol"].as_str().unwrap_or("");
            tx.execute(
                "INSERT INTO data_policies(tenant_id,product,symbol,policy_json,updated_at)
                 VALUES($1,$2,$3,$4,$5)",
                params![tenant_id, product, symbol, policy.to_string(), now_ms()],
            )
            .map_err(db_error)?;
        }
        tx.commit().map_err(db_error)
    }

    pub fn policies(&self, tenant_id: &str) -> Result<Vec<Value>, ControlError> {
        let conn = self.lock()?;
        let mut q = conn
            .prepare(
                "SELECT policy_json FROM data_policies WHERE tenant_id=$1 ORDER BY product,symbol",
            )
            .map_err(db_error)?;
        let rows = q
            .query_map(params![tenant_id], |r| r.get::<_, String>(0))
            .map_err(db_error)?;
        rows.map(|r| parse(&r.map_err(db_error)?)).collect()
    }

    pub fn queue_data_job(
        &self,
        tenant_id: &str,
        job_id: &str,
        operation: &str,
        payload: &Value,
    ) -> Result<Value, ControlError> {
        if operation == "backtest" { return Err(ControlError::Conflict("回测必须进入独立队列".into())); }
        let conn = self.lock()?;
        let now = now_ms();
        let payload_text = payload.to_string();
        let inserted=conn.execute(
            "INSERT INTO data_jobs(tenant_id,job_id,operation,payload_json,status,created_at,updated_at)
             VALUES($1,$2,$3,$4,'queued',$5,$5) ON CONFLICT DO NOTHING",
            params![tenant_id,job_id,operation,&payload_text,now],
        ).map_err(db_error)?;
        let stored = conn
            .query_row(
                "SELECT operation,payload_json,status,result_json,created_at,updated_at
             FROM data_jobs WHERE tenant_id=$1 AND job_id=$2",
                params![tenant_id, job_id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, i64>(4)?,
                        r.get::<_, i64>(5)?,
                    ))
                },
            )
            .map_err(db_error)?;
        if inserted == 0 && (stored.0 != operation || stored.1 != payload_text) {
            return Err(ControlError::Conflict("任务编号已用于其他请求。".into()));
        }
        Ok(
            json!({"id":job_id,"action":stored.0,"payload":parse(&stored.1)?,
            "status":stored.2,"result":stored.3.map(|r|parse(&r)).transpose()?,
            "created_at":stored.4,"updated_at":stored.5}),
        )
    }

    pub fn data_job(&self, tenant_id: &str, job_id: &str) -> Result<Option<Value>, ControlError> {
        let conn = self.lock()?;
        let row = conn
            .query_row(
                "SELECT operation,payload_json,status,result_json,created_at,updated_at
             FROM data_jobs WHERE tenant_id=$1 AND job_id=$2",
                params![tenant_id, job_id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, i64>(4)?,
                        r.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(db_error)?;
        row.map(
            |(operation, payload, status, result, created_at, updated_at)| {
                Ok(
                    json!({"id":job_id,"action":operation,"payload":parse(&payload)?,
                "status":status,"result":result.map(|x|parse(&x)).transpose()?,
                "created_at":created_at,"updated_at":updated_at}),
                )
            },
        )
        .transpose()
    }

    pub fn data_jobs(&self, tenant_id: &str) -> Result<Vec<Value>, ControlError> {
        let conn = self.lock()?;
        let mut stmt=conn.prepare(
            "SELECT job_id FROM data_jobs WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 200"
        ).map_err(db_error)?;
        let rows = stmt
            .query_map(params![tenant_id], |r| r.get::<_, String>(0))
            .map_err(db_error)?;
        let ids = rows.collect::<Result<Vec<_>, _>>().map_err(db_error)?;
        drop(stmt);
        drop(conn);
        ids.iter()
            .map(|id| {
                self.data_job(tenant_id, id)?
                    .ok_or_else(|| ControlError::Conflict("数据任务消失。".into()))
            })
            .collect()
    }

    /// Compare-and-swap: only the owner who claimed queued work may finish it.
    pub fn transition_data_job(
        &self,
        tenant_id: &str,
        job_id: &str,
        from: &str,
        to: &str,
        result: Option<&Value>,
    ) -> Result<bool, ControlError> {
        let conn = self.lock()?;
        let n = conn
            .execute(
                "UPDATE data_jobs SET status=$1,result_json=$2,updated_at=$3
             WHERE tenant_id=$4 AND job_id=$5 AND status=$6",
                params![
                    to,
                    result.map(Value::to_string),
                    now_ms(),
                    tenant_id,
                    job_id,
                    from
                ],
            )
            .map_err(db_error)?;
        Ok(n == 1)
    }


}

impl ControlStore {
    pub fn restore_interrupted_data_jobs(&self) -> Result<usize, ControlError> {
        let conn = self.lock()?;
        let total = conn.execute(
            "UPDATE data_jobs SET status='interrupted', result_json='{\"detail\":\"引擎重启后任务状态未核实，禁止自动重试。\"}', updated_at=$1 WHERE status='running'",
            params![now_ms()],
        ).map_err(db_error)?;
        Ok(total)
    }


}

#[cfg(test)]
mod tests {
    use super::*;
    fn isolated() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("seealpha-control-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()))
    }

    #[test]
    fn data_jobs_recover_after_restart_without_retry() {
        let path = isolated();
        let store = ControlStore::open(&path).unwrap();
        let payload = json!({"symbol":"ETHUSDT"});
        store.queue_data_job("t", "data-1", "backfill", &payload).unwrap();
        assert!(store.queue_data_job("t", "bad", "backtest", &payload).is_err());
        store.transition_data_job("t", "data-1", "queued", "running", None).unwrap();
        drop(store);
        let reopened = ControlStore::open(&path).unwrap();
        assert_eq!(reopened.restore_interrupted_data_jobs().unwrap(), 1);
        assert_eq!(reopened.data_job("t", "data-1").unwrap().unwrap()["status"], "interrupted");
        assert!(reopened.data_jobs("other").unwrap().is_empty());
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }
}
