//! Durable non-financial Pipeline and research control-plane state.
use rusqlite::{Connection,OptionalExtension,TransactionBehavior,params};
use serde_json::{Value,json};
use std::{path::Path,sync::Mutex,time::{SystemTime,UNIX_EPOCH}};
use thiserror::Error;
#[derive(Debug,Error)]
pub enum ControlError {
    #[error("控制状态库不可用：{0}")] Sqlite(String),
    #[error("{0}")] Conflict(String),
}
fn sqlite(e:rusqlite::Error)->ControlError{ControlError::Sqlite(e.to_string())}
fn now_ms()->i64{SystemTime::now().duration_since(UNIX_EPOCH).ok().and_then(|d|i64::try_from(d.as_millis()).ok()).unwrap_or(0)}
fn parse(s:&str)->Result<Value,ControlError>{serde_json::from_str(s).map_err(|_|ControlError::Conflict("控制记录 JSON 损坏".into()))}
pub struct ControlStore { conn:Mutex<Connection> }
const SCHEMA:&str=r#"
CREATE TABLE IF NOT EXISTS subscriptions(tenant_id TEXT NOT NULL,account_id TEXT NOT NULL,product TEXT NOT NULL,symbol TEXT NOT NULL,role TEXT NOT NULL,state TEXT NOT NULL,progress_json TEXT NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(tenant_id,account_id,product,symbol,role));
CREATE TABLE IF NOT EXISTS data_policies(tenant_id TEXT NOT NULL,product TEXT NOT NULL,symbol TEXT NOT NULL,policy_json TEXT NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(tenant_id,product,symbol));
CREATE TABLE IF NOT EXISTS strategy_market_subscriptions(tenant_id TEXT NOT NULL,account_id TEXT NOT NULL,issuance_id TEXT NOT NULL,streams_json TEXT NOT NULL,lookback_bars INTEGER NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(tenant_id,account_id,issuance_id));
CREATE TABLE IF NOT EXISTS data_jobs(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,operation TEXT NOT NULL,payload_json TEXT NOT NULL,status TEXT NOT NULL,result_json TEXT,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(tenant_id,job_id));
CREATE INDEX IF NOT EXISTS data_jobs_status ON data_jobs(tenant_id,status,updated_at);
CREATE TABLE IF NOT EXISTS backtest_jobs(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,operation TEXT NOT NULL,payload_json TEXT NOT NULL,status TEXT NOT NULL,result_json TEXT,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(tenant_id,job_id));
CREATE TABLE IF NOT EXISTS worker_settings(singleton_id INTEGER PRIMARY KEY CHECK(singleton_id=1),max_workers INTEGER NOT NULL,timeout_seconds INTEGER NOT NULL);
INSERT OR IGNORE INTO worker_settings(singleton_id,max_workers,timeout_seconds) VALUES(1,1,3600);
"#;
impl ControlStore {
    /// Current control state only: no legacy book import or migration tracking.
    pub fn open(path: &Path) -> Result<Self, ControlError> {
        let connection = Connection::open(path).map_err(sqlite)?;
        connection.execute_batch("PRAGMA journal_mode=DELETE;").map_err(sqlite)?;
        connection.execute_batch(SCHEMA).map_err(sqlite)?;
        Ok(Self { conn: Mutex::new(connection) })
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, ControlError> {
        self.conn.lock().map_err(|_| ControlError::Conflict("控制状态锁不可用".into()))
    }
}

impl ControlStore {
    #[allow(clippy::too_many_arguments)]
    pub fn record_subscription(
        &self,
        tenant_id: &str,
        account_id: &str,
        product: &str,
        symbol: &str,
        role: &str,
        state: &str,
        progress: &Value,
    ) -> Result<Value, ControlError> {
        let conn = self.lock()?;
        let stamp = now_ms();
        conn.execute(
            "INSERT INTO subscriptions(tenant_id,account_id,product,symbol,role,state,progress_json,updated_at)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
             ON CONFLICT(tenant_id,account_id,product,symbol,role)
             DO UPDATE SET state=excluded.state,progress_json=excluded.progress_json,
             updated_at=excluded.updated_at",
            params![tenant_id,account_id,product,symbol,role,state,progress.to_string(),stamp],
        ).map_err(sqlite)?;
        Ok(
            json!({"tenant_id":tenant_id,"account_id":account_id,"product":product,
            "symbol":symbol,"role":role,"state":state}),
        )
    }

    pub fn subscriptions(
        &self,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<Vec<Value>, ControlError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT product,symbol,role,state,progress_json,updated_at FROM subscriptions
             WHERE tenant_id=?1 AND account_id=?2 ORDER BY product,symbol,role",
            )
            .map_err(sqlite)?;
        let rows = stmt
            .query_map(params![tenant_id, account_id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, i64>(5)?,
                ))
            })
            .map_err(sqlite)?;
        let mut out = Vec::new();
        for row in rows {
            let (product, symbol, role, state, progress, updated_at) = row.map_err(sqlite)?;
            out.push(
                json!({"tenant_id":tenant_id,"account_id":account_id,"product":product,
                "symbol":symbol,"role":role,"state":state,"progress":parse(&progress)?,
                "updated_at":updated_at}),
            );
        }
        Ok(out)
    }

    /// Atomic replacement of the tenant's declared producer policies.
    /// Existing readers and order state are untouched.
    pub fn replace_policies(&self, tenant_id: &str, policies: &[Value]) -> Result<(), ControlError> {
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite)?;
        tx.execute(
            "DELETE FROM data_policies WHERE tenant_id=?1",
            params![tenant_id],
        )
        .map_err(sqlite)?;
        for policy in policies {
            let product = policy["product"].as_str().unwrap_or("");
            let symbol = policy["symbol"].as_str().unwrap_or("");
            tx.execute(
                "INSERT INTO data_policies(tenant_id,product,symbol,policy_json,updated_at)
                 VALUES(?1,?2,?3,?4,?5)",
                params![tenant_id, product, symbol, policy.to_string(), now_ms()],
            )
            .map_err(sqlite)?;
        }
        tx.commit().map_err(sqlite)
    }

    pub fn policies(&self, tenant_id: &str) -> Result<Vec<Value>, ControlError> {
        let conn = self.lock()?;
        let mut q = conn
            .prepare(
                "SELECT policy_json FROM data_policies WHERE tenant_id=?1 ORDER BY product,symbol",
            )
            .map_err(sqlite)?;
        let rows = q
            .query_map(params![tenant_id], |r| r.get::<_, String>(0))
            .map_err(sqlite)?;
        rows.map(|r| parse(&r.map_err(sqlite)?)).collect()
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
            "INSERT OR IGNORE INTO data_jobs(tenant_id,job_id,operation,payload_json,status,created_at,updated_at)
             VALUES(?1,?2,?3,?4,'queued',?5,?5)",
            params![tenant_id,job_id,operation,&payload_text,now],
        ).map_err(sqlite)?;
        let stored = conn
            .query_row(
                "SELECT operation,payload_json,status,result_json,created_at,updated_at
             FROM data_jobs WHERE tenant_id=?1 AND job_id=?2",
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
            .map_err(sqlite)?;
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
             FROM data_jobs WHERE tenant_id=?1 AND job_id=?2",
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
            .map_err(sqlite)?;
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
            "SELECT job_id FROM data_jobs WHERE tenant_id=?1 ORDER BY created_at DESC LIMIT 200"
        ).map_err(sqlite)?;
        let rows = stmt
            .query_map(params![tenant_id], |r| r.get::<_, String>(0))
            .map_err(sqlite)?;
        let ids = rows.collect::<Result<Vec<_>, _>>().map_err(sqlite)?;
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
                "UPDATE data_jobs SET status=?1,result_json=?2,updated_at=?3
             WHERE tenant_id=?4 AND job_id=?5 AND status=?6",
                params![
                    to,
                    result.map(Value::to_string),
                    now_ms(),
                    tenant_id,
                    job_id,
                    from
                ],
            )
            .map_err(sqlite)?;
        Ok(n == 1)
    }

    pub fn queue_backtest_job(
        &self,
        tenant_id: &str,
        job_id: &str,
        operation: &str,
        payload: &Value,
    ) -> Result<Value, ControlError> {
        if operation != "backtest" { return Err(ControlError::Conflict("非回测任务".into())); }
        let conn = self.lock()?;
        let now = now_ms();
        let payload_text = payload.to_string();
        let inserted=conn.execute(
            "INSERT OR IGNORE INTO backtest_jobs(tenant_id,job_id,operation,payload_json,status,created_at,updated_at)
             VALUES(?1,?2,?3,?4,'queued',?5,?5)",
            params![tenant_id,job_id,operation,&payload_text,now],
        ).map_err(sqlite)?;
        let stored = conn
            .query_row(
                "SELECT operation,payload_json,status,result_json,created_at,updated_at
             FROM backtest_jobs WHERE tenant_id=?1 AND job_id=?2",
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
            .map_err(sqlite)?;
        if inserted == 0 && (stored.0 != operation || stored.1 != payload_text) {
            return Err(ControlError::Conflict("任务编号已用于其他请求。".into()));
        }
        Ok(
            json!({"id":job_id,"action":stored.0,"payload":parse(&stored.1)?,
            "status":stored.2,"result":stored.3.map(|r|parse(&r)).transpose()?,
            "created_at":stored.4,"updated_at":stored.5}),
        )
    }

    pub fn backtest_job(&self, tenant_id: &str, job_id: &str) -> Result<Option<Value>, ControlError> {
        let conn = self.lock()?;
        let row = conn
            .query_row(
                "SELECT operation,payload_json,status,result_json,created_at,updated_at
             FROM backtest_jobs WHERE tenant_id=?1 AND job_id=?2",
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
            .map_err(sqlite)?;
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

    pub fn backtest_jobs(&self, tenant_id: &str) -> Result<Vec<Value>, ControlError> {
        let conn = self.lock()?;
        let mut stmt=conn.prepare(
            "SELECT job_id FROM backtest_jobs WHERE tenant_id=?1 ORDER BY created_at DESC LIMIT 200"
        ).map_err(sqlite)?;
        let rows = stmt
            .query_map(params![tenant_id], |r| r.get::<_, String>(0))
            .map_err(sqlite)?;
        let ids = rows.collect::<Result<Vec<_>, _>>().map_err(sqlite)?;
        drop(stmt);
        drop(conn);
        ids.iter()
            .map(|id| {
                self.backtest_job(tenant_id, id)?
                    .ok_or_else(|| ControlError::Conflict("数据任务消失。".into()))
            })
            .collect()
    }

    /// Compare-and-swap: only the owner who claimed queued work may finish it.
    pub fn transition_backtest_job(
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
                "UPDATE backtest_jobs SET status=?1,result_json=?2,updated_at=?3
             WHERE tenant_id=?4 AND job_id=?5 AND status=?6",
                params![
                    to,
                    result.map(Value::to_string),
                    now_ms(),
                    tenant_id,
                    job_id,
                    from
                ],
            )
            .map_err(sqlite)?;
        Ok(n == 1)
    }

    pub fn worker_settings(&self) -> Result<Value, ControlError> {
        let conn = self.lock()?;
        let (workers, timeout): (i64, i64) = conn
            .query_row(
                "SELECT max_workers,timeout_seconds FROM worker_settings WHERE singleton_id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(sqlite)?;
        Ok(json!({"max_workers":workers,"timeout_seconds":timeout}))
    }

    pub fn update_worker_settings(&self, workers: i64, timeout: i64) -> Result<Value, ControlError> {
        if !(1..=8).contains(&workers) || !(1..=86400).contains(&timeout) {
            return Err(ControlError::Conflict("回测 Worker 配置超出范围。".into()));
        }
        let conn = self.lock()?;
        conn.execute(
            "UPDATE worker_settings SET max_workers=?1,timeout_seconds=?2 WHERE singleton_id=1",
            params![workers, timeout],
        )
        .map_err(sqlite)?;
        Ok(json!({"max_workers":workers,"timeout_seconds":timeout}))
    }
}

impl ControlStore {
    pub fn restore_interrupted_data_jobs(&self) -> Result<usize, ControlError> {
        let conn = self.lock()?;
        let mut total=0;
        for table in ["data_jobs", "backtest_jobs"] {
            total+=conn.execute(&format!("UPDATE {table} SET status='interrupted', result_json='{{\"detail\":\"引擎重启后任务状态未核实，禁止自动重试。\"}}', updated_at=?1 WHERE status='running'"),params![now_ms()]).map_err(sqlite)?;
        }
        Ok(total)
    }

    pub fn set_strategy_reader(
        &self,
        tenant_id: &str,
        account_id: &str,
        issuance_id: &str,
        streams: &Value,
        lookback: i64,
    ) -> Result<Value, ControlError> {
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO strategy_market_subscriptions
             (tenant_id,account_id,issuance_id,streams_json,lookback_bars,updated_at)
             VALUES(?1,?2,?3,?4,?5,?6)
             ON CONFLICT(tenant_id,account_id,issuance_id)
             DO UPDATE SET streams_json=excluded.streams_json,
               lookback_bars=excluded.lookback_bars,updated_at=excluded.updated_at",
            params![
                tenant_id,
                account_id,
                issuance_id,
                streams.to_string(),
                lookback,
                now_ms()
            ],
        )
        .map_err(sqlite)?;
        Ok(json!({"tenant_id":tenant_id,"account_id":account_id,
            "issuance_id":issuance_id,"streams":streams,"lookback_bars":lookback}))
    }

    pub fn remove_strategy_reader(
        &self,
        tenant_id: &str,
        account_id: &str,
        issuance_id: &str,
    ) -> Result<(), ControlError> {
        let conn = self.lock()?;
        conn.execute(
            "DELETE FROM strategy_market_subscriptions
             WHERE tenant_id=?1 AND account_id=?2 AND issuance_id=?3",
            params![tenant_id, account_id, issuance_id],
        )
        .map_err(sqlite)?;
        Ok(())
    }

    pub fn strategy_readers(&self, tenant_id: &str) -> Result<Vec<Value>, ControlError> {
        let conn = self.lock()?;
        let mut stmt=conn.prepare(
            "SELECT account_id,issuance_id,streams_json,lookback_bars FROM strategy_market_subscriptions
             WHERE tenant_id=?1 ORDER BY account_id,issuance_id"
        ).map_err(sqlite)?;
        let rows = stmt
            .query_map(params![tenant_id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            })
            .map_err(sqlite)?;
        let mut result = Vec::new();
        for row in rows {
            let (account, issuance, streams, lookback) = row.map_err(sqlite)?;
            result.push(
                json!({"tenant_id":tenant_id,"account_id":account,"issuance_id":issuance,
                "streams":parse(&streams)?,"lookback_bars":lookback}),
            );
        }
        Ok(result)
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
    fn pipeline_and_backtest_jobs_have_disjoint_queues_and_recover_safely() {
        let path=isolated();
        let store=ControlStore::open(&path).unwrap();
        let payload=json!({"symbol":"ETHUSDT"});
        store.queue_data_job("t","data-1","backfill",&payload).unwrap();
        store.queue_backtest_job("t","test-1","backtest",&payload).unwrap();
        assert!(store.queue_data_job("t","bad","backtest",&payload).is_err());
        assert!(store.queue_backtest_job("t","bad","backfill",&payload).is_err());
        assert_eq!(store.data_jobs("t").unwrap().len(),1);
        assert_eq!(store.backtest_jobs("t").unwrap().len(),1);
        store.transition_data_job("t","data-1","queued","running",None).unwrap();
        store.transition_backtest_job("t","test-1","queued","running",None).unwrap();
        drop(store);
        let reopened=ControlStore::open(&path).unwrap();
        assert_eq!(reopened.restore_interrupted_data_jobs().unwrap(),2);
        assert_eq!(reopened.data_job("t","data-1").unwrap().unwrap()["status"],"interrupted");
        assert_eq!(reopened.backtest_job("t","test-1").unwrap().unwrap()["status"],"interrupted");
        assert!(reopened.data_jobs("other").unwrap().is_empty());
        drop(reopened);
        let _=std::fs::remove_file(path);
    }

}
