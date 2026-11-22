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
CREATE TABLE IF NOT EXISTS control_migrations(version INTEGER PRIMARY KEY);
"#;
impl ControlStore {
 pub fn open(path:&Path,old:&Path)->Result<Self,ControlError>{
  let mut c=Connection::open(path).map_err(sqlite)?;
  c.execute_batch("PRAGMA journal_mode=DELETE;").map_err(sqlite)?;
  c.execute_batch(SCHEMA).map_err(sqlite)?;
  let migrated:bool=c.query_row("SELECT 1 FROM control_migrations WHERE version=1",[],|r|r.get::<_,i64>(0)).optional().map_err(sqlite)?.is_some();
  if old.exists() && !migrated {
   c.execute("ATTACH DATABASE ?1 AS legacy",params![old.to_string_lossy().as_ref()]).map_err(sqlite)?;
   {
    let tx=c.transaction_with_behavior(TransactionBehavior::Immediate).map_err(sqlite)?;
    for (table,cols) in [
      ("subscriptions","tenant_id,account_id,product,symbol,role,state,progress_json,updated_at"),
      ("data_policies","tenant_id,product,symbol,policy_json,updated_at"),
      ("strategy_market_subscriptions","tenant_id,account_id,issuance_id,streams_json,lookback_bars,updated_at"),
    ] {
      let exists=tx.query_row("SELECT 1 FROM legacy.sqlite_master WHERE type='table' AND name=?1",params![table],|r|r.get::<_,i64>(0)).optional().map_err(sqlite)?.is_some();
      if exists {
       tx.execute(&format!("INSERT OR IGNORE INTO main.{table} ({cols}) SELECT {cols} FROM legacy.{table}"),[]).map_err(sqlite)?;
       // Reject a conflicting record instead of silently overwriting migrated values.
       let mismatch=format!("SELECT 1 FROM (SELECT {cols} FROM legacy.{table} EXCEPT SELECT {cols} FROM main.{table}) LIMIT 1");
       if tx.query_row(&mismatch,[],|r|r.get::<_,i64>(0)).optional().map_err(sqlite)?.is_some(){
        return Err(ControlError::Conflict("旧控制状态与新存储冲突，拒绝启动".into()));
       }
      }
    }
    let jobs_exist=tx.query_row("SELECT 1 FROM legacy.sqlite_master WHERE type='table' AND name='data_jobs'",[],|r|r.get::<_,i64>(0)).optional().map_err(sqlite)?.is_some();
    if jobs_exist {
      let cols="tenant_id,job_id,operation,payload_json,status,result_json,created_at,updated_at";
      for (target,where_clause) in [("data_jobs","operation <> 'backtest'"),("backtest_jobs","operation = 'backtest'")] {
       tx.execute(&format!("INSERT OR IGNORE INTO main.{target} ({cols}) SELECT {cols} FROM legacy.data_jobs WHERE {where_clause}"),[]).map_err(sqlite)?;
       let mismatch=format!("SELECT 1 FROM (SELECT {cols} FROM legacy.data_jobs WHERE {where_clause} EXCEPT SELECT {cols} FROM main.{target}) LIMIT 1");
       if tx.query_row(&mismatch,[],|r|r.get::<_,i64>(0)).optional().map_err(sqlite)?.is_some(){return Err(ControlError::Conflict("旧任务状态冲突，拒绝启动".into()));}
      }
    }
    let settings=tx.query_row("SELECT 1 FROM legacy.sqlite_master WHERE type='table' AND name='worker_settings'",[],|r|r.get::<_,i64>(0)).optional().map_err(sqlite)?.is_some();
    if settings {
      let old_values:Option<(i64,i64)>=tx.query_row("SELECT max_workers,timeout_seconds FROM legacy.worker_settings WHERE singleton_id=1",[],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(sqlite)?;
      if let Some((workers,timeout))=old_values {
        // Preserve a user-modified runtime setting; only replace the pristine default.
        tx.execute("UPDATE main.worker_settings SET max_workers=?1,timeout_seconds=?2 WHERE singleton_id=1 AND max_workers=1 AND timeout_seconds=3600",params![workers,timeout]).map_err(sqlite)?;
      }
    }
    tx.execute("INSERT INTO control_migrations(version) VALUES(1)",[]).map_err(sqlite)?;
    tx.commit().map_err(sqlite)?;
   }
   c.execute_batch("DETACH DATABASE legacy;").map_err(sqlite)?;
  }
  Ok(Self{conn:Mutex::new(c)})
 }
 fn lock(&self)->Result<std::sync::MutexGuard<'_,Connection>,ControlError>{self.conn.lock().map_err(|_|ControlError::Conflict("控制状态锁不可用".into()))}
}
impl ControlStore {
    // Keep the legacy subscription record fields explicit until v2 contract parity is verified.
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
