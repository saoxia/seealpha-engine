//! Persisted Rust-side compatibility records for the Python Engine's account,
//! market-data and task endpoints. These are data only: reading a subscription
//! must never start a collector or send an exchange order.
use crate::{Ledger, LedgerError};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};

fn sqlite(err: rusqlite::Error) -> LedgerError {
    LedgerError::Sqlite(err.to_string())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|x| i64::try_from(x.as_millis()).ok())
        .unwrap_or(0)
}

fn parse(text: &str) -> Result<Value, LedgerError> {
    serde_json::from_str(text).map_err(|_| LedgerError::Conflict("存储的 JSON 记录损坏。".into()))
}

impl Ledger {
    pub fn positions(
        &self,
        tenant_id: &str,
        account_id: &str,
        environment: &str,
    ) -> Result<Vec<Value>, LedgerError> {
        let conn = self.lock()?;
        let mut query = conn
            .prepare(
                "SELECT position_json FROM positions WHERE tenant_id=?1 AND account_id=?2
             AND environment=?3 ORDER BY position_id",
            )
            .map_err(sqlite)?;
        let rows = query
            .query_map(params![tenant_id, account_id, environment], |row| {
                row.get::<_, String>(0)
            })
            .map_err(sqlite)?;
        rows.map(|row| parse(&row.map_err(sqlite)?)).collect()
    }

    pub fn request(
        &self,
        tenant_id: &str,
        account_id: &str,
        request_id: &str,
    ) -> Result<Option<Value>, LedgerError> {
        self.get_order_request(tenant_id, account_id, request_id)?
            .map(|value| parse(&value))
            .transpose()
    }

    pub fn gaps_for_scope(
        &self,
        tenant_id: &str,
        account_id: &str,
        scope: Option<&str>,
    ) -> Result<Vec<Value>, LedgerError> {
        let conn = self.lock()?;
        let mut query = conn
            .prepare(
                "SELECT gap_json FROM gaps WHERE tenant_id=?1 AND account_id=?2
             AND (?3 IS NULL OR scope=?3) ORDER BY scope,gap_key",
            )
            .map_err(sqlite)?;
        let rows = query
            .query_map(params![tenant_id, account_id, scope], |r| {
                r.get::<_, String>(0)
            })
            .map_err(sqlite)?;
        rows.map(|r| parse(&r.map_err(sqlite)?)).collect()
    }

    pub fn event_page(
        &self,
        tenant_id: &str,
        account_id: &str,
        after: i64,
        limit: i64,
    ) -> Result<Value, LedgerError> {
        let conn = self.lock()?;
        let mut query = conn
            .prepare(
                "SELECT id,kind,body_json,created_at FROM events
             WHERE tenant_id=?1 AND account_id=?2 AND id>?3 ORDER BY id LIMIT ?4",
            )
            .map_err(sqlite)?;
        let rows = query
            .query_map(
                params![tenant_id, account_id, after, limit.clamp(1, 1000)],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                },
            )
            .map_err(sqlite)?;
        let mut items = Vec::new();
        let mut next = after;
        for row in rows {
            let (id, kind, body, time) = row.map_err(sqlite)?;
            let body = parse(&body)?;
            items.push(json!({"id":id,"kind":kind,"body":body,"created_at":time}));
            next = id;
        }
        Ok(json!({"events":items,"next":next}))
    }

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
    ) -> Result<Value, LedgerError> {
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
    ) -> Result<Vec<Value>, LedgerError> {
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
    pub fn replace_policies(&self, tenant_id: &str, policies: &[Value]) -> Result<(), LedgerError> {
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

    pub fn policies(&self, tenant_id: &str) -> Result<Vec<Value>, LedgerError> {
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
    ) -> Result<Value, LedgerError> {
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
            return Err(LedgerError::Conflict("任务编号已用于其他请求。".into()));
        }
        Ok(
            json!({"id":job_id,"action":stored.0,"payload":parse(&stored.1)?,
            "status":stored.2,"result":stored.3.map(|r|parse(&r)).transpose()?,
            "created_at":stored.4,"updated_at":stored.5}),
        )
    }

    pub fn data_job(&self, tenant_id: &str, job_id: &str) -> Result<Option<Value>, LedgerError> {
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

    pub fn data_jobs(&self, tenant_id: &str) -> Result<Vec<Value>, LedgerError> {
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
                    .ok_or_else(|| LedgerError::Conflict("数据任务消失。".into()))
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
    ) -> Result<bool, LedgerError> {
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

    pub fn worker_settings(&self) -> Result<Value, LedgerError> {
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

    pub fn update_worker_settings(&self, workers: i64, timeout: i64) -> Result<Value, LedgerError> {
        if !(1..=8).contains(&workers) || !(1..=86400).contains(&timeout) {
            return Err(LedgerError::Conflict("回测 Worker 配置超出范围。".into()));
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

impl Ledger {
    /// A verified native import never silently rewrites pre-existing evidence.
    /// Whole-batch insertion rolls back on any conflicting historical bar.
    pub fn import_verified_catalog_bars(
        &self,
        tenant_id: &str,
        product: &str,
        symbol: &str,
        interval: &str,
        rows: &[crate::CatalogBar],
    ) -> Result<usize, LedgerError> {
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite)?;
        for bar in rows {
            let old:Option<(String,String,String,String,String)>=tx.query_row(
                "SELECT open,high,low,close,volume FROM catalog_bars
                 WHERE tenant_id=?1 AND product=?2 AND symbol=?3 AND interval=?4 AND open_time_ms=?5",
                params![tenant_id,product,symbol,interval,bar.open_time_ms],
                |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))
            ).optional().map_err(sqlite)?;
            if let Some(value) = old {
                if value.0.as_str() != bar.open.as_str()
                    || value.1.as_str() != bar.high.as_str()
                    || value.2.as_str() != bar.low.as_str()
                    || value.3.as_str() != bar.close.as_str()
                    || value.4.as_str() != bar.volume.as_str()
                {
                    return Err(LedgerError::Conflict(
                        "历史 K 线与已保存证据冲突，未写入。".into(),
                    ));
                }
                continue;
            }
            tx.execute(
                "INSERT INTO catalog_bars
                 (tenant_id,product,symbol,interval,open_time_ms,open,high,low,close,volume)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![
                    tenant_id,
                    product,
                    symbol,
                    interval,
                    bar.open_time_ms,
                    bar.open.as_str(),
                    bar.high.as_str(),
                    bar.low.as_str(),
                    bar.close.as_str(),
                    bar.volume.as_str()
                ],
            )
            .map_err(sqlite)?;
        }
        tx.commit().map_err(sqlite)?;
        Ok(rows.len())
    }

    pub fn restore_interrupted_data_jobs(&self) -> Result<usize, LedgerError> {
        let conn = self.lock()?;
        conn.execute(
            "UPDATE data_jobs SET status='interrupted',
               result_json='{\"detail\":\"引擎重启后任务状态未核实，禁止自动重试。\"}',
               updated_at=?1 WHERE status='running'",
            params![now_ms()],
        )
        .map_err(sqlite)
    }

    pub fn set_strategy_reader(
        &self,
        tenant_id: &str,
        account_id: &str,
        issuance_id: &str,
        streams: &Value,
        lookback: i64,
    ) -> Result<Value, LedgerError> {
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
    ) -> Result<(), LedgerError> {
        let conn = self.lock()?;
        conn.execute(
            "DELETE FROM strategy_market_subscriptions
             WHERE tenant_id=?1 AND account_id=?2 AND issuance_id=?3",
            params![tenant_id, account_id, issuance_id],
        )
        .map_err(sqlite)?;
        Ok(())
    }

    pub fn strategy_readers(&self, tenant_id: &str) -> Result<Vec<Value>, LedgerError> {
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
    fn book() -> Ledger {
        let stamp = now_ms();
        let path = std::env::temp_dir().join(format!(
            "seealpha-parity-{}-{}.db",
            std::process::id(),
            stamp
        ));
        Ledger::open(&path).unwrap()
    }
    #[test]
    fn subscription_is_a_status_record_not_a_collector_start() {
        let ledger = book();
        ledger
            .record_subscription(
                "t",
                "a",
                "spot",
                "ETHUSDT",
                "collection",
                "running",
                &json!({"source":"test"}),
            )
            .unwrap();
        let rows = ledger.subscriptions("t", "a").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["progress"]["source"], "test");
        assert!(ledger.subscriptions("other", "a").unwrap().is_empty());
    }
    #[test]
    fn jobs_are_durable_idempotent_and_cas_cancelled() {
        let ledger = book();
        let body = json!({"action":"backfill","symbol":"ETHUSDT"});
        ledger.queue_data_job("t", "j", "backfill", &body).unwrap();
        ledger.queue_data_job("t", "j", "backfill", &body).unwrap();
        assert!(ledger.queue_data_job("t", "j", "trim", &body).is_err());
        assert!(
            ledger
                .transition_data_job("t", "j", "queued", "cancelled", None)
                .unwrap()
        );
        assert!(
            !ledger
                .transition_data_job("t", "j", "queued", "running", None)
                .unwrap()
        );
        assert_eq!(
            ledger.data_job("t", "j").unwrap().unwrap()["status"],
            "cancelled"
        );
    }
    #[test]
    fn policies_are_tenant_isolated() {
        let ledger = book();
        ledger
            .replace_policies(
                "t",
                &[json!({"product":"spot","symbol":"ETHUSDT","enabled":true})],
            )
            .unwrap();
        assert_eq!(ledger.policies("t").unwrap().len(), 1);
        assert!(ledger.policies("other").unwrap().is_empty());
    }
}
