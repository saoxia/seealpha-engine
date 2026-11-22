use crate::{Ledger, LedgerError, now_ms};
use seealpha_database::{OptionalExtension, params};

impl Ledger {
    pub fn get_order_request(
        &self,
        tenant_id: &str,
        account_id: &str,
        request_id: &str,
    ) -> Result<Option<String>, LedgerError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT COALESCE(
                    (SELECT body_json FROM order_request_views
                     WHERE tenant_id=$1 AND account_id=$2 AND request_id=$3
                     ORDER BY id DESC LIMIT 1),
                    body_json)
                 FROM order_requests
                 WHERE tenant_id=$1 AND account_id=$2 AND request_id=$3",
                params![tenant_id, account_id, request_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| LedgerError::Database(error.to_string()))
    }

    /// Append a durable API projection without changing the immutable pre-send
    /// UNKNOWN request. Only the original request identity may be observed.
    /// This view is not an execution/fill authorization.
    pub fn append_order_request_view(
        &self,
        tenant_id: &str,
        account_id: &str,
        request_id: &str,
        body: &str,
    ) -> Result<(), LedgerError> {
        if tenant_id.is_empty()
            || account_id.is_empty()
            || request_id.is_empty()
            || body.len() > 1024 * 1024
        {
            return Err(LedgerError::Conflict(
                "invalid request observation scope".into(),
            ));
        }
        let next: serde_json::Value = serde_json::from_str(body)
            .map_err(|_| LedgerError::Conflict("invalid request observation JSON".into()))?;
        let connection = self.lock()?;
        let raw: String = connection
            .query_row(
                "SELECT body_json FROM order_requests WHERE tenant_id=$1 AND account_id=$2 AND request_id=$3",
                params![tenant_id, account_id, request_id],
                |row| row.get(0),
            )
            .map_err(|_| LedgerError::Conflict("missing immutable request".into()))?;
        let original: serde_json::Value = serde_json::from_str(&raw)
            .map_err(|_| LedgerError::Conflict("corrupt immutable request".into()))?;
        if next.get("replay").is_some()
            || !matches!(
                next["response"]["status"].as_str(),
                Some("unknown" | "completed")
            )
            || [
                "fingerprint",
                "symbol",
                "action",
                "environment",
                "product",
                "order_list",
                "algo_order",
            ]
            .iter()
            .any(|key| next.get(*key) != original.get(*key))
            || next.pointer("/response/request_id") != original.pointer("/response/request_id")
            || next.pointer("/response/client_order_id")
                != original.pointer("/response/client_order_id")
        {
            return Err(LedgerError::Conflict(
                "request observation cannot rewrite immutable identity".into(),
            ));
        }
        let latest: Option<String> = connection
            .query_row(
                "SELECT body_json FROM order_request_views
                 WHERE tenant_id=$1 AND account_id=$2 AND request_id=$3
                 ORDER BY id DESC LIMIT 1",
                params![tenant_id, account_id, request_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        if latest.as_deref() == Some(body) {
            return Ok(());
        }
        connection
            .execute(
                "INSERT INTO order_request_views
                 (tenant_id,account_id,request_id,body_json,recorded_at)
                 VALUES ($1,$2,$3,$4,$5)",
                params![tenant_id, account_id, request_id, body, now_ms()],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        Ok(())
    }

    pub fn fills(
        &self,
        tenant_id: &str,
        account_id: &str,
        environment: &str,
    ) -> Result<Vec<String>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT fill_json FROM fills WHERE tenant_id=$1 AND account_id=$2 AND environment=$3 ORDER BY fill_id",
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        let rows = statement
            .query_map(params![tenant_id, account_id, environment], |row| {
                row.get(0)
            })
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Database(error.to_string()))
    }

    pub fn put_gap(
        &self,
        tenant_id: &str,
        account_id: &str,
        scope: &str,
        gap_key: &str,
        gap_json: &str,
    ) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO gaps (tenant_id, account_id, scope, gap_key, gap_json, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT(tenant_id, account_id, scope, gap_key) DO UPDATE SET
                   gap_json=excluded.gap_json, updated_at=excluded.updated_at",
                params![tenant_id, account_id, scope, gap_key, gap_json, now_ms()],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        Ok(())
    }

    /// Read the persisted account connection registrations without credentials.
    /// This is a registration record, not evidence of live exchange access.
    pub fn account_connections(
        &self,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<Vec<serde_json::Value>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT tenant_id,account_id,environment,status,updated_at
                 FROM connections
                 WHERE tenant_id=$1 AND account_id=$2
                   AND environment IN ('production','sandbox')
                 ORDER BY CASE environment WHEN 'production' THEN 0 ELSE 1 END",
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        let rows = statement
            .query_map(params![tenant_id, account_id], |row| {
                Ok(serde_json::json!({
                    "tenant_id": row.get::<_, String>(0)?,
                    "account_id": row.get::<_, String>(1)?,
                    "environment": row.get::<_, String>(2)?,
                    "status": row.get::<_, String>(3)?,
                    "updated_at": row.get::<_, i64>(4)?,
                }))
            })
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Database(error.to_string()))
    }

    pub fn upsert_connection(
        &self,
        tenant_id: &str,
        account_id: &str,
        environment: &str,
        status: &str,
    ) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO connections (tenant_id, account_id, environment, status, updated_at)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT(tenant_id, account_id, environment) DO UPDATE SET
                   status=excluded.status, updated_at=excluded.updated_at",
                params![tenant_id, account_id, environment, status, now_ms()],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        Ok(())
    }

    pub fn add_event(
        &self,
        tenant_id: &str,
        account_id: &str,
        kind: &str,
        body: &str,
    ) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO events (tenant_id, account_id, kind, body_json, created_at) VALUES ($1, $2, $3, $4, $5)",
                params![tenant_id, account_id, kind, body, now_ms()],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        Ok(())
    }

    pub fn save_backtest(
        &self,
        tenant_id: &str,
        account_id: &str,
        kind: &str,
        report: &str,
    ) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO backtest_results (tenant_id, account_id, kind, report_json, updated_at)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT(tenant_id, account_id, kind) DO UPDATE SET
                   report_json=excluded.report_json, updated_at=excluded.updated_at",
                params![tenant_id, account_id, kind, report, now_ms()],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        Ok(())
    }

    pub fn latest_backtest(
        &self,
        tenant_id: &str,
        account_id: &str,
        kind: &str,
    ) -> Result<Option<String>, LedgerError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT report_json FROM backtest_results WHERE tenant_id=$1 AND account_id=$2 AND kind=$3",
                params![tenant_id, account_id, kind],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| LedgerError::Database(error.to_string()))
    }

    pub fn save_snapshot(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
        sha256: &str,
        body: &str,
    ) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO research_snapshots (tenant_id, snapshot_id, sha256, body_json, created_at)
                 VALUES ($1, $2, $3, $4, $5)",
                params![tenant_id, snapshot_id, sha256, body, now_ms()],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        Ok(())
    }

    pub fn snapshot(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
    ) -> Result<Option<(String, String)>, LedgerError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT sha256, body_json FROM research_snapshots WHERE tenant_id=$1 AND snapshot_id=$2",
                params![tenant_id, snapshot_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| LedgerError::Database(error.to_string()))
    }
}

#[cfg(test)]
mod safety_tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn restart_retains_unknown_intent_without_new_send_permission() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "seealpha-ledger-reopen-{}-{stamp}.sqlite",
            std::process::id()
        ));
        let intent = serde_json::json!({"action":"NEW","environment":"sandbox",
            "client_order_id":"c","symbol":"ETHUSDT","side":"SELL","quantity":"0.1"});
        {
            let db = Ledger::open(&path).unwrap();
            db.ensure_tenant("t", "development", "active").unwrap();
            assert_eq!(
                db.preregister_order_intent(
                    "t",
                    "a",
                    "sandbox",
                    "r",
                    "c",
                    &"f".repeat(64),
                    &intent
                )
                .unwrap(),
                crate::IntentStart::Fresh
            );
        }
        {
            let db = Ledger::open(&path).unwrap();
            assert_eq!(
                db.preregister_order_intent(
                    "t",
                    "a",
                    "sandbox",
                    "r",
                    "c",
                    &"f".repeat(64),
                    &intent
                )
                .unwrap(),
                crate::IntentStart::ReconcileOnly
            );
            assert_eq!(
                db.request("t", "a", "r").unwrap().unwrap()["status"],
                "UNKNOWN"
            );
            assert!(db.fills("t", "a", "sandbox").unwrap().is_empty());
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
