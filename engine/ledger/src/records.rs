use crate::{Ledger, LedgerError};
use rusqlite::{OptionalExtension, params};

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
                     WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3
                     ORDER BY id DESC LIMIT 1),
                    body_json)
                 FROM order_requests
                 WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3",
                params![tenant_id, account_id, request_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
    }

    /// Legacy request insert, now immutable. Once a request identity has been
    /// stored, use a separate reconciliation record; do not rewrite UNKNOWN.
    pub fn save_order_request(
        &self,
        tenant_id: &str,
        account_id: &str,
        request_id: &str,
        body: &str,
    ) -> Result<(), LedgerError> {
        if tenant_id.is_empty()
            || account_id.is_empty()
            || request_id.is_empty()
            || serde_json::from_str::<serde_json::Value>(body).is_err()
        {
            return Err(LedgerError::Conflict("invalid order request".into()));
        }
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT OR IGNORE INTO order_requests
             (tenant_id,account_id,request_id,body_json,updated_at) VALUES(?1,?2,?3,?4,?5)",
                params![tenant_id, account_id, request_id, body, now_ms()],
            )
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        let original: String=connection.query_row(
            "SELECT body_json FROM order_requests WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3",
            params![tenant_id,account_id,request_id],|r|r.get(0),
        ).map_err(|e|LedgerError::Sqlite(e.to_string()))?;
        if original != body {
            return Err(LedgerError::Conflict(
                "request is immutable; reconcile instead of replay".into(),
            ));
        }
        Ok(())
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
                "SELECT body_json FROM order_requests WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3",
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
                 WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3
                 ORDER BY id DESC LIMIT 1",
                params![tenant_id, account_id, request_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        if latest.as_deref() == Some(body) {
            return Ok(());
        }
        connection
            .execute(
                "INSERT INTO order_request_views
                 (tenant_id,account_id,request_id,body_json,recorded_at)
                 VALUES (?1,?2,?3,?4,?5)",
                params![tenant_id, account_id, request_id, body, now_ms()],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        Ok(())
    }

    /// Preserve the first order row as historical evidence. Updated venue
    /// statuses belong in append_order_observation(), never a blind UPSERT.
    pub fn upsert_order_row(
        &self,
        tenant_id: &str,
        account_id: &str,
        environment: &str,
        client_order_id: &str,
        order_json: &str,
    ) -> Result<(), LedgerError> {
        if serde_json::from_str::<serde_json::Value>(order_json).is_err() {
            return Err(LedgerError::Conflict("invalid order JSON".into()));
        }
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT OR IGNORE INTO orders
             (tenant_id,account_id,environment,client_order_id,order_json,updated_at)
             VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    tenant_id,
                    account_id,
                    environment,
                    client_order_id,
                    order_json,
                    now_ms()
                ],
            )
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        let previous: String = connection
            .query_row(
                "SELECT order_json FROM orders WHERE tenant_id=?1 AND account_id=?2
             AND environment=?3 AND client_order_id=?4",
                params![tenant_id, account_id, environment, client_order_id],
                |r| r.get(0),
            )
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        if previous != order_json {
            return Err(LedgerError::Conflict(
                "order evidence is immutable; append an observation".into(),
            ));
        }
        Ok(())
    }

    /// Legacy raw fill writer is restricted to isolated paper simulation.
    /// Exchange fills MUST use import_verified_fills() with real trade IDs.
    pub fn upsert_fill_row(
        &self,
        tenant_id: &str,
        account_id: &str,
        environment: &str,
        fill_id: &str,
        fill_json: &str,
    ) -> Result<(), LedgerError> {
        if environment != "paper" || serde_json::from_str::<serde_json::Value>(fill_json).is_err() {
            return Err(LedgerError::Conflict(
                "live fills require verified exchange trade evidence".into(),
            ));
        }
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT OR IGNORE INTO fills
             (tenant_id,account_id,environment,fill_id,fill_json,updated_at)
             VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    tenant_id,
                    account_id,
                    environment,
                    fill_id,
                    fill_json,
                    now_ms()
                ],
            )
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        let original: String = connection
            .query_row(
                "SELECT fill_json FROM fills WHERE tenant_id=?1 AND account_id=?2
             AND environment=?3 AND fill_id=?4",
                params![tenant_id, account_id, environment, fill_id],
                |r| r.get(0),
            )
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        if original != fill_json {
            return Err(LedgerError::Conflict("fill evidence is immutable".into()));
        }
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
                "SELECT fill_json FROM fills WHERE tenant_id=?1 AND account_id=?2 AND environment=?3 ORDER BY fill_id",
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        let rows = statement
            .query_map(params![tenant_id, account_id, environment], |row| {
                row.get(0)
            })
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
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
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(tenant_id, account_id, scope, gap_key) DO UPDATE SET
                   gap_json=excluded.gap_json, updated_at=excluded.updated_at",
                params![tenant_id, account_id, scope, gap_key, gap_json, now_ms()],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        Ok(())
    }

    pub fn gaps(&self, tenant_id: &str, account_id: &str) -> Result<Vec<String>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT gap_json FROM gaps WHERE tenant_id=?1 AND account_id=?2 ORDER BY gap_key",
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        let rows = statement
            .query_map(params![tenant_id, account_id], |row| row.get(0))
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
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
                 WHERE tenant_id=?1 AND account_id=?2
                   AND environment IN ('production','sandbox')
                 ORDER BY CASE environment WHEN 'production' THEN 0 ELSE 1 END",
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
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
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
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
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(tenant_id, account_id, environment) DO UPDATE SET
                   status=excluded.status, updated_at=excluded.updated_at",
                params![tenant_id, account_id, environment, status, now_ms()],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
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
                "INSERT INTO events (tenant_id, account_id, kind, body_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![tenant_id, account_id, kind, body, now_ms()],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        Ok(())
    }

    pub fn events(
        &self,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<Vec<(String, String)>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare("SELECT kind, body_json FROM events WHERE tenant_id=?1 AND account_id=?2 ORDER BY id")
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        let rows = statement
            .query_map(params![tenant_id, account_id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
    }

}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0),
    )
    .unwrap_or(0)
}

#[cfg(test)]
mod safety_tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn legacy_mutable_writers_now_preserve_first_evidence() {
        let db = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        db.save_order_request("t", "a", "r", r#"{"status":"UNKNOWN"}"#)
            .unwrap();
        db.save_order_request("t", "a", "r", r#"{"status":"UNKNOWN"}"#)
            .unwrap();
        assert!(
            db.save_order_request("t", "a", "r", r#"{"status":"FILLED"}"#)
                .is_err()
        );
        assert_eq!(
            db.request("t", "a", "r").unwrap().unwrap()["status"],
            "UNKNOWN"
        );
        db.upsert_order_row("t", "a", "production", "cl", r#"{"status":"NEW"}"#)
            .unwrap();
        assert!(
            db.upsert_order_row("t", "a", "production", "cl", r#"{"status":"FILLED"}"#)
                .is_err()
        );
        assert_eq!(db.orders("t", "a", "production").unwrap().len(), 1);
        assert!(
            db.upsert_fill_row("t", "a", "production", "not-a-trade", "{}")
                .is_err()
        );
        db.upsert_fill_row("t", "a", "paper", "f", r#"{"quantity":"1"}"#)
            .unwrap();
        assert!(
            db.upsert_fill_row("t", "a", "paper", "f", r#"{"quantity":"2"}"#)
                .is_err()
        );
        assert_eq!(db.fills("t", "a", "paper").unwrap().len(), 1);
    }

    #[test]
    fn request_replay_reads_appended_result_without_mutating_pre_send_unknown() {
        let db = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        let base = serde_json::json!({
            "fingerprint":"f".repeat(64),
            "action":"submit","symbol":"ETHUSDT-PERP",
            "environment":"production","product":"futures_um",
            "order_list":null,"algo_order":false,
            "response":{"request_id":"r","client_order_id":"c","status":"unknown"},
            "ledger_synced":false
        });
        db.save_order_request("t", "a", "r", &base.to_string())
            .unwrap();
        let mut observed = base.clone();
        observed["response"]["status"] = serde_json::json!("completed");
        db.append_order_request_view("t", "a", "r", &observed.to_string())
            .unwrap();
        assert_eq!(
            db.get_order_request("t", "a", "r").unwrap().unwrap(),
            observed.to_string()
        );
        assert!(
            db.save_order_request("t", "a", "r", &observed.to_string())
                .is_err()
        );
        let mut wrong = observed.clone();
        wrong["fingerprint"] = serde_json::json!("0".repeat(64));
        assert!(
            db.append_order_request_view("t", "a", "r", &wrong.to_string())
                .is_err()
        );
        assert!(db.get_order_request("t", "other", "r").unwrap().is_none());
    }

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
