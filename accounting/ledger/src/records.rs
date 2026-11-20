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
                "SELECT body_json FROM order_requests WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3",
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

    pub fn import_bars(
        &self,
        tenant_id: &str,
        product: &str,
        symbol: &str,
        interval: &str,
        rows: &[CatalogBar],
    ) -> Result<usize, LedgerError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        for row in rows {
            transaction
                .execute(
                    "INSERT INTO catalog_bars (
                        tenant_id, product, symbol, interval, open_time_ms, open, high, low, close, volume
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                     ON CONFLICT(tenant_id, product, symbol, interval, open_time_ms) DO UPDATE SET
                       open=excluded.open, high=excluded.high, low=excluded.low,
                       close=excluded.close, volume=excluded.volume",
                    params![
                        tenant_id, product, symbol, interval, row.open_time_ms, row.open, row.high, row.low, row.close, row.volume
                    ],
                )
                .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        }
        transaction
            .commit()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        Ok(rows.len())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn catalog_bars(
        &self,
        tenant_id: &str,
        product: &str,
        symbol: &str,
        interval: &str,
        start_ms: i64,
        end_ms: i64,
        limit: i64,
    ) -> Result<Vec<CatalogBar>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT open_time_ms, open, high, low, close, volume FROM catalog_bars
                 WHERE tenant_id=?1 AND product=?2 AND symbol=?3 AND interval=?4
                   AND open_time_ms>=?5 AND open_time_ms<?6
                 ORDER BY open_time_ms LIMIT ?7",
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        let rows = statement
            .query_map(
                params![
                    tenant_id, product, symbol, interval, start_ms, end_ms, limit
                ],
                |row| {
                    Ok(CatalogBar {
                        open_time_ms: row.get(0)?,
                        open: row.get(1)?,
                        high: row.get(2)?,
                        low: row.get(3)?,
                        close: row.get(4)?,
                        volume: row.get(5)?,
                    })
                },
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
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
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(tenant_id, account_id, kind) DO UPDATE SET
                   report_json=excluded.report_json, updated_at=excluded.updated_at",
                params![tenant_id, account_id, kind, report, now_ms()],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
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
                "SELECT report_json FROM backtest_results WHERE tenant_id=?1 AND account_id=?2 AND kind=?3",
                params![tenant_id, account_id, kind],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
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
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![tenant_id, snapshot_id, sha256, body, now_ms()],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
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
                "SELECT sha256, body_json FROM research_snapshots WHERE tenant_id=?1 AND snapshot_id=?2",
                params![tenant_id, snapshot_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogBar {
    pub open_time_ms: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
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
