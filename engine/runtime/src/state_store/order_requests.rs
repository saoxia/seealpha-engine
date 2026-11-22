//! Durable execution request projections, not economic ledger facts.
use super::{StateStore, now_ms};
use seealpha_ledger::LedgerError;
use seealpha_database::{OptionalExtension, params};

impl StateStore {
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

    pub fn orders(&self, tenant_id: &str, account_id: &str, environment: &str) -> Result<Vec<StoredOrder>, LedgerError> {
        let conn=self.lock()?;
        let mut stmt=conn.prepare("SELECT order_json FROM orders WHERE tenant_id=$1 AND account_id=$2 AND environment=$3 ORDER BY updated_at, client_order_id").map_err(|e| LedgerError::Database(e.to_string()))?;
        let rows=stmt.query_map(params![tenant_id,account_id,environment], |r| Ok(StoredOrder { order_json: r.get(0)? })).map_err(|e| LedgerError::Database(e.to_string()))?;
        rows.collect::<Result<Vec<_>,_>>().map_err(|e| LedgerError::Database(e.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct StoredOrder { pub order_json: String }
