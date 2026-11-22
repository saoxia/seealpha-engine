//! Durable execution request projections, not economic ledger facts.
use super::StateStore;
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

    pub fn orders(&self, tenant_id: &str, account_id: &str, environment: &str) -> Result<Vec<StoredOrder>, LedgerError> {
        let conn=self.lock()?;
        let mut stmt=conn.prepare("SELECT order_json FROM orders WHERE tenant_id=$1 AND account_id=$2 AND environment=$3 ORDER BY updated_at, client_order_id").map_err(|e| LedgerError::Database(e.to_string()))?;
        let rows=stmt.query_map(params![tenant_id,account_id,environment], |r| Ok(StoredOrder { order_json: r.get(0)? })).map_err(|e| LedgerError::Database(e.to_string()))?;
        rows.collect::<Result<Vec<_>,_>>().map_err(|e| LedgerError::Database(e.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct StoredOrder { pub order_json: String }
