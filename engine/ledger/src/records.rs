use crate::{Ledger, LedgerError, now_ms};
use seealpha_database::params;

impl Ledger {
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


}

