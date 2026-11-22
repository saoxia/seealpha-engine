//! Account connection registration metadata only; live balances and positions belong to Nautilus.
use super::{StateStore, now_ms};
use seealpha_ledger::LedgerError;
use seealpha_database::params;

impl StateStore {
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




}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn connection_registry_is_account_and_tenant_scoped() {
        let db = StateStore::open(std::path::Path::new(":memory:")).unwrap();
        db.ensure_tenant("t", "production", "active").unwrap();
        db.ensure_tenant("other", "production", "active").unwrap();
        db.upsert_connection("t", "a", "sandbox", "closed").unwrap();
        assert_eq!(db.account_connections("t", "a").unwrap().len(), 1);
        assert!(db.account_connections("other", "a").unwrap().is_empty());
        assert!(db.account_connections("t", "other").unwrap().is_empty());
    }
}
