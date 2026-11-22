//! Accounts — tenant-scoped registration view, not an exchange balance cache.
//! Trading balances and positions are authoritative only in Nautilus.
use crate::{Ledger, LedgerError, now_ms};

/// Local registration metadata synchronized from Platform, not user ownership or authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tenant {
    pub tenant_id: String,
    pub purpose: String,
    pub status: String,
}
use seealpha_database::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

impl Ledger {
    /// Enumerate active tenant scopes for safe recovery of never-started jobs.
    /// This does not grant a new execution or exchange order permission.
    pub fn active_tenant_ids(&self) -> Result<Vec<String>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare("SELECT tenant_id FROM tenants WHERE status='active' ORDER BY tenant_id")
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        let rows = statement
            .query_map(&[], |row| row.get::<_, String>(0))
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        let tenants = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        Ok(tenants)
    }

    pub fn ensure_tenant(
        &self,
        tenant_id: &str,
        purpose: &str,
        status: &str,
    ) -> Result<Tenant, LedgerError> {
        if !matches!(purpose, "production" | "development")
            || !matches!(status, "active" | "archived")
        {
            return Err(LedgerError::Conflict("租户类型或状态无效。".into()));
        }
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO tenants (tenant_id, purpose, status) VALUES ($1, $2, $3)
                 ON CONFLICT(tenant_id) DO UPDATE SET purpose=excluded.purpose, status=excluded.status",
                params![tenant_id, purpose, status],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        self.tenant_with(&connection, tenant_id)?
            .ok_or_else(|| LedgerError::Database("missing row".into()))
    }

    pub fn tenant(&self, tenant_id: &str) -> Result<Option<Tenant>, LedgerError> {
        let connection = self.lock()?;
        self.tenant_with(&connection, tenant_id)
    }

    fn tenant_with(
        &self,
        connection: &Connection,
        tenant_id: &str,
    ) -> Result<Option<Tenant>, LedgerError> {
        connection
            .query_row(
                "SELECT tenant_id, purpose, status FROM tenants WHERE tenant_id=$1",
                params![tenant_id],
                |row| {
                    Ok(Tenant {
                        tenant_id: row.get(0)?,
                        purpose: row.get(1)?,
                        status: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(|error| LedgerError::Database(error.to_string()))
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



    /// Returns registered account connection metadata only. Does not certify
    /// the exchange account identity, connectivity, balance or send permission.
    pub fn account_registry_view(&self, tenant: &str, account: &str) -> Result<Value, LedgerError> {
        if tenant.is_empty() || account.is_empty() {
            return Err(LedgerError::Conflict("invalid account scope".into()));
        }
        let registered_tenant = self.tenant(tenant)?
            .ok_or_else(|| LedgerError::Conflict("tenant not registered".into()))?;
        let connections = self.account_connections(tenant, account)?;
        Ok(json!({
            "tenant_id": tenant,
            "account_id": account,
            "tenant_status": registered_tenant.status,
            "connections": connections,
            "live_account_verified": false,
            "balance_source": "nautilus_only",
            "execution_authorized": false
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registry_is_tenant_scoped_and_never_claims_live_evidence() {
        let db = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        db.ensure_tenant("t", "production", "active").unwrap();
        db.ensure_tenant("other", "production", "active").unwrap();
        db.upsert_connection("t", "a", "sandbox", "closed").unwrap();
        let view = db.account_registry_view("t", "a").unwrap();
        assert_eq!(view["connections"].as_array().unwrap().len(), 1);
        assert_eq!(view["live_account_verified"], false);
        assert_eq!(view["execution_authorized"], false);
        assert!(db.account_registry_view("other", "a").unwrap()["connections"].as_array().unwrap().is_empty());
        assert!(db.account_registry_view("unknown", "a").is_err());
    }
}
