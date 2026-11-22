//! Minimal trusted tenant scope for strategy and order safety checks.
use super::StateStore;
use seealpha_ledger::LedgerError;

/// Trusted local tenant scope, not an exchange account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tenant {
    pub tenant_id: String,
    pub purpose: String,
    pub status: String,
}
use seealpha_database::{Connection, OptionalExtension, params};

impl StateStore {
    /// Enumerate active tenant scopes for read-only outstanding order recovery.
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_scope_and_status_are_persisted() {
        let store = StateStore::open(std::path::Path::new(":memory:")).unwrap();
        let tenant = store.ensure_tenant("t", "development", "active").unwrap();
        assert_eq!(tenant.tenant_id, "t");
        assert_eq!(tenant.purpose, "development");
        assert!(store.tenant("other").unwrap().is_none());
    }
}
