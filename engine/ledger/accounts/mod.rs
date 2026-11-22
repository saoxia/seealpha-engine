//! Accounts — tenant-scoped registration view, not an exchange balance cache.
//! Trading balances and positions are authoritative only in Nautilus.
use crate::{Ledger, LedgerError};
use serde_json::{Value, json};

impl Ledger {
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
