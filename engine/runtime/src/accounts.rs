//! Native-only account API boundary.
//!
//! The legacy Binance REST client has been removed. Credentials can be
//! registered for a future Nautilus native execution actor, but registration
//! is NOT evidence of connectivity or a trading permission. Until the native
//! account/report subscriber is wired, evidence-dependent methods fail closed.
use crate::{Runtime, RuntimeError, map_ledger};
use seealpha_portfolio::basket::Product;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

#[derive(Clone)]
struct NativeCredentials {
    api_key: String,
    secret: String,
}
type SecretMap = HashMap<(String, String, String), NativeCredentials>;

fn secrets() -> &'static Mutex<SecretMap> {
    static SECRETS: OnceLock<Mutex<SecretMap>> = OnceLock::new();
    SECRETS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn identity(tenant: &str, account: &str, environment: &str) -> (String, String, String) {
    (tenant.to_owned(), account.to_owned(), environment.to_owned())
}

fn validate_secret(api_key: &str, secret: &str) -> Result<(), RuntimeError> {
    if api_key.is_empty() || secret.is_empty() || api_key == secret
        || api_key.contains(['\n', '\r']) || secret.contains(['\n', '\r'])
    {
        return Err(RuntimeError::Conflict("交易账户凭据无效。".into()));
    }
    Ok(())
}

fn native_evidence_unavailable() -> RuntimeError {
    RuntimeError::Conflict(
        "Nautilus 原生账户及成交证据读取尚未接入，拒绝使用已删除的 REST 连接器。".into(),
    )
}

impl Runtime {
    pub fn register_connection(
        &self, tenant_id: &str, account_id: &str, environment: &str,
        api_key: &str, secret: &str,
    ) -> Result<Value, RuntimeError> {
        let tenant = self.require_active(tenant_id)?;
        if tenant.purpose != "production" {
            return Err(RuntimeError::Conflict("开发租户不能登记交易账户。".into()));
        }
        if !matches!(environment, "production" | "sandbox") {
            return Err(RuntimeError::Unprocessable("账户环境无效。".into()));
        }
        validate_secret(api_key, secret)?;
        secrets().lock()
            .map_err(|_| RuntimeError::Conflict("账户凭据锁不可用。".into()))?
            .insert(identity(tenant_id, account_id, environment), NativeCredentials {
                api_key: api_key.into(), secret: secret.into(),
            });
        // "registered" means an in-process secret exists; NEVER "connected".
        self.ledger.upsert_connection(
            tenant_id, account_id, environment, "registered",
        ).map_err(map_ledger)?;
        self.ledger.add_event(
            tenant_id, account_id, "connection",
            &json!({"environment":environment,"status":"registered",
                "backend":"nautilus_binance_native","connected":false}).to_string(),
        ).map_err(map_ledger)?;
        Ok(json!({
            "tenant_id":tenant_id,"account_id":account_id,
            "environment":environment,"status":"registered",
            "execution_backend":"nautilus_binance_native","connected":false,
        }))
    }

    pub fn forget_connection(
        &self, tenant_id: &str, account_id: &str, environment: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        if !matches!(environment, "production" | "sandbox") {
            return Err(RuntimeError::Unprocessable("账户环境无效。".into()));
        }
        secrets().lock()
            .map_err(|_| RuntimeError::Conflict("账户凭据锁不可用。".into()))?
            .remove(&identity(tenant_id, account_id, environment));
        self.ledger.upsert_connection(
            tenant_id, account_id, environment, "closed",
        ).map_err(map_ledger)?;
        self.ledger.add_event(
            tenant_id, account_id, "connection",
            &json!({"environment":environment,"status":"closed"}).to_string(),
        ).map_err(map_ledger)?;
        Ok(json!({"tenant_id":tenant_id,"account_id":account_id,
            "environment":environment,"status":"closed"}))
    }

    /// Native reports are not yet authenticated to Ledger. Do not manufacture
    /// a signed GET or silently mark a reconciliation job finished.
    pub fn run_one_registered_order_reconciliation(
        &self, tenant: &str, _worker: &str, _venue_base: &str,
    ) -> Result<Option<Value>, RuntimeError> {
        self.require_active(tenant)?;
        Err(native_evidence_unavailable())
    }

    pub fn sync_registered_fills(
        &self, tenant: &str, _account: &str, _environment: &str,
        _request_id: &str, _venue_base: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        Err(native_evidence_unavailable())
    }

    pub fn account_snapshot(
        &self, tenant_id: &str, _account_id: &str, _environment: &str,
        _api_key: &str, _secret: &str, _venue_base: Option<&str>,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        Err(native_evidence_unavailable())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reconcile_account_product(
        &self, tenant: &str, _account: &str, _environment: &str,
        _product: Product, _expected: &Value, _venue_base: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        Err(native_evidence_unavailable())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_registered_settlements(
        &self, tenant: &str, _account: &str, _environment: &str,
        _product: Product, _symbol: &str,
        _start_ms: i64, _end_ms: i64, _venue_base: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        Err(native_evidence_unavailable())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_malformed_credentials_without_persisting_them() {
        assert!(validate_secret("", "secret").is_err());
        assert!(validate_secret("same", "same").is_err());
        assert!(validate_secret("key\n", "secret").is_err());
        assert!(validate_secret("key", "secret").is_ok());
    }
}
