//! Native-only account API boundary.
//!
//! The legacy Binance REST client and unused Runtime credential cache are
//! removed. Native account/execution reports are not yet wired into Runtime.
//! Until then, all remote-account evidence and registration fail closed.
use crate::{Runtime, RuntimeError, map_ledger};
use serde_json::{Value, json};

fn native_evidence_unavailable() -> RuntimeError {
    RuntimeError::Conflict(
        "Nautilus 原生账户/成交报告尚未接入 Runtime；拒绝使用旧 REST 连接器或未经认证的数据。".into(),
    )
}

impl Runtime {
    /// Do not retain credentials for an execution client that does not exist.
    /// A native LiveNode must own its own credentials and authenticated account.
    pub fn register_connection(
        &self, tenant_id: &str, _account_id: &str, environment: &str,
        _api_key: &str, _secret: &str,
    ) -> Result<Value, RuntimeError> {
        let tenant = self.require_active(tenant_id)?;
        if tenant.purpose != "production" {
            return Err(RuntimeError::Conflict("开发租户不能登记交易账户。".into()));
        }
        if !matches!(environment, "production" | "sandbox") {
            return Err(RuntimeError::Unprocessable("账户环境无效。".into()));
        }
        Err(RuntimeError::Conflict(
            "原生执行节点尚未与 Runtime 连接；不再接受没有实际连接效果的账户凭据登记。".into(),
        ))
    }

    pub fn forget_connection(
        &self, tenant_id: &str, account_id: &str, environment: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        if !matches!(environment, "production" | "sandbox") {
            return Err(RuntimeError::Unprocessable("账户环境无效。".into()));
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_evidence_is_not_marked_available_by_stub() {
        assert!(matches!(native_evidence_unavailable(), RuntimeError::Conflict(_)));
    }
}
