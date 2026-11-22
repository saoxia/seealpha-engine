//! Native-only account API boundary.
//!
//! The legacy Binance REST client and unused Runtime credential cache are
//! removed. Native account/execution reports are not yet wired into Runtime.
//! Until then, all remote-account evidence and registration fail closed.
use crate::{Runtime, RuntimeError, map_ledger};
use serde_json::{Value, json};
#[cfg(unix)]
use std::{
    io::Read,
    os::unix::{fs::{FileTypeExt, PermissionsExt}, net::UnixStream},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};


fn native_evidence_unavailable() -> RuntimeError {
    RuntimeError::Conflict(
        "Nautilus 原生账户/成交报告尚未接入 Runtime；拒绝使用旧 REST 连接器或未经认证的数据。".into(),
    )
}

/// Read an already validated Nautilus testnet event from a private local node.
/// Never accept an endpoint from HTTP input and never treat this as a send permit.
#[cfg(unix)]
fn read_native_account(tenant: &str, account: &str) -> Result<Value, RuntimeError> {
    fn unavailable() -> RuntimeError {
        RuntimeError::Conflict("可信 Nautilus testnet 账户进程尚未就绪。".into())
    }
    for identity in [tenant, account] {
        if identity.is_empty() || identity.len() > 64
            || !identity.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        {
            return Err(RuntimeError::Unprocessable("账户身份无效。".into()));
        }
    }
    let directory = std::env::var("ENGINE_NATIVE_ACCOUNT_SOCKET_DIR")
        .map(PathBuf::from).map_err(|_| unavailable())?;
    if !directory.is_absolute() {
        return Err(unavailable());
    }
    let dir_meta = std::fs::symlink_metadata(&directory).map_err(|_| unavailable())?;
    if !dir_meta.is_dir() || dir_meta.file_type().is_symlink()
        || dir_meta.permissions().mode() & 0o077 != 0
    {
        return Err(unavailable());
    }
    let path = directory.join(format!("{tenant}--{account}--sandbox.sock"));
    let meta = std::fs::symlink_metadata(&path).map_err(|_| unavailable())?;
    if !meta.file_type().is_socket() || meta.permissions().mode() & 0o077 != 0 {
        return Err(unavailable());
    }
    let mut stream = UnixStream::connect(&path).map_err(|_| unavailable())?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).map_err(|_| unavailable())?;
    stream.set_write_timeout(Some(Duration::from_secs(2))).map_err(|_| unavailable())?;
    let mut response = Vec::new();
    stream.take(65_537).read_to_end(&mut response).map_err(|_| unavailable())?;
    if response.len() > 65_536 {
        return Err(unavailable());
    }
    let report: Value = serde_json::from_slice(&response).map_err(|_| unavailable())?;
    if report["tenant_id"].as_str() != Some(tenant)
        || report["account_id"].as_str() != Some(account)
        || report["environment"] != "sandbox"
        || report["source"] != "nautilus_testnet_read_only"
        || report["native_account"]["account_id"].as_str().is_none()
        || report["native_account"]["event_id"].as_str().is_none()
        || !report["native_account"]["balances"].is_object()
        || !report["native_account"]["margins"].is_array()
    {
        return Err(unavailable());
    }
    let ts = report["native_account"]["observed_at_ns"].as_u64().ok_or_else(unavailable)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)
        .map_err(|_| unavailable())?.as_nanos();
    if ts == 0 || u128::from(ts) > now || now - u128::from(ts) > 60_000_000_000 {
        return Err(unavailable());
    }
    Ok(report)
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
        &self, tenant_id: &str, account_id: &str, environment: &str,
        api_key: &str, secret: &str, _venue_base: Option<&str>,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        if environment != "sandbox" || !api_key.is_empty() || !secret.is_empty() {
            return Err(native_evidence_unavailable());
        }
        #[cfg(unix)]
        { read_native_account(tenant_id, account_id) }
        #[cfg(not(unix))]
        { Err(native_evidence_unavailable()) }
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
