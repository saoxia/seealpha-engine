//! Cross-domain orchestration for tenant scopes and issued strategy previews.
mod config;
mod session;

pub use config::{AuthMode, Config, ensure_state_dir};
pub use session::PreviewRequest;
pub use seealpha_strategy::native_issuance::{IssuedNativeInput as StrategyInput, NativeSubmission as StrategySubmission};

use seealpha_ledger::{Ledger, LedgerError};
use seealpha_database::{Connection, OptionalExtension, params};
use seealpha_strategy::store::{StrategyStore, StoreError};
use seealpha_catalog::{ControlStore, ControlError};
use seealpha_strategy::native_issuance::inspect_native_package;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Unprocessable(String),
}

pub struct Runtime {
    pub ledger: Ledger,
    tenant_connection: Connection,
    pub strategy_store: StrategyStore,
    pub control: ControlStore,
    inflight: AtomicUsize,
}

impl Runtime {
    pub fn open(config: &Config) -> Result<Self, RuntimeError> {
        // PostgreSQL is mandatory; no ephemeral or SQLite fallback is permitted.
        Self::open_book(config, &config.book_path())
    }

    fn open_book(config: &Config, book: &std::path::Path) -> Result<Self, RuntimeError> {
        if let Some(parent) = book.parent() {
            ensure_state_dir(parent)
                .map_err(|_| RuntimeError::BadRequest("状态目录不可用。".into()))?;
        }
        // Bootstrap shared tables before either application or economic store opens.
        let schema_connection = seealpha_database::Connection::open(book)
            .map_err(|e| RuntimeError::BadRequest(format!("状态库不可用。{e}")))?;
        seealpha_database::bootstrap_engine_schema(&schema_connection)
            .map_err(|e| RuntimeError::BadRequest(format!("状态库不可用。{e}")))?;
        drop(schema_connection);
        let ledger = Ledger::open(book).map_err(map_ledger)?;
        let tenant_connection = Connection::open(book).map_err(map_database)?;
        let strategy_store = StrategyStore::open(&book.with_file_name("strategies")).map_err(map_store)?;
        let control = ControlStore::open(&book.with_file_name("control-state")).map_err(map_control)?;
        // A previous Rust version invented empty fills for NEW order
        // acknowledgements. Preserve those rows in an audit quarantine,
        // never treat them as real exchange-executed trades.
        ledger.quarantine_synthetic_fills().map_err(map_ledger)?;
        Ok(Self {
            ledger,
            tenant_connection,
            strategy_store,
            control,
            inflight: AtomicUsize::new(0),
        })
    }

    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::Relaxed)
    }

    pub fn ensure_tenant(
        &self,
        tenant_id: &str,
        purpose: &str,
        status: &str,
    ) -> Result<Tenant, RuntimeError> {
        upsert_tenant(&self.tenant_connection, tenant_id, purpose, status)
    }

    pub fn runnable(&self, tenant_id: &str, action: &str) -> Result<Value, RuntimeError> {
        let tenant = self.require_active(tenant_id)?;
        match action {
            // A production tenant does not imply permission to trade.
            // Enable only after separately reviewed end-to-end order admission,
            // signed evidence reconciliation, and explicit cutover authorization.
            "live" => Err(RuntimeError::Conflict(
                "Rust 实盘交易尚未通过集成验收，当前禁止交易。".into(),
            )),
            "backtest" => Ok(json!({
                "tenant_id": tenant.tenant_id,
                "action": action,
                "status": "runnable",
            })),
            "paper" => Err(RuntimeError::Conflict(
                "自定义 Paper 会话已退役；模拟执行须使用独立的 Nautilus 原生后端。".into(),
            )),
            _ => Err(RuntimeError::Conflict("未知的租户操作。".into())),
        }
    }

    pub fn inspect_strategy_package(
        &self,
        tenant_id: &str,
        bytes: &[u8],
        parameters: &Value,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        if !parameters.is_object() {
            return Err(RuntimeError::Unprocessable("策略包参数必须为对象。".into()));
        }
        let (metadata, prepared) = inspect_native_package(bytes, parameters).map_err(map_strategy)?;
        Ok(json!({
            "metadata": {
                "name": metadata.name,
                "version": metadata.version,
                "entry_point": metadata.entry_point,
                "package_hash": metadata.digest,
            },
            "parameters": prepared,
        }))
    }

    pub fn accept_strategy(
        &self,
        submission: &StrategySubmission<'_>,
    ) -> Result<Value, RuntimeError> {
        let tenant = tenant_record(&self.tenant_connection, submission.tenant_id)?
            .ok_or_else(|| RuntimeError::Conflict("租户不存在。".into()))?;
        if tenant.status != "active" {
            return Err(RuntimeError::Conflict("已归档的租户不能加载策略。".into()));
        }
        let accepted = self.strategy_store
            .issue_native(submission, &tenant.purpose)
            .map_err(map_strategy)?;
        Ok(json!({
            "tenant_id": submission.tenant_id,
            "account_id": submission.account_id,
            "issuance_id": submission.issuance_id,
            "code_key": accepted.code_key,
            "version": accepted.version,
            "parameters": accepted.parameters,
        }))
    }

    pub fn strategy_loads(
        &self,
        tenant_id: &str,
        account_id: Option<&str>,
    ) -> Result<Value, RuntimeError> {
        let loads = if let Some(account_id) = account_id {
            self.strategy_store
                .list(tenant_id, account_id)
                .map_err(map_store)?
        } else {
            Vec::new()
        };
        let items = loads
            .into_iter()
            .map(|load| {
                json!({
                    "account_id": load.account_id,
                    "issuance_id": load.issuance_id,
                    "code_key": load.code_key,
                    "version": load.version,
                    "grant_purpose": load.grant_purpose,
                    "entry_point": load.entry_point,
                    "package_hash": load.package_hash,
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({ "items": items }))
    }

    pub fn stored_strategies(
        &self,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<Vec<StrategyInput>, RuntimeError> {
        self.strategy_store.issued_native_inputs(tenant_id, account_id)
            .map_err(map_strategy)
    }

    pub fn require_active(&self, tenant_id: &str) -> Result<Tenant, RuntimeError> {
        let tenant = tenant_record(&self.tenant_connection, tenant_id)?
            .ok_or_else(|| RuntimeError::Conflict("租户不存在。".into()))?;
        if tenant.status != "active" {
            return Err(RuntimeError::Conflict(
                "已归档的租户不能交易或回测。".into(),
            ));
        }
        Ok(tenant)
    }

    pub(crate) fn track_start(&self) {
        self.inflight.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn track_end(&self) {
        self.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Local tenant scope only. Nautilus remains the account/position authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tenant {
    pub tenant_id: String,
    pub purpose: String,
    pub status: String,
}

fn tenant_record(connection: &Connection, tenant_id: &str) -> Result<Option<Tenant>, RuntimeError> {
    connection.query_row(
        "SELECT tenant_id, purpose, status FROM tenants WHERE tenant_id=$1",
        params![tenant_id],
        |row| Ok(Tenant {
            tenant_id: row.get(0)?,
            purpose: row.get(1)?,
            status: row.get(2)?,
        }),
    ).optional().map_err(map_database)
}

fn upsert_tenant(
    connection: &Connection,
    tenant_id: &str,
    purpose: &str,
    status: &str,
) -> Result<Tenant, RuntimeError> {
    if !matches!(purpose, "production" | "development")
        || !matches!(status, "active" | "archived")
    {
        return Err(RuntimeError::Conflict("租户类型或状态无效。".into()));
    }
    connection.execute(
        "INSERT INTO tenants (tenant_id, purpose, status) VALUES ($1, $2, $3)
         ON CONFLICT(tenant_id) DO UPDATE SET purpose=excluded.purpose, status=excluded.status",
        params![tenant_id, purpose, status],
    ).map_err(map_database)?;
    tenant_record(connection, tenant_id)?
        .ok_or_else(|| RuntimeError::BadRequest("状态库不可用。missing row".into()))
}

fn map_database(error: seealpha_database::Error) -> RuntimeError {
    RuntimeError::BadRequest(format!("状态库不可用。{error}"))
}

#[cfg(test)]
mod tenant_tests {
    use super::*;

    #[test]
    fn persists_tenant_scope_and_rejects_invalid_updates() {
        let connection = Connection::open(std::path::Path::new(":memory:")).unwrap();
        seealpha_database::bootstrap_engine_schema(&connection).unwrap();
        let tenant = upsert_tenant(&connection, "t", "development", "active").unwrap();
        assert_eq!(tenant.tenant_id, "t");
        assert_eq!(tenant.purpose, "development");
        assert_eq!(tenant.status, "active");
        assert!(tenant_record(&connection, "other").unwrap().is_none());
        assert!(upsert_tenant(&connection, "t", "other", "active").is_err());
        assert!(upsert_tenant(&connection, "t", "development", "invalid").is_err());
        let archived = upsert_tenant(&connection, "t", "development", "archived").unwrap();
        assert_eq!(archived.status, "archived");
        assert_eq!(tenant_record(&connection, "t").unwrap(), Some(archived));
    }
}

pub fn map_ledger(error: LedgerError) -> RuntimeError {
    match error {
        LedgerError::Conflict(message) => RuntimeError::Conflict(message),
        LedgerError::Database(detail) => RuntimeError::BadRequest(format!("状态库不可用。{detail}")),
    }
}

pub(crate) fn map_store(error: StoreError) -> RuntimeError {
    RuntimeError::Conflict(error.to_string())
}

pub(crate) fn map_strategy(error: seealpha_strategy::StrategyError) -> RuntimeError {
    match error {
        seealpha_strategy::StrategyError::Package(message)
        | seealpha_strategy::StrategyError::Failed(message) => RuntimeError::Conflict(message),
        seealpha_strategy::StrategyError::Timeout => {
            RuntimeError::Conflict("策略执行超时。".into())
        }
        seealpha_strategy::StrategyError::Host => {
            RuntimeError::BadRequest("策略宿主不可用。".into())
        }
    }
}


pub fn map_control(error: ControlError) -> RuntimeError {
    match error {
        ControlError::Conflict(message) => RuntimeError::Conflict(message),
        ControlError::Database(detail) => RuntimeError::BadRequest(format!("控制状态库不可用。{detail}")),
    }
}
