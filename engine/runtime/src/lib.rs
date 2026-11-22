//! Paper session orchestration: feed snapshot, pipeline, strategy host, ledger.
mod accounts;
mod basket;
mod portfolio;
pub use portfolio::{NativePortfolioEvent, NativePortfolioResult, NativePortfolioState};
mod catalog;
mod clickhouse;
pub use catalog::CatalogBar;
mod config;
mod data;
mod evidence;
mod fills;
mod history;
mod verified_replay;
pub use verified_replay::VerifiedHistoryWindow;
mod inspection;
mod jobs;
mod session;
mod sources;
mod orders;
#[allow(dead_code)] // Native send remains disabled pending reviewed cutover.
mod native_handoff;
mod research;
mod state_store;
pub use orders::OrderCommand;
pub use state_store::{OrderRecoveryItem, ReconciliationTask};

pub use config::{AuthMode, Config, ensure_state_dir};
pub use session::{PackageBody, PaperRequest, StrategyInput};

use seealpha_ledger::{Ledger, LedgerError, Tenant};
use seealpha_strategy::store::{StrategyLoad, StrategyStore, StoreError};
use seealpha_catalog::{ControlStore, ControlError};
use seealpha_strategy::{self, native_manifest::inspect_native_manifest};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Child;
use std::sync::Mutex;
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

#[derive(Clone, Copy)]
pub struct StrategySubmission<'a> {
    pub tenant_id: &'a str,
    pub account_id: &'a str,
    pub issuance_id: &'a str,
    pub version: &'a str,
    pub code_key: &'a str,
    pub entry_point: &'a str,
    pub package_hash: &'a str,
    pub grant_purpose: &'a str,
    pub parameters: &'a Value,
    pub archive: &'a [u8],
}

pub struct Runtime {
    pub ledger: Ledger,
    state_store: state_store::StateStore,
    pub strategy_store: StrategyStore,
    pub control: ControlStore,
    pub project: PathBuf,
    pub python: PathBuf,
    pub host: PathBuf,
    inflight: AtomicUsize,
    source_processes: Mutex<HashMap<String, Child>>,
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
        let ledger = Ledger::open(book).map_err(map_ledger)?;
        let state_store = state_store::StateStore::open(book).map_err(map_ledger)?;
        let strategy_store = StrategyStore::open(&book.with_file_name("strategies")).map_err(map_store)?;
        // Interrupted writer tasks are never blindly retried after a process crash.
        let control = ControlStore::open(&book.with_file_name("control-state")).map_err(map_control)?;
        control.restore_interrupted_data_jobs().map_err(map_control)?;
        // A previous Rust version invented empty fills for NEW order
        // acknowledgements. Preserve those rows in an audit quarantine,
        // never treat them as real exchange-executed trades.
        ledger.quarantine_synthetic_fills().map_err(map_ledger)?;
        // Restart discovery is read-only. Without authenticated exchange
        // evidence, pending orders MUST remain unresolved; no submit/retry.
        // Query at most one page per tenant on startup; the paginated API
        // below exposes the entire durable queue without loading it at boot.
        for tenant in ledger.active_tenant_ids().map_err(map_ledger)? {
            let page = state_store
                .order_recovery_page(&tenant, 0, 500)
                .map_err(map_ledger)?;
            if !page.is_empty() {
                eprintln!(
                    "order recovery: tenant={tenant} pending_reconciliation_at_least={}{}; no automatic resend",
                    page.len(),
                    if page.len() == 500 {
                        " (more pages possible)"
                    } else {
                        ""
                    }
                );
            }
        }
        Ok(Self {
            ledger,
            state_store,
            strategy_store,
            control,
            project: config.project.clone(),
            python: config.python.clone(),
            host: config.host_script(),
            inflight: AtomicUsize::new(0),
            source_processes: Mutex::new(HashMap::new()),
        })
    }

    /// Read-only, paginated recovery inventory. This is a list of work
    /// requiring signed exchange queries and account reconciliation, not an
    /// authorization to resend or synthesize fills.
    pub fn order_recovery_page(
        &self,
        tenant: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<OrderRecoveryItem>, RuntimeError> {
        self.require_active(tenant)?;
        self.state_store
            .order_recovery_page(tenant, offset, limit)
            .map_err(map_ledger)
    }

    /// Explicit scheduling only; no network IO, no order POST and no claim
    /// that an observed NEW/ACK implies a completed settlement.
    pub fn enqueue_order_reconciliations(
        &self,
        tenant: &str,
        now_ms: i64,
    ) -> Result<usize, RuntimeError> {
        self.require_active(tenant)?;
        self.state_store
            .enqueue_order_reconciliations(tenant, now_ms)
            .map_err(map_ledger)
    }

    /// Atomically lease one read-side reconciliation task. The caller may
    /// perform authenticated order/trade/account GETs, never resend the order.
    pub fn claim_order_reconciliation(
        &self,
        tenant: &str,
        worker: &str,
        now_ms: i64,
        lease_ms: i64,
    ) -> Result<Option<ReconciliationTask>, RuntimeError> {
        self.require_active(tenant)?;
        self.state_store
            .claim_order_reconciliation(tenant, worker, now_ms, lease_ms)
            .map_err(map_ledger)
    }

    /// A successful GET observation is still NOT a settled fill. Keep task
    /// retriable until separately verified financial reconciliation completes.
    #[allow(clippy::too_many_arguments)]
    pub fn finish_order_reconciliation(
        &self,
        tenant: &str,
        account: &str,
        request: &str,
        worker: &str,
        expected_attempts: i64,
        now_ms: i64,
        outcome: &str,
        error_code: Option<&str>,
    ) -> Result<(), RuntimeError> {
        self.require_active(tenant)?;
        self.state_store
            .finish_order_reconciliation(
                tenant,
                account,
                request,
                worker,
                expected_attempts,
                now_ms,
                outcome,
                error_code,
            )
            .map_err(map_ledger)
    }

    /// Read persisted order projections owned by Runtime execution.
    pub fn stored_orders(&self, tenant: &str, account: &str, env: &str) -> Result<Vec<state_store::StoredOrder>, RuntimeError> {
        self.require_active(tenant)?;
        self.state_store.orders(tenant, account, env).map_err(map_ledger)
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
        self.ledger
            .ensure_tenant(tenant_id, purpose, status)
            .map_err(map_ledger)
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
            "backtest" | "paper" => Ok(json!({
                "tenant_id": tenant.tenant_id,
                "action": action,
                "status": "runnable",
            })),
            _ => Err(RuntimeError::Conflict("未知的租户操作。".into())),
        }
    }

    pub fn accept_strategy(
        &self,
        submission: &StrategySubmission<'_>,
    ) -> Result<Value, RuntimeError> {
        let StrategySubmission {
            tenant_id,
            account_id,
            issuance_id,
            version,
            code_key,
            entry_point,
            package_hash,
            grant_purpose,
            parameters,
            archive,
        } = *submission;
        let tenant = self
            .ledger
            .tenant(tenant_id)
            .map_err(map_ledger)?
            .ok_or_else(|| RuntimeError::Conflict("租户不存在。".into()))?;
        if tenant.status != "active" {
            return Err(RuntimeError::Conflict("已归档的租户不能加载策略。".into()));
        }
        let grant_purpose = if grant_purpose.is_empty() { tenant.purpose.as_str() } else { grant_purpose };
        if tenant.purpose != grant_purpose {
            return Err(RuntimeError::Conflict(if tenant.purpose == "production" {
                "正式租户只能加载正式授权。".into()
            } else {
                "下发用途与租户类型不一致。".into()
            }));
        }
        if account_id.is_empty()
            || issuance_id.is_empty()
            || account_id.len() > 40
            || issuance_id.len() > 64
            || version.len() > 40
        {
            return Err(RuntimeError::Conflict(
                "策略下发缺少账户、编号或版本。".into(),
            ));
        }
        // Only a known compiled-in Rust strategy may be admitted. Arbitrary
        // uploaded Python archives are not executable in the native runtime.
        let is_zip = archive.starts_with(&[0x50, 0x4b, 0x03, 0x04]);
        let native_bytes = if is_zip {
            seealpha_strategy::store::unpack_native_zip(archive).map_err(map_store)?.0
        } else { archive.to_vec() };
        let archive = native_bytes.as_slice();
        let metadata = inspect_native_manifest(archive).map_err(map_strategy)?;
        let code_key = if code_key.is_empty() { metadata.name.as_str() } else { code_key };
        let version = if version.is_empty() { metadata.version.as_str() } else { version };
        let entry_point = if entry_point.is_empty() { metadata.entry_point.as_str() } else { entry_point };
        let package_hash = if package_hash.is_empty() { metadata.digest.as_str() } else { package_hash };
        if version.len() > 40
            || metadata.digest != package_hash
            || metadata.name != code_key
            || metadata.version != version
            || metadata.entry_point != entry_point
        {
            return Err(RuntimeError::Conflict("策略包与下发记录不一致。".into()));
        }
        seealpha_strategy::native_manifest::authorize_native_manifest(
            archive,
            seealpha_strategy::IssuedStrategy {
                tenant_id, account_id, issuance_id, grant_purpose, code_key,
                version, entry_point, package_hash,
            },
            seealpha_strategy::AccountScope {
                tenant_id, account_id, purpose: &tenant.purpose,
            },
        ).map_err(map_strategy)?;
        let prepared = seealpha_strategy::native_ema::run_native_ema(
            parameters,
            "ETHUSDT",
            &[],
        ).map_err(map_strategy)?;
        let load = StrategyLoad {
            tenant_id: tenant_id.to_string(),
            account_id: account_id.to_string(),
            issuance_id: issuance_id.to_string(),
            code_key: code_key.to_string(),
            version: version.to_string(),
            grant_purpose: grant_purpose.to_string(),
            parameters_json: prepared.prepared.to_string(),
            entry_point: entry_point.to_string(),
            package_hash: package_hash.to_string(),
            archive: archive.to_vec(),
        };
        if is_zip {
            self.strategy_store.insert_zip(&load, submission.archive).map_err(map_store)?;
        } else {
            self.strategy_store.insert(&load).map_err(map_store)?;
        }
        Ok(json!({
            "tenant_id": tenant_id,
            "account_id": account_id,
            "issuance_id": issuance_id,
            "code_key": code_key,
            "version": version,
            "parameters": prepared.prepared,
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
        let loads = self
            .strategy_store
            .strategies(tenant_id, account_id)
            .map_err(map_store)?;
        if loads.is_empty() {
            return Err(RuntimeError::Conflict(
                "该租户账户没有已下发的策略。".into(),
            ));
        }
        loads
            .into_iter()
            .map(|load| {
                let parameters = serde_json::from_str(&load.parameters_json).map_err(|_| {
                    RuntimeError::Conflict("已下发策略参数记录损坏，禁止用默认参数执行。".into())
                })?;
                Ok(StrategyInput {
                    member_id: load.issuance_id,
                    package: PackageBody::Archive(load.archive),
                    parameters,
                })
            })
            .collect()
    }

    pub fn require_active(&self, tenant_id: &str) -> Result<Tenant, RuntimeError> {
        let tenant = self
            .ledger
            .tenant(tenant_id)
            .map_err(map_ledger)?
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

impl Drop for Runtime {
    fn drop(&mut self) {
        // A running Rust Engine must never orphan its child producer processes
        // on normal shutdown. Crash recovery stays fail-closed via writer.lock.
        if let Ok(mut sources) = self.source_processes.lock() {
            for (_, mut child) in sources.drain() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
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

pub(crate) fn write_archive(bytes: &[u8]) -> Result<PathBuf, RuntimeError> {
    let path = std::env::temp_dir().join(format!(
        "seealpha-archive-{}-{}.zip",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::write(&path, bytes)
        .map_err(|_| RuntimeError::BadRequest("策略包无法写入。".into()))?;
    Ok(path)
}

pub fn map_control(error: ControlError) -> RuntimeError {
    match error {
        ControlError::Conflict(message) => RuntimeError::Conflict(message),
        ControlError::Database(detail) => RuntimeError::BadRequest(format!("控制状态库不可用。{detail}")),
    }
}
