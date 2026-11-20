//! Paper session orchestration: feed snapshot, pipeline, strategy host, ledger.
mod accounts;
mod config;
mod data;
mod history;
mod inspection;
mod jobs;
mod sources;
mod session;
mod trading;
pub use trading::OrderCommand;

pub use config::{AuthMode, Config, ensure_state_dir};
pub use session::{PackageBody, PaperRequest, StrategyInput};

use seealpha_ledger::{Ledger, LedgerError, StrategyLoad, Tenant};
use seealpha_strategy::{self, HostCommand, inspect_archive};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::collections::HashMap;
use std::process::Child;
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
    pub project: PathBuf,
    pub python: PathBuf,
    pub host: PathBuf,
    source_state_dir: PathBuf,
    inflight: AtomicUsize,
    source_processes: Mutex<HashMap<String,Child>>,
}

impl Runtime {
    pub fn open(config: &Config) -> Result<Self, RuntimeError> {
        match Self::open_book(config, &config.book_path()) {
            Ok(runtime) => Ok(runtime),
            Err(error) => {
                // The Platform handshake bind-mounts a 0700 directory and drops
                // every capability, so uid 0 cannot create the book there.
                let fallback =
                    std::env::temp_dir().join(format!("seealpha-engine-{}", std::process::id()));
                eprintln!(
                    "state database {} is not writable ({error}); using {}",
                    config.book_path().display(),
                    fallback.display()
                );
                Self::open_book(config, &fallback.join("book.sqlite"))
            }
        }
    }

    fn open_book(config: &Config, book: &std::path::Path) -> Result<Self, RuntimeError> {
        if let Some(parent) = book.parent() {
            ensure_state_dir(parent)
                .map_err(|_| RuntimeError::BadRequest("状态目录不可用。".into()))?;
        }
        let ledger = Ledger::open(book).map_err(map_ledger)?;
        // Interrupted writer tasks are never blindly retried after a process crash.
        ledger.restore_interrupted_data_jobs().map_err(map_ledger)?;
        Ok(Self {
            ledger,
            project: config.project.clone(),
            python: config.python.clone(),
            host: config.host_script(),
            source_state_dir: book.parent().map(std::path::Path::to_path_buf).unwrap_or_else(||config.state_dir.clone()),
            inflight: AtomicUsize::new(0),
            source_processes: Mutex::new(HashMap::new()),
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
        self.ledger
            .ensure_tenant(tenant_id, purpose, status)
            .map_err(map_ledger)
    }

    pub fn runnable(&self, tenant_id: &str, action: &str) -> Result<Value, RuntimeError> {
        let tenant = self.require_active(tenant_id)?;
        match action {
            "live" if tenant.purpose == "production" => Ok(json!({
                "tenant_id": tenant.tenant_id,
                "action": action,
                "status": "runnable",
            })),
            "live" => Err(RuntimeError::Conflict("开发租户不能向交易所下单。".into())),
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
        if !matches!(grant_purpose, "production" | "development") {
            return Err(RuntimeError::Conflict("下发用途无效。".into()));
        }
        let tenant = self
            .ledger
            .tenant(tenant_id)
            .map_err(map_ledger)?
            .ok_or_else(|| RuntimeError::Conflict("租户不存在。".into()))?;
        if tenant.status != "active" {
            return Err(RuntimeError::Conflict("已归档的租户不能加载策略。".into()));
        }
        if tenant.purpose != grant_purpose {
            return Err(RuntimeError::Conflict(if tenant.purpose == "production" {
                "正式租户只能加载正式授权。".into()
            } else {
                "下发用途与租户类型不一致。".into()
            }));
        }
        if account_id.is_empty()
            || issuance_id.is_empty()
            || version.is_empty()
            || account_id.len() > 40
            || issuance_id.len() > 64
            || version.len() > 40
        {
            return Err(RuntimeError::Conflict(
                "策略下发缺少账户、编号或版本。".into(),
            ));
        }
        let metadata = inspect_archive(archive).map_err(map_strategy)?;
        if metadata.digest != package_hash
            || metadata.name != code_key
            || metadata.version != version
        {
            return Err(RuntimeError::Conflict("策略包与下发记录不一致。".into()));
        }
        if metadata.entry_point != entry_point {
            return Err(RuntimeError::Conflict(
                "策略包入口与下发记录不一致。".into(),
            ));
        }
        let path = write_archive(archive)?;
        let prepared = seealpha_strategy::run_host(&HostCommand {
            python: &self.python,
            host: &self.host,
            package_dir: None,
            archive_path: Some(&path),
            parameters,
            symbol: code_key,
            interval: "1m",
            bars: &[],
            op: "prepare",
        })
        .map_err(map_strategy)?;
        let _ = std::fs::remove_file(&path);
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
        self.ledger.upsert_strategy(&load).map_err(map_ledger)?;
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
            self.ledger
                .strategies(tenant_id, account_id)
                .map_err(map_ledger)?
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
            .ledger
            .strategies(tenant_id, account_id)
            .map_err(map_ledger)?;
        if loads.is_empty() {
            return Err(RuntimeError::Conflict(
                "该租户账户没有已下发的策略。".into(),
            ));
        }
        Ok(loads
            .into_iter()
            .map(|load| StrategyInput {
                member_id: load.issuance_id,
                package: PackageBody::Archive(load.archive),
                parameters: serde_json::from_str(&load.parameters_json).unwrap_or(json!({})),
            })
            .collect())
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
        if let Ok(mut sources)=self.source_processes.lock() {
            for (_,mut child) in sources.drain() {
                let _=child.kill();
                let _=child.wait();
            }
        }
    }
}

pub fn map_ledger(error: LedgerError) -> RuntimeError {
    match error {
        LedgerError::Conflict(message) => RuntimeError::Conflict(message),
        LedgerError::Sqlite(detail) => RuntimeError::BadRequest(format!("状态库不可用。{detail}")),
    }
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
