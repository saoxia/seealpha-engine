//! Issuance-bound strategy execution contract.
//!
//! Callers must obtain the issued record from the trusted Ledger and compare it
//! with the authenticated tenant/account. No claim made by the Python package
//! grants authority. Rust never exposes an order or data-control operation.

use crate::{HostCommand, PackageMetadata, StrategyError, StrategyRun, inspect_archive, run_host};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static STAGING_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy)]
pub struct IssuedStrategy<'a> {
    pub tenant_id: &'a str,
    pub account_id: &'a str,
    pub issuance_id: &'a str,
    pub grant_purpose: &'a str,
    pub code_key: &'a str,
    pub version: &'a str,
    pub entry_point: &'a str,
    pub package_hash: &'a str,
}

#[derive(Debug, Clone, Copy)]
pub struct AccountScope<'a> {
    /// Obtained from the authenticated request / selected Runtime tenant.
    pub tenant_id: &'a str,
    pub account_id: &'a str,
    pub purpose: &'a str,
}

/// Compare all immutable issuance fields with verified package bytes and the
/// trusted caller scope. This does not create or approve a strategy issuance.
pub fn authorize_issued(
    issued: IssuedStrategy<'_>,
    caller: AccountScope<'_>,
    metadata: &PackageMetadata,
) -> Result<(), StrategyError> {
    if !valid_identifier(issued.tenant_id, 128)
        || !valid_identifier(issued.account_id, 40)
        || !valid_identifier(issued.issuance_id, 64)
        || issued.tenant_id != caller.tenant_id
        || issued.account_id != caller.account_id
        || issued.grant_purpose != caller.purpose
        || !matches!(caller.purpose, "development" | "production")
        || !super::valid_name(issued.code_key)
        || !super::valid_version(issued.version)
        || !super::valid_entry(issued.entry_point)
        || issued.package_hash.len() != 64
        || !issued
            .package_hash
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        || issued.code_key != metadata.name
        || issued.version != metadata.version
        || issued.entry_point != metadata.entry_point
        || issued.package_hash != metadata.digest
    {
        return Err(StrategyError::Package(
            "策略包与账户下发授权不一致。".into(),
        ));
    }
    Ok(())
}

/// Re-read and verify the archive, then stage those exact bytes for the host.
///
/// Prevents a mutable origin path from being used after the identity check.
/// The host still needs operating-system isolation from untrusted Python.
pub fn run_issued_host(
    command: &HostCommand<'_>,
    issued: IssuedStrategy<'_>,
    caller: AccountScope<'_>,
) -> Result<StrategyRun, StrategyError> {
    if command.package_dir.is_some() {
        return Err(StrategyError::Package("已授权策略必须使用不可变归档。".into()));
    }
    let origin = command
        .archive_path
        .ok_or_else(|| StrategyError::Package("已授权策略缺少归档。".into()))?;
    if fs::symlink_metadata(origin)
        .map_err(|_| StrategyError::Package("策略包不可用。".into()))?
        .file_type()
        .is_symlink()
    {
        return Err(StrategyError::Package("策略包不能是符号链接。".into()));
    }
    let bytes = fs::read(origin).map_err(|_| StrategyError::Package("策略包不可用。".into()))?;
    let metadata = inspect_archive(&bytes)?;
    authorize_issued(issued, caller, &metadata)?;
    let staging = StagedArchive::create(&bytes)?;
    let checked = HostCommand {
        python: command.python,
        host: command.host,
        package_dir: None,
        archive_path: Some(&staging.path),
        parameters: command.parameters,
        symbol: command.symbol,
        interval: command.interval,
        bars: command.bars,
        op: command.op,
    };
    run_host(&checked)
}

fn valid_identifier(value: &str, max: usize) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= max
        && !value.contains("..")
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..].iter().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-' | b'_' | b':'))
}

pub(super) struct StagedArchive {
    path: PathBuf,
    _file: File,
}

impl StagedArchive {
    pub(super) fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub(super) fn create(bytes: &[u8]) -> Result<Self, StrategyError> {
        for _ in 0..16 {
            let n = STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "seealpha-issued-{}-{n}.zip",
                std::process::id(),
            ));
            let mut options = OpenOptions::new();
            options.write(true).read(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&path) {
                Ok(mut file) => {
                    if file.write_all(bytes).is_err() || file.sync_all().is_err() {
                        let _ = fs::remove_file(&path);
                        return Err(StrategyError::Host);
                    }
                    return Ok(Self { path, _file: file });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(_) => return Err(StrategyError::Host),
            }
        }
        Err(StrategyError::Host)
    }
}

impl Drop for StagedArchive {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn meta() -> PackageMetadata {
        PackageMetadata {
            name: "alpha_strategy".into(),
            version: "1.2".into(),
            entry_point: "strategy:MyStrategy".into(),
            digest: "a".repeat(64),
        }
    }

    fn issued<'a>(hash: &'a str) -> IssuedStrategy<'a> {
        IssuedStrategy {
            tenant_id: "tenant-1",
            account_id: "account-1",
            issuance_id: "issue-1",
            grant_purpose: "development",
            code_key: "alpha_strategy",
            version: "1.2",
            entry_point: "strategy:MyStrategy",
            package_hash: hash,
        }
    }

    #[test]
    fn only_matching_issuance_can_run_in_the_pinned_account_and_tenant() {
        let hash = "a".repeat(64);
        let record = issued(&hash);
        let scope = AccountScope {
            tenant_id: "tenant-1",
            account_id: "account-1",
            purpose: "development",
        };
        authorize_issued(record, scope, &meta()).unwrap();
        assert!(authorize_issued(record, AccountScope { account_id: "other", ..scope }, &meta()).is_err());
        assert!(authorize_issued(record, AccountScope { tenant_id: "tenant-2", ..scope }, &meta()).is_err());
        assert!(authorize_issued(record, AccountScope { purpose: "production", ..scope }, &meta()).is_err());
        let mut wrong = meta();
        wrong.version = "1.3".into();
        assert!(authorize_issued(record, scope, &wrong).is_err());
        wrong = meta();
        wrong.digest = "b".repeat(64);
        assert!(authorize_issued(record, scope, &wrong).is_err());
        wrong = meta();
        wrong.entry_point = "evil:Strategy".into();
        assert!(authorize_issued(record, scope, &wrong).is_err());
    }

    #[test]
    fn no_bare_package_without_an_issuance_or_local_temp_archive() {
        let hash = "a".repeat(64);
        let record = issued(&hash);
        let scope = AccountScope {
            tenant_id: "tenant-1", account_id: "account-1", purpose: "development",
        };
        let params = serde_json::json!({});
        let command = HostCommand {
            python: Path::new("python3"),
            host: Path::new("host.py"),
            package_dir: Some(Path::new(".")),
            archive_path: None,
            parameters: &params,
            symbol: "ETHUSDT",
            interval: "1m",
            bars: &[],
            op: "run",
        };
        assert!(run_issued_host(&command, record, scope).is_err());
    }
}
