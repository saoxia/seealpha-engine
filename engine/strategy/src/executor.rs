//! Language-neutral strategy execution boundary for target-holding research.
//!
//! The Rust runtime consumes StrategyRun regardless of implementation language.
//! Package issuance, account authorization and risk/ledger decisions remain at
//! the runtime boundary; this trait does not confer permission to run packages.

use crate::{AccountScope, HostCommand, IssuedStrategy, StrategyBar, StrategyError, StrategyRun, run_host, run_issued_host};
use serde_json::Value;
use std::path::Path;

/// Immutable per-invocation market data and parameters.
#[derive(Debug, Clone, Copy)]
pub struct ExecutionInput<'a> {
    pub parameters: &'a Value,
    pub symbol: &'a str,
    pub interval: &'a str,
    pub bars: &'a [StrategyBar],
}

/// An execution backend emits targets; it never places exchange orders.
///
/// Caller-side authorization and target/weight validation are mandatory.
/// A host implementation may need additional OS process isolation.
pub trait StrategyExecutor {
    fn execute(&self, input: ExecutionInput<'_>) -> Result<StrategyRun, StrategyError>;
}

/// Bundled deterministic EMA strategy evaluated directly in Rust.
#[derive(Debug, Default, Clone, Copy)]
pub struct NativeEmaExecutor;

impl StrategyExecutor for NativeEmaExecutor {
    fn execute(&self, input: ExecutionInput<'_>) -> Result<StrategyRun, StrategyError> {
        crate::native_ema::run_native_ema(input.parameters, input.symbol, input.bars)
    }
}

/// Account-issued, immutable built-in Rust strategy. This does not load or
/// execute arbitrary uploaded code: manifest bytes select one compiled-in
/// implementation only after their exact hash and account scope are authorized.
///
/// Callers must obtain `issued` and `scope` from trusted persisted state.
/// No HTTP issuance or automatic cutover from Python packages is enabled here.
#[derive(Debug, Clone, Copy)]
pub struct IssuedNativeEmaExecutor<'a> {
    pub manifest: &'a [u8],
    pub issued: IssuedStrategy<'a>,
    pub scope: AccountScope<'a>,
}

impl StrategyExecutor for IssuedNativeEmaExecutor<'_> {
    fn execute(&self, input: ExecutionInput<'_>) -> Result<StrategyRun, StrategyError> {
        crate::native_manifest::authorize_native_manifest(self.manifest, self.issued, self.scope)?;
        NativeEmaExecutor.execute(input)
    }
}

/// Directory-based Python adapter only. Issued archives must
/// continue through `run_issued_host` with account-scoped authorization.
#[derive(Debug, Clone, Copy)]
pub struct DevelopmentPythonExecutor<'a> {
    pub python: &'a Path,
    pub host: &'a Path,
    pub package_dir: &'a Path,
}

impl StrategyExecutor for DevelopmentPythonExecutor<'_> {
    fn execute(&self, input: ExecutionInput<'_>) -> Result<StrategyRun, StrategyError> {
        run_host(&HostCommand {
            python: self.python,
            host: self.host,
            package_dir: Some(self.package_dir),
            archive_path: None,
            parameters: input.parameters,
            symbol: input.symbol,
            interval: input.interval,
            bars: input.bars,
            op: "run",
        })
    }
}

/// Account-issued Python archive executor. Unlike the development adapter,
/// this always runs the immutable archive authorization checks before the host.
/// The runtime must first load the issuance from its trusted ledger and verify
/// the requested bytes and parameters match the persisted record.
#[derive(Debug, Clone, Copy)]
pub struct IssuedPythonExecutor<'a> {
    pub python: &'a Path,
    pub host: &'a Path,
    pub archive_path: &'a Path,
    pub issued: IssuedStrategy<'a>,
    pub scope: AccountScope<'a>,
}

impl StrategyExecutor for IssuedPythonExecutor<'_> {
    fn execute(&self, input: ExecutionInput<'_>) -> Result<StrategyRun, StrategyError> {
        run_issued_host(
            &HostCommand {
                python: self.python,
                host: self.host,
                package_dir: None,
                archive_path: Some(self.archive_path),
                parameters: input.parameters,
                symbol: input.symbol,
                interval: input.interval,
                bars: input.bars,
                op: "run",
            },
            self.issued,
            self.scope,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn bar(value: &str) -> StrategyBar {
        StrategyBar {
            open_time_ms: 0,
            open: value.into(),
            high: value.into(),
            low: value.into(),
            close: value.into(),
            volume: "1".into(),
        }
    }

    #[test]
    fn rust_executor_does_not_depend_on_python() {
        let bars = ["1", "1", "1", "10", "10", "0"].map(bar);
        let parameters = json!({"fast_period": 2, "slow_period": 3});
        let input = ExecutionInput {
            parameters: &parameters,
            symbol: "ETHUSDT",
            interval: "1m",
            bars: &bars,
        };
        let output = NativeEmaExecutor.execute(input).unwrap();
        assert_eq!(output.publications.len(), 2);
        assert_eq!(output.publications[0].quantity, "1");
        assert_eq!(output.publications[1].quantity, "0");
    }

    #[test]
    fn issued_native_executor_requires_exact_manifest_digest_and_account_scope() {
        const MANIFEST: &[u8] = br#"{"format":1,"runtime":"rust-builtin","name":"ema_crossover","version":"1","entry_point":"native:ema_crossover"}"#;
        let digest = crate::sha256_hex(MANIFEST);
        let issued = IssuedStrategy {
            account_id: "account-1",
            issuance_id: "issue-1",
            code_key: "ema_crossover",
            version: "1",
            entry_point: "native:ema_crossover",
            package_hash: &digest,
        };
        let scope = AccountScope {
            account_id: "account-1",
        };
        let bars = ["1", "1", "1", "10", "10", "0"].map(bar);
        let parameters = json!({"fast_period": 2, "slow_period": 3});
        let input = ExecutionInput {
            parameters: &parameters,
            symbol: "ETHUSDT",
            interval: "1m",
            bars: &bars,
        };
        let executor = IssuedNativeEmaExecutor { manifest: MANIFEST, issued, scope };
        assert_eq!(executor.execute(input).unwrap().publications.len(), 2);

        let wrong_scope = IssuedNativeEmaExecutor {
            scope: AccountScope { account_id: "another-account", ..scope },
            ..executor
        };
        assert!(matches!(wrong_scope.execute(input), Err(StrategyError::Package(_))));
        let wrong_hash = "0".repeat(64);
        let wrong_issuance = IssuedNativeEmaExecutor {
            issued: IssuedStrategy { package_hash: &wrong_hash, ..issued },
            ..executor
        };
        assert!(matches!(wrong_issuance.execute(input), Err(StrategyError::Package(_))));
        let modified_manifest = [MANIFEST, b" "].concat();
        let tampered = IssuedNativeEmaExecutor { manifest: &modified_manifest, ..executor };
        assert!(matches!(tampered.execute(input), Err(StrategyError::Package(_))));
    }

    #[test]
    fn issued_executor_fails_closed_without_verified_archive() {
        let parameters = json!({});
        let issued = IssuedStrategy {
            account_id: "account-1",
            issuance_id: "issue-1",
            code_key: "alpha_strategy",
            version: "1.0",
            entry_point: "strategy:Strategy",
            package_hash: "a",
        };
        let executor = IssuedPythonExecutor {
            python: Path::new("/absent/python"),
            host: Path::new("/absent/host.py"),
            archive_path: Path::new("/absent/archive.zip"),
            issued,
            scope: AccountScope {
                account_id: "account-1",
            },
        };
        assert!(executor
            .execute(ExecutionInput {
                parameters: &parameters,
                symbol: "ETHUSDT",
                interval: "1m",
                bars: &[],
            })
            .is_err());
    }

    #[test]
    fn issued_executor_rejects_valid_but_unauthorized_archive_before_python() {
        use std::io::{Cursor, Write};

        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();
        writer.start_file("seealpha-package.json", options).unwrap();
        writer.write_all(br#"{"format":1,"name":"alpha_strategy","version":"1.0","entry_point":"strategy:Strategy"}"#).unwrap();
        writer.start_file("strategy.py", options).unwrap();
        writer.write_all(b"class Strategy: pass").unwrap();
        let bytes = writer.finish().unwrap().into_inner();
        let digest = crate::sha256_hex(&bytes);
        let archive = crate::issued::StagedArchive::create(&bytes).unwrap();
        let parameters = json!({});
        let mismatched_hash = "a".repeat(64);
        assert_ne!(digest, mismatched_hash);
        let issued = IssuedStrategy {
            account_id: "account-1",
            issuance_id: "issue-1",
            code_key: "alpha_strategy",
            version: "1.0",
            entry_point: "strategy:Strategy",
            package_hash: &mismatched_hash,
        };
        let executor = IssuedPythonExecutor {
            python: Path::new("/absent/python"),
            host: Path::new("/absent/host.py"),
            archive_path: archive.path(),
            issued,
            scope: AccountScope {
                account_id: "account-1",
            },
        };
        let result = executor.execute(ExecutionInput {
            parameters: &parameters,
            symbol: "ETHUSDT",
            interval: "1m",
            bars: &[],
        });
        assert!(matches!(result, Err(StrategyError::Package(_))));
    }

    #[test]
    fn development_python_adapter_fails_closed_without_host() {
        let executor = DevelopmentPythonExecutor {
            python: Path::new("/absent/python3"),
            host: Path::new("/absent/host.py"),
            package_dir: Path::new("/absent/package"),
        };
        let params = json!({});
        let request = ExecutionInput {
            parameters: &params,
            symbol: "ETHUSDT",
            interval: "1m",
            bars: &[],
        };
        assert!(executor.execute(request).is_err());
    }
}
