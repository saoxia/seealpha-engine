//! Language-neutral strategy execution boundary for target-holding research.
//!
//! The Rust runtime consumes StrategyRun regardless of implementation language.
//! Package issuance, tenant authorization and risk/ledger decisions remain at
//! the runtime boundary; this trait does not confer permission to run packages.

use crate::{HostCommand, StrategyBar, StrategyError, StrategyRun, run_host};
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

/// Development-directory Python adapter only. Issued/production archives must
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
