//! Research snapshots and historical/backtest use cases.
use crate::{Runtime, RuntimeError, map_ledger};
use serde_json::json;
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

fn cancel_set() -> &'static Mutex<HashSet<String>> {
    static CANCELLED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    CANCELLED.get_or_init(|| Mutex::new(HashSet::new()))
}

fn cancel_key(tenant_id: &str, account_id: &str, name: &str) -> String {
    format!("{tenant_id}:{account_id}:{name}")
}

impl Runtime {
    #[allow(clippy::too_many_arguments)]
    pub fn run_account_backtest(
        &self,
        tenant_id: &str,
        account_id: &str,
        symbol: &str,
        quote: &str,
        starting: rust_decimal::Decimal,
        risk_limit: rust_decimal::Decimal,
        bars: &[seealpha_backtest::HistoryBar],
        targets: &[seealpha_backtest::PublishedTarget],
    ) -> Result<serde_json::Value, RuntimeError> {
        self.require_development(tenant_id)?;
        let report = seealpha_backtest::run_backtest(bars, targets, starting, risk_limit)
            .map_err(|error| RuntimeError::Conflict(error.to_string()))?;
        let body = seealpha_backtest::report_json(symbol, quote, &report);
        self.state_store
            .save_backtest(tenant_id, account_id, "history", &body.to_string())
            .map_err(map_ledger)?;
        Ok(body)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_carry_window(
        &self,
        tenant_id: &str,
        account_id: &str,
        symbol: &str,
        bar_count: usize,
        starting_spot: rust_decimal::Decimal,
        starting_perp: rust_decimal::Decimal,
        max_gross: rust_decimal::Decimal,
    ) -> Result<serde_json::Value, RuntimeError> {
        self.require_development(tenant_id)?;
        let body = seealpha_backtest::matching::simulate_carry_window(
            symbol,
            bar_count,
            starting_spot,
            starting_perp,
            max_gross,
        )
        .map_err(|error| RuntimeError::Conflict(error.to_owned()))?;
        self.state_store
            .save_backtest(tenant_id, account_id, "carry", &body.to_string())
            .map_err(map_ledger)?;
        Ok(body)
    }

    pub fn run_protected_window(
        &self,
        tenant_id: &str,
        account_id: &str,
        bar_count: usize,
        options_balance: rust_decimal::Decimal,
        perp_balance: rust_decimal::Decimal,
        max_gross: rust_decimal::Decimal,
    ) -> Result<serde_json::Value, RuntimeError> {
        self.require_development(tenant_id)?;
        let body = seealpha_backtest::matching::simulate_protected_window(
            bar_count,
            options_balance,
            perp_balance,
            max_gross,
        )
        .map_err(|error| RuntimeError::Conflict(error.to_owned()))?;
        self.state_store
            .save_backtest(tenant_id, account_id, "protected_option", &body.to_string())
            .map_err(map_ledger)?;
        Ok(body)
    }

    pub fn run_candidate_backtest(
        &self,
        tenant_id: &str,
        account_id: &str,
        name: &str,
        ending: rust_decimal::Decimal,
    ) -> Result<serde_json::Value, RuntimeError> {
        self.require_development(tenant_id)?;
        if self.cancelled(tenant_id, account_id, name) {
            return Err(RuntimeError::Conflict("候选回测已取消。".into()));
        }
        let body = seealpha_backtest::matching::candidate_report(name, ending);
        self.state_store
            .save_backtest(tenant_id, account_id, "candidate", &body.to_string())
            .map_err(map_ledger)?;
        Ok(body)
    }

    pub fn cancel_candidate(
        &self,
        tenant_id: &str,
        account_id: &str,
        name: &str,
    ) -> Result<serde_json::Value, RuntimeError> {
        self.require_development(tenant_id)?;
        self.remember_cancel(tenant_id, account_id, name);
        let body = json!({"kind":"candidate","name":name,"status":"cancelled","exchange_orders":0,"venue":"backtest"});
        self.state_store
            .save_backtest(tenant_id, account_id, "candidate", &body.to_string())
            .map_err(map_ledger)?;
        Ok(body)
    }

    pub fn latest_kind(
        &self,
        tenant_id: &str,
        account_id: &str,
        kind: &str,
        missing: &str,
    ) -> Result<serde_json::Value, RuntimeError> {
        match self
            .state_store
            .latest_backtest(tenant_id, account_id, kind)
            .map_err(map_ledger)?
        {
            Some(report) => serde_json::from_str(&report)
                .map_err(|_| RuntimeError::BadRequest("回测记录损坏。".into())),
            None => Err(RuntimeError::NotFound(missing.into())),
        }
    }

    fn require_development(&self, tenant_id: &str) -> Result<(), RuntimeError> {
        let tenant = self.require_active(tenant_id)?;
        if tenant.purpose != "development" {
            return Err(RuntimeError::Conflict(
                "正式租户不能使用这条历史回测。".into(),
            ));
        }
        Ok(())
    }

    fn cancelled(&self, tenant_id: &str, account_id: &str, name: &str) -> bool {
        cancel_set()
            .lock()
            .map(|guard| guard.contains(&cancel_key(tenant_id, account_id, name)))
            .unwrap_or(false)
    }

    fn remember_cancel(&self, tenant_id: &str, account_id: &str, name: &str) {
        if let Ok(mut guard) = cancel_set().lock() {
            guard.insert(cancel_key(tenant_id, account_id, name));
        }
    }

    pub fn latest_history_backtest(
        &self,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<serde_json::Value, RuntimeError> {
        self.latest_kind(tenant_id, account_id, "history", "还没有回测")
    }
}
