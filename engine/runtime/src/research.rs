//! Historical/backtest use cases.
use crate::{Runtime, RuntimeError, map_ledger};
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



    pub fn latest_history_backtest(
        &self,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<serde_json::Value, RuntimeError> {
        self.latest_kind(tenant_id, account_id, "history", "还没有回测")
    }
}
