//! Strategy-issued native historical replay. Reads an exact, closed, contiguous
//! local catalog window and executes an authorized built-in Rust strategy.
//! Rust owns cash/position matching, risk limits, ledger and equity accounting.
use crate::{Runtime, RuntimeError, map_ledger, map_strategy};
use rust_decimal::Decimal;
use seealpha_backtest::{
    AllocatedPublishedTarget, HistoryBar, report_json, run_allocated_backtest_with_curve,
};
use seealpha_market::{check_symbol, interval_ms};
use seealpha_portfolio::TargetHolding;
use seealpha_strategy::{AccountScope, IssuedStrategy, StrategyBar, executor::{ExecutionInput, IssuedNativeEmaExecutor, StrategyExecutor}};
use serde_json::{Value, json};

fn invalid(message: &str) -> RuntimeError {
    RuntimeError::Conflict(message.into())
}

impl Runtime {
    #[allow(clippy::too_many_arguments)]
    pub fn run_issued_catalog_backtest(
        &self,
        tenant: &str,
        account: &str,
        product: &str,
        symbol: &str,
        interval: &str,
        start_ms: i64,
        end_ms: i64,
        starting_balance: Decimal,
        quote: &str,
    ) -> Result<Value, RuntimeError> {
        let t = self.require_active(tenant)?;
        if t.purpose != "development" {
            return Err(invalid("正式租户不能使用这条历史回测。"));
        }
        if !matches!(product, "spot" | "futures_um") || check_symbol(symbol).is_err() {
            return Err(invalid("回测行情产品或品种无效。"));
        }
        let step = interval_ms(interval).map_err(|_| invalid("回测周期无效。"))?;
        if start_ms < 0 || end_ms <= start_ms || start_ms % step != 0 || end_ms % step != 0 {
            return Err(invalid("回测窗口必须使用完整、对齐的 UTC K 线。"));
        }
        let count = (end_ms - start_ms) / step;
        if count <= 0 || count > 5000 {
            return Err(invalid("回测窗口超过 5000 根 K 线的原生策略宿主上限。"));
        }
        let rows = self.read_contiguous_market_bars(
            tenant,
            product,
            symbol,
            interval,
            start_ms,
            end_ms,
            count + 1,
        )?;
        if rows.len() != count as usize {
            return Err(invalid("历史 K 线缺失，禁止自动填补或从公开行情临时订阅。"));
        }
        let mut host_bars = Vec::with_capacity(rows.len());
        let mut history = Vec::with_capacity(rows.len());
        for (index, bar) in rows.iter().enumerate() {
            let expected = start_ms
                .checked_add(index as i64 * step)
                .ok_or_else(|| invalid("历史 K 线时间溢出。"))?;
            if bar.open_time_ms != expected {
                return Err(invalid("历史 K 线存在时间缺口或重复。"));
            }
            let open = bar
                .open
                .parse::<Decimal>()
                .map_err(|_| invalid("历史 K 线价格无效。"))?;
            let high = bar
                .high
                .parse::<Decimal>()
                .map_err(|_| invalid("历史 K 线价格无效。"))?;
            let low = bar
                .low
                .parse::<Decimal>()
                .map_err(|_| invalid("历史 K 线价格无效。"))?;
            let close = bar
                .close
                .parse::<Decimal>()
                .map_err(|_| invalid("历史 K 线价格无效。"))?;
            let volume = bar
                .volume
                .parse::<Decimal>()
                .map_err(|_| invalid("历史 K 线成交量无效。"))?;
            if open <= Decimal::ZERO
                || close <= Decimal::ZERO
                || low <= Decimal::ZERO
                || high < open
                || high < close
                || low > open
                || low > close
                || volume < Decimal::ZERO
            {
                return Err(invalid("历史 K 线 OHLCV 证据无效。"));
            }
            host_bars.push(StrategyBar {
                open_time_ms: bar.open_time_ms,
                open: open.to_string(),
                high: high.to_string(),
                low: low.to_string(),
                close: close.to_string(),
                volume: volume.to_string(),
            });
            history.push(HistoryBar {
                open_time_ms: bar.open_time_ms,
                close,
            });
        }
        // Load the immutable issuance and its archive from the trusted Ledger.
        // Never authorize a historical session from caller-supplied strategy metadata.
        let strategies = self.strategy_store.strategies(tenant, account)
            .map_err(crate::map_store)?;
        if strategies.is_empty() {
            return Err(invalid("该账户没有已下发的策略。"));
        }
        let strategy_count = strategies.len();
        let mut publications = Vec::new();
        let mut risk: Option<Decimal> = None;
        for strategy in strategies {
            let parameters: Value = serde_json::from_str(&strategy.parameters_json)
                .map_err(|_| invalid("已下发策略参数记录损坏。"))?;
            // Execute only immutable, authorized built-in Rust strategy code.
            // No Python host process or temporary archive is involved.
            self.track_start();
            let executed = IssuedNativeEmaExecutor {
                manifest: &strategy.archive,
                issued: IssuedStrategy {
                    tenant_id: &strategy.tenant_id,
                    account_id: &strategy.account_id,
                    issuance_id: &strategy.issuance_id,
                    grant_purpose: &strategy.grant_purpose,
                    code_key: &strategy.code_key,
                    version: &strategy.version,
                    entry_point: &strategy.entry_point,
                    package_hash: &strategy.package_hash,
                },
                scope: AccountScope {
                    tenant_id: tenant,
                    account_id: account,
                    purpose: &t.purpose,
                },
            }.execute(ExecutionInput {
                parameters: &parameters,
                symbol,
                interval,
                bars: &host_bars,
            });
            self.track_end();
            let result = executed.map_err(map_strategy)?;
            let this_risk = result.prepared["risk_limit"]
                .as_str()
                .ok_or_else(|| invalid("策略未声明有效风险额度。"))?
                .parse::<Decimal>()
                .map_err(|_| invalid("策略风险额度无效。"))?;
            if this_risk <= Decimal::ZERO {
                return Err(invalid("策略风险额度无效。"));
            }
            risk = Some(risk.map_or(this_risk, |prior| prior.min(this_risk)));
            let allocation = result.prepared["allocation"]
                .as_str()
                .ok_or_else(|| invalid("策略未声明有效分配数量。"))?
                .parse::<Decimal>()
                .map_err(|_| invalid("策略分配数量无效。"))?;
            if allocation <= Decimal::ZERO {
                return Err(invalid("策略分配数量无效。"));
            }
            for p in result.publications {
                if p.symbol != symbol || p.bar_index >= history.len() {
                    return Err(invalid("策略发布了未授权的品种或时间索引。"));
                }
                let quantity = if p.quantity.is_empty() {
                    None
                } else {
                    Some(
                        p.quantity
                            .parse::<Decimal>()
                            .map_err(|_| invalid("策略目标数量无效。"))?,
                    )
                };
                let weight = p
                    .weight
                    .as_deref()
                    .map(|value| {
                        value
                            .parse::<Decimal>()
                            .map_err(|_| invalid("策略目标权重无效。"))
                    })
                    .transpose()?;
                publications.push(AllocatedPublishedTarget {
                    member_id: strategy.issuance_id.clone(),
                    bar_index: p.bar_index,
                    holding: TargetHolding::new(symbol, quantity, weight)
                        .map_err(|e| invalid(&e.to_string()))?,
                    allocation,
                });
            }
        }
        let risk = risk.ok_or_else(|| invalid("该账户没有已下发的策略。"))?;
        let (result, curve) = run_allocated_backtest_with_curve(
            &history,
            &publications,
            symbol,
            starting_balance,
            risk,
        )
        .map_err(|e| invalid(&e.to_string()))?;
        let mut body = report_json(symbol, quote, &result);
        body["equity_curve"] = json!(curve);
        body["product"] = json!(product);
        body["interval"] = json!(interval);
        body["start_ms"] = json!(start_ms);
        body["end_ms"] = json!(end_ms);
        body["strategy_count"] = json!(strategy_count);
        body["strategy_members"] = json!(
            publications
                .iter()
                .map(|p| p.member_id.as_str())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        );
        body["risk_limit"] = json!(risk.to_string());
        body["history_source"] = json!("tenant_local_catalog_sqlite");
        body["orders_submitted"] = json!(0);
        self.ledger
            .save_backtest(tenant, account, "history", &body.to_string())
            .map_err(map_ledger)?;
        Ok(body)
    }
}
