//! Native cross-product portfolio replay from local verified bar evidence.
//! Unlike the old synthetic carry-window demonstration, neither historical
//! prices nor funding settlements are fabricated by this execution path.
use crate::{Runtime, RuntimeError, map_ledger};
use rust_decimal::Decimal;
use seealpha_backtest::evidence::{CarryPosition, MarkBar, SettledFunding, run_evidence_carry};
use seealpha_market::interval_ms;
use serde_json::{Value, json};

fn error(message: &str) -> RuntimeError {
    RuntimeError::Conflict(message.into())
}

use seealpha_backtest::evidence::{OptionHedgeMark, ProtectedPosition, run_evidence_protection};
use seealpha_market::options::{OptionRight, parse_option_symbol};

impl Runtime {
    #[allow(clippy::too_many_arguments)]
    pub fn run_evidence_carry_backtest(
        &self,
        tenant: &str,
        account: &str,
        symbol: &str,
        interval: &str,
        start_ms: i64,
        end_ms: i64,
        spot_qty: Decimal,
        perp_qty: Decimal,
        starting_balance: Decimal,
        max_gross_notional: Decimal,
        fee_rate: Decimal,
    ) -> Result<Value, RuntimeError> {
        let t = self.require_active(tenant)?;
        if t.purpose != "development" {
            return Err(error("只有开发租户可以回放真实历史证据。"));
        }
        seealpha_market::check_symbol(symbol).map_err(|_| error("回测合约无效。"))?;
        let step = interval_ms(interval).map_err(|_| error("回测 K 线周期无效。"))?;
        if start_ms < 0
            || end_ms <= start_ms
            || start_ms % step != 0
            || end_ms % step != 0
            || (end_ms - start_ms) / step > 5000
        {
            return Err(error("跨市场历史回测必须使用 5000 根以内对齐的闭合 K 线。"));
        }
        let expected = ((end_ms - start_ms) / step) as usize;
        let spot = self.read_contiguous_market_bars(
            tenant, "spot", symbol, interval, start_ms, end_ms, 5000,
        )?;
        let perp = self.read_contiguous_market_bars(
            tenant,
            "futures_um",
            symbol,
            interval,
            start_ms,
            end_ms,
            5000,
        )?;
        if spot.len() != expected || perp.len() != expected {
            return Err(error("跨市场历史 K 线缺失，不允许静默填充。"));
        }
        let mut marks = Vec::with_capacity(expected);
        for (index, (s, p)) in spot.iter().zip(&perp).enumerate() {
            let at = start_ms
                .checked_add((index as i64) * step)
                .ok_or_else(|| error("历史 K 线时间溢出。"))?;
            if s.open_time_ms != at || p.open_time_ms != at {
                return Err(error("现货与永续历史时间轴不一致。"));
            }
            let spot_close = s
                .close
                .parse::<Decimal>()
                .map_err(|_| error("现货历史价格无效。"))?;
            let perp_close = p
                .close
                .parse::<Decimal>()
                .map_err(|_| error("永续历史价格无效。"))?;
            marks.push(MarkBar {
                mark_time_ms: at
                    .checked_add(step - 1)
                    .ok_or_else(|| error("历史时间溢出。"))?,
                spot: spot_close,
                perp: perp_close,
            });
        }
        let funding_start = marks
            .first()
            .map(|b| b.mark_time_ms.saturating_add(1))
            .ok_or_else(|| error("历史 K 线为空。"))?;
        let events = self
            .funding_observations(tenant, symbol, funding_start, end_ms, 5000)?;
        let mut settlement = Vec::with_capacity(events.len());
        for row in &events {
            let time = row["funding_time_ms"]
                .as_i64()
                .ok_or_else(|| error("结算时间无效。"))?;
            let rate = row["funding_rate"]
                .as_str()
                .ok_or_else(|| error("结算资金费率无效。"))?
                .parse::<Decimal>()
                .map_err(|_| error("结算资金费率无效。"))?;
            let mark = row["mark_price"]
                .as_str()
                .ok_or_else(|| error("资金费结算价格无效。"))?
                .parse::<Decimal>()
                .map_err(|_| error("资金费结算价格无效。"))?;
            settlement.push(SettledFunding {
                at_ms: time,
                rate,
                mark_price: mark,
            });
        }
        let mut report = run_evidence_carry(
            &marks,
            &settlement,
            CarryPosition {
                spot_qty,
                perp_qty,
                starting_balance,
                max_gross_notional,
                fee_rate,
            },
        )
        .map_err(error)?;
        report["symbol"] = json!(symbol);
        report["interval"] = json!(interval);
        report["start_ms"] = json!(start_ms);
        report["end_ms"] = json!(end_ms);
        report["spot_data_source"] = json!("tenant_catalog_spot");
        report["perp_data_source"] = json!("tenant_catalog_futures_um");
        report["funding_data_source"] = json!("pipeline_catalog_settled_funding");
        self.state_store
            .save_backtest(tenant, account, "carry", &report.to_string())
            .map_err(map_ledger)?;
        Ok(report)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn run_evidence_protected_backtest(
        &self,
        tenant: &str,
        account: &str,
        option_symbol: &str,
        perp_symbol: &str,
        interval: &str,
        start_ms: i64,
        end_ms: i64,
        option_qty: Decimal,
        option_multiplier: Decimal,
        perp_qty: Decimal,
        starting_balance: Decimal,
        max_gross_notional: Decimal,
        fee_rate: Decimal,
    ) -> Result<Value, RuntimeError> {
        let t = self.require_active(tenant)?;
        if t.purpose != "development" {
            return Err(error("仅开发租户可运行期权保护回测。"));
        }
        let contract =
            parse_option_symbol(option_symbol).map_err(|_| error("期权合约标识无效。"))?;
        if contract.right != OptionRight::Put {
            return Err(error("保护性期权回测要求买入 Put。"));
        }
        seealpha_market::check_symbol(perp_symbol).map_err(|_| error("永续合约无效。"))?;
        if !perp_symbol.starts_with(&contract.underlying)
            || !(perp_symbol.ends_with("USDT") || perp_symbol.ends_with("USDC"))
        {
            return Err(error("期权标的与永续合约不一致。"));
        }
        let step = interval_ms(interval).map_err(|_| error("K 线周期无效。"))?;
        if !matches!(interval, "1m" | "5m" | "15m")
            || start_ms < 0
            || end_ms <= start_ms
            || start_ms % step != 0
            || end_ms % step != 0
            || (end_ms - start_ms) / step > 5000
        {
            return Err(error("期权历史时间范围无效或超过 5000 根。"));
        }
        let count = ((end_ms - start_ms) / step) as usize;
        let options = self.read_contiguous_market_bars(
            tenant,
            "options",
            option_symbol,
            interval,
            start_ms,
            end_ms,
            5000,
        )?;
        let perps = self.read_contiguous_market_bars(
            tenant,
            "futures_um",
            perp_symbol,
            interval,
            start_ms,
            end_ms,
            5000,
        )?;
        if options.len() != count || perps.len() != count {
            return Err(error("期权和永续原始证据缺失，禁止回测补造。"));
        }
        let mut marks = Vec::with_capacity(count);
        for (index, (option, perp)) in options.iter().zip(&perps).enumerate() {
            let expected = start_ms
                .checked_add((index as i64) * step)
                .ok_or_else(|| error("K 线时间溢出。"))?;
            if option.open_time_ms != expected || perp.open_time_ms != expected {
                return Err(error("期权与永续历史 K 线时间不对齐。"));
            }
            let option_price = option
                .close
                .parse::<Decimal>()
                .map_err(|_| error("期权历史价格无效。"))?;
            let perp_price = perp
                .close
                .parse::<Decimal>()
                .map_err(|_| error("永续历史价格无效。"))?;
            marks.push(OptionHedgeMark {
                mark_time_ms: expected
                    .checked_add(step - 1)
                    .ok_or_else(|| error("时间溢出。"))?,
                option_close: option_price,
                perp_close: perp_price,
            });
        }
        let first = marks.first().ok_or_else(|| error("历史数据为空。"))?;
        let events = self
            .funding_observations(
                tenant,
                perp_symbol,
                first.mark_time_ms.saturating_add(1),
                end_ms,
                5000,
            )?;
        let settlement = events
            .iter()
            .map(|row| {
                Ok(SettledFunding {
                    at_ms: row["funding_time_ms"]
                        .as_i64()
                        .ok_or_else(|| error("结算时间无效。"))?,
                    rate: row["funding_rate"]
                        .as_str()
                        .ok_or_else(|| error("资金费率缺失。"))?
                        .parse::<Decimal>()
                        .map_err(|_| error("资金费率无效。"))?,
                    mark_price: row["mark_price"]
                        .as_str()
                        .ok_or_else(|| error("结算标记价缺失。"))?
                        .parse::<Decimal>()
                        .map_err(|_| error("结算标记价无效。"))?,
                })
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        let mut report = run_evidence_protection(
            &marks,
            &settlement,
            ProtectedPosition {
                option_qty,
                option_multiplier,
                perp_qty,
                starting_balance,
                fee_rate,
                max_gross_notional,
            },
        )
        .map_err(error)?;
        report["option_symbol"] = json!(option_symbol);
        report["perp_symbol"] = json!(perp_symbol);
        report["interval"] = json!(interval);
        report["start_ms"] = json!(start_ms);
        report["end_ms"] = json!(end_ms);
        report["option_multiplier_source"] = json!("explicit_input_not_exchange_verified");
        report["exercise_settlement"] = json!("not_modeled");
        self.state_store
            .save_backtest(tenant, account, "protected_option", &report.to_string())
            .map_err(map_ledger)?;
        Ok(report)
    }
}
