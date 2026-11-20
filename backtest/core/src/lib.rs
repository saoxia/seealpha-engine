//! Bar-close backtest for strategies that publish absolute target holdings.
//!
//! Each publication replaces that member's target. The account trades the
//! quantized delta at that bar's close. Cash is quote currency. Ending equity
//! is cash plus the marked position. No exchange call is made.
pub mod matching;
pub mod timeline;
pub mod evidence;

use rust_decimal::Decimal;
use seealpha_portfolio::{MemberTarget, PaperFill, TargetPublication, replay_targets,
    clip_member_targets, net_signed_quantity, quantize_toward_zero};
use serde_json::{Value, json};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BacktestError {
    #[error("指定区间没有可用的本地 K 线。")]
    Empty,
    #[error("虚拟账户资金无效。")]
    Balance,
    #[error("{0}")]
    Portfolio(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryBar {
    pub open_time_ms: i64,
    pub close: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedTarget {
    pub member_id: String,
    pub bar_index: usize,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestReport {
    pub bar_count: usize,
    pub starting_balance: Decimal,
    pub ending_balance: Decimal,
    pub cash: Decimal,
    pub position: Decimal,
    pub fills: Vec<PaperFill>,
}

pub fn run_backtest(
    bars: &[HistoryBar],
    publications: &[PublishedTarget],
    starting_balance: Decimal,
    risk_limit: Decimal,
) -> Result<BacktestReport, BacktestError> {
    if bars.is_empty() {
        return Err(BacktestError::Empty);
    }
    if starting_balance <= Decimal::ZERO {
        return Err(BacktestError::Balance);
    }
    let mut targets = Vec::with_capacity(publications.len());
    for publication in publications {
        let price = bars
            .get(publication.bar_index)
            .ok_or(BacktestError::Empty)?
            .close;
        targets.push(TargetPublication {
            member_id: publication.member_id.clone(),
            quantity: publication.quantity,
            price,
        });
    }
    let (fills, position) = replay_targets(&targets, Decimal::ZERO, risk_limit, Decimal::new(1, 8))
        .map_err(|error| BacktestError::Portfolio(error.to_string()))?;
    let mut cash = starting_balance;
    for fill in &fills {
        let notion = fill
            .quantity
            .checked_mul(fill.price)
            .ok_or_else(|| BacktestError::Portfolio("overflow".into()))?;
        cash = if fill.side == "buy" {
            cash.checked_sub(notion)
                .ok_or_else(|| BacktestError::Portfolio("overflow".into()))?
        } else {
            cash.checked_add(notion)
                .ok_or_else(|| BacktestError::Portfolio("overflow".into()))?
        };
    }
    let mark = bars.last().map(|bar| bar.close).unwrap_or(Decimal::ZERO);
    let marked = position
        .checked_mul(mark)
        .ok_or_else(|| BacktestError::Portfolio("overflow".into()))?;
    let ending = cash
        .checked_add(marked)
        .ok_or_else(|| BacktestError::Portfolio("overflow".into()))?;
    Ok(BacktestReport {
        bar_count: bars.len(),
        starting_balance,
        ending_balance: ending,
        cash,
        position,
        fills,
    })
}

pub fn report_json(symbol: &str, quote: &str, report: &BacktestReport) -> Value {
    json!({
        "symbol": symbol,
        "quote_currency": quote,
        "bar_count": report.bar_count,
        "starting_balance": report.starting_balance.to_string(),
        "ending_balance": report.ending_balance.to_string(),
        "cash": report.cash.to_string(),
        "position": report.position.to_string(),
        "exchange_orders": 0,
        "venue": "backtest",
        "fills": report.fills.iter().map(|fill| json!({
            "side": fill.side,
            "quantity": fill.quantity.to_string(),
            "price": fill.price.to_string(),
        })).collect::<Vec<_>>(),
    })
}

/// A bar-by-bar, multi-member native portfolio replay with a persisted equity
/// curve. All fills are paper fills; market exchange APIs are never used.
pub fn run_backtest_with_curve(
    bars: &[HistoryBar],
    publications: &[PublishedTarget],
    starting_balance: Decimal,
    risk_limit: Decimal,
) -> Result<(BacktestReport, Vec<Value>), BacktestError> {
    use std::collections::BTreeMap;
    if bars.is_empty() {return Err(BacktestError::Empty);}
    if starting_balance<=Decimal::ZERO {return Err(BacktestError::Balance);}
    if risk_limit<=Decimal::ZERO {
        return Err(BacktestError::Portfolio("风险额度无效。".into()));
    }
    let mut timeline=publications.to_vec();
    timeline.sort_by(|a,b|a.bar_index.cmp(&b.bar_index).then(a.member_id.cmp(&b.member_id)));
    if timeline.iter().any(|p|p.bar_index>=bars.len()) {
        return Err(BacktestError::Portfolio("目标持仓不属于回测区间。".into()));
    }
    let mut current=Decimal::ZERO;
    let mut cash=starting_balance;
    let mut latest=BTreeMap::<String,Decimal>::new();
    let mut fills=Vec::new();
    let mut equity_curve=Vec::with_capacity(bars.len());
    let mut cursor=0_usize;
    for (bar_index,bar) in bars.iter().enumerate() {
        if bar.close<=Decimal::ZERO {
            return Err(BacktestError::Portfolio("K 线收盘价无效。".into()));
        }
        let mut changed=false;
        while cursor<timeline.len() && timeline[cursor].bar_index==bar_index {
            let p=&timeline[cursor];
            latest.insert(p.member_id.clone(),p.quantity);
            cursor+=1;
            changed=true;
        }
        if changed {
            // A bar's publications are merged before any execution. Never
            // turn opposing simultaneous strategies into artificial churn.
            let targets=latest.iter().map(|(member_id,quantity)|MemberTarget{
                member_id:member_id.clone(),signed_quantity:*quantity,risk_exit:false,
            }).collect::<Vec<_>>();
            let clipped=clip_member_targets(&targets,risk_limit)
                .map_err(|e|BacktestError::Portfolio(e.to_string()))?;
            let net=net_signed_quantity(&clipped)
                .map_err(|e|BacktestError::Portfolio(e.to_string()))?;
            let delta=net.checked_sub(current)
                .ok_or_else(||BacktestError::Portfolio("数值溢出。".into()))?;
            let delta=quantize_toward_zero(delta,Decimal::new(1,8))
                .map_err(|e|BacktestError::Portfolio(e.to_string()))?;
            let position=current.checked_add(delta)
                .ok_or_else(||BacktestError::Portfolio("数值溢出。".into()))?;
            let new_fills=if delta.is_zero(){Vec::new()}else{vec![PaperFill{
                side:if delta.is_sign_negative(){"sell".into()}else{"buy".into()},
                quantity:delta.abs(),price:bar.close,
            }]};
            for fill in new_fills {
                let notional=fill.quantity.checked_mul(fill.price)
                    .ok_or_else(||BacktestError::Portfolio("数值溢出。".into()))?;
                cash=if fill.side=="buy" {
                    cash.checked_sub(notional)
                } else {cash.checked_add(notional)}
                    .ok_or_else(||BacktestError::Portfolio("数值溢出。".into()))?;
                fills.push(fill);
            }
            current=position;
        }
        let equity=cash.checked_add(
            current.checked_mul(bar.close).ok_or_else(||BacktestError::Portfolio("数值溢出。".into()))?
        ).ok_or_else(||BacktestError::Portfolio("数值溢出。".into()))?;
        equity_curve.push(json!({"open_time_ms":bar.open_time_ms,
            "cash":cash.to_string(),"position":current.to_string(),
            "mark_price":bar.close.to_string(),"equity":equity.to_string()}));
    }
    let ending=equity_curve.last()
        .and_then(|row|row["equity"].as_str())
        .and_then(|s|s.parse::<Decimal>().ok())
        .ok_or_else(||BacktestError::Portfolio("权益记录无效。".into()))?;
    Ok((BacktestReport{
        bar_count:bars.len(),starting_balance,ending_balance:ending,
        cash,position:current,fills,
    },equity_curve))
}

pub fn basket_gross_notional(legs: &[(Decimal, Decimal)]) -> Result<Decimal, BacktestError> {
    let mut total = Decimal::ZERO;
    for (quantity, price) in legs {
        let notion = quantity
            .abs()
            .checked_mul(*price)
            .ok_or_else(|| BacktestError::Portfolio("overflow".into()))?;
        total = total
            .checked_add(notion)
            .ok_or_else(|| BacktestError::Portfolio("overflow".into()))?;
    }
    Ok(total)
}

pub fn hedge_within_drift(spot: Decimal, perpetual: Decimal, max_drift: Decimal) -> bool {
    (spot + perpetual).abs() <= max_drift
}

pub fn clip_to_risk(quantity: Decimal, risk_limit: Decimal) -> Result<Decimal, BacktestError> {
    let targets = [MemberTarget {
        member_id: "one".into(),
        signed_quantity: quantity,
        risk_exit: false,
    }];
    let clipped = seealpha_portfolio::clip_member_targets(&targets, risk_limit)
        .map_err(|error| BacktestError::Portfolio(error.to_string()))?;
    Ok(clipped[0].signed_quantity)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(index: i64, close: i64) -> HistoryBar {
        HistoryBar {
            open_time_ms: index * 60_000,
            close: Decimal::from(close),
        }
    }

    #[test]
    fn buy_then_flat_restores_cash_without_an_exchange_order() {
        let bars = vec![bar(0, 100), bar(1, 110), bar(2, 90)];
        let publications = vec![
            PublishedTarget {
                member_id: "ema".into(),
                bar_index: 0,
                quantity: Decimal::ONE,
            },
            PublishedTarget {
                member_id: "ema".into(),
                bar_index: 2,
                quantity: Decimal::ZERO,
            },
        ];
        let report = run_backtest(
            &bars,
            &publications,
            Decimal::from(10_000),
            Decimal::from(10),
        )
        .unwrap();
        assert_eq!(report.position, Decimal::ZERO);
        // Buy 1 at 100, sell 1 at 90: cash 10000 - 100 + 90 = 9990. Mark is flat.
        assert_eq!(report.ending_balance, Decimal::from(9990));
        assert_eq!(report.fills.len(), 2);
        assert_eq!(
            report_json("ETHUSDT", "USDT", &report)["exchange_orders"],
            0
        );
    }

    #[test]
    fn empty_history_is_rejected() {
        let error = run_backtest(&[], &[], Decimal::from(1), Decimal::ONE);
        assert_eq!(error, Err(BacktestError::Empty));
    }

    #[test]
    fn risk_limit_clips_a_target_before_the_fill() {
        assert_eq!(
            clip_to_risk(Decimal::from(5), Decimal::from(2)).unwrap(),
            Decimal::from(2)
        );
    }

    #[test]
    fn basket_gross_sums_absolute_notionals_and_hedge_checks_drift() {
        let gross = basket_gross_notional(&[
            (Decimal::ONE, Decimal::from(100)),
            (Decimal::from(-1), Decimal::from(100)),
        ])
        .unwrap();
        assert_eq!(gross, Decimal::from(200));
        assert!(hedge_within_drift(
            Decimal::ONE,
            Decimal::from(-1),
            Decimal::new(1, 3)
        ));
        assert!(!hedge_within_drift(
            Decimal::ONE,
            Decimal::ZERO,
            Decimal::new(1, 3)
        ));
    }

    #[test]
    fn issued_multi_member_history_has_contiguous_equity_and_netting() {
        let bars=vec![bar(0,100),bar(60_000,110),bar(120_000,90)];
        let publications=vec![
            PublishedTarget{member_id:"a".into(),bar_index:0,quantity:Decimal::ONE},
            PublishedTarget{member_id:"b".into(),bar_index:1,quantity:-Decimal::ONE},
            PublishedTarget{member_id:"a".into(),bar_index:2,quantity:Decimal::ZERO},
        ];
        let (result,curve)=run_backtest_with_curve(
            &bars,&publications,Decimal::from(1000),Decimal::from(10)
        ).unwrap();
        assert_eq!(result.fills.len(),3);
        assert_eq!(curve.len(),bars.len());
        assert_eq!(result.ending_balance,Decimal::from(1010));
        assert_eq!(curve[2]["equity"],"1010");
    }

    #[test]
    fn throughput_fixture_marks_a_flat_book() {
        let bars = (0..1_000).map(|index| bar(index, 100)).collect::<Vec<_>>();
        let report = run_backtest(&bars, &[], Decimal::from(1_000), Decimal::ONE).unwrap();
        assert_eq!(report.bar_count, 1_000);
        assert_eq!(report.ending_balance, Decimal::from(1_000));
    }
}
