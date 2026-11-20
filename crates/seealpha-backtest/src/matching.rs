//! Bar matching for offline backtests.
//!
//! A market order fills at the bar open. A limit buy fills when the bar trades
//! at or below the limit, and a limit sell fills when the bar trades at or
//! above it. A gap through the limit fills at the open. Nothing here calls an
//! exchange.
use rust_decimal::Decimal;
use serde_json::{Value, json};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ohlc {
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkingOrder {
    pub id: String,
    pub side: Side,
    pub limit: Option<Decimal>,
    pub quantity: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MatchFill {
    pub id: String,
    pub side: &'static str,
    pub quantity: Decimal,
    pub price: Decimal,
}

pub fn match_bar(bar: &Ohlc, orders: &[WorkingOrder]) -> Vec<MatchFill> {
    let mut fills = Vec::new();
    for order in orders {
        if order.quantity <= Decimal::ZERO {
            continue;
        }
        let price = match (order.side, order.limit) {
            (Side::Buy, None) => Some(bar.open),
            (Side::Sell, None) => Some(bar.open),
            (Side::Buy, Some(limit)) if bar.low <= limit => {
                Some(if bar.open < limit { bar.open } else { limit })
            }
            (Side::Sell, Some(limit)) if bar.high >= limit => {
                Some(if bar.open > limit { bar.open } else { limit })
            }
            _ => None,
        };
        if let Some(price) = price {
            fills.push(MatchFill {
                id: order.id.clone(),
                side: if order.side == Side::Buy {
                    "buy"
                } else {
                    "sell"
                },
                quantity: order.quantity,
                price,
            });
        }
    }
    fills
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CarryBar {
    pub spot: Decimal,
    pub perp: Decimal,
    pub funding_rate: Decimal,
}

pub fn run_carry(
    bars: &[CarryBar],
    spot_qty: Decimal,
    perp_qty: Decimal,
    cash: Decimal,
) -> Result<Value, &'static str> {
    let Some(first) = bars.first() else {
        return Err("指定区间没有可用的本地 K 线。");
    };
    let last = bars.last().unwrap_or(first);
    let mut equity_cash = cash - spot_qty * first.spot - perp_qty * first.perp;
    let mut funding = Decimal::ZERO;
    for bar in bars.iter().skip(1) {
        let payment = -perp_qty * bar.perp * bar.funding_rate;
        funding += payment;
        equity_cash += payment;
    }
    let ending = equity_cash + spot_qty * last.spot + perp_qty * last.perp;
    Ok(json!({
        "kind": "carry",
        "spot_qty": spot_qty.to_string(),
        "perp_qty": perp_qty.to_string(),
        "funding": funding.to_string(),
        "ending_balance": ending.to_string(),
        "exchange_orders": 0,
        "venue": "backtest",
    }))
}

#[allow(clippy::too_many_arguments)]
pub fn run_protected_option(
    final_spot: Decimal,
    strike: Decimal,
    premium: Decimal,
    multiplier: Decimal,
    perp_qty: Decimal,
    perp_entry: Decimal,
    perp_exit: Decimal,
    cash: Decimal,
) -> Value {
    let payoff = (final_spot - strike).max(Decimal::ZERO) * multiplier;
    let perp_pnl = perp_qty * (perp_exit - perp_entry);
    let ending = cash - premium + payoff + perp_pnl;
    json!({
        "kind": "protected_option",
        "premium": premium.to_string(),
        "payoff": payoff.to_string(),
        "perp_pnl": perp_pnl.to_string(),
        "ending_balance": ending.to_string(),
        "exchange_orders": 0,
        "venue": "backtest",
    })
}

pub fn candidate_report(name: &str, ending: Decimal) -> Value {
    json!({
        "kind": "candidate",
        "name": name,
        "ending_balance": ending.to_string(),
        "status": "completed",
        "exchange_orders": 0,
        "venue": "backtest",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(open: i64, high: i64, low: i64, close: i64) -> Ohlc {
        Ohlc {
            open: Decimal::from(open),
            high: Decimal::from(high),
            low: Decimal::from(low),
            close: Decimal::from(close),
        }
    }

    #[test]
    fn limit_buy_fills_when_the_bar_trades_through_and_gaps_fill_at_the_open() {
        let limit = WorkingOrder {
            id: "L".into(),
            side: Side::Buy,
            limit: Some(Decimal::from(100)),
            quantity: Decimal::ONE,
        };
        let missed = match_bar(&bar(110, 120, 105, 115), std::slice::from_ref(&limit));
        assert!(missed.is_empty());
        let touched = match_bar(&bar(110, 112, 99, 101), std::slice::from_ref(&limit));
        assert_eq!(touched[0].price, Decimal::from(100));
        let gapped = match_bar(&bar(90, 95, 80, 92), std::slice::from_ref(&limit));
        assert_eq!(gapped[0].price, Decimal::from(90));
    }

    #[test]
    fn market_orders_fill_at_the_open() {
        let buy = WorkingOrder {
            id: "M".into(),
            side: Side::Buy,
            limit: None,
            quantity: Decimal::ONE,
        };
        let fills = match_bar(&bar(101, 110, 99, 105), &[buy]);
        assert_eq!(fills[0].price, Decimal::from(101));
        assert_eq!(fills[0].side, "buy");
    }

    #[test]
    fn carry_short_perp_receives_positive_funding() {
        let bars = [
            CarryBar {
                spot: Decimal::from(100),
                perp: Decimal::from(100),
                funding_rate: Decimal::ZERO,
            },
            CarryBar {
                spot: Decimal::from(100),
                perp: Decimal::from(100),
                funding_rate: Decimal::new(1, 2),
            },
        ];
        let report = run_carry(
            &bars,
            Decimal::ONE,
            Decimal::from(-1),
            Decimal::from(10_000),
        )
        .unwrap();
        let funding: Decimal = report["funding"].as_str().unwrap().parse().unwrap();
        let ending: Decimal = report["ending_balance"].as_str().unwrap().parse().unwrap();
        assert_eq!(funding, Decimal::ONE);
        assert_eq!(ending, Decimal::from(10001));
        assert_eq!(report["exchange_orders"], 0);
    }

    #[test]
    fn protected_call_adds_payoff_and_perp_hedge() {
        let report = run_protected_option(
            Decimal::from(110),
            Decimal::from(100),
            Decimal::from(3),
            Decimal::ONE,
            Decimal::from(-1),
            Decimal::from(100),
            Decimal::from(110),
            Decimal::from(1_000),
        );
        assert_eq!(report["payoff"], "10");
        assert_eq!(report["perp_pnl"], "-10");
        assert_eq!(report["ending_balance"], "997");
        assert_eq!(report["venue"], "backtest");
    }

    #[test]
    fn candidate_report_stays_offline() {
        let report = candidate_report("ema", Decimal::from(10));
        assert_eq!(report["status"], "completed");
        assert_eq!(report["exchange_orders"], 0);
    }
}
