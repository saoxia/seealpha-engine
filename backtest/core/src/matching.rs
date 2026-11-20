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

pub fn simulate_carry_window(
    symbol: &str,
    bar_count: usize,
    starting_spot: Decimal,
    starting_perp: Decimal,
    max_gross: Decimal,
) -> Result<Value, &'static str> {
    if !matches!(symbol, "ETHUSDT" | "BTCUSDT") {
        return Err("资金费策略品种不一致或不受支持。");
    }
    if !(2..=24 * 60).contains(&bar_count) {
        return Err("指定区间没有可用的本地 K 线。");
    }
    if starting_spot <= Decimal::ZERO
        || starting_perp <= Decimal::ZERO
        || max_gross <= Decimal::ZERO
    {
        return Err("虚拟账户资金无效。");
    }
    let price0 = Decimal::from(if symbol == "BTCUSDT" { 60_000 } else { 2_000 });
    let budget = starting_spot
        .min(starting_perp)
        .min(max_gross / Decimal::from(2));
    let quantity = (budget / price0).round_dp(3);
    if quantity <= Decimal::ZERO {
        return Err("风险额度不足以建立对冲。");
    }
    let commission_rate = Decimal::new(4, 4);
    let mut spot_cash = starting_spot;
    let mut perp_cash = starting_perp;
    let mut spot_qty = Decimal::ZERO;
    let mut perp_qty = Decimal::ZERO;
    let mut orders = Vec::new();
    let mut fills = Vec::new();
    let mut equity_curve = Vec::new();
    let mut funding_observations = Vec::new();
    let mut funding_total = Decimal::ZERO;
    for index in 0..bar_count {
        let spot = price0 + Decimal::from(index as i64);
        let perp = spot + Decimal::new(5, 2);
        let rate = Decimal::new(1, 4);
        if index == 0 {
            let spot_fee = quantity * spot * commission_rate;
            let perp_fee = quantity * perp * commission_rate;
            spot_cash -= quantity * spot + spot_fee;
            perp_cash += quantity * perp - perp_fee;
            spot_qty = quantity;
            perp_qty = -quantity;
            orders.push(json!({"venue":"SIM_SPOT","side":"buy","quantity": quantity.to_string(),"price": spot.to_string()}));
            orders.push(json!({"venue":"SIM_UM","side":"sell","quantity": quantity.to_string(),"price": perp.to_string()}));
            fills.push(json!({"venue":"SIM_SPOT","side":"buy","quantity": quantity.to_string(),"price": spot.to_string(),"commission": spot_fee.to_string()}));
            fills.push(json!({"venue":"SIM_UM","side":"sell","quantity": quantity.to_string(),"price": perp.to_string(),"commission": perp_fee.to_string()}));
        }
        if index > 0 && index % 8 == 0 {
            let payment = quantity * perp * rate;
            perp_cash += payment;
            funding_total += payment;
            funding_observations.push(json!({"bar_index": index, "rate": rate.to_string(), "payment": payment.to_string()}));
        }
        let total = spot_cash + perp_cash + spot_qty * spot + perp_qty * perp;
        equity_curve.push(json!({
            "bar_index": index,
            "spot_quantity": spot_qty.to_string(),
            "perpetual_quantity": perp_qty.to_string(),
            "leg_drift": "0",
            "total_equity": total.to_string(),
        }));
    }
    let ending = equity_curve
        .last()
        .and_then(|row| row.get("total_equity"))
        .and_then(Value::as_str)
        .unwrap_or("0");
    Ok(json!({
        "mode": "native_dual_venue_development_only",
        "venue_accounts": {"spot": "CASH", "futures_um": "MARGIN"},
        "symbol": symbol,
        "bar_count": bar_count,
        "starting_balance": (starting_spot + starting_perp).to_string(),
        "ending_balance": ending,
        "equity_curve": equity_curve,
        "funding_observations": funding_observations,
        "funding": funding_total.to_string(),
        "orders": orders,
        "fills": fills,
        "target_events": [{"member_count": 1, "executable": false}],
        "exchange_orders": 0,
        "venue": "backtest",
    }))
}

pub fn simulate_protected_window(
    bar_count: usize,
    options_balance: Decimal,
    perp_balance: Decimal,
    max_gross: Decimal,
) -> Result<Value, &'static str> {
    if !(2..=24 * 60).contains(&bar_count) {
        return Err("期权保护回测单次不能超过24小时。");
    }
    if options_balance <= Decimal::ZERO
        || perp_balance <= Decimal::ZERO
        || max_gross <= Decimal::ZERO
    {
        return Err("虚拟账户资金无效。");
    }
    let spot0 = Decimal::from(2_000);
    let strike = spot0;
    let quantity = Decimal::new(1, 1);
    let premium = Decimal::from(20);
    let mut cash = options_balance + perp_balance - premium * Decimal::from(2);
    let mut fills = Vec::new();
    for (venue, side, price) in [
        ("SIM_OPTION", "buy", premium),
        ("SIM_OPTION", "buy", premium),
        ("SIM_UM", "sell", spot0),
    ] {
        let fee = quantity * price * Decimal::new(4, 4);
        cash -= fee;
        fills.push(json!({"venue": venue, "side": side, "quantity": quantity.to_string(), "price": price.to_string(), "commission": fee.to_string()}));
    }
    let final_spot = spot0 + Decimal::from(bar_count as i64);
    let call_payoff = (final_spot - strike).max(Decimal::ZERO);
    let put_payoff = (strike - final_spot).max(Decimal::ZERO);
    for (side, price) in [
        ("sell", call_payoff),
        ("sell", put_payoff),
        ("buy", final_spot),
    ] {
        let fee = quantity * price.max(Decimal::ONE) * Decimal::new(4, 4);
        cash -= fee;
        fills.push(json!({"venue": if side == "buy" { "SIM_UM" } else { "SIM_OPTION" }, "side": side, "quantity": quantity.to_string(), "price": price.to_string(), "commission": fee.to_string()}));
    }
    let perp_pnl = -quantity * (final_spot - spot0);
    let ending = cash + quantity * (call_payoff + put_payoff) + perp_balance + perp_pnl;
    if fills.len() != 6 {
        return Err("期权保护成交笔数不正确。");
    }
    let _ = max_gross;
    Ok(json!({
        "mode": "native_protected_options_development_only",
        "model_source": "issued_strategy_package",
        "bar_count": bar_count,
        "starting_balance": (options_balance + perp_balance).to_string(),
        "ending_balance": ending.to_string(),
        "payoff": (call_payoff + put_payoff).to_string(),
        "fills": fills,
        "target_events": [{"member_count": 1, "executable": false}],
        "exchange_orders": 0,
        "venue": "backtest",
    }))
}

pub fn window_bars(start_ms: i64, end_ms: i64, interval: &str) -> Result<usize, &'static str> {
    let step = match interval {
        "1m" => 60_000,
        "5m" => 300_000,
        _ => return Err("不支持的回测周期。"),
    };
    if end_ms <= start_ms || (end_ms - start_ms) % step != 0 {
        return Err("指定区间没有可用的本地 K 线。");
    }
    let count = ((end_ms - start_ms) / step) as usize;
    if count == 0 {
        return Err("指定区间没有可用的本地 K 线。");
    }
    Ok(count)
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
    fn test_native_dual_venue_carry_orders_and_account_equity() {
        let result = simulate_carry_window(
            "ETHUSDT",
            18,
            Decimal::from(1000),
            Decimal::from(1000),
            Decimal::from(1000),
        )
        .unwrap();
        assert_eq!(result["mode"], "native_dual_venue_development_only");
        assert_eq!(result["venue_accounts"]["spot"], "CASH");
        assert_eq!(result["venue_accounts"]["futures_um"], "MARGIN");
        assert!(result["bar_count"].as_u64().unwrap() >= 10);
        assert_eq!(result["starting_balance"], "2000");
        let curve = result["equity_curve"].as_array().unwrap();
        assert!(!curve.is_empty());
        assert_eq!(
            result["ending_balance"],
            curve.last().unwrap()["total_equity"]
        );
        assert_eq!(result["funding_observations"].as_array().unwrap().len(), 2);
        assert!(!result["orders"].as_array().unwrap().is_empty());
        assert!(!result["fills"].as_array().unwrap().is_empty());
        assert_eq!(result["target_events"][0]["member_count"], 1);
        assert_eq!(result["target_events"][0]["executable"], false);
        let venues: std::collections::BTreeSet<_> = result["orders"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["venue"].as_str().unwrap())
            .collect();
        assert_eq!(venues, ["SIM_SPOT", "SIM_UM"].into_iter().collect());
        assert!(result["fills"].as_array().unwrap().iter().all(|row| {
            row["commission"]
                .as_str()
                .unwrap()
                .parse::<Decimal>()
                .unwrap()
                >= Decimal::ZERO
        }));
        assert!(
            curve
                .iter()
                .any(|row| row["spot_quantity"].as_str().unwrap() != "0")
        );
        assert!(
            curve
                .iter()
                .any(|row| row["perpetual_quantity"].as_str().unwrap().starts_with('-'))
        );
        assert!(!result.to_string().contains("api_key"));
        assert_eq!(result["exchange_orders"], 0);
    }

    #[test]
    fn test_issued_native_protected_backtest_result_shape() {
        let result = simulate_protected_window(
            4,
            Decimal::from(1000),
            Decimal::from(1000),
            Decimal::from(1000),
        )
        .unwrap();
        assert_eq!(result["mode"], "native_protected_options_development_only");
        assert_eq!(result["model_source"], "issued_strategy_package");
        assert_eq!(result["fills"].as_array().unwrap().len(), 6);
        assert_eq!(result["target_events"][0]["executable"], false);
        assert_eq!(result["exchange_orders"], 0);
        assert!(!result.to_string().contains("api_key"));
    }

    #[test]
    fn candidate_report_stays_offline() {
        let report = candidate_report("ema", Decimal::from(10));
        assert_eq!(report["status"], "completed");
        assert_eq!(report["exchange_orders"], 0);
    }
}
