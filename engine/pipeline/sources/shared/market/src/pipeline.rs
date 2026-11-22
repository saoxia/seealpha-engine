use crate::{Bar, BarFeature, MarketError, PublicTrade};
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::BTreeMap;

pub fn interval_ms(interval: &str) -> Result<i64, MarketError> {
    // Source chooses supported Binance REST periods; Catalog owns UTC durations.
    if !matches!(interval, "1m" | "3m" | "5m" | "15m" | "30m" | "1h" | "4h" | "1d") {
        return Err(MarketError::Interval);
    }
    seealpha_catalog::interval_ms(interval)
        .map_err(|_| MarketError::Interval)?
        .ok_or(MarketError::Interval)
}

pub fn parse_binance_klines(value: &Value, interval_ms: i64) -> Result<Vec<Bar>, MarketError> {
    let rows = value.as_array().ok_or(MarketError::Payload)?;
    let mut bars = Vec::with_capacity(rows.len());
    for row in rows {
        let columns = row.as_array().ok_or(MarketError::Payload)?;
        if columns.len() < 7 {
            return Err(MarketError::Payload);
        }
        let open_time_ms = json_i64(&columns[0])?;
        let close_time_ms = json_i64(&columns[6])?;
        bars.push(Bar {
            open_time_ms,
            close_time_ms,
            open: json_decimal(&columns[1])?,
            high: json_decimal(&columns[2])?,
            low: json_decimal(&columns[3])?,
            close: json_decimal(&columns[4])?,
            volume: json_decimal(&columns[5])?,
        });
    }
    validate_bars(&bars, interval_ms)?;
    Ok(bars)
}

pub fn validate_bars(bars: &[Bar], interval_ms: i64) -> Result<(), MarketError> {
    if bars.is_empty() {
        return Err(MarketError::Empty);
    }
    if interval_ms <= 0 {
        return Err(MarketError::Interval);
    }
    // Keep the source's contiguous, closed-bar checks without the previous
    // Decimal -> f64 -> quality-crate conversion.
    let mut previous: Option<i64> = None;
    for bar in bars {
        if !(0..100_000_000_000_000).contains(&bar.open_time_ms)
            || bar.open_time_ms % interval_ms != 0
        {
            return Err(MarketError::Time);
        }
        if let Some(before) = previous
            && (bar.open_time_ms <= before
                || bar.open_time_ms.checked_sub(before) != Some(interval_ms))
        {
            return Err(MarketError::Time);
        }
        if bar.volume < Decimal::ZERO
            || bar.low > bar.high
            || bar.low > bar.open
            || bar.low > bar.close
            || bar.high < bar.open
            || bar.high < bar.close
        {
            return Err(MarketError::Ohlc);
        }
        let expected_close = bar
            .open_time_ms
            .checked_add(interval_ms)
            .and_then(|time| time.checked_sub(1))
            .ok_or(MarketError::Time)?;
        if bar.close_time_ms != expected_close {
            return Err(MarketError::Time);
        }
        previous = Some(bar.open_time_ms);
    }
    Ok(())
}

/// Audit public trade identities without assuming that missing IDs can be
/// reconstructed from one recent REST window. Identical overlaps are safe;
/// conflicting reuse of an ID is an integrity failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradeIdAudit {
    pub unique_count: usize,
    pub suspected_gap: bool,
    pub input_out_of_order: bool,
}
pub fn audit_trade_ids(trades: &[PublicTrade]) -> Result<TradeIdAudit, MarketError> {
    let mut seen = BTreeMap::new();
    let mut input_out_of_order = false;
    let mut last_time_id = None;
    for trade in trades {
        if trade.id < 0
            || trade.timestamp_ms < 0
            || trade.price <= Decimal::ZERO
            || trade.quantity <= Decimal::ZERO
        {
            return Err(MarketError::Trade);
        }
        let key = (trade.timestamp_ms, trade.id);
        if last_time_id.is_some_and(|previous| key < previous) {
            input_out_of_order = true;
        }
        last_time_id = Some(key);
        if let Some(previous) = seen.insert(trade.id, *trade)
            && previous != *trade
        {
            return Err(MarketError::Trade);
        }
    }
    let ids: Vec<_> = seen.keys().copied().collect();
    let suspected_gap = ids
        .windows(2)
        .any(|pair| pair[0].checked_add(1) != Some(pair[1]));
    Ok(TradeIdAudit {
        unique_count: seen.len(),
        suspected_gap,
        input_out_of_order,
    })
}

pub fn aggregate_trades(trades: &[PublicTrade], interval_ms: i64) -> Result<Vec<Bar>, MarketError> {
    if interval_ms <= 0 {
        return Err(MarketError::Interval);
    }
    if trades.is_empty() {
        return Ok(Vec::new());
    }
    audit_trade_ids(trades)?;
    let mut unique = BTreeMap::new();
    for trade in trades {
        unique.insert(trade.id, *trade);
    }
    let mut ordered: Vec<_> = unique.into_values().collect();
    ordered.sort_by_key(|trade| (trade.timestamp_ms, trade.id));
    let mut bars: Vec<Bar> = Vec::new();
    for trade in ordered {
        if trade.price <= Decimal::ZERO || trade.quantity <= Decimal::ZERO {
            return Err(MarketError::Trade);
        }
        let bucket = trade
            .timestamp_ms
            .div_euclid(interval_ms)
            .checked_mul(interval_ms)
            .ok_or(MarketError::Overflow)?;
        if let Some(bar) = bars.last_mut()
            && bar.open_time_ms == bucket
        {
            if trade.price > bar.high {
                bar.high = trade.price;
            }
            if trade.price < bar.low {
                bar.low = trade.price;
            }
            bar.close = trade.price;
            bar.volume = bar
                .volume
                .checked_add(trade.quantity)
                .ok_or(MarketError::Overflow)?;
            continue;
        }
        let close_time_ms = bucket
            .checked_add(interval_ms)
            .and_then(|time| time.checked_sub(1))
            .ok_or(MarketError::Time)?;
        bars.push(Bar {
            open_time_ms: bucket,
            close_time_ms,
            open: trade.price,
            high: trade.price,
            low: trade.price,
            close: trade.price,
            volume: trade.quantity,
        });
    }
    Ok(bars)
}

pub fn bar_features(bars: &[Bar]) -> Result<Vec<BarFeature>, MarketError> {
    let mut features = Vec::with_capacity(bars.len());
    let mut previous: Option<Decimal> = None;
    for bar in bars {
        if bar.close.is_zero() {
            return Err(MarketError::Ohlc);
        }
        let range = bar.high.checked_sub(bar.low).ok_or(MarketError::Overflow)?;
        let range_ratio = range.checked_div(bar.close).ok_or(MarketError::Overflow)?;
        let simple_return = match previous {
            Some(prior) if !prior.is_zero() => Some(
                bar.close
                    .checked_div(prior)
                    .ok_or(MarketError::Overflow)?
                    .checked_sub(Decimal::ONE)
                    .ok_or(MarketError::Overflow)?,
            ),
            _ => None,
        };
        previous = Some(bar.close);
        features.push(BarFeature {
            open_time_ms: bar.open_time_ms,
            range_ratio,
            simple_return,
        });
    }
    Ok(features)
}

pub(crate) fn json_decimal(value: &Value) -> Result<Decimal, MarketError> {
    let text = match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        _ => return Err(MarketError::Decimal),
    };
    text.trim()
        .parse::<Decimal>()
        .map_err(|_| MarketError::Decimal)
}

fn json_i64(value: &Value) -> Result<i64, MarketError> {
    value.as_i64().ok_or(MarketError::Time)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn source_validation_rejects_internal_gaps_and_out_of_order_bars() {
        let bar = |open_time_ms| Bar {
            open_time_ms,
            close_time_ms: open_time_ms + 59_999,
            open: Decimal::from(100),
            high: Decimal::from(101),
            low: Decimal::from(99),
            close: Decimal::from(100),
            volume: Decimal::ONE,
        };
        assert!(validate_bars(&[bar(0), bar(60_000)], 60_000).is_ok());
        assert!(validate_bars(&[bar(0), bar(120_000)], 60_000).is_err());
        assert!(validate_bars(&[bar(60_000), bar(0)], 60_000).is_err());
        assert!(validate_bars(&[bar(0), bar(0)], 60_000).is_err());
    }

    #[test]
    fn source_validation_preserves_decimal_precision_and_ohlcv_checks() {
        let mut bar = Bar {
            open_time_ms: 0, close_time_ms: 59_999,
            open: "0.0000000000000000000000000001".parse().unwrap(),
            high: "0.0000000000000000000000000002".parse().unwrap(),
            low: "0.0000000000000000000000000001".parse().unwrap(),
            close: "0.0000000000000000000000000002".parse().unwrap(),
            volume: Decimal::ZERO,
        };
        assert!(validate_bars(&[bar], 60_000).is_ok());
        bar.low = "0.0000000000000000000000000003".parse().unwrap();
        assert!(validate_bars(&[bar], 60_000).is_err());
        bar.low = bar.open;
        bar.close_time_ms = 60_000;
        assert!(validate_bars(&[bar], 60_000).is_err());
    }

    #[test]
    fn source_validation_rejects_out_of_range_timestamp() {
        let bar = Bar {
            open_time_ms: 100_000_000_000_000,
            close_time_ms: 100_000_000_059_999,
            open: Decimal::ONE, high: Decimal::ONE,
            low: Decimal::ONE, close: Decimal::ONE, volume: Decimal::ZERO,
        };
        assert!(validate_bars(&[bar], 60_000).is_err());
    }

    #[test]
    fn parses_and_validates_a_closed_binance_kline() {
        let payload = json!([[
            0, "100", "110", "90", "105", "2", 59999, "0", 1, "0", "0", "0"
        ]]);
        let bars = parse_binance_klines(&payload, 60_000).unwrap();
        assert_eq!(bars[0].close, Decimal::new(105, 0));
        assert_eq!(
            bar_features(&bars).unwrap()[0].range_ratio,
            Decimal::new(20, 0) / Decimal::new(105, 0)
        );
    }

    #[test]
    fn aggregates_trades_into_ohlcv_buckets() {
        let trades = [
            trade(1, 1_000, "10", "1"),
            trade(2, 1_500, "12", "1"),
            trade(3, 61_000, "9", "2"),
        ];
        let bars = aggregate_trades(&trades, 60_000).unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].open, Decimal::from(10));
        assert_eq!(bars[0].high, Decimal::from(12));
        assert_eq!(bars[0].close, Decimal::from(12));
        assert_eq!(bars[0].volume, Decimal::from(2));
        assert_eq!(bars[1].volume, Decimal::from(2));
        assert_eq!(bars[0].close_time_ms, 59_999);
    }

    fn trade(id: i64, timestamp_ms: i64, price: &str, quantity: &str) -> PublicTrade {
        PublicTrade {
            id,
            timestamp_ms,
            price: price.parse().unwrap(),
            quantity: quantity.parse().unwrap(),
            buyer_maker: false,
        }
    }

    #[test]
    fn trade_overlap_is_idempotent_but_id_conflicts_are_rejected() {
        let a = trade(1, 100, "10", "2");
        let b = trade(3, 200, "12", "3");
        let audit = audit_trade_ids(&[b, a, a]).unwrap();
        assert_eq!(audit.unique_count, 2);
        assert!(audit.suspected_gap);
        assert!(audit.input_out_of_order);
        let bars = aggregate_trades(&[b, a, a], 60_000).unwrap();
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].volume, Decimal::from(5));
        let wrong = trade(1, 100, "99", "2");
        assert!(aggregate_trades(&[a, wrong], 60_000).is_err());
        assert!(aggregate_trades(&[trade(-1, 100, "1", "1")], 60_000).is_err());
    }

    #[test]
    fn closed_bars_require_aligned_nonnegative_open_times() {
        let payload = json!([[1, "1", "1", "1", "1", "0", 60_000]]);
        assert!(parse_binance_klines(&payload, 60_000).is_err());
        let negative = json!([[-60_000, "1", "1", "1", "1", "0", -1]]);
        assert!(parse_binance_klines(&negative, 60_000).is_err());
    }
}
