use crate::{Bar, BarFeature, MarketError, PublicTrade};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use seealpha_quality::{self, Kline};
use serde_json::Value;

pub fn interval_ms(interval: &str) -> Result<i64, MarketError> {
    Ok(match interval {
        "1m" => 60_000,
        "3m" => 180_000,
        "5m" => 300_000,
        "15m" => 900_000,
        "30m" => 1_800_000,
        "1h" => 3_600_000,
        "4h" => 14_400_000,
        "1d" => 86_400_000,
        _ => return Err(MarketError::Interval),
    })
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
    let mut projected = Vec::with_capacity(bars.len());
    for bar in bars {
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
        projected.push(Kline {
            open_time_ms: bar.open_time_ms,
            close_time_ms: bar.close_time_ms,
            open: dec_f64(bar.open)?,
            high: dec_f64(bar.high)?,
            low: dec_f64(bar.low)?,
            close: dec_f64(bar.close)?,
            volume: dec_f64(bar.volume)?,
        });
    }
    seealpha_quality::validate_klines(&projected, interval_ms)
        .map_err(|error| MarketError::Quality(error.to_string()))
}

pub fn aggregate_trades(trades: &[PublicTrade], interval_ms: i64) -> Result<Vec<Bar>, MarketError> {
    if interval_ms <= 0 {
        return Err(MarketError::Interval);
    }
    if trades.is_empty() {
        return Ok(Vec::new());
    }
    let mut ordered = trades.to_vec();
    ordered.sort_by_key(|trade| (trade.timestamp_ms, trade.id));
    let mut bars: Vec<Bar> = Vec::new();
    for trade in ordered {
        if trade.price <= Decimal::ZERO || trade.quantity <= Decimal::ZERO {
            return Err(MarketError::Trade);
        }
        let bucket = trade
            .timestamp_ms
            .div_euclid(interval_ms)
            .saturating_mul(interval_ms);
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

fn dec_f64(value: Decimal) -> Result<f64, MarketError> {
    value
        .to_f64()
        .filter(|number| number.is_finite())
        .ok_or(MarketError::Decimal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
}
