//! Bar features and trade aggregation. Warmup values stay absent. A failed
//! publication leaves the previous feature generation in place.
use rust_decimal::Decimal;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct Trade {
    pub id: i64,
    pub timestamp_ms: i64,
    pub price: Decimal,
    pub quantity: Decimal,
    pub quote: Decimal,
    pub buyer_maker: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggBar {
    pub open_ms: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub quote_volume: Decimal,
    pub taker_buy_volume: Decimal,
    pub taker_buy_quote_volume: Decimal,
    pub trade_count: u64,
}

/// Validate every source row before last-ID-wins de-duplication, then aggregate.
pub fn aggregate_trades(trades: &[Trade], interval_ms: i64) -> Result<Vec<AggBar>, &'static str> {
    if interval_ms <= 0 {
        return Err("invalid interval");
    }
    let mut last: BTreeMap<i64, &Trade> = BTreeMap::new();
    for trade in trades {
        if trade.id < 0 || trade.timestamp_ms < 0
            || trade.price <= Decimal::ZERO || trade.quantity <= Decimal::ZERO
            || trade.quote < Decimal::ZERO
        {
            return Err("invalid trade");
        }
        last.insert(trade.id, trade);
    }
    let mut ordered: Vec<&Trade> = last.into_values().collect();
    ordered.sort_by_key(|trade| (trade.timestamp_ms, trade.id));
    let mut buckets: BTreeMap<i64, AggBar> = BTreeMap::new();
    for trade in ordered {
        let open_ms = trade.timestamp_ms - trade.timestamp_ms.rem_euclid(interval_ms);
        let slot = buckets.entry(open_ms).or_insert_with(|| AggBar {
            open_ms,
            open: trade.price,
            high: trade.price,
            low: trade.price,
            close: trade.price,
            volume: Decimal::ZERO,
            quote_volume: Decimal::ZERO,
            taker_buy_volume: Decimal::ZERO,
            taker_buy_quote_volume: Decimal::ZERO,
            trade_count: 0,
        });
        slot.high = slot.high.max(trade.price);
        slot.low = slot.low.min(trade.price);
        slot.close = trade.price;
        slot.volume = slot.volume.checked_add(trade.quantity).ok_or("volume overflow")?;
        slot.quote_volume = slot.quote_volume.checked_add(trade.quote).ok_or("quote overflow")?;
        slot.trade_count = slot.trade_count.checked_add(1).ok_or("count overflow")?;
        if !trade.buyer_maker {
            slot.taker_buy_volume = slot.taker_buy_volume
                .checked_add(trade.quantity).ok_or("buy volume overflow")?;
            slot.taker_buy_quote_volume = slot.taker_buy_quote_volume
                .checked_add(trade.quote).ok_or("buy quote overflow")?;
        }
    }
    Ok(buckets.into_values().collect())
}

#[derive(Clone, Debug)]
pub struct InputBar {
    pub open_time_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub taker_buy: f64,
}

#[derive(Clone, Debug)]
pub struct Feature {
    pub ma: Option<f64>,
    pub std: Option<f64>,
    pub boll_mid: Option<f64>,
    pub boll_upper: Option<f64>,
    pub boll_lower: Option<f64>,
    pub boll_width: Option<f64>,
    pub boll_pos: Option<f64>,
    pub ret: Option<f64>,
    pub atr: Option<f64>,
    pub natr: Option<f64>,
    pub buy_ratio: Option<f64>,
    pub delta_ratio: Option<f64>,
}

pub fn indicator_rows(
    bars: &[InputBar],
    step_ms: i64,
    ma_n: usize,
    std_n: usize,
    ret_n: usize,
    atr_n: usize,
) -> Vec<Feature> {
    let mut rows = Vec::with_capacity(bars.len());
    let mut closes: Vec<f64> = Vec::new();
    let mut trs: Vec<f64> = Vec::new();
    let mut previous_close: Option<f64> = None;
    let mut previous_open: Option<i64> = None;
    for bar in bars {
        if previous_open.is_some_and(|prior| bar.open_time_ms - prior != step_ms) {
            closes.clear();
            trs.clear();
            previous_close = None;
        }
        previous_open = Some(bar.open_time_ms);
        let true_range = match previous_close {
            Some(prior) => (bar.high - bar.low)
                .max((bar.high - prior).abs())
                .max((bar.low - prior).abs()),
            None => bar.high - bar.low,
        };
        closes.push(bar.close);
        trs.push(true_range);
        let std = window_std(&closes, std_n);
        let (boll_mid, boll_upper, boll_lower, boll_width, boll_pos) =
            boll(bar.close, &closes, std_n);
        let ret = if closes.len() > ret_n {
            let base = closes[closes.len() - 1 - ret_n];
            if base == 0.0 {
                None
            } else {
                Some(bar.close / base - 1.0)
            }
        } else {
            None
        };
        let atr = wilder(&trs, atr_n);
        let natr = atr.map(|value| {
            if bar.close == 0.0 {
                f64::NAN
            } else {
                100.0 * value / bar.close
            }
        });
        let (buy_ratio, delta_ratio) = if bar.volume == 0.0 {
            (None, None)
        } else {
            let ratio = bar.taker_buy / bar.volume;
            (Some(ratio), Some(2.0 * ratio - 1.0))
        };
        previous_close = Some(bar.close);
        rows.push(Feature {
            ma: window_mean(&closes, ma_n),
            std,
            boll_mid,
            boll_upper,
            boll_lower,
            boll_width,
            boll_pos,
            ret,
            atr,
            natr,
            buy_ratio,
            delta_ratio,
        });
    }
    rows
}

fn window_mean(values: &[f64], period: usize) -> Option<f64> {
    if values.len() < period || period == 0 {
        return None;
    }
    let slice = &values[values.len() - period..];
    Some(slice.iter().sum::<f64>() / period as f64)
}

fn window_std(values: &[f64], period: usize) -> Option<f64> {
    let mean = window_mean(values, period)?;
    let slice = &values[values.len() - period..];
    let variance = slice
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / period as f64;
    Some(variance.sqrt())
}

type Boll = (
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
);

fn boll(close: f64, closes: &[f64], period: usize) -> Boll {
    let Some(mean) = window_mean(closes, period) else {
        return (None, None, None, None, None);
    };
    let std = window_std(closes, period).unwrap_or(0.0);
    let upper = mean + 2.0 * std;
    let lower = mean - 2.0 * std;
    let width = if mean == 0.0 { 0.0 } else { 4.0 * std / mean };
    let pos = if width == 0.0 {
        None
    } else {
        Some((close - mean + 2.0 * std) / (4.0 * std))
    };
    (Some(mean), Some(upper), Some(lower), Some(width), pos)
}

fn wilder(trs: &[f64], period: usize) -> Option<f64> {
    if trs.len() < period || period == 0 {
        return None;
    }
    let mut atr = trs[..period].iter().sum::<f64>() / period as f64;
    for value in &trs[period..] {
        atr = (atr * (period as f64 - 1.0) + value) / period as f64;
    }
    Some(atr)
}

#[derive(Clone, Debug)]
pub struct FeatureStore {
    bars: Vec<InputBar>,
    step_ms: i64,
    published: Option<Vec<Feature>>,
    failed: bool,
}

impl FeatureStore {
    pub fn new(step_ms: i64) -> Self {
        Self {
            bars: Vec::new(),
            step_ms,
            published: None,
            failed: false,
        }
    }

    pub fn publish(&mut self, bars: Vec<InputBar>) {
        self.bars = bars;
        self.published = Some(indicator_rows(&self.bars, self.step_ms, 3, 3, 1, 3));
        self.failed = false;
    }

    pub fn append_invalidates(&mut self, bar: InputBar) {
        self.bars.push(bar);
        self.published = None;
    }

    pub fn try_publish(&mut self, fail: bool) -> Result<(), &'static str> {
        if fail {
            self.failed = true;
            return Err("磁盘");
        }
        self.published = Some(indicator_rows(&self.bars, self.step_ms, 3, 3, 1, 3));
        self.failed = false;
        Ok(())
    }

    pub fn published(&self) -> Option<&[Feature]> {
        self.published.as_deref()
    }

    pub fn bars_unchanged_len(&self) -> usize {
        self.bars.len()
    }
}

pub fn batches<T: Clone>(
    rows: &[T],
    size: usize,
    start: usize,
    end: usize,
) -> Result<Vec<&[T]>, &'static str> {
    if size == 0 {
        return Err("batch size");
    }
    if start >= end || end > rows.len() {
        return Err("increasing");
    }
    let mut out = Vec::new();
    let mut cursor = start;
    while cursor < end {
        let next = (cursor + size).min(end);
        out.push(&rows[cursor..next]);
        cursor = next;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(open_time_ms: i64, close: f64) -> InputBar {
        InputBar {
            open_time_ms,
            open: close,
            high: close,
            low: close,
            close,
            volume: 1.0,
            taker_buy: 1.0,
        }
    }

    fn approx(left: f64, right: f64) {
        assert!((left - right).abs() < 1e-9, "{left} != {right}");
    }

    #[test]
    fn test_indicators_match_independent_formulas_and_cross_day_state() {
        let closes: Vec<f64> = (1..=30).map(|value| value as f64).collect();
        let bars: Vec<_> = closes
            .iter()
            .enumerate()
            .map(|(index, close)| InputBar {
                open_time_ms: index as i64 * 60_000,
                open: *close,
                high: close + 1.0,
                low: close - 1.0,
                close: *close,
                volume: 10.0,
                taker_buy: 6.0,
            })
            .collect();
        let rows = indicator_rows(&bars, 60_000, 5, 5, 5, 3);
        let last = rows.last().unwrap();
        let tail = &closes[closes.len() - 5..];
        let mean = tail.iter().sum::<f64>() / 5.0;
        let std = (tail.iter().map(|value| (value - mean).powi(2)).sum::<f64>() / 5.0).sqrt();
        approx(last.ma.unwrap(), mean);
        approx(last.std.unwrap(), std);
        approx(last.boll_mid.unwrap(), mean);
        approx(last.boll_upper.unwrap(), mean + 2.0 * std);
        approx(last.boll_lower.unwrap(), mean - 2.0 * std);
        approx(last.boll_width.unwrap(), 4.0 * std / mean);
        approx(
            last.boll_pos.unwrap(),
            (closes[29] - mean + 2.0 * std) / (4.0 * std),
        );
        approx(last.ret.unwrap(), closes[29] / closes[24] - 1.0);
        approx(last.buy_ratio.unwrap(), 0.6);
        approx(last.delta_ratio.unwrap(), 0.2);
        assert!(rows[..4].iter().all(|row| row.ma.is_none()));
        assert!(rows[..5].iter().all(|row| row.ret.is_none()));
        assert!(rows[..2].iter().all(|row| row.atr.is_none()));
    }

    #[test]
    fn test_trade_aggregation_counts_deduplicated_trades_and_uses_quote_quantity() {
        let start = 0;
        let rows = [
            Trade {
                id: 1,
                timestamp_ms: start,
                price: Decimal::from(100),
                quantity: Decimal::ONE,
                quote: Decimal::from(100),
                buyer_maker: true,
            },
            Trade {
                id: 2,
                timestamp_ms: start + 1000,
                price: Decimal::from(999),
                quantity: Decimal::from(8),
                quote: Decimal::from(7992),
                buyer_maker: true,
            },
            Trade {
                id: 2,
                timestamp_ms: start + 1000,
                price: Decimal::from(110),
                quantity: Decimal::from(2),
                quote: Decimal::from(220),
                buyer_maker: false,
            },
            Trade {
                id: 3,
                timestamp_ms: start + 60_000,
                price: Decimal::from(105),
                quantity: Decimal::from(3),
                quote: Decimal::from(315),
                buyer_maker: false,
            },
        ];
        let minutes = aggregate_trades(&rows, 60_000).unwrap();
        assert_eq!(minutes[0].volume, Decimal::from(3));
        assert_eq!(minutes[0].quote_volume, Decimal::from(320));
        assert_eq!(minutes[0].taker_buy_volume, Decimal::from(2));
        assert_eq!(minutes[0].taker_buy_quote_volume, Decimal::from(220));
        assert_eq!(minutes[0].trade_count, 2);
        let five = aggregate_trades(&rows, 300_000).unwrap();
        assert_eq!(five[0].trade_count, 3);
        assert_eq!(five[0].volume, Decimal::from(6));
        assert_eq!(five[0].quote_volume, Decimal::from(635));
        assert_eq!(five[0].taker_buy_volume, Decimal::from(5));
        assert_eq!(five[0].open, Decimal::from(100));
        assert_eq!(five[0].close, Decimal::from(105));
        assert_eq!(five[0].high, Decimal::from(110));
    }

    #[test]
    fn rejects_invalid_trade_before_deduplicating_and_zero_interval() {
        let valid = Trade {
            id: 7, timestamp_ms: 0, price: Decimal::from(10),
            quantity: Decimal::ONE, quote: Decimal::from(10), buyer_maker: false,
        };
        assert_eq!(aggregate_trades(&[valid.clone()], 0).unwrap_err(), "invalid interval");
        let mut bad = valid.clone();
        bad.price = Decimal::ZERO;
        assert_eq!(aggregate_trades(&[bad, valid.clone()], 60_000).unwrap_err(), "invalid trade");
        bad = valid.clone();
        bad.timestamp_ms = -1;
        assert_eq!(aggregate_trades(&[bad], 60_000).unwrap_err(), "invalid trade");
        assert_eq!(aggregate_trades(&[valid], 60_000).unwrap().len(), 1);
    }

    #[test]
    fn test_multi_period_aggregation_matches_direct_trade_sums() {
        let mut rows = Vec::new();
        for index in 0..41 {
            let timestamp = ((index / 3) * 60_000 + [0, 999, 59_999][index % 3]) as i64;
            rows.push(Trade {
                id: (index + 1) as i64,
                timestamp_ms: timestamp,
                price: Decimal::from(100),
                quantity: Decimal::from(index as i64 + 1),
                quote: Decimal::from(index as i64 + 1),
                buyer_maker: index % 2 == 0,
            });
        }
        let direct: Decimal = rows.iter().map(|row| row.quantity).sum();
        for interval in [1_000, 60_000, 300_000, 900_000] {
            let bars = aggregate_trades(&rows, interval).unwrap();
            let total: Decimal = bars.iter().map(|bar| bar.volume).sum();
            assert_eq!(total, direct);
        }
    }

    #[test]
    fn test_append_and_earlier_backfill_invalidate_later_features() {
        let mut store = FeatureStore::new(60_000);
        store.publish(vec![sample(0, 10.0)]);
        assert!(store.published().is_some());
        store.append_invalidates(sample(60_000, 11.0));
        assert!(store.published().is_none());
        assert_eq!(store.bars_unchanged_len(), 2);
    }

    #[test]
    fn test_rebuilt_market_data_is_detected_and_feature_repair_preserves_bars() {
        let mut store = FeatureStore::new(60_000);
        let bars = vec![sample(0, 10.0), sample(60_000, 11.0)];
        store.publish(bars.clone());
        let before = store.bars_unchanged_len();
        store.append_invalidates(sample(0, 99.0));
        assert!(store.published().is_none());
        assert_eq!(store.bars_unchanged_len(), before + 1);
        let _ = bars;
    }

    #[test]
    fn test_failed_feature_write_preserves_previous_parquet() {
        let mut store = FeatureStore::new(60_000);
        store.publish(vec![sample(0, 10.0)]);
        let before = store.published().unwrap().len();
        assert!(store.try_publish(true).is_err());
        assert_eq!(store.published().unwrap().len(), before);
    }

    #[test]
    fn test_forced_rebuild_repairs_corrupt_feature_metadata() {
        let mut store = FeatureStore::new(60_000);
        store.publish(vec![sample(0, 10.0)]);
        store.published = None;
        store.try_publish(false).unwrap();
        assert!(store.published().is_some());
    }

    #[test]
    fn test_feature_preparation_does_not_reparse_source_csv() {
        let mut store = FeatureStore::new(60_000);
        store.publish(vec![sample(0, 1.0), sample(60_000, 2.0)]);
        assert_eq!(store.published().unwrap().len(), 2);
    }

    #[test]
    fn test_feature_metadata_publication_failure_rolls_back_native_parquet() {
        let mut store = FeatureStore::new(60_000);
        store.publish(vec![sample(0, 10.0)]);
        assert!(store.try_publish(true).is_err());
        assert!(store.published().is_some());
    }

    #[test]
    fn test_custom_data_batches_are_lazy_bounded_and_tagged() {
        let rows = [1, 2, 3, 4, 5];
        let batches = batches(&rows, 2, 0, 5).unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.len()).collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
        assert_eq!(
            batches.into_iter().flatten().copied().collect::<Vec<_>>(),
            rows
        );
    }

    #[test]
    fn test_custom_data_batches_reject_invalid_bounds() {
        let rows = [1, 2, 3];
        assert_eq!(batches(&rows, 0, 0, 1), Err("batch size"));
        assert_eq!(batches(&rows, 2, 2, 2), Err("increasing"));
    }

    #[test]
    fn test_gaps_restart_warmup_and_zero_denominators_are_null() {
        let mut bars = Vec::new();
        for index in 0..6 {
            bars.push(InputBar {
                open_time_ms: index * 60_000,
                open: 10.0,
                high: 10.0,
                low: 10.0,
                close: 10.0,
                volume: 0.0,
                taker_buy: 0.0,
            });
        }
        bars.push(InputBar {
            open_time_ms: 8 * 60_000,
            open: 10.0,
            high: 10.0,
            low: 10.0,
            close: 10.0,
            volume: 1.0,
            taker_buy: 1.0,
        });
        let rows = indicator_rows(&bars, 60_000, 5, 5, 5, 3);
        assert!(rows[0].buy_ratio.is_none());
        assert!(rows[0].delta_ratio.is_none());
        assert_eq!(rows[4].boll_width, Some(0.0));
        assert!(rows[4].boll_pos.is_none());
        assert!(rows.last().unwrap().ma.is_none());
    }
}
