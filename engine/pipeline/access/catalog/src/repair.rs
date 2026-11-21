//! Strict UTC bar-gap planner extracted from the legacy Python repair quality rules.
//! Pure, bounded and read-only: missing windows must never be silently filled.
use crate::{BarGap, CatalogError};

pub fn missing_bar_windows(
    opens_ms: &[i64],
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<Vec<BarGap>, CatalogError> {
    if start_ms < 0 || step_ms <= 0 || end_ms <= start_ms || (end_ms - start_ms) % step_ms != 0 {
        return Err(CatalogError::Invalid(
            "历史修复范围必须非空且按周期对齐。".into(),
        ));
    }
    let count = (end_ms - start_ms) / step_ms;
    if count > 50_000 || opens_ms.len() > 50_000 {
        return Err(CatalogError::Limit("历史修复缺口规划超出单次上限。".into()));
    }
    let mut missing = Vec::new();
    let mut expected = start_ms;
    for &open in opens_ms {
        if open < expected || open < start_ms || open >= end_ms || (open - start_ms) % step_ms != 0
        {
            return Err(CatalogError::Invalid(
                "历史 K 线时间重复、倒退或不对齐。".into(),
            ));
        }
        if open > expected {
            missing.push(BarGap {
                start_ms: expected,
                end_ms: open,
            });
        }
        expected = open
            .checked_add(step_ms)
            .ok_or_else(|| CatalogError::Invalid("历史时间溢出。".into()))?;
    }
    if expected < end_ms {
        missing.push(BarGap {
            start_ms: expected,
            end_ms,
        });
    }
    Ok(missing)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_middle_and_tail_gaps_use_half_open_windows() {
        assert_eq!(
            missing_bar_windows(&[0, 60_000, 180_000, 240_000], 0, 360_000, 60_000).unwrap(),
            vec![
                BarGap {
                    start_ms: 120_000,
                    end_ms: 180_000
                },
                BarGap {
                    start_ms: 300_000,
                    end_ms: 360_000
                },
            ]
        );
        assert_eq!(
            missing_bar_windows(&[], 0, 120_000, 60_000).unwrap(),
            vec![BarGap {
                start_ms: 0,
                end_ms: 120_000
            }]
        );
    }

    #[test]
    fn fail_closed_on_duplicates_misalignment_and_bounds() {
        assert!(missing_bar_windows(&[0, 0], 0, 120_000, 60_000).is_err());
        assert!(missing_bar_windows(&[10], 0, 120_000, 60_000).is_err());
        assert!(missing_bar_windows(&[120_000], 0, 120_000, 60_000).is_err());
        assert!(missing_bar_windows(&[], 1, 120_000, 60_000).is_err());
        assert!(missing_bar_windows(&[], 0, 120_000, 0).is_err());
    }
}

/// Native, read-only validation of exchange K-line evidence before repair.
/// A venue response must be contiguous, UTC-aligned and closed. Callers may
/// permit a leading unavailable prefix, but never an internal or trailing gap.
#[derive(Clone, Debug, PartialEq)]
pub struct VenueKline {
    pub bar: crate::CatalogBar,
    /// Exclusive close in UTC milliseconds (Binance close-time + 1).
    pub end_ms: i64,
    pub trade_count: i64,
}

pub fn validate_venue_klines(
    rows: &[VenueKline],
    interval: &str,
    start_ms: i64,
    end_ms: i64,
    available_ms: i64,
    product: crate::MarketProduct,
) -> Result<(), CatalogError> {
    if start_ms < 0 || end_ms <= start_ms || rows.is_empty() || rows.len() > 50_000 {
        return Err(CatalogError::Invalid(
            "交易所 K 线对照范围无效或为空。".into(),
        ));
    }
    let mut cursor = start_ms;
    let available_start = crate::bar_window(interval, available_ms)?.0;
    for (index, row) in rows.iter().enumerate() {
        let left = row.bar.open_time_ms;
        let (aligned_left, aligned_end) = crate::bar_window(interval, left)?;
        if aligned_left != left
            || row.end_ms != aligned_end
            || row.end_ms > end_ms
            || row.end_ms <= cursor
            || row.trade_count < 0
        {
            return Err(CatalogError::Invalid(
                "交易所 K 线边界或成交数量无效。".into(),
            ));
        }
        if left != cursor && !(index == 0 && cursor < available_ms && left == available_start) {
            return Err(CatalogError::Invalid("交易所 K 线存在缺口或乱序。".into()));
        }
        row.bar.validate(interval, product)?;
        cursor = row.end_ms;
    }
    if cursor != end_ms {
        return Err(CatalogError::Missing(
            "交易所 K 线对照缺少尾部数据。".into(),
        ));
    }
    Ok(())
}

/// Original saved Nautilus bar, with its exact close timestamp in nanoseconds.
#[derive(Clone, Debug, PartialEq)]
pub struct SavedKline {
    pub event_ns: i64,
    pub values: [rust_decimal::Decimal; 5],
}

/// Complete or partial OHLCV bucket built from the saved source bars.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregatedKline {
    pub start_ms: i64,
    pub end_ms: i64,
    pub values: [rust_decimal::Decimal; 5],
    pub complete: bool,
}

/// Replaces the legacy Python `local_klines` bucketing and completeness logic.
/// No data is written. Duplicate/out-of-order rows cannot be marked complete.
pub fn aggregate_saved_klines(
    saved: &[SavedKline],
    source_interval: &str,
    target_interval: &str,
) -> Result<Vec<AggregatedKline>, CatalogError> {
    use std::collections::BTreeMap;

    if saved.len() > 50_000 {
        return Err(CatalogError::Limit("本地 K 线聚合记录超出上限。".into()));
    }
    // Validate interval names even for empty saved catalogs.
    crate::interval_ms(source_interval)?;
    crate::interval_ms(target_interval)?;
    let mut buckets = BTreeMap::<i64, AggregatedKline>::new();
    for bar in saved {
        let (opening, closing) =
            crate::bar_window(source_interval, bar.event_ns.div_euclid(crate::NANO_PER_MS))?;
        let expected_event = closing
            .checked_sub(1)
            .and_then(|ms| ms.checked_mul(crate::NANO_PER_MS))
            .ok_or_else(|| CatalogError::Invalid("本地 K 线纳秒时间溢出。".into()))?;
        if bar.event_ns != expected_event {
            return Err(CatalogError::Invalid("已保存 K 线时间边界无效。".into()));
        }
        let (left, right) = crate::bar_window(target_interval, opening)?;
        if let Some(bucket) = buckets.get_mut(&left) {
            bucket.complete &= opening == bucket.end_ms;
            bucket.values[1] = bucket.values[1].max(bar.values[1]);
            bucket.values[2] = bucket.values[2].min(bar.values[2]);
            bucket.values[3] = bar.values[3];
            bucket.values[4] = bucket.values[4]
                .checked_add(bar.values[4])
                .ok_or_else(|| CatalogError::Invalid("本地 K 线成交量溢出。".into()))?;
            bucket.end_ms = closing;
        } else {
            buckets.insert(
                left,
                AggregatedKline {
                    start_ms: left,
                    end_ms: closing,
                    values: bar.values,
                    complete: opening == left && closing <= right,
                },
            );
        }
    }
    for (left, bucket) in &mut buckets {
        bucket.complete &= bucket.end_ms == crate::bar_window(target_interval, *left)?.1;
    }
    Ok(buckets.into_values().collect())
}

/// Read the Nautilus v2 Catalog directly in Rust before aggregating repair evidence.
/// Prefer the saved target interval; fall back to saved minutes only when no
/// target bars exist. Neither missing source data nor partial windows are filled.
pub fn read_aggregated_catalog_klines(
    catalog_root: &std::path::Path,
    symbol: &str,
    product: crate::MarketProduct,
    target_interval: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<AggregatedKline>, CatalogError> {
    if start_ms < 0 || end_ms <= start_ms {
        return Err(CatalogError::Invalid("本地 K 线读取区间无效。".into()));
    }
    let start_ns = start_ms
        .checked_mul(crate::NANO_PER_MS)
        .ok_or_else(|| CatalogError::Invalid("本地 K 线起点溢出。".into()))?;
    let end_ns = end_ms
        .checked_mul(crate::NANO_PER_MS)
        .ok_or_else(|| CatalogError::Invalid("本地 K 线终点溢出。".into()))?;
    let catalog = crate::Catalog::open_existing(catalog_root)?;
    let mut source = target_interval;
    let mut bars = catalog.read_bars(symbol, product, source, start_ns, end_ns, 50_000)?;
    if bars.is_empty() && target_interval != "1m" {
        source = "1m";
        bars = catalog.read_bars(symbol, product, source, start_ns, end_ns, 50_000)?;
    }
    let saved = bars
        .iter()
        .map(|bar| {
            Ok(SavedKline {
                event_ns: bar.event_ns(source)?,
                values: [bar.open, bar.high, bar.low, bar.close, bar.volume],
            })
        })
        .collect::<Result<Vec<_>, CatalogError>>()?;
    aggregate_saved_klines(&saved, source, target_interval)
}

/// Exact, read-only decision for an existing or missing minute repair candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MinuteRepairDecision {
    Insert,
    Match,
    Conflict,
}

/// Validate the official one-minute candle against instrument precision before
/// any Python/Nautilus repair writer sees it. Existing differing evidence must
/// be marked as conflict rather than overwritten.
pub fn classify_minute_repair(
    candidate: &crate::CatalogBar,
    existing: Option<&crate::CatalogBar>,
    product: crate::MarketProduct,
    price_precision: u32,
    size_precision: u32,
) -> Result<MinuteRepairDecision, CatalogError> {
    if price_precision > 16 || size_precision > 16 {
        return Err(CatalogError::Invalid("修复来源合约精度不受支持。".into()));
    }
    candidate.validate("1m", product)?;
    if [
        candidate.open,
        candidate.high,
        candidate.low,
        candidate.close,
    ]
    .iter()
    .any(|value| value.round_dp(price_precision) != *value)
        || candidate.volume.round_dp(size_precision) != candidate.volume
    {
        return Err(CatalogError::Invalid("补齐来源精度与合约不一致。".into()));
    }
    Ok(match existing {
        None => MinuteRepairDecision::Insert,
        Some(saved) if saved == candidate => MinuteRepairDecision::Match,
        Some(_) => MinuteRepairDecision::Conflict,
    })
}

/// Read existing target minutes once and plan an additive repair, without
/// opening any writer or modifying saved evidence. Candidates must be sorted
/// and unique; reject malformed windows before returning any insert decision.
pub fn plan_minute_repairs(
    catalog_root: &std::path::Path,
    symbol: &str,
    product: crate::MarketProduct,
    price_precision: u32,
    size_precision: u32,
    candidates: &[crate::CatalogBar],
) -> Result<Vec<MinuteRepairDecision>, CatalogError> {
    use std::collections::BTreeMap;

    if candidates.len() > 50_000 {
        return Err(CatalogError::Limit("分钟修复候选数量超过上限。".into()));
    }
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let mut previous = None;
    for bar in candidates {
        if previous.is_some_and(|before| bar.open_time_ms <= before) {
            return Err(CatalogError::Invalid("分钟修复候选重复或乱序。".into()));
        }
        classify_minute_repair(bar, None, product, price_precision, size_precision)?;
        previous = Some(bar.open_time_ms);
    }
    let first = candidates[0]
        .open_time_ms
        .checked_mul(crate::NANO_PER_MS)
        .ok_or_else(|| CatalogError::Invalid("分钟修复时间溢出。".into()))?;
    let end_ms = crate::bar_window("1m", candidates[candidates.len() - 1].open_time_ms)?.1;
    let end = end_ms
        .checked_mul(crate::NANO_PER_MS)
        .ok_or_else(|| CatalogError::Invalid("分钟修复时间溢出。".into()))?;
    let catalog = crate::Catalog::open_existing(catalog_root)?;
    let existing = catalog
        .read_bars(symbol, product, "1m", first, end, 50_000)?
        .into_iter()
        .map(|bar| (bar.open_time_ms, bar))
        .collect::<BTreeMap<_, _>>();
    candidates
        .iter()
        .map(|candidate| {
            classify_minute_repair(
                candidate,
                existing.get(&candidate.open_time_ms),
                product,
                price_precision,
                size_precision,
            )
        })
        .collect()
}

/// A saved candle's OHLCV evidence and whether its constituent windows are complete.
/// The legacy repair path sends decimal text, which the native bridge parses
/// before invoking this comparison; no floating-point conversion is permitted.
#[derive(Clone, Debug, PartialEq)]
pub struct LocalKline {
    pub values: [rust_decimal::Decimal; 5],
    pub complete: bool,
}

/// Decide whether a verified venue candle needs repair, replacing the legacy
/// Python `differs` predicate. A missing all-zero slice is not a mismatch
/// for the tick/second source, but incomplete saved bars always are.
pub fn venue_kline_differs(local: Option<&LocalKline>, remote: &VenueKline, source: &str) -> bool {
    match local {
        Some(saved) => {
            let observed = [
                remote.bar.open,
                remote.bar.high,
                remote.bar.low,
                remote.bar.close,
                remote.bar.volume,
            ];
            !saved.complete || saved.values != observed
        }
        None => {
            !((source == "tick" || source == "1s")
                && remote.trade_count == 0
                && remote.bar.volume.is_zero())
        }
    }
}

#[cfg(test)]
mod venue_tests {
    use super::*;
    use rust_decimal::Decimal;

    fn candle(at: i64, end: i64) -> VenueKline {
        VenueKline {
            bar: crate::CatalogBar {
                open_time_ms: at,
                open: Decimal::from(10),
                high: Decimal::from(12),
                low: Decimal::from(9),
                close: Decimal::from(11),
                volume: Decimal::from(2),
            },
            end_ms: end,
            trade_count: 1,
        }
    }

    #[test]
    fn accepts_complete_closed_minute_sequence() {
        let rows = [candle(0, 60_000), candle(60_000, 120_000)];
        assert!(
            validate_venue_klines(&rows, "1m", 0, 120_000, 0, crate::MarketProduct::FuturesUm)
                .is_ok()
        );
    }

    #[test]
    fn rejects_missing_middle_or_tail_rows_and_bad_close() {
        let product = crate::MarketProduct::FuturesUm;
        assert!(
            validate_venue_klines(
                &[candle(0, 60_000), candle(120_000, 180_000)],
                "1m",
                0,
                180_000,
                0,
                product
            )
            .is_err()
        );
        assert!(validate_venue_klines(&[candle(0, 60_000)], "1m", 0, 120_000, 0, product).is_err());
        assert!(validate_venue_klines(&[candle(0, 59_999)], "1m", 0, 60_000, 0, product).is_err());
        assert!(
            validate_venue_klines(
                &[candle(0, 60_000), candle(0, 60_000)],
                "1m",
                0,
                120_000,
                0,
                product
            )
            .is_err()
        );
    }

    #[test]
    fn permits_only_explicit_unavailable_leading_prefix() {
        let product = crate::MarketProduct::FuturesUm;
        assert!(
            validate_venue_klines(
                &[candle(60_000, 120_000)],
                "1m",
                0,
                120_000,
                60_000,
                product
            )
            .is_ok()
        );
        assert!(
            validate_venue_klines(&[candle(60_000, 120_000)], "1m", 0, 120_000, 0, product)
                .is_err()
        );
    }

    #[test]
    fn rejects_bad_prices_and_trade_counts() {
        let mut row = candle(0, 60_000);
        row.trade_count = -1;
        assert!(
            validate_venue_klines(
                &[row.clone()],
                "1m",
                0,
                60_000,
                0,
                crate::MarketProduct::Spot
            )
            .is_err()
        );
        row.trade_count = 1;
        row.bar.high = Decimal::from(8);
        assert!(
            validate_venue_klines(&[row], "1m", 0, 60_000, 0, crate::MarketProduct::Spot).is_err()
        );
    }

    #[test]
    fn aggregates_saved_minutes_and_preserves_partial_window_evidence() {
        fn saved(minute: i64, volume: i64) -> SavedKline {
            SavedKline {
                event_ns: (minute * 60_000 + 59_999) * 1_000_000,
                values: [
                    Decimal::from(10),
                    Decimal::from(12),
                    Decimal::from(9),
                    Decimal::from(11),
                    Decimal::from(volume),
                ],
            }
        }
        let complete = aggregate_saved_klines(&[saved(0, 2), saved(1, 3)], "1m", "2m");
        // Binance catalog supports standard source/target periods only.
        assert!(complete.is_err());
        let partial = aggregate_saved_klines(&[saved(0, 2), saved(2, 3)], "1m", "5m").unwrap();
        assert_eq!(partial.len(), 1);
        assert_eq!(partial[0].values[4], Decimal::from(5));
        assert!(!partial[0].complete);
        let full = aggregate_saved_klines(
            &[
                saved(0, 1),
                saved(1, 1),
                saved(2, 1),
                saved(3, 1),
                saved(4, 1),
            ],
            "1m",
            "5m",
        )
        .unwrap();
        assert!(full[0].complete);
        assert_eq!(full[0].values[4], Decimal::from(5));
        let bad = SavedKline {
            event_ns: 0,
            ..saved(0, 1)
        };
        assert!(aggregate_saved_klines(&[bad], "1m", "1m").is_err());
    }

    #[test]
    fn minute_repair_classification_preserves_exact_precision_and_conflicts() {
        let product = crate::MarketProduct::FuturesUm;
        let candidate = candle(0, 60_000).bar;
        assert_eq!(
            classify_minute_repair(&candidate, None, product, 2, 3).unwrap(),
            MinuteRepairDecision::Insert
        );
        assert_eq!(
            classify_minute_repair(&candidate, Some(&candidate), product, 2, 3).unwrap(),
            MinuteRepairDecision::Match
        );
        let mut saved = candidate.clone();
        saved.volume = Decimal::from(9);
        assert_eq!(
            classify_minute_repair(&candidate, Some(&saved), product, 2, 3).unwrap(),
            MinuteRepairDecision::Conflict
        );
        let mut excessive = candidate.clone();
        excessive.close = "11.001".parse().unwrap();
        assert!(classify_minute_repair(&excessive, None, product, 2, 3).is_err());
        assert!(classify_minute_repair(&candidate, None, product, 17, 3).is_err());
    }

    #[test]
    fn native_repair_difference_preserves_legacy_zero_trade_exception() {
        let mut remote = candle(0, 60_000);
        let complete = LocalKline {
            values: [
                Decimal::from(10),
                Decimal::from(12),
                Decimal::from(9),
                Decimal::from(11),
                Decimal::from(2),
            ],
            complete: true,
        };
        assert!(!venue_kline_differs(Some(&complete), &remote, "1m"));
        assert!(venue_kline_differs(None, &remote, "1m"));
        assert!(venue_kline_differs(None, &remote, "tick"));
        remote.trade_count = 0;
        remote.bar.volume = Decimal::ZERO;
        assert!(!venue_kline_differs(None, &remote, "tick"));
        assert!(!venue_kline_differs(None, &remote, "1s"));
        assert!(venue_kline_differs(None, &remote, "1m"));
        assert!(venue_kline_differs(Some(&complete), &remote, "tick"));
        let incomplete = LocalKline {
            complete: false,
            ..complete
        };
        assert!(venue_kline_differs(Some(&incomplete), &remote, "1m"));
    }
}
