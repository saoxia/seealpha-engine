//! Shared Binance Spot / USD-M coarse-to-fine historical repair scanner.
//! Trading runtime only schedules the repair; all data comparison and archive
//! decision policy lives in the market-data pipeline.
use crate::{Bar, BinanceSource, Product};
use seealpha_catalog::{
    Catalog, CatalogError, NANO_PER_MS, bar_window, bar_type,
};
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
    seealpha_catalog::interval_ms(source_interval)?;
    seealpha_catalog::interval_ms(target_interval)?;
    let mut buckets = BTreeMap::<i64, AggregatedKline>::new();
    for bar in saved {
        let (opening, closing) =
            bar_window(source_interval, bar.event_ns.div_euclid(NANO_PER_MS))?;
        let expected_event = closing
            .checked_sub(1)
            .and_then(|ms| ms.checked_mul(NANO_PER_MS))
            .ok_or_else(|| CatalogError::Invalid("本地 K 线纳秒时间溢出。".into()))?;
        if bar.event_ns != expected_event {
            return Err(CatalogError::Invalid("已保存 K 线时间边界无效。".into()));
        }
        let (left, right) = bar_window(target_interval, opening)?;
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
        bucket.complete &= bucket.end_ms == bar_window(target_interval, *left)?.1;
    }
    Ok(buckets.into_values().collect())
}

/// Read the Nautilus v2 Catalog directly in Rust before aggregating repair evidence.
/// Prefer the saved target interval; fall back to saved minutes only when no
/// target bars exist. Neither missing source data nor partial windows are filled.
pub fn read_aggregated_catalog_klines(
    catalog_root: &std::path::Path,
    instrument: &str,
    target_interval: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<AggregatedKline>, CatalogError> {
    if start_ms < 0 || end_ms <= start_ms {
        return Err(CatalogError::Invalid("本地 K 线读取区间无效。".into()));
    }
    let start_ns = start_ms
        .checked_mul(NANO_PER_MS)
        .ok_or_else(|| CatalogError::Invalid("本地 K 线起点溢出。".into()))?;
    let end_ns = end_ms
        .checked_mul(NANO_PER_MS)
        .ok_or_else(|| CatalogError::Invalid("本地 K 线终点溢出。".into()))?;
    let catalog = Catalog::open_existing(catalog_root)?;
    let mut source = target_interval;
    let mut kind = bar_type(instrument, source)?;
    let mut bars = catalog.read_bars(&kind, source, start_ns, end_ns, 50_000)?;
    if bars.is_empty() && target_interval != "1m" {
        source = "1m";
        kind = bar_type(instrument, source)?;
        bars = catalog.read_bars(&kind, source, start_ns, end_ns, 50_000)?;
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


use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0}")]
pub struct RepairError(String);

const LEVELS: [&str; 6] = ["1d", "4h", "1h", "15m", "5m", "1m"];

fn error(message: impl Into<String>) -> RepairError {
    RepairError(message.into())
}

fn source_instrument(product: Product, symbol: &str) -> Result<String, RepairError> {
    use crate::identity::{MarketProduct, instrument_id};
    let product = match product {
        Product::Spot => MarketProduct::Spot,
        Product::FuturesUm => MarketProduct::FuturesUm,
    };
    instrument_id(symbol, product).map_err(|e| error(e.to_string()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Difference {
    Match,
    Missing,
    Conflict,
}

fn difference(saved: Option<&AggregatedKline>, remote: &Bar) -> Difference {
    match saved {
        None => Difference::Missing,
        Some(local) => {
            let values = [
                remote.open,
                remote.high,
                remote.low,
                remote.close,
                remote.volume,
            ];
            if local.complete && local.values == values {
                Difference::Match
            } else {
                Difference::Conflict
            }
        }
    }
}

fn split_windows(start_ms: i64, end_ms: i64, next: &str) -> Result<Vec<i64>, RepairError> {
    let mut cursor = start_ms;
    let mut result = Vec::new();
    while cursor < end_ms {
        let (left, right) = bar_window(next, cursor).map_err(|e| error(e.to_string()))?;
        if left != cursor || right > end_ms || right <= cursor || result.len() >= 1_440 {
            return Err(error("跨周期修复窗口不对齐或超出每日上限。"));
        }
        result.push(cursor);
        cursor = right;
    }
    Ok(result)
}

#[derive(Default, Debug)]
pub struct DailyScan {
    pub levels_checked: Vec<String>,
    pub missing_minutes: Vec<i64>,
    pub conflicting_minutes: Vec<i64>,
    pub unresolved_parent_windows: Vec<Value>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RepairNextStep {
    Nothing,
    RecalculateParent,
    FetchOfficialArchive,
}

impl DailyScan {
    /// Product-independent archive trigger: only missing minute windows require ZIP.
    pub fn next_step(&self) -> RepairNextStep {
        // An existing minute candle with differing OHLCV may be missing
        // underlying trades. Official ZIP evidence is needed to distinguish
        // missing IDs from actual same-ID conflicts before any import.
        if !self.conflicting_minutes.is_empty() || !self.missing_minutes.is_empty() {
            RepairNextStep::FetchOfficialArchive
        } else if !self.unresolved_parent_windows.is_empty() {
            RepairNextStep::RecalculateParent
        } else {
            RepairNextStep::Nothing
        }
    }
}

/// A closed UTC day only. Read-only until missing minute windows have
/// actually been located; the runner may then request the official ZIP.
pub async fn scan_day(
    product: Product,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
    catalog_root: Option<&Path>,
) -> Result<DailyScan, RepairError> {
    let source = BinanceSource::new().map_err(|e| error(e.to_string()))?;
    let mut suspects = vec![start_ms];
    let mut parents: Vec<(String, i64, i64)> = Vec::new();
    let mut result = DailyScan::default();

    for (idx, interval) in LEVELS.iter().enumerate() {
        if suspects.is_empty() {
            break;
        }
        // At most six REST history reads per day. Each response is a
        // bounded, complete closed K-line sequence (max 1,440 rows).
        let remote = source
            .load_history(product, symbol, interval, start_ms, end_ms)
            .await
            .map_err(|e| error(format!("Binance {interval} 历史 K 线读取失败：{e}")))?;
        let exchange: BTreeMap<i64, Bar> = remote.into_iter()
            .map(|bar| (bar.open_time_ms, bar)).collect();
        let local = if let Some(catalog_root) = catalog_root {
            read_aggregated_catalog_klines(
                catalog_root,
                &source_instrument(product, symbol)?,
                interval,
                start_ms,
                end_ms,
            )
            .map_err(|e| error(format!("Catalog {interval} 读取失败：{e}")))?
        } else {
            Vec::new()
        };
        let saved: BTreeMap<i64, AggregatedKline> = local.into_iter()
            .map(|bar| (bar.start_ms, bar)).collect();

        let mut mismatches = Vec::new();
        for &start in &suspects {
            let remote = exchange.get(&start)
                .ok_or_else(|| error("交易所返回数据缺少预期的完整 K 线窗口。"))?;
            let kind = difference(saved.get(&start), remote);
            if kind != Difference::Match {
                mismatches.push((start, kind));
            }
        }
        // If a parent differs but all its children match, the parent bar
        // may be stale rather than the underlying trades. Do not trigger
        // a ZIP download without minute-level evidence.
        for (parent_level, start, end) in &parents {
            if !mismatches.iter().any(|(minute, _)| *minute >= *start && *minute < *end) {
                result.unresolved_parent_windows.push(json!({
                    "interval":parent_level,"start_ms":start,"end_ms":end,
                    "reason":"上层 K 线不一致，但下层窗口无差异"
                }));
            }
        }
        result.levels_checked.push((*interval).to_owned());
        if idx + 1 == LEVELS.len() {
            for (start, kind) in mismatches {
                match kind {
                    Difference::Missing => result.missing_minutes.push(start),
                    Difference::Conflict => result.conflicting_minutes.push(start),
                    Difference::Match => {}
                }
            }
            break;
        }
        parents = mismatches.iter().map(|(start, _)| {
            let end = bar_window(interval, *start)
                .map(|(_, end)| end)
                .unwrap_or(*start);
            ((*interval).to_owned(), *start, end)
        }).collect();

        let next = LEVELS[idx + 1];
        let mut children = Vec::new();
        for (_, start, end) in &parents {
            children.extend(split_windows(*start, *end, next)?);
        }
        suspects = children;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    #[test]
    fn saved_minutes_aggregate_only_when_window_is_complete() {
        fn saved(minute: i64) -> SavedKline {
            SavedKline {
                event_ns: (minute * 60_000 + 59_999) * 1_000_000,
                values: [
                    Decimal::from(10), Decimal::from(12), Decimal::from(9),
                    Decimal::from(11), Decimal::ONE,
                ],
            }
        }
        let partial = aggregate_saved_klines(
            &[saved(0), saved(2)], "1m", "5m").unwrap();
        assert_eq!(partial.len(), 1);
        assert!(!partial[0].complete);
        let full = aggregate_saved_klines(
            &[saved(0), saved(1), saved(2), saved(3), saved(4)],
            "1m", "5m").unwrap();
        assert!(full[0].complete);
        assert_eq!(full[0].values[4], Decimal::from(5));
        let invalid = SavedKline { event_ns: 0, ..saved(0) };
        assert!(aggregate_saved_klines(&[invalid], "1m", "1m").is_err());
    }


    use super::*;
    use rust_decimal::Decimal;
    use seealpha_catalog::DAY_MS;

    fn sample_bar(start: i64) -> Bar {
        Bar {
            open_time_ms: start,
            close_time_ms: start + 59_999,
            open: Decimal::from(10), high: Decimal::from(12),
            low: Decimal::from(9),
            close: Decimal::from(11),
            volume: Decimal::from(2),
        }
    }

    #[test]
    fn each_exchange_product_uses_its_own_catalog_identity() {
        assert_eq!(source_instrument(Product::Spot, "ETHUSDT").unwrap(), "ETHUSDT.BINANCE");
        assert_eq!(source_instrument(Product::FuturesUm, "ETHUSDT").unwrap(), "ETHUSDT-PERP.BINANCE");
    }

    #[test]
    fn mismatch_is_detected_without_altering_source_evidence() {
        let remote = sample_bar(0);
        assert_eq!(difference(None, &remote), Difference::Missing);
        let mut stored = AggregatedKline {
            start_ms: 0,
            end_ms: 60_000,
            values: [remote.open,remote.high,remote.low,remote.close,remote.volume],
            complete: true,
        };
        assert_eq!(difference(Some(&stored), &remote), Difference::Match);
        stored.complete = false;
        assert_eq!(difference(Some(&stored), &remote), Difference::Conflict);
        stored.complete = true;
        stored.values[4] += Decimal::ONE;
        assert_eq!(difference(Some(&stored), &remote), Difference::Conflict);
    }

    #[test]
    fn archive_trigger_is_a_pipeline_decision_not_runtime_policy() {
        let mut scan = DailyScan::default();
        assert_eq!(scan.next_step(), RepairNextStep::Nothing);
        scan.unresolved_parent_windows.push(json!({"interval":"1d"}));
        assert_eq!(scan.next_step(), RepairNextStep::RecalculateParent);
        scan.missing_minutes.push(60_000);
        assert_eq!(scan.next_step(), RepairNextStep::FetchOfficialArchive);
        scan.conflicting_minutes.push(120_000);
        assert_eq!(scan.next_step(), RepairNextStep::FetchOfficialArchive);
        let mut discrepancy = DailyScan::default();
        discrepancy.conflicting_minutes.push(120_000);
        assert_eq!(discrepancy.next_step(), RepairNextStep::FetchOfficialArchive);
    }

    #[test]
    fn descending_levels_split_only_suspect_parent_window() {
        assert_eq!(split_windows(0, DAY_MS, "4h").unwrap().len(), 6);
        assert_eq!(split_windows(0, 4 * 3_600_000, "1h").unwrap().len(), 4);
        assert_eq!(split_windows(0, 5 * 60_000, "1m").unwrap().len(), 5);
        assert!(split_windows(1, 60_000, "1m").is_err());
    }
}
