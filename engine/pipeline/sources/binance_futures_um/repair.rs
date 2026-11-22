//! Binance USD-M coarse-to-fine historical repair scanner.
//! Trading runtime only schedules the repair; all data comparison and archive
//! decision policy lives in the market-data pipeline.
use crate::{Bar, BinanceSource, Product};
use seealpha_catalog::{
    AggregatedKline, DAY_MS, MarketProduct, bar_window, read_aggregated_catalog_klines,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0}")]
pub struct FuturesRepairError(String);

const LEVELS: [&str; 6] = ["1d", "4h", "1h", "15m", "5m", "1m"];

fn error(message: impl Into<String>) -> FuturesRepairError {
    FuturesRepairError(message.into())
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

fn split_windows(start_ms: i64, end_ms: i64, next: &str) -> Result<Vec<i64>, FuturesRepairError> {
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
    ReviewMinuteConflict,
    FetchOfficialArchive,
}

impl DailyScan {
    /// The sole source of the USD-M archive trigger decision.
    pub fn next_step(&self) -> RepairNextStep {
        if !self.conflicting_minutes.is_empty() {
            RepairNextStep::ReviewMinuteConflict
        } else if !self.missing_minutes.is_empty() {
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
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
    catalog_root: Option<&Path>,
) -> Result<DailyScan, FuturesRepairError> {
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
            .load_history(Product::FuturesUm, symbol, interval, start_ms, end_ms)
            .await
            .map_err(|e| error(format!("Binance {interval} 历史 K 线读取失败：{e}")))?;
        let exchange: BTreeMap<i64, Bar> = remote.into_iter()
            .map(|bar| (bar.open_time_ms, bar)).collect();
        let local = if let Some(catalog_root) = catalog_root {
            read_aggregated_catalog_klines(
                catalog_root,
                symbol,
                MarketProduct::FuturesUm,
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
    use super::*;
    use rust_decimal::Decimal;

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
        assert_eq!(scan.next_step(), RepairNextStep::ReviewMinuteConflict);
    }

    #[test]
    fn descending_levels_split_only_suspect_parent_window() {
        assert_eq!(split_windows(0, DAY_MS, "4h").unwrap().len(), 6);
        assert_eq!(split_windows(0, 4 * 3_600_000, "1h").unwrap().len(), 4);
        assert_eq!(split_windows(0, 5 * 60_000, "1m").unwrap().len(), 5);
        assert!(split_windows(1, 60_000, "1m").is_err());
    }
}
