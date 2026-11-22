//! USD-M hierarchical daily repair orchestrator, reusing existing Catalog
//! comparison and Binance ZIP fetch/import jobs. No second collector or writer.
use crate::{Runtime, RuntimeError};
use seealpha_catalog::{AggregatedKline, DAY_MS, MarketProduct, bar_window,
    read_aggregated_catalog_klines};
use seealpha_market::{Bar, BinanceSource, Product};
use seealpha_binance_market_data_source::archive_fetch::ArchiveSpec;
use serde_json::{Value, json};
use std::collections::BTreeMap;

const LEVELS: [&str; 6] = ["1d", "4h", "1h", "15m", "5m", "1m"];

fn error(message: impl Into<String>) -> RuntimeError {
    RuntimeError::Conflict(message.into())
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

fn split_windows(start_ms: i64, end_ms: i64, next: &str) -> Result<Vec<i64>, RuntimeError> {
    let mut cursor = start_ms;
    let mut result = Vec::new();
    while cursor < end_ms {
        let (left, right) = bar_window(next, cursor)
            .map_err(|e| error(e.to_string()))?;
        if left != cursor || right > end_ms || right <= cursor || result.len() >= 1_440 {
            return Err(error("跨周期修复窗口不对齐或超出每日上限。"));
        }
        result.push(cursor);
        cursor = right;
    }
    Ok(result)
}

#[derive(Default, Debug)]
struct DailyScan {
    levels_checked: Vec<String>,
    missing_minutes: Vec<i64>,
    conflicting_minutes: Vec<i64>,
    unresolved_parent_windows: Vec<Value>,
}

impl Runtime {
    /// A closed UTC day only. Read-only until missing minute windows have
    /// actually been located; then an explicit job can request official ZIP.
    async fn scan_futures_day(
        &self,
        symbol: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<DailyScan, RuntimeError> {
        let source = BinanceSource::new().map_err(|e| error(e.to_string()))?;
        let catalog_exists = self.native_catalog()?.is_some();
        let catalog_root = self.native_catalog_root();
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
            let local = if catalog_exists {
                read_aggregated_catalog_klines(
                    &catalog_root,
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

    /// Explicit user/worker-submitted historical repair job. Never start a
    /// data source implicitly in a read request, or write an active LiveNode
    /// Catalog while that node owns its writer reservation.
    pub(crate) async fn repair_futures_day_job(
        &self,
        tenant: &str,
        payload: &Value,
    ) -> Result<Value, RuntimeError> {
        let symbol = payload["symbol"].as_str()
            .ok_or_else(|| error("修复合约缺失。"))?;
        let day = payload["day"].as_str()
            .ok_or_else(|| error("修复 UTC 日期缺失。"))?;
        let spec = ArchiveSpec::new("futures_um", "aggTrades", symbol, day)
            .map_err(|e| error(format!("USD-M 修复日期或合约无效：{e}")))?;
        let (start, end) = spec.utc_day_bounds_ms()
            .map_err(|e| error(format!("USD-M 修复 UTC 日期无效：{e}")))?;
        if start < 0 || start % DAY_MS != 0 || end - start != DAY_MS {
            return Err(error("修复日期必须按 UTC 天对齐。"));
        }
        let scan = self.scan_futures_day(symbol, start, end).await?;

        let mut response = json!({
            "action":"repair_futures_day","product":"futures_um",
            "symbol":symbol,"day":day,
            "levels_checked":scan.levels_checked,
            "missing_minute_count":scan.missing_minutes.len(),
            "missing_minutes":scan.missing_minutes,
            "conflicting_minute_count":scan.conflicting_minutes.len(),
            "conflicting_minutes":scan.conflicting_minutes,
            "unresolved_parent_windows":scan.unresolved_parent_windows,
            "data_ready":false,"allow_new_risk":false,
            "needs_recalculation":false,
            "exchange_orders":0
        });

        if response["conflicting_minute_count"] != 0 {
            response["result"] = json!("minute_conflicts_require_review");
            return Ok(response);
        }
        if response["missing_minute_count"] == 0 {
            response["result"] = json!(if response["unresolved_parent_windows"]
                .as_array().is_some_and(|v| !v.is_empty()) {
                    "parent_bar_recalculation_required"
                } else {
                    "no_missing_minutes"
                });
            return Ok(response);
        }

        // Preserve a durable, tenant-scoped ZIP receipt through the *existing*
        // downloader; never trust user-supplied archive paths or SHA hashes.
        let fetch = self.run_futures_repair_child(tenant, &json!({
            "action":"archive_fetch","product":"futures_um",
            "dataset":"aggTrades","symbol":symbol,"day":day
        })).await?;
        response["archive_fetch_job_id"] = fetch["id"].clone();

        let price = payload["price_precision"].as_u64()
            .filter(|value| *value <= 16)
            .ok_or_else(|| error("Instrument 价格精度缺失。"))?;
        let size = payload["size_precision"].as_u64()
            .filter(|value| *value <= 16)
            .ok_or_else(|| error("Instrument 数量精度缺失。"))?;
        let import = self.queue_data_job(tenant, &json!({
            "action":"archive_import",
            "archive_job_id":fetch["id"],
            "price_precision":price,
            "size_precision":size
        }))?;
        response["archive_import_job_id"] = import["id"].clone();

        let key = format!("{tenant}:futures_um");
        let active_writer = {
            let mut processes = self.source_processes.lock()
                .map_err(|_| error("实时节点状态锁不可用。"))?;
            if let Some(child) = processes.get_mut(&key) {
                child.try_wait().map_err(|_| error("实时节点状态未知。"))?.is_none()
            } else {
                false
            }
        };
        if active_writer {
            response["result"] = json!("archive_downloaded_import_deferred_live_writer");
            response["needs_recalculation"] = json!(true);
            return Ok(response);
        }

        let import = self.execute_or_reuse_repair_child(tenant, import).await?;
        response["archive_import_job_id"] = import["id"].clone();
        response["result"] = json!("archive_imported_recalculation_pending");
        response["needs_recalculation"] = json!(true);
        Ok(response)
    }

    /// The nested executor is boxed so a repair job can reuse the existing
    /// archive jobs without duplicating download/import safety code.
    async fn execute_or_reuse_repair_child(
        &self,
        tenant: &str,
        job: Value,
    ) -> Result<Value, RuntimeError> {
        let job = if job["status"] == "queued" {
            let id = job["id"].as_str()
                .ok_or_else(|| error("归档子任务 ID 无效。"))?;
            Box::pin(self.execute_data_job(tenant, id)).await?
        } else {
            job
        };
        if job["status"] != "completed" {
            return Err(error(format!(
                "历史归档子任务尚未成功：{}；任务编号：{}",
                job["status"], job["id"]
            )));
        }
        Ok(job)
    }

    async fn run_futures_repair_child(
        &self,
        tenant: &str,
        payload: &Value,
    ) -> Result<Value, RuntimeError> {
        let job = self.queue_data_job(tenant, payload)?;
        self.execute_or_reuse_repair_child(tenant, job).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn sample_bar(start: i64) -> Bar {
        Bar {
            open_time_ms: start, close_time_ms: start + 59_999,
            open: Decimal::from(10), high: Decimal::from(12),
            low: Decimal::from(9), close: Decimal::from(11),
            volume: Decimal::from(2),
        }
    }

    #[test]
    fn mismatch_is_detected_without_altering_source_evidence() {
        let remote = sample_bar(0);
        assert_eq!(difference(None, &remote), Difference::Missing);
        let mut stored = AggregatedKline {
            start_ms: 0,end_ms:60_000,
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
    fn descending_levels_split_only_suspect_parent_window() {
        assert_eq!(split_windows(0, DAY_MS, "4h").unwrap().len(), 6);
        assert_eq!(split_windows(0, 4 * 3_600_000, "1h").unwrap().len(), 4);
        assert_eq!(split_windows(0, 5 * 60_000, "1m").unwrap().len(), 5);
        assert!(split_windows(1, 60_000, "1m").is_err());
    }
}
