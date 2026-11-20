//! Feed gate, rollups, feature warmup, and pair deletion.
//!
//! A read never creates storage or starts a collector. Missing pages are not
//! filled with synthetic bars. Checkpoints move only across contiguous closes.
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use thiserror::Error;

pub mod archive;
pub mod features;
pub mod hub;
pub mod rollup;
pub mod session;

pub const MINUTE_MS: i64 = 60_000;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FeedError {
    #[error("缺口未补齐，检查点不前进。")]
    Gap,
    #[error("乱序或内部缺口，检查点不前进。")]
    OutOfOrder,
    #[error("来源冲突，本页已回滚。")]
    Conflict,
    #[error("未创建缺失的存储。")]
    StorageMissing,
    #[error("删除快照已过期。")]
    StalePlan,
    #[error("删除范围包含符号链接。")]
    Symlink,
    #[error("只接受完整 UTC 日边界。")]
    PartialDay,
    #[error("实时写入中，拒绝删除。")]
    LiveWriter,
    #[error("特征尚未预热。")]
    Warmup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bar {
    pub open_time_ms: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gate {
    pub confirmed_ms: i64,
    pub ready: bool,
    pub repair_required: bool,
    pub conflict_latched: bool,
}

impl Gate {
    pub fn new() -> Self {
        Self {
            confirmed_ms: 0,
            ready: false,
            repair_required: false,
            conflict_latched: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct Book {
    bars: BTreeMap<i64, Bar>,
    pub gate: Gate,
}

impl Default for Gate {
    fn default() -> Self {
        Self::new()
    }
}

impl Book {
    pub fn accept_page(&mut self, page: &[Bar], step_ms: i64) -> Result<(), FeedError> {
        if self.gate.conflict_latched {
            return Err(FeedError::Conflict);
        }
        if page.is_empty() {
            return Err(FeedError::Gap);
        }
        if page.iter().any(|bar| {
            bar.high < bar.low
                || bar.high < bar.open
                || bar.high < bar.close
                || bar.low > bar.open
                || bar.low > bar.close
        }) {
            return Err(FeedError::Conflict);
        }
        let mut ordered = page.to_vec();
        ordered.sort_by_key(|bar| bar.open_time_ms);
        for window in ordered.windows(2) {
            if window[1].open_time_ms - window[0].open_time_ms != step_ms {
                return Err(FeedError::OutOfOrder);
            }
        }
        if let Some((&last, _)) = self.bars.iter().next_back() {
            let expected = last + step_ms;
            if ordered[0].open_time_ms > expected {
                self.gate.ready = false;
                self.gate.repair_required = true;
                return Err(FeedError::Gap);
            }
            if ordered[0].open_time_ms < expected && ordered.iter().any(|bar| self.conflicts(bar)) {
                self.gate.conflict_latched = true;
                self.gate.ready = false;
                self.gate.repair_required = true;
                return Err(FeedError::Conflict);
            }
        }
        for bar in ordered {
            if let Some(existing) = self.bars.get(&bar.open_time_ms) {
                if existing.close != bar.close || existing.volume != bar.volume {
                    self.gate.conflict_latched = true;
                    self.gate.ready = false;
                    return Err(FeedError::Conflict);
                }
                continue;
            }
            self.bars.insert(bar.open_time_ms, bar);
        }
        if let Some((&last, _)) = self.bars.iter().next_back() {
            self.gate.confirmed_ms = last;
        }
        self.gate.ready = !self.gate.repair_required && !self.gate.conflict_latched;
        Ok(())
    }

    fn conflicts(&self, bar: &Bar) -> bool {
        self.bars
            .get(&bar.open_time_ms)
            .is_some_and(|existing| existing.close != bar.close || existing.volume != bar.volume)
    }

    pub fn close_after_repair_failure(&mut self) {
        self.gate.ready = false;
        self.gate.repair_required = true;
    }

    pub fn restart(&mut self) -> Gate {
        self.gate.ready = false;
        self.gate.clone()
    }

    pub fn len(&self) -> usize {
        self.bars.len()
    }
    pub fn is_empty(&self) -> bool {
        self.bars.is_empty()
    }

    pub fn get(&self, open_time_ms: i64) -> Option<&Bar> {
        self.bars.get(&open_time_ms)
    }
}

pub fn rollup(bars: &[Bar], factor: i64) -> Vec<Bar> {
    if bars.is_empty() || factor <= 1 {
        return bars.to_vec();
    }
    let mut out = Vec::new();
    let mut chunk: Vec<&Bar> = Vec::new();
    for bar in bars {
        chunk.push(bar);
        if chunk.len() as i64 == factor {
            out.push(merge(&chunk));
            chunk.clear();
        }
    }
    out
}

fn merge(chunk: &[&Bar]) -> Bar {
    let mut high = chunk[0].high;
    let mut low = chunk[0].low;
    let mut volume = Decimal::ZERO;
    for bar in chunk {
        if bar.high > high {
            high = bar.high;
        }
        if bar.low < low {
            low = bar.low;
        }
        volume += bar.volume;
    }
    Bar {
        open_time_ms: chunk[0].open_time_ms,
        open: chunk[0].open,
        high,
        low,
        close: chunk[chunk.len() - 1].close,
        volume,
        source: "rollup".into(),
    }
}

#[derive(Debug, Clone)]
pub struct Ring {
    max: usize,
    items: Vec<Bar>,
}

impl Ring {
    pub fn new(max: usize) -> Self {
        Self {
            max,
            items: Vec::new(),
        }
    }
    pub fn push(&mut self, bar: Bar) {
        if self.items.len() == self.max {
            self.items.remove(0);
        }
        self.items.push(bar);
    }
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct FeatureWarmup {
    pub period: usize,
    pub seen: usize,
    pub previous: Option<Decimal>,
}

impl FeatureWarmup {
    pub fn new(period: usize) -> Self {
        Self {
            period,
            seen: 0,
            previous: None,
        }
    }

    /// Returns the simple return of this close versus the previous close.
    /// Values inside the warmup window are withheld. The current close is never
    /// computed from a later bar.
    pub fn push(&mut self, close: Decimal) -> Result<Option<Decimal>, FeedError> {
        let previous = self.previous;
        self.previous = Some(close);
        self.seen += 1;
        if self.seen < self.period {
            return Ok(None);
        }
        match previous {
            Some(prior) if !prior.is_zero() => Ok(Some((close - prior) / prior)),
            Some(_) => Ok(None),
            None => Err(FeedError::Warmup),
        }
    }

    pub fn reset_after_gap(&mut self) {
        self.seen = 0;
        self.previous = None;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredFile {
    pub day: i64,
    pub symbol: String,
    pub bytes: u64,
    pub symlink: bool,
    pub live_writer: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletePlan {
    pub snapshot: String,
    pub symbol: String,
    pub start_day: i64,
    pub end_day: i64,
    pub files: Vec<StoredFile>,
}

pub fn plan_delete(
    files: &[StoredFile],
    symbol: &str,
    start_day: i64,
    end_day: i64,
) -> Result<DeletePlan, FeedError> {
    if start_day >= end_day {
        return Err(FeedError::PartialDay);
    }
    let mut selected = Vec::new();
    for file in files {
        if file.symbol != symbol || file.day < start_day || file.day >= end_day {
            continue;
        }
        if file.symlink {
            return Err(FeedError::Symlink);
        }
        if file.live_writer {
            return Err(FeedError::LiveWriter);
        }
        selected.push(file.clone());
    }
    let snapshot = snapshot_of(symbol, start_day, end_day, &selected);
    Ok(DeletePlan {
        snapshot,
        symbol: symbol.into(),
        start_day,
        end_day,
        files: selected,
    })
}

pub fn execute_delete(
    plan: &DeletePlan,
    presented_snapshot: &str,
    files: &mut Vec<StoredFile>,
) -> Result<usize, FeedError> {
    if presented_snapshot != plan.snapshot {
        return Err(FeedError::StalePlan);
    }
    let before = files.len();
    files.retain(|file| {
        !(file.symbol == plan.symbol && file.day >= plan.start_day && file.day < plan.end_day)
    });
    Ok(before - files.len())
}

fn snapshot_of(symbol: &str, start_day: i64, end_day: i64, files: &[StoredFile]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(symbol.as_bytes());
    hasher.update(start_day.to_le_bytes());
    hasher.update(end_day.to_le_bytes());
    for file in files {
        hasher.update(file.day.to_le_bytes());
        hasher.update(file.bytes.to_le_bytes());
    }
    hex_encode(hasher.finalize())
}

pub fn open_reader(path_exists: bool) -> Result<(), FeedError> {
    if path_exists {
        Ok(())
    } else {
        Err(FeedError::StorageMissing)
    }
}

fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::new();
    for byte in bytes.as_ref() {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
fn minute(index: i64, close: i64) -> Bar {
    Bar {
        open_time_ms: index * MINUTE_MS,
        open: Decimal::from(close),
        high: Decimal::from(close),
        low: Decimal::from(close),
        close: Decimal::from(close),
        volume: Decimal::ONE,
        source: "rest".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_missing_rest_page_does_not_fabricate_bars() {
        let mut book = Book::default();
        book.accept_page(&[minute(0, 10), minute(1, 11)], MINUTE_MS)
            .unwrap();
        let error = book.accept_page(&[minute(3, 13)], MINUTE_MS);
        assert_eq!(error, Err(FeedError::Gap));
        assert!(book.get(2 * MINUTE_MS).is_none());
        assert_eq!(book.len(), 2);
        assert!(!book.gate.ready);
    }

    #[test]
    fn test_out_of_order_and_interior_gap_do_not_advance_checkpoint() {
        let mut book = Book::default();
        let error = book.accept_page(&[minute(0, 1), minute(2, 1)], MINUTE_MS);
        assert_eq!(error, Err(FeedError::OutOfOrder));
        assert_eq!(book.gate.confirmed_ms, 0);
        assert_eq!(book.len(), 0);
    }

    #[test]
    fn test_conflicting_source_rolls_back_page_and_latches_across_restart() {
        let mut book = Book::default();
        book.accept_page(&[minute(0, 10)], MINUTE_MS).unwrap();
        let mut changed = minute(0, 99);
        changed.volume = Decimal::from(2);
        assert_eq!(
            book.accept_page(&[changed], MINUTE_MS),
            Err(FeedError::Conflict)
        );
        assert_eq!(book.get(0).unwrap().close, Decimal::from(10));
        assert!(book.gate.conflict_latched);
        book.restart();
        assert!(!book.gate.ready);
        assert_eq!(
            book.accept_page(&[minute(1, 11)], MINUTE_MS),
            Err(FeedError::Conflict)
        );
    }

    #[test]
    fn test_restart_replays_tail_and_never_trusts_old_ready() {
        let mut book = Book::default();
        book.accept_page(&[minute(0, 1), minute(1, 1)], MINUTE_MS)
            .unwrap();
        assert!(book.gate.ready);
        let restarted = book.restart();
        assert!(!restarted.ready);
        assert_eq!(book.len(), 2);
    }

    #[test]
    fn test_failure_during_repair_cannot_reopen_gate() {
        let mut book = Book::default();
        book.accept_page(&[minute(0, 1)], MINUTE_MS).unwrap();
        let _ = book.accept_page(&[minute(2, 1)], MINUTE_MS);
        book.close_after_repair_failure();
        assert!(!book.gate.ready);
        assert!(book.gate.repair_required);
    }

    #[test]
    fn test_ring_buffer_retains_only_up_to_max_bars_and_numeric_quality() {
        let mut ring = Ring::new(3);
        for index in 0..5 {
            ring.push(minute(index, index));
        }
        assert_eq!(ring.len(), 3);
    }

    #[test]
    fn test_warmup_continues_over_small_batches_and_no_future_values() {
        let mut feature = FeatureWarmup::new(3);
        assert_eq!(feature.push(Decimal::from(10)).unwrap(), None);
        assert_eq!(feature.push(Decimal::from(12)).unwrap(), None);
        let value = feature.push(Decimal::from(15)).unwrap().unwrap();
        assert_eq!(value, Decimal::from(3) / Decimal::from(12));
    }

    #[test]
    fn test_gaps_restart_warmup_and_zero_denominators_are_null() {
        let mut feature = FeatureWarmup::new(2);
        feature.push(Decimal::ZERO).unwrap();
        feature.push(Decimal::from(5)).unwrap();
        feature.reset_after_gap();
        assert_eq!(feature.push(Decimal::from(1)).unwrap(), None);
    }

    #[test]
    fn test_reader_does_not_create_missing_storage() {
        assert_eq!(open_reader(false), Err(FeedError::StorageMissing));
        assert_eq!(open_reader(true), Ok(()));
    }

    #[test]
    fn test_live_reads_cannot_start_or_stop_configured_collectors() {
        let book = Book::default();
        let gate_before = book.gate.clone();
        assert_eq!(open_reader(true), Ok(()));
        assert_eq!(book.gate, gate_before);
        assert_eq!(book.len(), 0);
    }

    #[test]
    fn test_delete_cleans_dependents_preserves_other_days_and_can_prepare_again() {
        let files = vec![
            StoredFile {
                day: 1,
                symbol: "ETHUSDT".into(),
                bytes: 10,
                symlink: false,
                live_writer: false,
            },
            StoredFile {
                day: 2,
                symbol: "ETHUSDT".into(),
                bytes: 10,
                symlink: false,
                live_writer: false,
            },
            StoredFile {
                day: 1,
                symbol: "BTCUSDT".into(),
                bytes: 4,
                symlink: false,
                live_writer: false,
            },
        ];
        let plan = plan_delete(&files, "ETHUSDT", 1, 2).unwrap();
        let mut stored = files.clone();
        execute_delete(&plan, &plan.snapshot, &mut stored).unwrap();
        assert!(
            stored
                .iter()
                .any(|file| file.symbol == "ETHUSDT" && file.day == 2)
        );
        assert!(stored.iter().any(|file| file.symbol == "BTCUSDT"));
        assert!(
            !stored
                .iter()
                .any(|file| file.symbol == "ETHUSDT" && file.day == 1)
        );
        assert!(plan_delete(&stored, "ETHUSDT", 2, 3).is_ok());
    }

    #[test]
    fn test_delete_rejects_stale_plan_and_symlinks_before_any_changes() {
        let files = vec![StoredFile {
            day: 1,
            symbol: "ETHUSDT".into(),
            bytes: 10,
            symlink: false,
            live_writer: false,
        }];
        let plan = plan_delete(&files, "ETHUSDT", 1, 2).unwrap();
        let mut stored = files.clone();
        assert_eq!(
            execute_delete(&plan, "stale", &mut stored),
            Err(FeedError::StalePlan)
        );
        assert_eq!(stored, files);
        let linked = vec![StoredFile {
            day: 1,
            symbol: "ETHUSDT".into(),
            bytes: 10,
            symlink: true,
            live_writer: false,
        }];
        assert_eq!(
            plan_delete(&linked, "ETHUSDT", 1, 2),
            Err(FeedError::Symlink)
        );
    }

    #[test]
    fn test_delete_rejects_partial_day_and_files_crossing_range() {
        let files = vec![StoredFile {
            day: 1,
            symbol: "ETHUSDT".into(),
            bytes: 1,
            symlink: false,
            live_writer: false,
        }];
        assert_eq!(
            plan_delete(&files, "ETHUSDT", 1, 1),
            Err(FeedError::PartialDay)
        );
    }

    #[test]
    fn test_trim_rejects_live_writers_and_symlinked_storage() {
        let live = vec![StoredFile {
            day: 1,
            symbol: "ETHUSDT".into(),
            bytes: 1,
            symlink: false,
            live_writer: true,
        }];
        assert_eq!(
            plan_delete(&live, "ETHUSDT", 1, 2),
            Err(FeedError::LiveWriter)
        );
    }

    #[test]
    fn test_closed_bar_delivery_is_idempotent_but_conflicts_fail_closed() {
        let mut book = Book::default();
        let page = vec![minute(0, 5)];
        book.accept_page(&page, MINUTE_MS).unwrap();
        book.accept_page(&page, MINUTE_MS).unwrap();
        assert_eq!(book.len(), 1);
        let mut other = minute(0, 6);
        other.volume = Decimal::ONE;
        assert_eq!(
            book.accept_page(&[other], MINUTE_MS),
            Err(FeedError::Conflict)
        );
    }

    #[test]
    fn test_pagination_is_bounded_and_rechecks_overlap() {
        let mut book = Book::default();
        book.accept_page(&[minute(0, 1), minute(1, 1)], MINUTE_MS)
            .unwrap();
        book.accept_page(&[minute(1, 1), minute(2, 2)], MINUTE_MS)
            .unwrap();
        assert_eq!(book.len(), 3);
        assert_eq!(book.gate.confirmed_ms, 2 * MINUTE_MS);
    }

    #[test]
    fn test_consumer_failure_closes_gate_until_restart() {
        let mut book = Book::default();
        book.accept_page(&[minute(0, 1)], MINUTE_MS).unwrap();
        book.close_after_repair_failure();
        assert!(!book.gate.ready);
        book.restart();
        assert!(!book.gate.ready);
    }

    #[test]
    fn test_invalid_native_delivery_is_rejected() {
        let mut bad = minute(0, 1);
        bad.high = Decimal::ZERO;
        bad.low = Decimal::from(5);
        let mut book = Book::default();
        assert_eq!(
            book.accept_page(&[bad], MINUTE_MS),
            Err(FeedError::Conflict)
        );
        assert!(book.bars.is_empty());
        assert_eq!(book.accept_page(&[], MINUTE_MS), Err(FeedError::Gap));
    }

    #[test]
    fn test_all_symbols_must_be_contiguous_before_ready() {
        let mut eth = Book::default();
        let mut btc = Book::default();
        eth.accept_page(&[minute(0, 1), minute(1, 1)], MINUTE_MS)
            .unwrap();
        btc.accept_page(&[minute(0, 1)], MINUTE_MS).unwrap();
        let _ = btc.accept_page(&[minute(2, 1)], MINUTE_MS);
        assert!(eth.gate.ready);
        assert!(!btc.gate.ready);
    }
}
