//! Trusted source evidence and fail-closed closed-bar readiness checks.
//! These are SeeAlpha integrity rules, not a parallel Nautilus subscription/cache.
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use thiserror::Error;

pub const MINUTE_MS: i64 = 60_000;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FeedError {
    #[error("缺口未补齐，检查点不前进。")]
    Gap,
    #[error("乱序或内部缺口，检查点不前进。")]
    OutOfOrder,
    #[error("来源冲突，本页已回滚。")]
    Conflict,
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

/// Evidence supplied by a trusted producer/reconciliation coordinator.
/// The reader must not manufacture these confirmations from a received bar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceProof {
    pub latest_closed_open_ms: i64,
    pub journal_committed_open_ms: i64,
    pub exchange_confirmed_ms: i64,
    pub checked_at_ms: i64,
    pub valid_until_ms: i64,
    pub connected: bool,
    pub clock_healthy: bool,
    pub overlap_verified: bool,
    pub history_contiguous: bool,
    pub unresolved_gaps: u32,
    pub backlog: u32,
}

#[derive(Debug, Default)]
pub struct Book {
    bars: BTreeMap<i64, Bar>,
    pub gate: Gate,
    step_ms: Option<i64>,
    proof: Option<SourceProof>,
}

impl Default for Gate {
    fn default() -> Self {
        Self::new()
    }
}

impl Book {
    pub fn accept_page(&mut self, page: &[Bar], step_ms: i64) -> Result<(), FeedError> {
        // A newly received page must be reconciled and committed again before
        // it can be exposed as verified live data, even if it is a duplicate.
        self.gate.ready = false;
        self.proof = None;
        if self.gate.conflict_latched {
            return Err(FeedError::Conflict);
        }
        if page.is_empty() {
            return Err(FeedError::Gap);
        }
        if step_ms <= 0
            || self.step_ms.is_some_and(|old| old != step_ms)
            || page.iter().any(|bar| {
                bar.open_time_ms < 0
                    || bar.open_time_ms % step_ms != 0
                    || bar.volume < Decimal::ZERO
            })
        {
            self.gate.repair_required = true;
            return Err(FeedError::OutOfOrder);
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
            if window[1].open_time_ms.checked_sub(window[0].open_time_ms) != Some(step_ms) {
                return Err(FeedError::OutOfOrder);
            }
        }
        if let Some((&last, _)) = self.bars.iter().next_back() {
            let expected = last.checked_add(step_ms).ok_or(FeedError::OutOfOrder)?;
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
                if !same_ohlcv(existing, &bar) {
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
        self.gate.ready = false;
        self.step_ms = Some(step_ms);
        Ok(())
    }

    /// Only an explicit evidence handoff can make a page READY. An empty
    /// REST page, an accepted candle, or a WebSocket heartbeat is not proof.
    pub fn confirm_source(&mut self, proof: SourceProof) -> Result<(), FeedError> {
        self.gate.ready = false;
        self.proof = None;
        let step = self.step_ms.ok_or(FeedError::Gap)?;
        if self.gate.conflict_latched {
            return Err(FeedError::Conflict);
        }
        let end_exclusive = self
            .gate
            .confirmed_ms
            .checked_add(step)
            .ok_or(FeedError::OutOfOrder)?;
        if !proof.connected
            || !proof.clock_healthy
            || !proof.overlap_verified
            || !proof.history_contiguous
            || proof.unresolved_gaps != 0
            || proof.backlog != 0
            || proof.latest_closed_open_ms != self.gate.confirmed_ms
            || proof.journal_committed_open_ms < proof.latest_closed_open_ms
            || proof.exchange_confirmed_ms < end_exclusive
            || proof.checked_at_ms > proof.exchange_confirmed_ms
            || proof.valid_until_ms <= proof.checked_at_ms
        {
            self.gate.repair_required = true;
            return Err(FeedError::Gap);
        }
        self.gate.repair_required = false;
        self.gate.ready = true;
        self.proof = Some(proof);
        Ok(())
    }

    pub fn verified_until_ms(&self) -> Option<i64> {
        if self.gate.ready {
            self.proof.as_ref().map(|proof| proof.valid_until_ms)
        } else {
            None
        }
    }

    /// Verify expiry at the caller's current exchange-checked time.
    pub fn verified_ready(&self, now_ms: i64) -> bool {
        self.gate.ready
            && !self.gate.repair_required
            && !self.gate.conflict_latched
            && self.proof.as_ref().is_some_and(|proof| {
                now_ms >= proof.checked_at_ms && now_ms <= proof.valid_until_ms
            })
    }

    fn conflicts(&self, bar: &Bar) -> bool {
        self.bars
            .get(&bar.open_time_ms)
            .is_some_and(|existing| !same_ohlcv(existing, bar))
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

fn same_ohlcv(left: &Bar, right: &Bar) -> bool {
    left.open == right.open
        && left.high == right.high
        && left.low == right.low
        && left.close == right.close
        && left.volume == right.volume
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
        assert!(!book.gate.ready);
        book.confirm_source(SourceProof {
            latest_closed_open_ms: MINUTE_MS,
            journal_committed_open_ms: MINUTE_MS,
            exchange_confirmed_ms: 2 * MINUTE_MS,
            checked_at_ms: 2 * MINUTE_MS,
            valid_until_ms: 3 * MINUTE_MS,
            connected: true,
            clock_healthy: true,
            overlap_verified: true,
            history_contiguous: true,
            unresolved_gaps: 0,
            backlog: 0,
        })
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
        assert!(!eth.gate.ready); // Not certified merely by ingesting bars.
        assert!(!btc.gate.ready);
    }

    fn proof(last_ms: i64) -> SourceProof {
        SourceProof {
            latest_closed_open_ms: last_ms,
            journal_committed_open_ms: last_ms,
            exchange_confirmed_ms: last_ms + MINUTE_MS + 50,
            checked_at_ms: last_ms + MINUTE_MS,
            valid_until_ms: last_ms + MINUTE_MS + 3_000,
            connected: true,
            clock_healthy: true,
            overlap_verified: true,
            history_contiguous: true,
            unresolved_gaps: 0,
            backlog: 0,
        }
    }

    #[test]
    fn test_ready_needs_committed_reconciled_live_source_not_a_candle() {
        let mut book = Book::default();
        book.accept_page(&[minute(0, 5)], MINUTE_MS).unwrap();
        assert!(!book.gate.ready);
        assert!(!book.verified_ready(MINUTE_MS));
        let mut incomplete = proof(0);
        incomplete.backlog = 1;
        assert_eq!(book.confirm_source(incomplete), Err(FeedError::Gap));
        assert!(!book.gate.ready);
        book.confirm_source(proof(0)).unwrap();
        assert!(book.verified_ready(MINUTE_MS + 50));
        assert!(!book.verified_ready(MINUTE_MS + 3_001));
        book.accept_page(&[minute(1, 6)], MINUTE_MS).unwrap();
        assert!(!book.verified_ready(2 * MINUTE_MS));
        book.restart();
        assert!(!book.gate.ready);
    }

    #[test]
    fn test_same_close_but_different_high_is_a_conflict_and_no_ready() {
        let mut book = Book::default();
        book.accept_page(&[minute(0, 5)], MINUTE_MS).unwrap();
        let mut different = minute(0, 5);
        different.high = Decimal::from(6);
        assert_eq!(
            book.accept_page(&[different], MINUTE_MS),
            Err(FeedError::Conflict)
        );
        assert!(book.gate.conflict_latched);
        assert!(!book.gate.ready);
    }

    #[test]
    fn test_unaligned_history_page_never_mutates_checkpoint() {
        let mut book = Book::default();
        assert_eq!(book.accept_page(&[minute(0, 1)], MINUTE_MS), Ok(()));
        let checkpoint = book.gate.confirmed_ms;
        let mut bad = minute(1, 2);
        bad.open_time_ms += 1;
        assert_eq!(
            book.accept_page(&[bad], MINUTE_MS),
            Err(FeedError::OutOfOrder)
        );
        assert_eq!(book.gate.confirmed_ms, checkpoint);
        assert!(!book.gate.ready);
    }
}
