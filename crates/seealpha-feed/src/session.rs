//! Live delivery, clock health, and retry. Catch-up bars do not allow new
//! risk. A heartbeat alone does not reopen a gate that still needs repair.
use std::collections::BTreeSet;

#[derive(Debug, PartialEq, Eq)]
pub enum SessionError {
    Future,
    Expired,
}

#[derive(Clone, Debug)]
pub struct Delivery {
    emitted: BTreeSet<i64>,
    pub allow_new_risk: bool,
    pub degraded: bool,
    pub repair_required: bool,
    pub clock_ms: i64,
    pub clock_healthy: bool,
    pub retry_after_ms: Option<i64>,
    ready_until_ms: i64,
    source_verified: bool,
}

impl Delivery {
    pub fn new(now_ms: i64) -> Self {
        Self {
            emitted: BTreeSet::new(),
            allow_new_risk: false,
            degraded: false,
            repair_required: false,
            clock_ms: now_ms,
            clock_healthy: true,
            retry_after_ms: None,
            ready_until_ms: now_ms,
            source_verified: false,
        }
    }

    pub fn deliver(&mut self, stamps: &[i64], live: bool) -> Vec<i64> {
        let fresh: Vec<i64> = stamps
            .iter()
            .copied()
            .filter(|stamp| self.emitted.insert(*stamp))
            .collect();
        self.allow_new_risk = live && self.source_verified
            && self.clock_ms <= self.ready_until_ms
            && !self.degraded && !self.repair_required && self.clock_healthy;
        fresh
    }

    pub fn degrade(&mut self) {
        self.source_verified = false;
        self.degraded = true;
        self.repair_required = true;
        self.allow_new_risk = false;
    }

    pub fn heartbeat(&mut self, now_ms: i64) {
        if now_ms >= self.clock_ms {
            self.clock_ms = now_ms;
        }
    }

    pub fn note_retry_after(&mut self, delay_ms: i64) {
        self.retry_after_ms = Some(delay_ms);
    }

    pub fn duplicate_page(&mut self, stamps: &[i64]) -> Vec<i64> {
        self.deliver(stamps, false)
    }

    pub fn fail_clock_sync(&mut self) {
        self.source_verified = false;
        self.clock_healthy = false;
        self.allow_new_risk = false;
        self.degraded = true;
    }

    pub fn advance_clock(&mut self, now_ms: i64) {
        if now_ms > self.clock_ms {
            self.clock_ms = now_ms;
        }
    }

    pub fn reject_future(&mut self, stamp_ms: i64) -> Result<(), SessionError> {
        if stamp_ms > self.clock_ms {
            return Err(SessionError::Future);
        }
        Ok(())
    }

    pub fn status(&self, now_ms: i64) -> Result<(), SessionError> {
        if !self.source_verified || self.degraded || self.repair_required
            || !self.clock_healthy || now_ms > self.ready_until_ms {
            Err(SessionError::Expired)
        } else {
            Ok(())
        }
    }

    /// A source proof must first pass Book's exchange-clock, journal,
    /// overlap and gap checks. Receiving live packets alone cannot open risk.
    pub fn certify_from_book(&mut self, book: &crate::Book, now_ms: i64) -> bool {
        self.source_verified = false;
        self.allow_new_risk = false;
        if !book.verified_ready(now_ms) || self.degraded || self.repair_required
            || !self.clock_healthy || now_ms < self.clock_ms {
            return false;
        }
        let Some(until_ms) = book.verified_until_ms() else { return false; };
        self.clock_ms = now_ms;
        self.ready_until_ms = until_ms;
        self.source_verified = true;
        true
    }

    pub fn set_ready_until(&mut self, until_ms: i64) {
        self.ready_until_ms = until_ms;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cold_start_overlap_and_live_delivery_are_distinct() {
        let mut delivery = Delivery::new(0);
        let first = delivery.deliver(&[0, 1, 2], false);
        assert_eq!(first, vec![0, 1, 2]);
        assert!(!delivery.allow_new_risk);
        let overlap = delivery.deliver(&[2, 3], true);
        assert_eq!(overlap, vec![3]);
        assert!(!delivery.allow_new_risk); // Live label alone is not provenance.
    }

    #[test]
    fn test_disconnect_catchup_does_not_emit_delayed_signals() {
        let mut delivery = Delivery::new(0);
        delivery.deliver(&[0, 1, 2], true);
        delivery.degrade();
        assert!(!delivery.allow_new_risk);
        let catchup = delivery.deliver(&[3, 4, 5], true);
        assert_eq!(catchup, vec![3, 4, 5]);
        assert!(!delivery.allow_new_risk);
        delivery.heartbeat(10);
        assert!(delivery.repair_required);
        assert!(!delivery.allow_new_risk);
    }

    #[test]
    fn test_gate_closes_and_requires_repair_after_health_recovers() {
        let mut delivery = Delivery::new(0);
        delivery.deliver(&[0], true);
        delivery.degrade();
        delivery.heartbeat(5);
        assert!(delivery.degraded);
        assert!(!delivery.allow_new_risk);
    }

    #[test]
    fn test_clock_sync_failure_invalidates_gate_without_losing_monotonic_clock() {
        let mut delivery = Delivery::new(50);
        assert!(delivery.clock_healthy);
        delivery.fail_clock_sync();
        assert!(!delivery.clock_healthy);
        delivery.advance_clock(40);
        assert_eq!(delivery.clock_ms, 50);
        delivery.advance_clock(60);
        assert_eq!(delivery.clock_ms, 60);
    }

    #[test]
    fn test_future_bar_rejected_atomically_and_volume_noise_is_tolerated() {
        let mut delivery = Delivery::new(60_000);
        let before = delivery.clock_ms;
        assert_eq!(delivery.reject_future(120_000), Err(SessionError::Future));
        assert_eq!(delivery.clock_ms, before);
        assert!(delivery.reject_future(60_000).is_ok());
    }

    #[test]
    fn test_retry_after_is_respected_without_duplicate_data() {
        let mut delivery = Delivery::new(0);
        delivery.note_retry_after(7);
        let first = delivery.deliver(&[1, 2], false);
        let second = delivery.duplicate_page(&[1, 2]);
        assert_eq!(first, vec![1, 2]);
        assert!(second.is_empty());
        assert_eq!(delivery.retry_after_ms, Some(7));
    }

    #[test]
    fn test_status_command_rejects_expired_ready() {
        let mut delivery = Delivery::new(0);
        delivery.set_ready_until(10);
        assert_eq!(delivery.status(10), Err(SessionError::Expired));
        assert_eq!(delivery.status(11), Err(SessionError::Expired));
    }

    #[test]
    fn test_completed_day_audit_conflict_blocks_feed() {
        let mut delivery = Delivery::new(0);
        delivery.deliver(&[0], true);
        delivery.degrade();
        assert!(delivery.repair_required);
        assert!(!delivery.allow_new_risk);
    }

    #[test]
    fn test_only_committed_reconciled_live_bars_can_open_session_gate() {
        use rust_decimal::Decimal;
        let mut book = crate::Book::default();
        let candle = crate::Bar {
            open_time_ms: 0, open: Decimal::ONE, high: Decimal::ONE,
            low: Decimal::ONE, close: Decimal::ONE, volume: Decimal::ONE,
            source: "official".into(),
        };
        book.accept_page(&[candle], crate::MINUTE_MS).unwrap();
        let mut delivery = Delivery::new(0);
        assert!(!delivery.certify_from_book(&book, crate::MINUTE_MS));
        assert!(!delivery.allow_new_risk);
        book.confirm_source(crate::SourceProof {
            latest_closed_open_ms: 0, journal_committed_open_ms: 0,
            exchange_confirmed_ms: 61_000,
            checked_at_ms: 60_000, valid_until_ms: 62_000,
            connected: true, clock_healthy: true, overlap_verified: true,
            history_contiguous: true, unresolved_gaps: 0, backlog: 0,
        }).unwrap();
        assert!(delivery.certify_from_book(&book, 60_000));
        assert!(delivery.deliver(&[0], true).is_empty() == false);
        assert!(delivery.allow_new_risk);
        assert_eq!(delivery.status(62_001), Err(SessionError::Expired));
        delivery.degrade();
        assert!(!delivery.allow_new_risk);
        assert!(!delivery.certify_from_book(&book, 61_000));
    }

}
