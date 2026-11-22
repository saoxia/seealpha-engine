//! Market-data quality primitives used by the native pipeline.
//! Preserve the previous Python gap and validation behavior on valid input,
//! and fail closed on overflow or malformed rows. Order sizing does not use these f64 checks.
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventRange {
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualityError {
    InvalidStep,
    InvalidRange,
    Overflow,
    Empty,
    InvalidMakerFlag,
    InvalidKline(&'static str),
    InvalidTrade(&'static str),
}

impl std::fmt::Display for QualityError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "{self:?}")
    }
}
impl std::error::Error for QualityError {}

/// Inclusive event-time gaps, matching validate_data.missing_event_ranges
/// for valid aligned intervals. Does not mutate the caller's slice.
pub fn missing_event_ranges(
    intervals: &[EventRange],
    required_start: i64,
    required_end: i64,
    step_ns: i64,
) -> Result<Vec<EventRange>, QualityError> {
    if step_ns <= 0 {
        return Err(QualityError::InvalidStep);
    }
    if required_start > required_end || intervals.iter().any(|r| r.start > r.end) {
        return Err(QualityError::InvalidRange);
    }
    let mut spans = intervals.to_vec();
    spans.sort_unstable_by_key(|r| (r.start, r.end));
    let mut cursor = required_start;
    let mut gaps = Vec::new();
    for span in spans {
        if span.end < cursor || span.start > required_end {
            continue;
        }
        if span.start > cursor {
            let gap_end = span
                .start
                .checked_sub(step_ns)
                .ok_or(QualityError::Overflow)?
                .min(required_end);
            if cursor <= gap_end {
                gaps.push(EventRange {
                    start: cursor,
                    end: gap_end,
                });
            }
        }
        if span.end >= required_end {
            return Ok(gaps);
        }
        let next = span
            .end
            .checked_add(step_ns)
            .ok_or(QualityError::Overflow)?;
        cursor = cursor.max(next);
        if cursor > required_end {
            return Ok(gaps);
        }
    }
    if cursor <= required_end {
        gaps.push(EventRange {
            start: cursor,
            end: required_end,
        });
    }
    Ok(gaps)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Kline {
    pub open_time_ms: i64,
    pub close_time_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// Validate normalized Binance closed OHLCV bars without per-row allocations.
pub fn validate_klines(rows: &[Kline], interval_ms: i64) -> Result<(), QualityError> {
    if rows.is_empty() {
        return Err(QualityError::Empty);
    }
    if interval_ms <= 0 {
        return Err(QualityError::InvalidStep);
    }
    let mut previous: Option<i64> = None;
    for row in rows {
        if !(0..100_000_000_000_000).contains(&row.open_time_ms) {
            return Err(QualityError::InvalidKline("timestamp unit"));
        }
        if let Some(before) = previous
            && (row.open_time_ms <= before
                || row.open_time_ms.checked_sub(before) != Some(interval_ms))
        {
            return Err(QualityError::InvalidKline("open time order/gap"));
        }
        let expected_close = row
            .open_time_ms
            .checked_add(interval_ms)
            .and_then(|t| t.checked_sub(1))
            .ok_or(QualityError::Overflow)?;
        if row.close_time_ms != expected_close {
            return Err(QualityError::InvalidKline("close time"));
        }
        if ![row.open, row.high, row.low, row.close, row.volume]
            .iter()
            .all(|v| v.is_finite())
        {
            return Err(QualityError::InvalidKline("nonfinite"));
        }
        if !(row.low <= row.open
            && row.low <= row.close
            && row.high >= row.open
            && row.high >= row.close
            && row.low <= row.high)
        {
            return Err(QualityError::InvalidKline("OHLC"));
        }
        if row.volume < 0.0 {
            return Err(QualityError::InvalidKline("negative volume"));
        }
        previous = Some(row.open_time_ms);
    }
    Ok(())
}

pub fn parse_buyer_maker(text: &str) -> Result<bool, QualityError> {
    let text = text.trim();
    if text.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if text.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(QualityError::InvalidMakerFlag)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Trade {
    pub trade_id: i64,
    pub timestamp: i64,
    pub price: f64,
    pub quantity: f64,
    pub quote_quantity: f64,
    pub buyer_maker: bool,
}

/// Retain the last duplicate trade ID while preserving source order.
/// Memory is bounded by input batch length, which callers must limit.
pub fn validate_trades(rows: &[Trade], microseconds: bool) -> Result<Vec<Trade>, QualityError> {
    if rows.is_empty() {
        return Err(QualityError::Empty);
    }
    let (lower, upper) = if microseconds {
        (100_000_000_000_000, 100_000_000_000_000_000)
    } else {
        (0, 100_000_000_000_000)
    };
    for row in rows {
        if !(lower..upper).contains(&row.timestamp) {
            return Err(QualityError::InvalidTrade("timestamp unit"));
        }
        if !row.price.is_finite() || row.price <= 0.0 {
            return Err(QualityError::InvalidTrade("price"));
        }
        if !row.quantity.is_finite() || row.quantity <= 0.0 {
            return Err(QualityError::InvalidTrade("quantity"));
        }
        if !row.quote_quantity.is_finite() || row.quote_quantity < 0.0 {
            return Err(QualityError::InvalidTrade("quote quantity"));
        }
    }
    // Common already-sorted case avoids allocating a HashSet.
    if rows.windows(2).all(|w| w[0].trade_id < w[1].trade_id) {
        if rows.windows(2).any(|w| w[0].timestamp > w[1].timestamp) {
            return Err(QualityError::InvalidTrade("timestamp order"));
        }
        return Ok(rows.to_vec());
    }
    let mut seen = HashSet::with_capacity(rows.len());
    let mut kept = Vec::with_capacity(rows.len());
    for row in rows.iter().rev() {
        if seen.insert(row.trade_id) {
            kept.push(*row);
        }
    }
    kept.reverse();
    if kept.windows(2).any(|w| w[0].timestamp > w[1].timestamp) {
        return Err(QualityError::InvalidTrade("timestamp order"));
    }
    if kept.windows(2).any(|w| w[0].trade_id >= w[1].trade_id) {
        return Err(QualityError::InvalidTrade("trade ID order"));
    }
    Ok(kept)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn range(start: i64, end: i64) -> EventRange {
        EventRange { start, end }
    }
    fn bar(t: i64) -> Kline {
        Kline {
            open_time_ms: t,
            close_time_ms: t + 59_999,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1.0,
        }
    }
    fn trade(id: i64, timestamp: i64) -> Trade {
        Trade {
            trade_id: id,
            timestamp,
            price: 100.0,
            quantity: 1.0,
            quote_quantity: 100.0,
            buyer_maker: false,
        }
    }

    #[test]
    fn finds_inclusive_gaps_with_unordered_coverage() {
        assert_eq!(
            missing_event_ranges(&[range(500, 500), range(200, 300)], 100, 600, 100),
            Ok(vec![range(100, 100), range(400, 400), range(600, 600)])
        );
        assert_eq!(
            missing_event_ranges(&[range(100, 200), range(200, 400)], 100, 500, 100),
            Ok(vec![range(500, 500)])
        );
        assert_eq!(
            missing_event_ranges(&[range(100, 500)], 100, 500, 100),
            Ok(vec![])
        );
        assert_eq!(
            missing_event_ranges(&[], 100, 300, 100),
            Ok(vec![range(100, 300)])
        );
    }

    #[test]
    fn gaps_reject_invalid_ranges_and_overflow() {
        assert_eq!(
            missing_event_ranges(&[], 1, 3, 0),
            Err(QualityError::InvalidStep)
        );
        assert_eq!(
            missing_event_ranges(&[range(4, 3)], 1, 6, 1),
            Err(QualityError::InvalidRange)
        );
        assert_eq!(
            missing_event_ranges(&[], 6, 1, 1),
            Err(QualityError::InvalidRange)
        );
        assert_eq!(
            missing_event_ranges(&[range(i64::MIN + 1, 100)], i64::MIN, 200, 4),
            Err(QualityError::Overflow)
        );
    }

    #[test]
    fn kline_validation_rejects_bad_prices_and_time() {
        assert_eq!(validate_klines(&[bar(0), bar(60_000)], 60_000), Ok(()));
        assert_eq!(
            validate_klines(&[bar(0), bar(120_000)], 60_000),
            Err(QualityError::InvalidKline("open time order/gap"))
        );
        let mut invalid = bar(0);
        invalid.low = 102.0;
        assert_eq!(
            validate_klines(&[invalid], 60_000),
            Err(QualityError::InvalidKline("OHLC"))
        );
        invalid = bar(0);
        invalid.volume = f64::NAN;
        assert_eq!(
            validate_klines(&[invalid], 60_000),
            Err(QualityError::InvalidKline("nonfinite"))
        );
    }

    #[test]
    fn maker_flag_is_strict() {
        assert_eq!(parse_buyer_maker(" TRUE "), Ok(true));
        assert_eq!(parse_buyer_maker("false"), Ok(false));
        assert_eq!(parse_buyer_maker("1"), Err(QualityError::InvalidMakerFlag));
    }

    #[test]
    fn trade_dedup_keeps_last_and_checks_order() {
        assert_eq!(
            validate_trades(&[trade(1, 1), trade(1, 2), trade(2, 2)], false),
            Ok(vec![trade(1, 2), trade(2, 2)])
        );
        assert_eq!(
            validate_trades(&[trade(1, 3), trade(2, 2)], false),
            Err(QualityError::InvalidTrade("timestamp order"))
        );
        assert_eq!(
            validate_trades(&[trade(2, 1), trade(1, 2)], false),
            Err(QualityError::InvalidTrade("trade ID order"))
        );
    }

    #[test]
    fn trade_units_and_values_fail_closed() {
        assert_eq!(
            validate_trades(&[trade(1, 1_700_000_000_000_000)], false),
            Err(QualityError::InvalidTrade("timestamp unit"))
        );
        assert!(validate_trades(&[trade(1, 1_700_000_000_000_000)], true).is_ok());
        let mut invalid = trade(1, 1);
        invalid.price = f64::INFINITY;
        assert_eq!(
            validate_trades(&[invalid], false),
            Err(QualityError::InvalidTrade("price"))
        );
    }

    #[test]
    fn test_archive_validation_rejects_internal_gap() {
        assert_eq!(
            validate_klines(&[bar(0), bar(120_000)], 60_000),
            Err(QualityError::InvalidKline("open time order/gap"))
        );
    }

    #[test]
    fn test_archive_validation_rejects_invalid_ohlc() {
        let mut row = bar(0);
        row.high = 1.0;
        row.low = 5.0;
        assert_eq!(
            validate_klines(&[row], 60_000),
            Err(QualityError::InvalidKline("OHLC"))
        );
    }

    #[test]
    fn test_trade_archive_keeps_last_duplicate_id_across_chunks() {
        let kept = validate_trades(&[trade(1, 1), trade(1, 2), trade(2, 2)], false).unwrap();
        assert_eq!(kept, vec![trade(1, 2), trade(2, 2)]);
    }

    #[test]
    fn test_trade_archive_rejects_descending_ids() {
        assert_eq!(
            validate_trades(&[trade(2, 1), trade(1, 2)], false),
            Err(QualityError::InvalidTrade("trade ID order"))
        );
    }

    #[test]
    fn test_trade_archive_rejects_invalid_maker_flag() {
        assert_eq!(parse_buyer_maker("1"), Err(QualityError::InvalidMakerFlag));
        assert_eq!(
            parse_buyer_maker("yes"),
            Err(QualityError::InvalidMakerFlag)
        );
    }

    #[test]
    fn test_mixed_boolean_and_numeric_maker_is_not_silently_accepted() {
        assert_eq!(parse_buyer_maker("true"), Ok(true));
        assert_eq!(parse_buyer_maker("0"), Err(QualityError::InvalidMakerFlag));
    }

    #[test]
    fn test_trade_archive_validates_discarded_duplicates_and_normalizes_flags() {
        let kept = validate_trades(&[trade(1, 1), trade(1, 1), trade(2, 2)], false).unwrap();
        assert_eq!(kept.len(), 2);
        assert!(parse_buyer_maker(" TRUE ").unwrap());
    }
}

/// File-only quality observations for the native source supervisor.
pub mod monitor;

/// Source-authored reconciliation and data READY checks (not a socket health check).
pub mod readiness;
/// Fail-closed live delivery and recovery policy.
pub mod session;
pub use readiness::{Bar, Book, FeedError, Gate, MINUTE_MS, SourceProof};
