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
        return Err(CatalogError::Invalid("交易所 K 线对照范围无效或为空。".into()));
    }
    let mut cursor = start_ms;
    let available_start = crate::bar_window(interval, available_ms)?.0;
    for (index, row) in rows.iter().enumerate() {
        let left = row.bar.open_time_ms;
        let (aligned_left, aligned_end) = crate::bar_window(interval, left)?;
        if aligned_left != left || row.end_ms != aligned_end
            || row.end_ms > end_ms || row.end_ms <= cursor || row.trade_count < 0
        {
            return Err(CatalogError::Invalid("交易所 K 线边界或成交数量无效。".into()));
        }
        if left != cursor
            && !(index == 0 && cursor < available_ms && left == available_start)
        {
            return Err(CatalogError::Invalid("交易所 K 线存在缺口或乱序。".into()));
        }
        row.bar.validate(interval, product)?;
        cursor = row.end_ms;
    }
    if cursor != end_ms {
        return Err(CatalogError::Missing("交易所 K 线对照缺少尾部数据。".into()));
    }
    Ok(())
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
        assert!(validate_venue_klines(
            &rows, "1m", 0, 120_000, 0, crate::MarketProduct::FuturesUm
        ).is_ok());
    }

    #[test]
    fn rejects_missing_middle_or_tail_rows_and_bad_close() {
        let product = crate::MarketProduct::FuturesUm;
        assert!(validate_venue_klines(&[candle(0, 60_000), candle(120_000, 180_000)],
            "1m", 0, 180_000, 0, product).is_err());
        assert!(validate_venue_klines(&[candle(0, 60_000)],
            "1m", 0, 120_000, 0, product).is_err());
        assert!(validate_venue_klines(&[candle(0, 59_999)],
            "1m", 0, 60_000, 0, product).is_err());
        assert!(validate_venue_klines(&[candle(0, 60_000), candle(0, 60_000)],
            "1m", 0, 120_000, 0, product).is_err());
    }

    #[test]
    fn permits_only_explicit_unavailable_leading_prefix() {
        let product = crate::MarketProduct::FuturesUm;
        assert!(validate_venue_klines(&[candle(60_000, 120_000)],
            "1m", 0, 120_000, 60_000, product).is_ok());
        assert!(validate_venue_klines(&[candle(60_000, 120_000)],
            "1m", 0, 120_000, 0, product).is_err());
    }

    #[test]
    fn rejects_bad_prices_and_trade_counts() {
        let mut row = candle(0, 60_000);
        row.trade_count = -1;
        assert!(validate_venue_klines(&[row.clone()],
            "1m", 0, 60_000, 0, crate::MarketProduct::Spot).is_err());
        row.trade_count = 1;
        row.bar.high = Decimal::from(8);
        assert!(validate_venue_klines(&[row],
            "1m", 0, 60_000, 0, crate::MarketProduct::Spot).is_err());
    }
}
