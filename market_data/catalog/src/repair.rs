//! Strict UTC bar-gap planner extracted from the legacy Python repair quality rules.
//! Pure, bounded and read-only: missing windows must never be silently filled.
use crate::{CatalogError, BarGap};

pub fn missing_bar_windows(
    opens_ms: &[i64],
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<Vec<BarGap>, CatalogError> {
    if start_ms < 0 || step_ms <= 0 || end_ms <= start_ms
        || (end_ms - start_ms) % step_ms != 0
    {
        return Err(CatalogError::Invalid("历史修复范围必须非空且按周期对齐。".into()));
    }
    let count = (end_ms - start_ms) / step_ms;
    if count > 50_000 || opens_ms.len() > 50_000 {
        return Err(CatalogError::Limit("历史修复缺口规划超出单次上限。".into()));
    }
    let mut missing = Vec::new();
    let mut expected = start_ms;
    for &open in opens_ms {
        if open < expected || open < start_ms || open >= end_ms || (open - start_ms) % step_ms != 0 {
            return Err(CatalogError::Invalid("历史 K 线时间重复、倒退或不对齐。".into()));
        }
        if open > expected {
            missing.push(BarGap { start_ms: expected, end_ms: open });
        }
        expected = open.checked_add(step_ms)
            .ok_or_else(|| CatalogError::Invalid("历史时间溢出。".into()))?;
    }
    if expected < end_ms {
        missing.push(BarGap { start_ms: expected, end_ms });
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
                BarGap { start_ms: 120_000, end_ms: 180_000 },
                BarGap { start_ms: 300_000, end_ms: 360_000 },
            ]
        );
        assert_eq!(
            missing_bar_windows(&[], 0, 120_000, 60_000).unwrap(),
            vec![BarGap { start_ms: 0, end_ms: 120_000 }]
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
