//! Native, transport-independent historical replay window validation.
//! Consumers supply UTC epoch milliseconds after parsing the HTTP timestamp.
//! End is exclusive and a single replay is bounded to 366 days.
pub const MAX_WINDOW_MS: i64 = 366 * 24 * 60 * 60 * 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BarInterval {
    OneMinute,
    FiveMinutes,
}

impl BarInterval {
    pub fn parse(input: &str) -> Result<Self, &'static str> {
        match input {
            "1m" => Ok(Self::OneMinute),
            "5m" => Ok(Self::FiveMinutes),
            _ => Err("K 线周期只支持 1m 或 5m。"),
        }
    }

    pub const fn duration_ms(self) -> i64 {
        match self {
            Self::OneMinute => 60_000,
            Self::FiveMinutes => 300_000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryWindow {
    pub start_ms: i64,
    pub end_ms: i64,
    pub interval: BarInterval,
}

impl HistoryWindow {
    pub fn new(start_ms: i64, end_ms: i64, interval: &str) -> Result<Self, &'static str> {
        let interval = BarInterval::parse(interval)?;
        if start_ms >= end_ms {
            return Err("开始时间必须早于结束时间。结束时间不包含在区间内。");
        }
        if end_ms.checked_sub(start_ms).is_none_or(|duration| duration > MAX_WINDOW_MS) {
            return Err("单次回测区间不能超过 366 天。");
        }
        Ok(Self { start_ms, end_ms, interval })
    }

    pub fn contains(self, timestamp_ms: i64) -> bool {
        timestamp_ms >= self.start_ms && timestamp_ms < self.end_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclusive_end_and_intervals() {
        let window = HistoryWindow::new(0, 300_000, "5m").unwrap();
        assert!(window.contains(0));
        assert!(window.contains(299_999));
        assert!(!window.contains(300_000));
        assert_eq!(window.interval.duration_ms(), 300_000);
        assert_eq!(BarInterval::OneMinute.duration_ms(), 60_000);
    }

    #[test]
    fn invalid_or_oversized_windows_fail() {
        assert!(HistoryWindow::new(0, 1, "15m").is_err());
        assert!(HistoryWindow::new(1, 1, "1m").is_err());
        assert!(HistoryWindow::new(10, 0, "1m").is_err());
        assert!(HistoryWindow::new(0, MAX_WINDOW_MS + 1, "1m").is_err());
        assert!(HistoryWindow::new(0, MAX_WINDOW_MS, "1m").is_ok());
    }
}
