//! Generic Nautilus BarType and UTC bar-time storage conventions.
use crate::CatalogError;
use chrono::{Datelike, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub const NANO_PER_MS: i64 = 1_000_000;
pub const DAY_MS: i64 = 86_400_000;
/// Nautilus bar identity is supplied by Source; Catalog only formats its storage key.
pub fn bar_type(instrument_id: &str, interval: &str) -> Result<String, CatalogError> {
    crate::reader::safe_component(instrument_id)?;
    let (multiple, unit) = interval_unit(interval)?;
    Ok(format!("{instrument_id}-{multiple}-{unit}-LAST-EXTERNAL"))
}
fn interval_unit(interval: &str) -> Result<(u32, &'static str), CatalogError> {
    match interval {
        "1s" => Ok((1, "SECOND")),
        "1m" => Ok((1, "MINUTE")),
        "3m" => Ok((3, "MINUTE")),
        "5m" => Ok((5, "MINUTE")),
        "15m" => Ok((15, "MINUTE")),
        "30m" => Ok((30, "MINUTE")),
        "1h" => Ok((1, "HOUR")),
        "2h" => Ok((2, "HOUR")),
        "4h" => Ok((4, "HOUR")),
        "6h" => Ok((6, "HOUR")),
        "8h" => Ok((8, "HOUR")),
        "12h" => Ok((12, "HOUR")),
        "1d" => Ok((1, "DAY")),
        "3d" => Ok((3, "DAY")),
        "1w" => Ok((1, "WEEK")),
        "1M" => Ok((1, "MONTH")),
        _ => Err(CatalogError::Invalid("不支持的 Nautilus K 线周期。".into())),
    }
}
/// Decode the storage BarType independently of any exchange or product.
pub fn parse_bar_type(value: &str) -> Result<(String, String), CatalogError> {
    crate::reader::safe_component(value)?;
    let mut parts = value.rsplitn(5, '-');
    let (Some("EXTERNAL"), Some("LAST"), Some(unit), Some(factor), Some(instrument)) =
        (parts.next(), parts.next(), parts.next(), parts.next(), parts.next())
    else { return Err(CatalogError::Invalid("Nautilus BarType 无效。".into())); };
    let interval = match (factor, unit) {
        ("1", "SECOND") => "1s",
        ("1", "MINUTE") => "1m",
        ("3", "MINUTE") => "3m",
        ("5", "MINUTE") => "5m",
        ("15", "MINUTE") => "15m",
        ("30", "MINUTE") => "30m",
        ("1", "HOUR") => "1h",
        ("2", "HOUR") => "2h",
        ("4", "HOUR") => "4h",
        ("6", "HOUR") => "6h",
        ("8", "HOUR") => "8h",
        ("12", "HOUR") => "12h",
        ("1", "DAY") => "1d",
        ("3", "DAY") => "3d",
        ("1", "WEEK") => "1w",
        ("1", "MONTH") => "1M",
        _ => return Err(CatalogError::Invalid("Nautilus BarType 周期无效。".into())),
    };
    if bar_type(instrument, interval)? != value {
        return Err(CatalogError::Invalid("Nautilus BarType 非规范化。".into()));
    }
    Ok((instrument.to_owned(), interval.to_owned()))
}
pub fn interval_ms(interval: &str) -> Result<Option<i64>, CatalogError> {
    Ok(match interval {
        "1s" => Some(1_000),
        "1m" => Some(60_000),
        "3m" => Some(180_000),
        "5m" => Some(300_000),
        "15m" => Some(900_000),
        "30m" => Some(1_800_000),
        "1h" => Some(3_600_000),
        "2h" => Some(7_200_000),
        "4h" => Some(14_400_000),
        "6h" => Some(21_600_000),
        "8h" => Some(28_800_000),
        "12h" => Some(43_200_000),
        "1d" => Some(DAY_MS),
        "3d" => Some(3 * DAY_MS),
        "1w" => Some(7 * DAY_MS),
        "1M" => None,
        _ => return Err(CatalogError::Invalid("不支持的 Nautilus K 线周期。".into())),
    })
}
/// Matches core.market.instruments.bar_window:
/// 3d windows start one UTC day after 1970-01-01; weeks start Monday.
pub fn bar_window(interval: &str, ts_ms: i64) -> Result<(i64, i64), CatalogError> {
    if ts_ms < 0 {
        return Err(CatalogError::Invalid("K 线时间必须为正 UTC 毫秒。".into()));
    }
    if let Some(width) = interval_ms(interval)? {
        let offset = match interval {
            "1w" => 4 * DAY_MS,
            "3d" => DAY_MS,
            _ => 0,
        };
        let start = (ts_ms - offset).div_euclid(width) * width + offset;
        return Ok((
            start,
            start
                .checked_add(width)
                .ok_or_else(|| CatalogError::Invalid("K 线结束时间溢出。".into()))?,
        ));
    }
    let at = Utc
        .timestamp_millis_opt(ts_ms)
        .single()
        .ok_or_else(|| CatalogError::Invalid("K 线月份时间无效。".into()))?;
    let year = at.year();
    let month = at.month();
    let first = Utc
        .with_ymd_and_hms(year, month, 1, 0, 0, 0)
        .single()
        .ok_or_else(|| CatalogError::Invalid("K 线月份边界无效。".into()))?;
    let (next_y, next_m) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let after = Utc
        .with_ymd_and_hms(next_y, next_m, 1, 0, 0, 0)
        .single()
        .ok_or_else(|| CatalogError::Invalid("K 线月份边界无效。".into()))?;
    Ok((first.timestamp_millis(), after.timestamp_millis()))
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogBar {
    pub open_time_ms: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}
impl CatalogBar {
    pub fn event_ns(&self, interval: &str) -> Result<i64, CatalogError> {
        let (start, end) = bar_window(interval, self.open_time_ms)?;
        if start != self.open_time_ms {
            return Err(CatalogError::Invalid(
                "K 线没有对齐 UTC 存储边界。".into(),
            ));
        }
        end.checked_sub(1)
            .and_then(|ms| ms.checked_mul(NANO_PER_MS))
            .ok_or_else(|| CatalogError::Invalid("K 线纳秒时间溢出。".into()))
    }
    pub fn validate(&self, interval: &str) -> Result<(), CatalogError> {
        self.event_ns(interval)?;
        if self.open < Decimal::ZERO
            || self.high < Decimal::ZERO
            || self.low < Decimal::ZERO
            || self.close < Decimal::ZERO
            || self.volume < Decimal::ZERO
            || self.high < self.open
            || self.high < self.close
            || self.low > self.open
            || self.low > self.close
        {
            return Err(CatalogError::Invalid("K 线 OHLCV 质量校验失败。".into()));
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generic_bar_type_accepts_any_exchange_identity() {
        let value = bar_type("BTCUSDT-PERP.BINANCE", "1m").unwrap();
        assert_eq!(value, "BTCUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL");
        assert_eq!(parse_bar_type(&value).unwrap(), ("BTCUSDT-PERP.BINANCE".into(), "1m".into()));
        let other = bar_type("ETH-USDT.OKX", "5m").unwrap();
        assert_eq!(parse_bar_type(&other).unwrap().0, "ETH-USDT.OKX");
        assert!(bar_type("../ETHUSDT", "1m").is_err());
        assert!(parse_bar_type("ETHUSDT.BINANCE-1-MINUTE-LAST-FAKE").is_err());
    }
    #[test]
    fn utc_windows_match_python_three_day_week_and_month() {
        assert_eq!(bar_window("1w", 0).unwrap().0, -3 * DAY_MS);
        assert_eq!(bar_window("3d", DAY_MS).unwrap(), (DAY_MS, 4 * DAY_MS));
        let at = Utc
            .with_ymd_and_hms(2026, 2, 15, 1, 2, 3)
            .single()
            .unwrap()
            .timestamp_millis();
        let (from, until) = bar_window("1M", at).unwrap();
        assert_eq!(until - from, 28 * DAY_MS);
        assert_eq!(
            from,
            Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0)
                .single()
                .unwrap()
                .timestamp_millis()
        );
    }
}
