//! Native SQLite state for closed-minute OHLCV aggregation.
//! Uses separate native tables: never opens a legacy Python writer database.
//! A closed time boundary is NOT proof of complete minute coverage.
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use rust_decimal::Decimal;
use std::path::Path;

const MINUTE: i64 = 60_000;
const PERIODS: [(&str, i64); 3] = [("1m", 1), ("5m", 5), ("15m", 15)];

#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("sqlite: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("invalid minute or decimal")]
    Invalid,
    #[error("existing minute has conflicting OHLCV")]
    Conflict,
    #[error("arithmetic overflow")]
    Overflow,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MinuteBar {
    pub symbol: String,
    pub open_ms: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CalculatedBar {
    pub symbol: String,
    pub interval: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub complete: bool,
    pub revision: i64,
    // A materialized candle is not independent exchange/clock proof.
    pub allow_new_risk: bool,
}

pub struct PersistentRollups {
    connection: Connection,
}

impl PersistentRollups {
    pub fn open(path: &Path) -> Result<Self, PersistError> {
        let connection = Connection::open(path)?;
        Self::from_connection(connection)
    }

    pub fn memory() -> Result<Self, PersistError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, PersistError> {
        connection.execute_batch("
            PRAGMA busy_timeout=5000;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS native_minute_bars (
                symbol TEXT NOT NULL, open_ms INTEGER NOT NULL,
                open TEXT NOT NULL, high TEXT NOT NULL, low TEXT NOT NULL,
                close TEXT NOT NULL, volume TEXT NOT NULL,
                PRIMARY KEY(symbol, open_ms)
            );
            CREATE TABLE IF NOT EXISTS native_rollups (
                symbol TEXT NOT NULL, interval TEXT NOT NULL,
                start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL,
                open TEXT NOT NULL, high TEXT NOT NULL, low TEXT NOT NULL,
                close TEXT NOT NULL, volume TEXT NOT NULL,
                complete INTEGER NOT NULL, revision INTEGER NOT NULL,
                PRIMARY KEY(symbol, interval, start_ms)
            );
            CREATE TABLE IF NOT EXISTS native_rollup_checkpoints (
                symbol TEXT NOT NULL PRIMARY KEY, next_ms INTEGER NOT NULL
            );
        ")?;
        Ok(Self { connection })
    }

    /// Closed source minutes are immutable; corrections must use a separately
    /// audited repair operation. This method never enables new trading risk.
    pub fn accept_closed(&mut self, bar: &MinuteBar) -> Result<Vec<CalculatedBar>, PersistError> {
        if bar.symbol.is_empty() || bar.open_ms < 0 || bar.open_ms % MINUTE != 0
            || bar.volume < Decimal::ZERO
            || bar.low > bar.open || bar.low > bar.close
            || bar.high < bar.open || bar.high < bar.close || bar.high < bar.low
        {
            return Err(PersistError::Invalid);
        }
        bar.open_ms.checked_add(MINUTE).ok_or(PersistError::Overflow)?;
        let tx = self.connection.transaction()?;
        let stored: Option<(String, String, String, String, String)> = tx.query_row(
            "SELECT open, high, low, close, volume FROM native_minute_bars WHERE symbol=?1 AND open_ms=?2",
            params![bar.symbol, bar.open_ms],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        ).optional()?;
        if let Some(old) = stored {
            let values = [old.0, old.1, old.2, old.3, old.4];
            let incoming = [bar.open, bar.high, bar.low, bar.close, bar.volume];
            for (raw, expected) in values.iter().zip(incoming) {
                if raw.parse::<Decimal>().map_err(|_| PersistError::Invalid)? != expected {
                    return Err(PersistError::Conflict);
                }
            }
            return Ok(Vec::new());
        }
        tx.execute(
            "INSERT INTO native_minute_bars VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![bar.symbol, bar.open_ms, bar.open.to_string(),
                bar.high.to_string(), bar.low.to_string(), bar.close.to_string(),
                bar.volume.to_string()],
        )?;
        let mut output = Vec::new();
        for (interval, factor) in PERIODS {
            let width = MINUTE.checked_mul(factor).ok_or(PersistError::Overflow)?;
            let start = bar.open_ms - bar.open_ms.rem_euclid(width);
            let end = start.checked_add(width).ok_or(PersistError::Overflow)?;
            let calculated = Self::recalculate(&tx, &bar.symbol, interval, factor, start, end)?;
            output.push(calculated);
        }
        // A checkpoint is only the longest proven contiguous sequence of
        // closed source minutes, never the largest timestamp observed.
        Self::advance_checkpoint(&tx, &bar.symbol)?;
        tx.commit()?;
        Ok(output)
    }

    fn recalculate(
        tx: &Transaction<'_>, symbol: &str, interval: &str,
        factor: i64, start: i64, end: i64,
    ) -> Result<CalculatedBar, PersistError> {
        let mut stmt = tx.prepare(
            "SELECT open_ms, open, high, low, close, volume
             FROM native_minute_bars WHERE symbol=?1 AND open_ms>=?2 AND open_ms<?3
             ORDER BY open_ms"
        )?;
        let mut rows = stmt.query(params![symbol, start, end])?;
        let mut first: Option<Decimal> = None;
        let mut high: Option<Decimal> = None;
        let mut low: Option<Decimal> = None;
        let mut close = Decimal::ZERO;
        let mut volume = Decimal::ZERO;
        let mut expected = start;
        let mut count = 0_i64;
        let mut continuous = true;
        while let Some(row) = rows.next()? {
            let stamp: i64 = row.get(0)?;
            if stamp != expected { continuous = false; }
            expected = stamp.checked_add(MINUTE).ok_or(PersistError::Overflow)?;
            let opening = parse_value(row.get(1)?)?;
            let h = parse_value(row.get(2)?)?;
            let l = parse_value(row.get(3)?)?;
            let c = parse_value(row.get(4)?)?;
            let v = parse_value(row.get(5)?)?;
            if first.is_none() { first = Some(opening); }
            high = Some(high.map_or(h, |prior| prior.max(h)));
            low = Some(low.map_or(l, |prior| prior.min(l)));
            close = c;
            volume = volume.checked_add(v).ok_or(PersistError::Overflow)?;
            count += 1;
        }
        let complete = continuous && count == factor && expected == end;
        let revision: i64 = tx.query_row(
            "SELECT revision FROM native_rollups WHERE symbol=?1 AND interval=?2 AND start_ms=?3",
            params![symbol, interval, start], |row| row.get(0),
        ).optional()?.unwrap_or(0);
        let revision = revision.checked_add(1).ok_or(PersistError::Overflow)?;
        let result = CalculatedBar {
            symbol: symbol.to_owned(), interval: interval.to_owned(),
            start_ms: start, end_ms: end,
            open: first.ok_or(PersistError::Invalid)?,
            high: high.ok_or(PersistError::Invalid)?,
            low: low.ok_or(PersistError::Invalid)?,
            close, volume, complete, revision, allow_new_risk: false,
        };
        tx.execute(
            "INSERT INTO native_rollups VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
             ON CONFLICT(symbol,interval,start_ms) DO UPDATE SET
             end_ms=excluded.end_ms,open=excluded.open,high=excluded.high,
             low=excluded.low,close=excluded.close,volume=excluded.volume,
             complete=excluded.complete,revision=excluded.revision",
            params![symbol, interval, start, end, result.open.to_string(),
                result.high.to_string(), result.low.to_string(),
                result.close.to_string(), result.volume.to_string(),
                result.complete as i64, revision],
        )?;
        Ok(result)
    }

    fn advance_checkpoint(tx: &Transaction<'_>, symbol: &str) -> Result<(), PersistError> {
        let checkpoint: Option<i64> = tx.query_row(
            "SELECT next_ms FROM native_rollup_checkpoints WHERE symbol=?1",
            [symbol], |row| row.get(0),
        ).optional()?;
        let Some(mut cursor) = checkpoint.or_else(|| {
            tx.query_row(
                "SELECT MIN(open_ms) FROM native_minute_bars WHERE symbol=?1",
                [symbol], |row| row.get::<_, Option<i64>>(0),
            ).ok().flatten()
        }) else { return Ok(()); };
        loop {
            let found: Option<i64> = tx.query_row(
                "SELECT open_ms FROM native_minute_bars WHERE symbol=?1 AND open_ms=?2",
                params![symbol, cursor], |row| row.get(0),
            ).optional()?;
            if found.is_none() { break; }
            cursor = cursor.checked_add(MINUTE).ok_or(PersistError::Overflow)?;
        }
        tx.execute(
            "INSERT INTO native_rollup_checkpoints VALUES (?1,?2)
             ON CONFLICT(symbol) DO UPDATE SET next_ms=MAX(next_ms,excluded.next_ms)",
            params![symbol, cursor],
        )?;
        Ok(())
    }

    pub fn checkpoint(&self, symbol: &str) -> Result<Option<i64>, PersistError> {
        Ok(self.connection.query_row(
            "SELECT next_ms FROM native_rollup_checkpoints WHERE symbol=?1",
            [symbol], |row| row.get(0),
        ).optional()?)
    }
}

fn parse_value(raw: String) -> Result<Decimal, PersistError> {
    raw.parse().map_err(|_| PersistError::Invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn minute(index: i64, price: i64) -> MinuteBar {
        let value = Decimal::from(price);
        MinuteBar {
            symbol: "ETHUSDT".into(), open_ms: index * MINUTE,
            open: value, high: value, low: value, close: value,
            volume: Decimal::ONE,
        }
    }
    #[test]
    fn missing_minutes_never_claim_complete_or_raise_risk() {
        let mut store = PersistentRollups::memory().unwrap();
        let first = store.accept_closed(&minute(0, 10)).unwrap();
        assert!(first[0].complete && !first[1].complete && !first[1].allow_new_risk);
        store.accept_closed(&minute(2, 12)).unwrap();
        assert_eq!(store.checkpoint("ETHUSDT").unwrap(), Some(MINUTE));
        let filled = store.accept_closed(&minute(1, 11)).unwrap();
        assert!(!filled[1].complete);
        assert_eq!(store.checkpoint("ETHUSDT").unwrap(), Some(3 * MINUTE));
        assert_eq!(store.accept_closed(&minute(1, 11)).unwrap().len(), 0);
        assert!(matches!(store.accept_closed(&minute(1, 99)), Err(PersistError::Conflict)));
    }
    #[test]
    fn five_minute_ohlcv_is_complete_only_with_all_five_minutes() {
        let mut store = PersistentRollups::memory().unwrap();
        let mut last = Vec::new();
        for index in 0..5 {
            last = store.accept_closed(&minute(index, 10 + index)).unwrap();
        }
        assert!(last[1].complete);
        assert_eq!(last[1].open, Decimal::from(10));
        assert_eq!(last[1].close, Decimal::from(14));
        assert_eq!(last[1].volume, Decimal::from(5));
        assert!(!last[1].allow_new_risk);
    }
    #[test]
    fn invalid_input_never_moves_checkpoint() {
        let mut store = PersistentRollups::memory().unwrap();
        let mut invalid = minute(0, 10);
        invalid.high = Decimal::ZERO;
        assert!(matches!(store.accept_closed(&invalid), Err(PersistError::Invalid)));
        assert_eq!(store.checkpoint("ETHUSDT").unwrap(), None);
    }
}
