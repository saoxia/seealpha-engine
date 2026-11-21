//! Remaining feed and pipeline rules that do not depend on Nautilus catalogs.
//! Missing pages, partial days, and failed writes leave the previous bytes in place.
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trade {
    pub id: i64,
    pub timestamp_ms: i64,
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Ingest {
    Accepted,
    Duplicate,
    FilteredZero,
}

#[derive(Debug, Default)]
pub struct TradeLog {
    rows: BTreeMap<i64, Trade>,
    pub conflicts: Vec<String>,
    pub conflicted: bool,
    pub gaps: Vec<(i64, i64)>,
}

impl TradeLog {
    pub fn ingest(&mut self, trade: Trade) -> Result<Ingest, &'static str> {
        if trade.price.is_sign_negative() || trade.quantity.is_sign_negative() {
            return Err("Invalid raw trade");
        }
        if trade.price.is_zero() || trade.quantity.is_zero() {
            self.conflicts
                .push(format!("filtered_zero_trade:{}", trade.id));
            return Ok(Ingest::FilteredZero);
        }
        if let Some(existing) = self.rows.get(&trade.id) {
            if existing.price != trade.price || existing.quantity != trade.quantity {
                self.conflicted = true;
                self.conflicts.push(format!("conflict:{}", trade.id));
                return Err("Conflicting raw");
            }
            return Ok(Ingest::Duplicate);
        }
        self.rows.insert(trade.id, trade);
        Ok(Ingest::Accepted)
    }

    pub fn ids(&self) -> Vec<i64> {
        self.rows.keys().copied().collect()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candle {
    pub open_ms: i64,
    pub open: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub closed: bool,
    pub complete: bool,
    pub revision: u64,
    pub source: String,
}

#[derive(Debug, Default)]
pub struct CandleBook {
    pub candles: BTreeMap<(String, i64), Candle>,
    pub pending: BTreeSet<(String, i64)>,
    pub latest: BTreeMap<String, i64>,
    pub revision: u64,
}

impl CandleBook {
    pub fn put(&mut self, level: &str, candle: Candle, replace_complete: bool) -> bool {
        let key = (level.to_owned(), candle.open_ms);
        if let Some(existing) = self.candles.get(&key) {
            if existing.volume == candle.volume
                && existing.close == candle.close
                && existing.open == candle.open
                && existing.complete == candle.complete
            {
                return false;
            }
            if existing.complete && !replace_complete {
                return false;
            }
        }
        let mut stored = candle;
        stored.revision = self.revision + 1;
        self.revision = stored.revision;
        self.latest.insert(level.to_owned(), stored.open_ms);
        self.candles.insert(key, stored);
        true
    }

    pub fn get(&self, level: &str, open_ms: i64) -> Option<&Candle> {
        self.candles.get(&(level.to_owned(), open_ms))
    }

    pub fn drop_latest(&mut self, level: &str) {
        self.latest.remove(level);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredBytes {
    pub symbol: String,
    pub day: i64,
    pub level: String,
    pub bytes: Vec<u8>,
    pub symlink: bool,
    pub live_writer: bool,
    pub event_times: Vec<i64>,
}

#[derive(Clone, Debug)]
pub struct Dataset {
    pub files: Vec<StoredBytes>,
    pub verified_days: BTreeSet<(String, i64)>,
    pub wait_until_ms: BTreeMap<(String, i64), i64>,
    pub created: bool,
}

impl Dataset {
    pub fn new() -> Self {
        Self {
            files: Vec::new(),
            verified_days: BTreeSet::new(),
            wait_until_ms: BTreeMap::new(),
            created: false,
        }
    }

    pub fn preview(&self, symbol: &str, start: i64, end: i64) -> Vec<(i64, usize)> {
        let mut rows = Vec::new();
        for file in &self.files {
            if file.symbol != symbol {
                continue;
            }
            for (index, ts) in file.event_times.iter().enumerate() {
                if *ts >= start && *ts < end {
                    rows.push((*ts, index));
                }
            }
        }
        rows
    }

    pub fn replace_day(
        &mut self,
        symbol: &str,
        day: i64,
        level: &str,
        bytes: Vec<u8>,
        force: bool,
    ) -> Result<(), &'static str> {
        let exists = self
            .files
            .iter()
            .any(|file| file.symbol == symbol && file.day == day && file.level == level);
        if exists && !force {
            return Err("--force");
        }
        self.files
            .retain(|file| !(file.symbol == symbol && file.day == day && file.level == level));
        self.files.push(StoredBytes {
            symbol: symbol.to_owned(),
            day,
            level: level.to_owned(),
            bytes,
            symlink: false,
            live_writer: false,
            event_times: Vec::new(),
        });
        Ok(())
    }

    pub fn delete_symbol_days(&mut self, symbol: &str, start: i64, end: i64) -> usize {
        let before = self.files.len();
        self.files
            .retain(|file| !(file.symbol == symbol && file.day >= start && file.day < end));
        before - self.files.len()
    }
}

impl Default for Dataset {
    fn default() -> Self {
        Self::new()
    }
}

pub fn resume_range(received: u64, total: u64) -> Option<(u64, u64)> {
    if received >= total {
        None
    } else {
        Some((received, total))
    }
}

pub fn align_utc_day(ms: i64) -> i64 {
    let day = 86_400_000;
    ms.div_euclid(day) * day
}

pub fn bar_complete(level: &str, present: i64, expected: i64) -> bool {
    matches!(
        level,
        "1m" | "5m" | "15m" | "1h" | "1d" | "3d" | "1w" | "1M"
    ) && present == expected
}

#[cfg(test)]
fn check_symbol(symbol: &str) -> Result<(), &'static str> {
    if symbol.is_empty() {
        return Err("empty");
    }
    if !symbol
        .chars()
        .all(|char| char.is_ascii_uppercase() || char.is_ascii_digit())
    {
        return Err("invalid");
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct Queue<T> {
    items: VecDeque<T>,
    pub stopped: bool,
}

impl<T> Queue<T> {
    pub fn new() -> Self {
        Self {
            items: VecDeque::new(),
            stopped: false,
        }
    }

    pub fn push(&mut self, item: T) {
        if !self.stopped {
            self.items.push_back(item);
        }
    }

    pub fn stop_and_drain(&mut self) -> Vec<T> {
        self.stopped = true;
        self.items.drain(..).collect()
    }
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
fn trade(id: i64, price: i64, qty: i64) -> Trade {
    Trade {
        id,
        timestamp_ms: id * 1000,
        price: Decimal::from(price),
        quantity: Decimal::from(qty),
    }
}

#[cfg(test)]
mod tests;
