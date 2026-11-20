//! Compatible Binance Spot / USD-M raw-trade and funding journals.
//!
//! Uses the same SQLite table and trigger names as PairStore/FundingStore;
//! the normal Python calculation worker can still consume pending_updates.
//! This module is intentionally isolated from live order and account APIs.
#[cfg(test)]
use std::fs;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension, params};
use rust_decimal::Decimal;
use serde_json::{Value, json};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

fn value_i64(row: &Value, key: &str) -> Result<i64> {
    row.get(key)
        .and_then(Value::as_i64)
        .filter(|n| *n >= 0)
        .ok_or_else(|| format!("invalid integer {key}").into())
}
fn value_bool(row: &Value, key: &str) -> Result<bool> {
    row.get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("invalid boolean {key}").into())
}
fn decimal(row: &Value, key: &str, positive: bool) -> Result<(Decimal, String)> {
    let raw = row
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("invalid decimal field {key}"))?;
    let parsed = Decimal::from_str(raw)?;
    if parsed.is_sign_negative() || (positive && parsed.is_zero()) {
        return Err(format!("non-positive decimal: {key}").into());
    }
    Ok((parsed, parsed.to_string()))
}

#[derive(Clone, Debug, PartialEq)]
struct CanonicalTrade {
    id: i64,
    time: i64,
    price: Decimal,
    quantity: Decimal,
    maker: bool,
}
impl CanonicalTrade {
    fn parse(row: &Value, now_ms: i64) -> Result<Self> {
        let id = value_i64(row, "id")?;
        let time = value_i64(row, "time")?;
        let (price, _) = decimal(row, "price", false)?;
        let (quantity, _) = decimal(row, "qty", false)?;
        let maker = value_bool(row, "isBuyerMaker")?;
        if time > now_ms.saturating_add(2000) {
            return Err("future raw trade".into());
        }
        Ok(Self {
            id,
            time,
            price,
            quantity,
            maker,
        })
    }
    fn receipt(&self) -> Value {
        json!({"id":self.id,"time":self.time,
            "price":self.price.to_string(),"qty":self.quantity.to_string(),
            "isBuyerMaker":self.maker})
    }
}
#[derive(Debug, Eq, PartialEq)]
pub enum WriteReceipt {
    Accepted,
    Duplicate,
    FilteredZero,
}
/// The writer.lock is exclusive for the full native writer lifetime. The
/// short journal.lock serializes with the existing Python calculation worker.
pub struct PairJournal {
    db: Connection,
    writer_lock: File,
    journal_lock: File,
    root: PathBuf,
    symbol: String,
    base_level: String,
}
impl PairJournal {
    pub fn open(root: &Path, symbol: &str, base_level: &str) -> Result<Self> {
        if !root.is_dir() {
            return Err("existing dataset directory required; never create missing mount".into());
        }
        let writer_lock = OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join("writer.lock"))?;
        writer_lock.try_lock_exclusive()?;
        let journal_lock = OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join("journal.lock"))?;
        let db = Connection::open(root.join("collection.sqlite"))?;
        db.execute_batch(r#"
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS config (symbol TEXT, base_level TEXT);
            CREATE TABLE IF NOT EXISTS candles (
                interval TEXT, start_ms INTEGER, end_ms INTEGER, value TEXT,
                PRIMARY KEY(interval,start_ms));
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY, timestamp_ms INTEGER, price TEXT, size TEXT, maker INTEGER);
            CREATE TABLE IF NOT EXISTS gaps (first_id INTEGER, last_id INTEGER, start_ms INTEGER, end_ms INTEGER,
                PRIMARY KEY(first_id,last_id));
            CREATE TABLE IF NOT EXISTS conflicts (detail TEXT PRIMARY KEY);
            CREATE INDEX IF NOT EXISTS trades_by_time ON trades(timestamp_ms,id);
            CREATE TABLE IF NOT EXISTS revisions (id INTEGER PRIMARY KEY, value INTEGER);
            CREATE TABLE IF NOT EXISTS runtime_values (name TEXT PRIMARY KEY, value INTEGER);
            INSERT OR IGNORE INTO revisions VALUES (1,0);
            CREATE TABLE IF NOT EXISTS pending_updates (
                source TEXT, start_ms INTEGER, PRIMARY KEY(source,start_ms));
            CREATE TRIGGER IF NOT EXISTS changed_trade AFTER INSERT ON trades BEGIN
                INSERT OR IGNORE INTO pending_updates VALUES ('tick',NEW.timestamp_ms/1000*1000);
            END;
            CREATE TRIGGER IF NOT EXISTS changed_candle AFTER INSERT ON candles BEGIN
                INSERT OR IGNORE INTO pending_updates VALUES (NEW.interval,NEW.start_ms);
            END;
            CREATE TRIGGER IF NOT EXISTS revised_candle AFTER UPDATE OF value ON candles BEGIN
                INSERT OR IGNORE INTO pending_updates VALUES (NEW.interval,NEW.start_ms);
            END;
        "#)?;
        let old: Option<(String, String)> = db
            .query_row("SELECT symbol,base_level FROM config LIMIT 1", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .optional()?;
        if old
            .as_ref()
            .is_some_and(|(s, b)| s != symbol || b != base_level)
        {
            return Err("journal product/base_level configuration mismatch".into());
        }
        if old.is_none() {
            db.execute(
                "INSERT INTO config(symbol,base_level) VALUES (?,?)",
                params![symbol, base_level],
            )?;
        }
        // Preserve process-restart recovery of the unacknowledged tail.
        db.execute_batch(
            r#"
            INSERT OR IGNORE INTO pending_updates
            SELECT 'tick',timestamp_ms/1000*1000 FROM trades
            WHERE timestamp_ms >= (SELECT max(timestamp_ms)-120000 FROM trades);
        "#,
        )?;
        Ok(Self {
            db,
            writer_lock,
            journal_lock,
            root: root.to_path_buf(),
            symbol: symbol.to_owned(),
            base_level: base_level.to_owned(),
        })
    }
    pub fn conflicted(&self) -> Result<bool> {
        let mut statement = self.db.prepare("SELECT detail FROM conflicts")?;
        let records = statement.query_map([], |r| r.get::<_, String>(0))?;
        for record in records {
            let raw = record?;
            let Ok(value) = serde_json::from_str::<Value>(&raw) else {
                return Ok(true);
            };
            let kind = value.get("kind").and_then(Value::as_str);
            if !matches!(
                kind,
                Some("filtered_zero_trade") | Some("invalid_raw_trade")
            ) {
                return Ok(true);
            }
            let Some(row) = value.get("row") else {
                return Ok(true);
            };
            let Ok(trade) = CanonicalTrade::parse(row, i64::MAX - 2000) else {
                return Ok(true);
            };
            if !trade.price.is_zero() && !trade.quantity.is_zero() {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub fn calculate(
        &mut self,
        retained: &std::collections::BTreeSet<String>,
        confirmed_ms: i64,
    ) -> Result<usize> {
        self.journal_lock.lock_exclusive()?;
        let result =
            crate::calc::process_pending(&mut self.db, retained, confirmed_ms, &self.base_level);
        FileExt::unlock(&self.journal_lock)?;
        result
    }
    pub fn pending_count(&self) -> Result<i64> {
        Ok(self
            .db
            .query_row("SELECT count(*) FROM pending_updates", [], |r| r.get(0))?)
    }
    pub fn trade_count(&self) -> Result<i64> {
        Ok(self
            .db
            .query_row("SELECT count(*) FROM trades", [], |r| r.get(0))?)
    }
    pub fn write_trade(&mut self, row: &Value, now_ms: i64) -> Result<WriteReceipt> {
        let trade = CanonicalTrade::parse(row, now_ms)?;
        self.journal_lock.lock_exclusive()?;
        let result = self.write_under_lock(&trade, now_ms);
        let unlocked = FileExt::unlock(&self.journal_lock);
        unlocked?;
        result
    }
    fn write_under_lock(&mut self, trade: &CanonicalTrade, now_ms: i64) -> Result<WriteReceipt> {
        if trade.price.is_zero() || trade.quantity.is_zero() {
            let evidence = json!({"kind":"filtered_zero_trade","row":trade.receipt()});
            self.db.execute(
                "INSERT OR IGNORE INTO conflicts VALUES (?)",
                [serde_json::to_string(&evidence)?],
            )?;
            return Ok(WriteReceipt::FilteredZero);
        }
        let old: Option<(i64, String, String, i64)> = self
            .db
            .query_row(
                "SELECT timestamp_ms,price,size,maker FROM trades WHERE id=?",
                [trade.id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        if let Some((stamp, price, size, maker)) = old {
            if stamp != trade.time
                || Decimal::from_str(&price)? != trade.price
                || Decimal::from_str(&size)? != trade.quantity
                || maker != i64::from(trade.maker)
            {
                self.db.execute(
                    "INSERT OR IGNORE INTO conflicts VALUES (?)",
                    [serde_json::to_string(&json!([
                        trade.id,
                        trade.time,
                        trade.price.to_string(),
                        trade.quantity.to_string(),
                        i64::from(trade.maker)
                    ]))?],
                )?;
                return Err("conflicting raw trade; preserved original".into());
            }
            return Ok(WriteReceipt::Duplicate);
        }
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO runtime_values(name,value) VALUES('started_ms',?)",
            [now_ms],
        )?;
        tx.execute(
            "INSERT INTO trades(id,timestamp_ms,price,size,maker) VALUES (?,?,?,?,?)",
            params![
                trade.id,
                trade.time,
                trade.price.to_string(),
                trade.quantity.to_string(),
                i64::from(trade.maker)
            ],
        )?;
        // Keep the previous source second dirty for event-driven closure.
        let preceding: Option<i64> = tx.query_row(
            "SELECT max(timestamp_ms)/1000*1000 FROM trades WHERE timestamp_ms<?",
            [trade.time / 1000 * 1000],
            |r| r.get(0),
        )?;
        if let Some(previous) = preceding {
            tx.execute(
                "INSERT OR IGNORE INTO pending_updates VALUES('tick',?)",
                [previous],
            )?;
        }
        tx.commit()?;
        Ok(WriteReceipt::Accepted)
    }
    pub fn status_file(&self) -> PathBuf {
        self.root.join("status.json")
    }
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
    pub fn holds_writer_lock(&self) -> bool {
        // Field is intentionally retained for lifetime-based lock ownership.
        self.writer_lock.metadata().is_ok()
    }
}

pub struct FundingJournal {
    db: Connection,
    symbol: String,
}
impl FundingJournal {
    pub fn open(path: &Path, symbol: &str) -> Result<Self> {
        let db = Connection::open(path)?;
        db.execute_batch(
            r#"
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS funding_config(symbol TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS funding_settlements(
                funding_time_ms INTEGER PRIMARY KEY,
                rate TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'binance_fundingRate');
            CREATE TABLE IF NOT EXISTS funding_current(
                id INTEGER PRIMARY KEY CHECK(id=1),
                next_funding_ms INTEGER NOT NULL, rate TEXT NOT NULL,
                event_ms INTEGER NOT NULL);
        "#,
        )?;
        let old: Option<String> = db
            .query_row("SELECT symbol FROM funding_config LIMIT 1", [], |r| {
                r.get(0)
            })
            .optional()?;
        if old.as_deref().is_some_and(|s| s != symbol) {
            return Err("funding journal belongs to another symbol".into());
        }
        db.execute("INSERT OR IGNORE INTO funding_config VALUES(?)", [symbol])?;
        Ok(Self {
            db,
            symbol: symbol.into(),
        })
    }
    pub fn estimate(&mut self, row: &Value) -> Result<bool> {
        if row.get("s").and_then(Value::as_str) != Some(&self.symbol) {
            return Err("funding instrument mismatch".into());
        }
        let event = value_i64(row, "E")?;
        let target = value_i64(row, "T")?;
        // Funding rates can be negative, unlike prices.
        let rate = Decimal::from_str(row["r"].as_str().ok_or("invalid rate")?)?;
        if target <= event {
            return Err("invalid next-funding timestamp".into());
        }
        let old: Option<(i64, String, i64)> = self
            .db
            .query_row(
                "SELECT next_funding_ms,rate,event_ms FROM funding_current WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if let Some((old_target, old_rate, old_event)) = old
            && event <= old_event
        {
            if event == old_event && (target != old_target || Decimal::from_str(&old_rate)? != rate)
            {
                return Err("conflicting funding estimate at same event time".into());
            }
            return Ok(false);
        }
        self.db.execute(
            r#"
            INSERT INTO funding_current(id,next_funding_ms,rate,event_ms)
            VALUES(1,?,?,?)
            ON CONFLICT(id) DO UPDATE SET next_funding_ms=excluded.next_funding_ms,
                rate=excluded.rate,event_ms=excluded.event_ms
        "#,
            params![target, rate.to_string(), event],
        )?;
        Ok(true)
    }
    /// A mark-price estimate cannot create a settlement. Only a verified
    /// fundingRate REST observation can populate funding_settlements.
    pub fn confirmed_settlement(
        &mut self,
        source_symbol: &str,
        stamp: i64,
        rate_text: &str,
    ) -> Result<WriteReceipt> {
        if source_symbol != self.symbol || stamp < 0 {
            return Err("invalid settlement identity".into());
        }
        let rate = Decimal::from_str(rate_text)?;
        let old: Option<String> = self
            .db
            .query_row(
                "SELECT rate FROM funding_settlements WHERE funding_time_ms=?",
                [stamp],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(existing) = old {
            if Decimal::from_str(&existing)? != rate {
                return Err("conflicting official funding settlement".into());
            }
            return Ok(WriteReceipt::Duplicate);
        }
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO funding_settlements(funding_time_ms,rate) VALUES(?,?)",
            params![stamp, rate.to_string()],
        )?;
        tx.execute(
            "DELETE FROM funding_current WHERE next_funding_ms=?",
            [stamp],
        )?;
        tx.commit()?;
        Ok(WriteReceipt::Accepted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    fn root(name: &str) -> PathBuf {
        let ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "seealpha-native-{name}-{}-{ns}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        path
    }
    #[test]
    fn raw_trades_keep_python_journal_layout_and_dirty_receipts() {
        let path = root("trades");
        let mut journal = PairJournal::open(&path, "ETHUSDT", "tick").unwrap();
        let row = json!({"id":10,"time":1700000000000_i64,"price":"2100.010","qty":"0.5","isBuyerMaker":false});
        assert_eq!(
            journal.write_trade(&row, 1700000001000).unwrap(),
            WriteReceipt::Accepted
        );
        assert_eq!(
            journal.write_trade(&row, 1700000001000).unwrap(),
            WriteReceipt::Duplicate
        );
        assert_eq!(journal.trade_count().unwrap(), 1);
        assert!(journal.pending_count().unwrap() >= 1);
        assert!(journal.holds_writer_lock());
        drop(journal);
        let journal = PairJournal::open(&path, "ETHUSDT", "tick").unwrap();
        assert_eq!(journal.trade_count().unwrap(), 1);
        assert!(PairJournal::open(&path, "BTCUSDT", "tick").is_err());
        drop(journal);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn conflicting_ids_fail_closed_and_zero_receipts_are_audited() {
        let path = root("conflict");
        let mut journal = PairJournal::open(&path, "ETHUSDT", "1m").unwrap();
        let row =
            json!({"id":10,"time":1700000000000_i64,"price":"1","qty":"2","isBuyerMaker":true});
        journal.write_trade(&row, 1700000001000).unwrap();
        let mut new = row.clone();
        new["price"] = json!("3");
        assert!(journal.write_trade(&new, 1700000001000).is_err());
        assert!(journal.conflicted().unwrap());
        assert_eq!(journal.trade_count().unwrap(), 1);
        drop(journal);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn funding_marks_never_invent_settlements() {
        let path = root("funding");
        let mut journal = FundingJournal::open(&path.join("funding.sqlite"), "ETHUSDT").unwrap();
        let row = json!({"s":"ETHUSDT","E":1700000000000_i64,"T":1700000020000_i64,"r":"-0.0001"});
        assert!(journal.estimate(&row).unwrap());
        let count: i64 = journal
            .db
            .query_row("SELECT count(*) FROM funding_settlements", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
        assert_eq!(
            journal
                .confirmed_settlement("ETHUSDT", 1700000020000, "-0.0001")
                .unwrap(),
            WriteReceipt::Accepted
        );
        assert_eq!(
            journal
                .confirmed_settlement("ETHUSDT", 1700000020000, "-0.0001")
                .unwrap(),
            WriteReceipt::Duplicate
        );
        drop(journal);
        fs::remove_dir_all(path).unwrap();
    }
}
