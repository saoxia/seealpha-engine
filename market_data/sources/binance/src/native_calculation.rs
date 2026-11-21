//! Rust-only deterministic calculation worker for existing pair SQLite.
//! The collection worker remains separate. Historical Catalog installation
//! is not handled here and status never claims data readiness by itself.
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension, params};
use rust_decimal::Decimal;
use seealpha_feed::persisted_rollup::{MinuteBar, PersistentRollups};
use seealpha_binance_market_data_source::{
    Product,
    calc::{self, bar_window},
};
use serde_json::{Value, json};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

fn time_ms() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}
fn update_json(path: &Path, value: &Value) -> Result<()> {
    let temporary = path.with_extension("json.tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temporary)?;
        file.write_all(serde_json::to_string(value)?.as_bytes())?;
        file.sync_all()?;
    }
    fs::rename(temporary, path)?;
    Ok(())
}
fn bar_updates(db: &Connection, retained: &BTreeSet<String>) -> Result<Vec<Value>> {
    let mut updates = Vec::new();
    for level in retained {
        let last:Option<String>=db.query_row(
            "SELECT value FROM candles WHERE interval=? AND json_extract(value,'$.retained')=1 ORDER BY start_ms DESC LIMIT 1",
            [level],|r|r.get(0)).optional()?;
        if let Some(row) = last {
            updates.push(serde_json::from_str(&row)?);
        }
    }
    Ok(updates)
}

/// Mirror only contiguous, closed and complete 1m source candles into the
/// separate Rust derivation database. No REST certification or live READY
/// permission is implied. The cursor remains at a gap until source repair.
fn mirror_confirmed_minutes(
    source: &Connection,
    derived: &mut PersistentRollups,
    symbol: &str,
    confirmed_ms: i64,
    cursor: &mut Option<i64>,
) -> Result<usize> {
    let from = cursor.unwrap_or(0);
    let mut stmt = source.prepare(
        "SELECT start_ms,end_ms,value FROM candles
         WHERE interval='1m' AND start_ms>=?1 AND end_ms<=?2
         ORDER BY start_ms LIMIT 256"
    )?;
    let mut rows = stmt.query(params![from, confirmed_ms])?;
    let mut processed = 0_usize;
    while let Some(row) = rows.next()? {
        let start: i64 = row.get(0)?;
        let end: i64 = row.get(1)?;
        let body: Value = serde_json::from_str(&row.get::<_, String>(2)?)?;
        if body["is_closed"] != true || body["is_complete"] != true {
            break;
        }
        if cursor.is_some_and(|expected| expected != start) {
            break;
        }
        if start < 0 || end != start.checked_add(60_000).ok_or("minute timestamp overflow")?
            || body["symbol"].as_str() != Some(symbol)
            || body["interval"].as_str() != Some("1m")
            || body["start_ms"] != start || body["end_ms"] != end
        {
            return Err("invalid native source minute evidence".into());
        }
        let decimal = |field: &str| -> Result<Decimal> {
            Ok(body[field].as_str().ok_or("source minute decimal missing")?.parse()?)
        };
        derived.accept_closed(&MinuteBar {
            symbol: symbol.to_string(),
            open_ms: start,
            open: decimal("open")?,
            high: decimal("high")?,
            low: decimal("low")?,
            close: decimal("close")?,
            volume: decimal("volume")?,
        })?;
        *cursor = Some(end);
        processed += 1;
    }
    Ok(processed)
}

#[cfg(test)]
mod native_mirror_tests {
    use super::*;

    fn candle(start: i64, complete: bool) -> String {
        json!({"symbol":"ETHUSDT","interval":"1m",
            "start_ms":start,"end_ms":start+60_000,
            "open":"10","high":"11","low":"9","close":"10","volume":"2",
            "is_closed":true,"is_complete":complete}).to_string()
    }

    #[test]
    fn mirror_stops_at_gap_then_advances_after_repair() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE candles(interval TEXT,start_ms INTEGER,end_ms INTEGER,value TEXT);").unwrap();
        for start in [0,120_000] {
            db.execute("INSERT INTO candles VALUES ('1m',?1,?2,?3)",
                params![start,start+60_000,candle(start,true)]).unwrap();
        }
        let mut derived = PersistentRollups::memory().unwrap();
        let mut cursor = None;
        assert_eq!(mirror_confirmed_minutes(&db,&mut derived,"ETHUSDT",180_000,&mut cursor).unwrap(),1);
        assert_eq!(cursor,Some(60_000));
        assert_eq!(derived.checkpoint("ETHUSDT").unwrap(),Some(60_000));
        db.execute("INSERT INTO candles VALUES ('1m',60000,120000,?1)",
            [candle(60_000,true)]).unwrap();
        assert_eq!(mirror_confirmed_minutes(&db,&mut derived,"ETHUSDT",180_000,&mut cursor).unwrap(),2);
        assert_eq!(derived.checkpoint("ETHUSDT").unwrap(),Some(180_000));
    }

    #[test]
    fn mirror_does_not_accept_incomplete_or_future_minutes() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE candles(interval TEXT,start_ms INTEGER,end_ms INTEGER,value TEXT);").unwrap();
        db.execute("INSERT INTO candles VALUES ('1m',0,60000,?1)",
            [candle(0,false)]).unwrap();
        let mut derived = PersistentRollups::memory().unwrap();
        let mut cursor = None;
        assert_eq!(mirror_confirmed_minutes(&db,&mut derived,"ETHUSDT",60000,&mut cursor).unwrap(),0);
        assert_eq!(cursor,None);
        assert_eq!(derived.checkpoint("ETHUSDT").unwrap(),None);
    }
}

fn execute() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if !(6..=7).contains(&args.len()) {
        return Err("usage: binance-native-calculation <spot|futures_um> <symbol> <base_level> <root> <interval1,interval2,...> [iterations]".into());
    }
    let product = Product::parse(&args[1]).ok_or("invalid product")?;
    if product == Product::Options {
        return Err(
            "options use exchange-closed Klines; dedicated native Catalog implementation pending"
                .into(),
        );
    }
    let symbol = &args[2];
    let base_level = &args[3];
    if base_level != "tick" {
        let _ = bar_window(base_level, 0)?;
    }
    let root = PathBuf::from(&args[4]).canonicalize()?;
    if !root.is_dir() || !root.join("collection.sqlite").is_file() {
        return Err("source dataset unavailable; no implicit creation".into());
    }
    let mut retained: BTreeSet<String> = args[5]
        .split(',')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    for level in &retained {
        let _ = bar_window(level, 0)?;
    }
    if base_level != "tick" {
        retained.insert(base_level.to_string());
    }
    if !retained.is_empty() {
        retained.insert("1s".to_string());
    }
    let iterations = args.get(6).map(|s| s.parse::<u64>()).transpose()?;
    let calculation_lock = OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("calculation.lock"))?;
    calculation_lock.try_lock_exclusive()?;
    let journal_lock = OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("journal.lock"))?;
    let mut db = Connection::open(root.join("collection.sqlite"))?;
    db.pragma_update(None, "journal_mode", "WAL")?;
    db.pragma_update(None, "synchronous", "FULL")?;
    let config: (String, String) =
        db.query_row("SELECT symbol,base_level FROM config LIMIT 1", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?;
    if config != (symbol.to_string(), base_level.to_string()) {
        return Err("dataset symbol/base_level mismatch".into());
    }
    let state_root = root.join("calculation");
    fs::create_dir_all(&state_root)?;
    // This database belongs only to the native calculation worker. It is
    // never shared with the legacy Python aggregation writer.
    let mut derived = PersistentRollups::open(&state_root.join("native-rollups.sqlite"))?;
    let mut mirror_cursor = None::<i64>;
    let mut last_processed = None::<i64>;
    let mut index = 0_u64;
    loop {
        let now = time_ms()?;
        let source = fs::read(root.join("status.json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok());
        let confirmed = source.as_ref().and_then(|v| {
            if now <= v["valid_until_ms"].as_i64().unwrap_or(0) {
                v["confirmed_ms"].as_i64()
            } else {
                None
            }
        });
        // Dirty source buckets may be resumed after a crash, but may not
        // be acknowledged while exchange trade IDs show a durable hole.
        let unresolved_gaps: i64 = db.query_row("SELECT count(*) FROM gaps", [], |r| r.get(0))?;
        let previous_error = if unresolved_gaps > 0 {
            Some("unresolved_raw_trade_id_gap".into())
        } else if let Some(confirmed_ms) = confirmed {
            journal_lock.lock_exclusive()?;
            let result = calc::process_pending(&mut db, &retained, confirmed_ms, base_level);
            FileExt::unlock(&journal_lock)?;
            match result {
                Ok(records) => {
                    if records > 0 {
                        last_processed = Some(confirmed_ms);
                    }
                    // Only after the source journal has processed its dirty
                    // receipts and its unresolved raw-ID gap count is zero.
                    match mirror_confirmed_minutes(
                        &db, &mut derived, symbol, confirmed_ms, &mut mirror_cursor,
                    ) {
                        Ok(_) => None,
                        Err(error) => Some(format!("native_rollup_mirror_failed: {error}")),
                    }
                }
                Err(error) => Some(error.to_string()),
            }
        } else {
            Some("exchange_confirmed_clock_missing_or_expired".into())
        };
        let pending: i64 =
            db.query_row("SELECT count(*) FROM pending_updates", [], |r| r.get(0))?;
        let history_backlog = match fs::read_dir(root.join("history/changes")) {
            Ok(entries) => entries
                .filter_map(std::result::Result::ok)
                .filter(|e| e.path().extension().is_some_and(|v| v == "json"))
                .count(),
            Err(_) => 0,
        };
        update_json(
            &state_root.join("status.json"),
            &json!({
                "state":"DEGRADED",
                "data_ready":false,
                "reason":previous_error.as_deref().unwrap_or("native_history_catalog_bridge_pending"),
                "checked_ms":now,"valid_until_ms":now+15000,
                "last_error":previous_error,"backlog":pending+history_backlog as i64,
                "realtime_pending":pending,"history_backlog":history_backlog,
                "gap_count":unresolved_gaps,
                "last_processed_ms":last_processed,"bar_updates":bar_updates(&db,&retained)?,
                "derived_minute_checkpoint_ms":derived.checkpoint(symbol)?,
                "allow_new_risk":false,"product":product.as_str()
            }),
        )?;
        index += 1;
        if iterations.is_some_and(|limit| index >= limit) {
            break;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    Ok(())
}
fn main() -> std::process::ExitCode {
    match execute() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Native calculation failed closed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
