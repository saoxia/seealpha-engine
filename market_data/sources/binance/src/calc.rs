//! Native deterministic OHLCV calculation over the existing PairStore journal.
//!
//! Dirty records are acknowledged only after downstream durable writes. Source
//! ticks and 1-second bars are never treated as sufficient evidence to close
//! an incomplete higher interval; no missing market data is synthesized.
use std::collections::BTreeSet;
use std::str::FromStr;

use rusqlite::{Connection, OptionalExtension, params};
use rust_decimal::Decimal;
use serde_json::{Value, json};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;
const DAY: i64 = 86_400_000;

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + (if mp < 10 { 3 } else { -9 });
    if month <= 2 {
        year += 1;
    }
    (year, month, day)
}
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let year = y - (if m <= 2 { 1 } else { 0 });
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let mp = m + (if m > 2 { -3 } else { 9 });
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}
/// Exact UTC Binance boundary convention, including Monday-week anchor,
/// 3-day offset and actual Gregorian month boundaries.
pub fn bar_window(level: &str, ts_ms: i64) -> Result<(i64, i64)> {
    if ts_ms < 0 {
        return Err("invalid negative timestamp".into());
    }
    if level == "1M" {
        let (y, m, _) = civil_from_days(ts_ms.div_euclid(DAY));
        let next = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
        return Ok((
            days_from_civil(y, m, 1) * DAY,
            days_from_civil(next.0, next.1, 1) * DAY,
        ));
    }
    let width: i64 = match level {
        "1s" => 1000,
        "1m" => 60_000,
        "3m" => 180_000,
        "5m" => 300_000,
        "15m" => 900_000,
        "30m" => 1_800_000,
        "1h" => 3_600_000,
        "2h" => 7_200_000,
        "4h" => 14_400_000,
        "6h" => 21_600_000,
        "8h" => 28_800_000,
        "12h" => 43_200_000,
        "1d" => DAY,
        "3d" => 3 * DAY,
        "1w" => 7 * DAY,
        _ => return Err("unsupported candle level".into()),
    };
    let offset = if level == "1w" {
        4 * DAY
    } else if level == "3d" {
        DAY
    } else {
        0
    };
    let left = (ts_ms - offset).div_euclid(width) * width + offset;
    Ok((left, left.checked_add(width).ok_or("time overflow")?))
}
fn parse_decimal(text: &str) -> Result<Decimal> {
    Ok(Decimal::from_str(text)?)
}
#[derive(Clone)]
struct Candle {
    left: i64,
    right: i64,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
    complete: bool,
}
fn from_trades(
    db: &Connection,
    left: i64,
    right: i64,
    started_ms: Option<i64>,
) -> Result<Option<Candle>> {
    let mut query=db.prepare("SELECT price,size FROM trades WHERE timestamp_ms>=? AND timestamp_ms<? ORDER BY timestamp_ms,id")?;
    let rows = query.query_map(params![left, right], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })?;
    let mut values = Vec::new();
    for row in rows {
        values.push(row?);
    }
    if values.is_empty() {
        return Ok(None);
    }
    let mut prices = values
        .iter()
        .map(|r| parse_decimal(&r.0))
        .collect::<Result<Vec<_>>>()?;
    let opening = prices[0];
    let close = *prices.last().ok_or("empty trade group")?;
    let high = *prices.iter().max().ok_or("empty trade group")?;
    let low = *prices.iter().min().ok_or("empty trade group")?;
    let mut volume = Decimal::ZERO;
    for (_, size) in values {
        volume = volume
            .checked_add(parse_decimal(&size)?)
            .ok_or("volume overflow")?;
    }
    prices.clear();
    Ok(Some(Candle {
        left,
        right,
        open: opening,
        high,
        low,
        close,
        volume,
        complete: started_ms.is_some_and(|started| left >= started),
    }))
}
fn from_candles(db: &Connection, source: &str, left: i64, right: i64) -> Result<Option<Candle>> {
    let mut statement=db.prepare(
        "SELECT value FROM candles WHERE interval=? AND start_ms>=? AND end_ms<=? ORDER BY start_ms"
    )?;
    let rows = statement.query_map(params![source, left, right], |r| r.get::<_, String>(0))?;
    let mut events = Vec::<Value>::new();
    for row in rows {
        events.push(serde_json::from_str(&row?)?);
    }
    if events.is_empty() {
        return Ok(None);
    }
    let first = &events[0];
    let last = events.last().ok_or("empty candle group")?;
    let mut volume = Decimal::ZERO;
    let mut highest: Option<Decimal> = None;
    let mut lowest: Option<Decimal> = None;
    let mut cursor = left;
    let mut complete = true;
    for row in &events {
        let stamp = row["start_ms"].as_i64().ok_or("bad stored start")?;
        let end = row["end_ms"].as_i64().ok_or("bad stored end")?;
        let high = parse_decimal(row["high"].as_str().ok_or("bad stored high")?)?;
        let low = parse_decimal(row["low"].as_str().ok_or("bad stored low")?)?;
        let quantity = parse_decimal(row["volume"].as_str().ok_or("bad stored volume")?)?;
        highest = Some(highest.map_or(high, |prior| prior.max(high)));
        lowest = Some(lowest.map_or(low, |prior| prior.min(low)));
        volume = volume.checked_add(quantity).ok_or("volume overflow")?;
        complete &= row["is_complete"] == true && stamp == cursor;
        cursor = end;
    }
    complete &= cursor == right;
    Ok(Some(Candle {
        left,
        right,
        open: parse_decimal(first["open"].as_str().ok_or("bad stored open")?)?,
        high: highest.ok_or("missing high")?,
        low: lowest.ok_or("missing low")?,
        close: parse_decimal(last["close"].as_str().ok_or("bad stored close")?)?,
        volume,
        complete,
    }))
}
fn persist(
    db: &mut Connection,
    level: &str,
    candle: Candle,
    source: &str,
    retained: bool,
    now_ms: i64,
) -> Result<bool> {
    // Never write a supposed closed bar based only on host wall-clock.
    if candle.right > now_ms {
        return Ok(false);
    }
    let old: Option<String> = db
        .query_row(
            "SELECT value FROM candles WHERE interval=? AND start_ms=?",
            params![level, candle.left],
            |r| r.get(0),
        )
        .optional()?;
    let previous = old
        .as_deref()
        .map(serde_json::from_str::<Value>)
        .transpose()?;
    let prior_retained = previous
        .as_ref()
        .and_then(|v| v["retained"].as_bool())
        .unwrap_or(true);
    let retained = retained || prior_retained && previous.is_some();
    let new = json!({
        "symbol":db.query_row::<String,_,_>("SELECT symbol FROM config LIMIT 1",[],|r|r.get(0))?,
        "interval":level,"start_ms":candle.left,"end_ms":candle.right,
        "open":candle.open.to_string(),"high":candle.high.to_string(),
        "low":candle.low.to_string(),"close":candle.close.to_string(),
        "volume":candle.volume.to_string(),
        "is_closed":true,"is_complete":candle.complete,
        "updated_ms":now_ms,"allow_new_risk":false,
        "source_levels":[source],"retained":retained
    });
    if let Some(old) = &previous {
        let differs = ["open", "high", "low", "close", "volume"]
            .into_iter()
            .try_fold(false, |acc, key| -> Result<bool> {
                Ok(acc
                    || parse_decimal(old[key].as_str().ok_or("bad prior candle")?)?
                        != parse_decimal(new[key].as_str().ok_or("bad candidate candle")?)?)
            })?;
        let old_source = old["source_levels"][0].as_str().unwrap_or("");
        let derived = matches!(source, "raw_trades" | "1s" | "1m")
            && matches!(old_source, "raw_trades" | "1s" | "1m");
        if old["is_complete"] == true
            && !candle.complete
            && (!derived || !differs || old_source != source)
        {
            return Ok(true);
        }
        if old["is_complete"] == true && differs && !derived {
            db.execute(
                "INSERT OR IGNORE INTO conflicts VALUES (?)",
                [serde_json::to_string(&new)?],
            )?;
            return Err("conflicting authoritative closed candle".into());
        }
        if !differs
            && old["is_closed"] == true
            && old["is_complete"] == candle.complete
            && old_source == source
            && old["retained"] == retained
        {
            return Ok(true);
        }
    }
    let tx = db.transaction()?;
    let revision: i64 = tx.query_row("SELECT value FROM revisions WHERE id=1", [], |r| r.get(0))?;
    let revision = revision.checked_add(1).ok_or("revision overflow")?;
    tx.execute("UPDATE revisions SET value=? WHERE id=1", [revision])?;
    let mut with_revision = new;
    with_revision["revision"] = json!(revision);
    tx.execute(
        "INSERT OR REPLACE INTO candles(interval,start_ms,end_ms,value) VALUES (?,?,?,?)",
        params![
            level,
            candle.left,
            candle.right,
            serde_json::to_string(&with_revision)?
        ],
    )?;
    tx.commit()?;
    Ok(true)
}

pub const ALL_LEVELS: &[&str] = &[
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M",
];

/// Drain dirty buckets after a trusted exchange-confirmed timestamp.
/// Transitive 1s -> interval updates and their receipts persist across crashes.
pub fn process_pending(
    db: &mut Connection,
    retained: &BTreeSet<String>,
    confirmed_ms: i64,
    base_level: &str,
) -> Result<usize> {
    if confirmed_ms < 0 {
        return Err("invalid confirmed time".into());
    }
    let started: Option<i64> = db
        .query_row(
            "SELECT value FROM runtime_values WHERE name='started_ms'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    let mut done = 0_usize;
    // A hard limit avoids unbounded monopolization; receipts remain on disk.
    while done < 4096 {
        let next:Option<(String,i64)>=db.query_row(r#"
            SELECT source,start_ms FROM pending_updates
            ORDER BY CASE source WHEN 'tick' THEN 0 WHEN '1s' THEN 1 WHEN '1m' THEN 2 ELSE 3 END,start_ms LIMIT 1
        "#,[],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        let Some((source, stamp)) = next else { break };
        if source == "tick" && !retained.is_empty() {
            let (left, right) = bar_window("1s", stamp)?;
            if right > confirmed_ms {
                break;
            }
            if let Some(bar) = from_trades(db, left, right, started)?
                && !persist(db, "1s", bar, "raw_trades", true, confirmed_ms)?
            {
                break;
            }
        } else if source == "1s" && retained.len() > 1 {
            let mut pending = db.prepare(
                "SELECT start_ms FROM pending_updates WHERE source='1s' ORDER BY start_ms",
            )?;
            let records = pending.query_map([], |r| r.get::<_, i64>(0))?;
            let mut stamps = Vec::new();
            for row in records {
                stamps.push(row?);
            }
            drop(pending);
            for target in ALL_LEVELS.iter().filter(|&&level| retained.contains(level)) {
                let mut openings = BTreeSet::new();
                for source_stamp in &stamps {
                    openings.insert(bar_window(target, *source_stamp)?.0);
                }
                for opening in openings {
                    let (left, right) = bar_window(target, opening)?;
                    // Existing implementation also stores partial higher bars
                    // after the first complete source second, as long as its
                    // own close is not future.
                    if right > confirmed_ms {
                        continue;
                    }
                    if let Some(bar) = from_candles(db, "1s", left, right)? {
                        persist(db, target, bar, "1s", true, confirmed_ms)?;
                    }
                }
            }
            let tx = db.transaction()?;
            for start in stamps {
                tx.execute(
                    "DELETE FROM pending_updates WHERE source='1s' AND start_ms=?",
                    [start],
                )?;
                done += 1;
            }
            tx.commit()?;
            continue;
        }
        db.execute(
            "DELETE FROM pending_updates WHERE source=? AND start_ms=?",
            params![source, stamp],
        )?;
        done += 1;
    }
    if base_level != "tick" {
        db.execute(
            "DELETE FROM trades WHERE timestamp_ms<?",
            [confirmed_ms.saturating_sub(120000)],
        )?;
    }
    Ok(done)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn covers_week_three_day_and_gregorian_month() {
        assert_eq!(bar_window("1s", 1234).unwrap(), (1000, 2000));
        assert_eq!(bar_window("1w", 0).unwrap(), (-3 * DAY, 4 * DAY));
        assert_eq!(bar_window("3d", 0).unwrap(), (-2 * DAY, DAY));
        let feb_2024 = days_from_civil(2024, 2, 29) * DAY;
        assert_eq!(
            bar_window("1M", feb_2024).unwrap(),
            (
                days_from_civil(2024, 2, 1) * DAY,
                days_from_civil(2024, 3, 1) * DAY
            )
        );
        for day in [0, 1, 30, 365, 19700, 20000] {
            let (y, m, d) = civil_from_days(day);
            assert_eq!(days_from_civil(y, m, d), day);
        }
    }
    #[test]
    fn tick_dirty_receipts_generate_one_second_and_never_invent_full_minute() {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch(r#"
            CREATE TABLE config(symbol TEXT,base_level TEXT);
            INSERT INTO config VALUES('ETHUSDT','tick');
            CREATE TABLE candles(interval TEXT,start_ms INTEGER,end_ms INTEGER,value TEXT,
                PRIMARY KEY(interval,start_ms));
            CREATE TABLE trades(id INTEGER PRIMARY KEY,timestamp_ms INTEGER,price TEXT,size TEXT,maker INTEGER);
            CREATE TABLE conflicts(detail TEXT PRIMARY KEY);
            CREATE TABLE revisions(id INTEGER PRIMARY KEY,value INTEGER);
            INSERT INTO revisions VALUES(1,0);
            CREATE TABLE runtime_values(name TEXT PRIMARY KEY,value INTEGER);
            INSERT INTO runtime_values VALUES('started_ms',1000);
            CREATE TABLE pending_updates(source TEXT,start_ms INTEGER,PRIMARY KEY(source,start_ms));
            INSERT INTO pending_updates VALUES('tick',1000);
            INSERT INTO trades VALUES(1,1200,'2.01','0.4',0);
            INSERT INTO trades VALUES(2,1500,'2.05','0.7',1);
            CREATE TRIGGER changed_candle AFTER INSERT ON candles BEGIN
                INSERT OR IGNORE INTO pending_updates VALUES(NEW.interval,NEW.start_ms);
            END;
            CREATE TRIGGER revised_candle AFTER UPDATE OF value ON candles BEGIN
                INSERT OR IGNORE INTO pending_updates VALUES(NEW.interval,NEW.start_ms);
            END;
        "#).unwrap();
        let retained = ["1s".into(), "1m".into()].into_iter().collect();
        process_pending(&mut db, &retained, 10_000, "tick").unwrap();
        let bar: String = db
            .query_row("SELECT value FROM candles WHERE interval='1s'", [], |r| {
                r.get(0)
            })
            .unwrap();
        let value: Value = serde_json::from_str(&bar).unwrap();
        assert_eq!(value["high"], "2.05");
        assert_eq!(value["volume"], "1.1");
        assert_eq!(value["is_complete"], true);
        let min: i64 = db
            .query_row(
                "SELECT count(*) FROM candles WHERE interval='1m'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(min, 0); // Never claim the incomplete 60-second interval.
    }
}
