//! Rust-only deterministic calculation worker for existing pair SQLite.
//! The collection worker remains separate. Historical Catalog installation
//! is not handled here and status never claims data readiness by itself.
use std::collections::BTreeSet;
use std::fs::{self,OpenOptions};
use std::io::Write;
use std::path::{Path,PathBuf};
use std::time::{Duration,SystemTime,UNIX_EPOCH};

use fs2::FileExt;
use rusqlite::{Connection,OptionalExtension};
use serde_json::{Value,json};
use seealpha_binance_market_data_source::{Product,calc::{self,bar_window}};

type Error=Box<dyn std::error::Error+Send+Sync>;
type Result<T>=std::result::Result<T,Error>;

fn time_ms()->Result<i64> {
    Ok(i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?)
}
fn update_json(path:&Path,value:&Value)->Result<()> {
    let temporary=path.with_extension("json.tmp");
    {
        let mut file=OpenOptions::new().create(true).write(true).truncate(true).open(&temporary)?;
        file.write_all(serde_json::to_string(value)?.as_bytes())?;
        file.sync_all()?;
    }
    fs::rename(temporary,path)?;
    Ok(())
}
fn bar_updates(db:&Connection,retained:&BTreeSet<String>)->Result<Vec<Value>> {
    let mut updates=Vec::new();
    for level in retained {
        let last:Option<String>=db.query_row(
            "SELECT value FROM candles WHERE interval=? AND json_extract(value,'$.retained')=1 ORDER BY start_ms DESC LIMIT 1",
            [level],|r|r.get(0)).optional()?;
        if let Some(row)=last {updates.push(serde_json::from_str(&row)?);}
    }
    Ok(updates)
}

fn execute()->Result<()> {
    let args:Vec<String>=std::env::args().collect();
    if !(6..=7).contains(&args.len()) {
        return Err("usage: binance-native-calculation <spot|futures_um> <symbol> <base_level> <root> <interval1,interval2,...> [iterations]".into());
    }
    let product=Product::parse(&args[1]).ok_or("invalid product")?;
    if product==Product::Options {return Err("options use exchange-closed Klines; dedicated native Catalog implementation pending".into());}
    let symbol=&args[2];
    let base_level=&args[3];
    if base_level!="tick" {let _=bar_window(base_level,0)?;}
    let root=PathBuf::from(&args[4]).canonicalize()?;
    if !root.is_dir() || !root.join("collection.sqlite").is_file() {
        return Err("source dataset unavailable; no implicit creation".into());
    }
    let mut retained:BTreeSet<String>=args[5].split(',').filter(|s|!s.is_empty())
        .map(str::to_string).collect();
    for level in &retained {let _=bar_window(level,0)?;}
    if base_level!="tick" {retained.insert(base_level.to_string());}
    if !retained.is_empty() {retained.insert("1s".to_string());}
    let iterations=args.get(6).map(|s|s.parse::<u64>()).transpose()?;
    let calculation_lock=OpenOptions::new().create(true).append(true).open(root.join("calculation.lock"))?;
    calculation_lock.try_lock_exclusive()?;
    let journal_lock=OpenOptions::new().create(true).append(true).open(root.join("journal.lock"))?;
    let mut db=Connection::open(root.join("collection.sqlite"))?;
    db.pragma_update(None,"journal_mode","WAL")?;
    db.pragma_update(None,"synchronous","FULL")?;
    let config:(String,String)=db.query_row("SELECT symbol,base_level FROM config LIMIT 1",[],
        |r|Ok((r.get(0)?,r.get(1)?)))?;
    if config!=(symbol.to_string(),base_level.to_string()) {return Err("dataset symbol/base_level mismatch".into());}
    let state_root=root.join("calculation");
    fs::create_dir_all(&state_root)?;
    let mut last_processed=None::<i64>;
    let mut previous_error=None::<String>;
    let mut index=0_u64;
    loop {
        let now=time_ms()?;
        let source=fs::read(root.join("status.json")).ok()
            .and_then(|b|serde_json::from_slice::<Value>(&b).ok());
        let confirmed=source.as_ref().and_then(|v|
            if now<=v["valid_until_ms"].as_i64().unwrap_or(0) {
                v["confirmed_ms"].as_i64()
            } else {None});
        if let Some(confirmed_ms)=confirmed {
            journal_lock.lock_exclusive()?;
            let result=calc::process_pending(&mut db,&retained,confirmed_ms,base_level);
            FileExt::unlock(&journal_lock)?;
            match result {
                Ok(records) => {
                    if records>0 {last_processed=Some(confirmed_ms);}
                    previous_error=None;
                }
                Err(error) => previous_error=Some(error.to_string()),
            }
        } else {
            previous_error=Some("exchange_confirmed_clock_missing_or_expired".into());
        }
        let pending:i64=db.query_row("SELECT count(*) FROM pending_updates",[],|r|r.get(0))?;
        let history_backlog=match fs::read_dir(root.join("history/changes")) {
            Ok(entries)=>entries.filter_map(std::result::Result::ok)
                .filter(|e|e.path().extension().is_some_and(|v|v=="json")).count(),
            Err(_)=>0
        };
        update_json(&state_root.join("status.json"),&json!({
            "state":"DEGRADED",
            "data_ready":false,
            "reason":previous_error.as_deref().unwrap_or("native_history_catalog_bridge_pending"),
            "checked_ms":now,"valid_until_ms":now+15000,
            "last_error":previous_error,"backlog":pending+history_backlog as i64,
            "realtime_pending":pending,"history_backlog":history_backlog,
            "last_processed_ms":last_processed,"bar_updates":bar_updates(&db,&retained)?,
            "allow_new_risk":false,"product":product.as_str()
        }))?;
        index+=1;
        if iterations.is_some_and(|limit|index>=limit){break;}
        std::thread::sleep(Duration::from_secs(1));
    }
    Ok(())
}
fn main()->std::process::ExitCode {
    match execute() {
        Ok(())=>std::process::ExitCode::SUCCESS,
        Err(error)=>{
            eprintln!("Native calculation failed closed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
