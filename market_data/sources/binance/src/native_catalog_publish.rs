//! One-shot Rust Feed -> Calculation -> Nautilus Catalog bridge.
//!
//! Python's pair calculation persists complete/retained candles and then
//! installs them into native data/catalog. This port reads the existing
//! collection.sqlite journal and independently compares every candidate Kline
//! against Binance's public REST plus a verified server clock before publication.
//!
//! Usage:
//! binance-native-catalog-publish <spot|futures_um> <symbol>
//!     <pair-root> <catalog-root> <interval> <price_precision> <size_precision>
//!
//! The Catalog publisher is append-only. The watermark is advanced *after*
//! a Parquet file has been atomically published; exact crash retries are safe.
use std::{fs, fs::{File, OpenOptions}, path::{Path, PathBuf}};
use fs2::FileExt;
use rusqlite::{Connection,OpenFlags,OptionalExtension,params};
use rust_decimal::Decimal;
use seealpha_catalog::{Catalog,CatalogBar,MarketProduct,bar_window,interval_ms};
use seealpha_market::{BinanceSource,Product};
use serde_json::{Value,json};

type Error=Box<dyn std::error::Error+Send+Sync>;
type Result<T>=std::result::Result<T,Error>;
const MAX_BATCH:usize=5000;

// Prevent concurrent publisher runs for the same source journal from racing
// between the Parquet hard-link and its SQLite publication checkpoint.
fn ensure_no_unresolved_source_gaps(db: &Connection) -> Result<()> {
    let count: i64 = db.query_row("SELECT count(*) FROM gaps", [], |r| r.get(0))?;
    if count != 0 {
        return Err("unreconciled raw trade-ID gaps prohibit Catalog publication".into());
    }
    Ok(())
}

fn publication_lease(pair: &Path) -> Result<File> {
    let path = pair.join("catalog-publish.lock");
    if let Ok(meta) = fs::symlink_metadata(&path)
        && !meta.file_type().is_file()
    {
        return Err("unsafe existing publication lease path".into());
    }
    let lock = OpenOptions::new().create(true).append(true).open(path)?;
    lock.try_lock_exclusive()?;
    Ok(lock)
}

#[derive(Debug)]
struct SourceCandle{
    start_ms:i64,
    end_ms:i64,
    body:Value,
}
fn parse_bar(row:&SourceCandle,interval:&str,product:MarketProduct)->Result<CatalogBar>{
    let start=row.start_ms;
    let (canonical,next)=bar_window(interval,start)?;
    if start!=canonical||row.end_ms!=next||
        row.body["is_closed"]!=true || row.body["is_complete"]!=true ||
        row.body["retained"]!=true || row.body["start_ms"]!=start ||
        row.body["end_ms"]!=next {
        return Err("source candle is partial, not retained or has invalid UTC boundaries".into());
    }
    let decimal=|key:&str|->Result<Decimal>{
        Ok(row.body[key].as_str().ok_or("missing source decimal")?
            .parse::<Decimal>()?)
    };
    let bar=CatalogBar{
        open_time_ms:start,
        open:decimal("open")?,high:decimal("high")?,
        low:decimal("low")?,close:decimal("close")?,
        volume:decimal("volume")?,
    };
    bar.validate(interval,product)?;
    Ok(bar)
}
async fn execute()->Result<Value>{
    let args=std::env::args().collect::<Vec<_>>();
    if args.len()!=8 {
        return Err("usage: binance-native-catalog-publish <product> <symbol> <pair-root> <catalog-root> <interval> <price_precision> <size_precision>".into());
    }
    let product=MarketProduct::parse(&args[1])?;
    if product==MarketProduct::Options {
        return Err("option Klines are exchange authored; use the separate options reader".into());
    }
    let symbol=args[2].as_str();
    let id=seealpha_catalog::instrument_id(symbol,product)?;
    let _=id;
    let interval=args[5].as_str();
    // This publisher compares each derived candle with a public REST Kline.
    // Do not accept other periods until native REST window logic is ported.
    let _=seealpha_market::interval_ms(interval)
        .map_err(|_|"REST-validation not supported for this interval")?;
    let _=interval_ms(interval)?;
    let price_precision=args[6].parse::<u32>()?;
    let size_precision=args[7].parse::<u32>()?;
    if price_precision>16||size_precision>16 {
        return Err("unverified instrument precision".into());
    }
    let pair=PathBuf::from(&args[3]).canonicalize()?;
    let catalog=Catalog::open_existing(&PathBuf::from(&args[4]))?;
    if !pair.is_dir(){return Err("pair journal is not a directory".into());}
    let _publication_lease = publication_lease(&pair)?;
    let db_path=pair.join("collection.sqlite");
    if !fs::symlink_metadata(&db_path)?.file_type().is_file() {
        return Err("pair journal must be an existing ordinary file".into());
    }
    // Source- and calculation-owned SQLite never changes Product scope.
    let mut db=Connection::open_with_flags(
        &db_path,OpenFlags::SQLITE_OPEN_READ_WRITE|OpenFlags::SQLITE_OPEN_NO_MUTEX
    )?;
    let pair_identity:(String,String)=db.query_row(
        "SELECT symbol,base_level FROM config LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?))
    )?;
    if pair_identity.0!=symbol {
        return Err("wrong source symbol".into());
    }
    ensure_no_unresolved_source_gaps(&db)?;
    // The existing PairJournal writer and calculation workers use WAL.
    // We add only our own checkpoint table, never mutate raw trades/candles.
    db.execute_batch("CREATE TABLE IF NOT EXISTS catalog_publications (
        interval TEXT PRIMARY KEY, last_end_ms INTEGER NOT NULL, file_path TEXT NOT NULL,
        acknowledged_at_ms INTEGER NOT NULL);")?;
    let previous:Option<(i64,String)>=db.query_row(
        "SELECT last_end_ms,file_path FROM catalog_publications WHERE interval=?1",
        [interval],|r|Ok((r.get(0)?,r.get(1)?)),
    ).optional()?;
    if let Some((_,relative))=&previous {
        // A journal checkpoint is not proof that its Parquet artifact still
        // exists. Detect accidental deletion before acknowledging newer bars.
        let rel=std::path::Path::new(relative);
        if rel.is_absolute() || rel.components().any(|part|
            !matches!(part,std::path::Component::Normal(_))) ||
            !catalog.root().join(rel).is_file() {
            return Err("previous Nautilus Catalog publication is missing or unsafe".into());
        }
    }
    let from=previous.as_ref().map(|v|v.0).unwrap_or(0);
    let mut stmt=db.prepare(
        "SELECT start_ms,end_ms,value FROM candles
         WHERE interval=?1 AND start_ms>=?2 ORDER BY start_ms LIMIT ?3"
    )?;
    let rows=stmt.query_map(params![interval,from,MAX_BATCH as i64],|r|{
        Ok(SourceCandle{
            start_ms:r.get(0)?,end_ms:r.get(1)?,
            body:serde_json::from_str::<Value>(&r.get::<_,String>(2)?)
                .map_err(|err|rusqlite::Error::FromSqlConversionFailure(
                    2,rusqlite::types::Type::Text,Box::new(err)))?,
        })
    })?;
    let mut batch=Vec::<CatalogBar>::new();
    let mut expected=previous.as_ref().map(|v|v.0);
    let mut first_utc_day=None::<i64>;
    let mut initialized=false;
    for current in rows {
        let current=current?;
        // Only contiguous finalized source evidence may cross into Parquet.
        // Do not skip a missing interval, a partial day or an in-progress bar.
        if current.body["is_closed"]!=true ||current.body["is_complete"]!=true||
            current.body["retained"]!=true {
            break;
        }
        if let Some(next)=expected {
            if current.start_ms!=next {
                return Err("calculation journal has a history gap or duplicate; no publication".into());
            }
        }
        let bar=parse_bar(&current,interval,product)?;
        let day=bar.open_time_ms.div_euclid(86_400_000);
        if first_utc_day.is_some_and(|value|value!=day) {break;}
        first_utc_day=Some(day);
        expected=Some(current.end_ms);
        batch.push(bar);
        initialized=true;
    }
    drop(stmt);
    if !initialized {
        return Ok(json!({"published":0,"status":"no_complete_candles",
            "exchange_orders":0,"data_ready":false}));
    }
    // Python's Coordinator.fetch() validates the overlap against exchange
    // REST before it allows a Catalog update. "is_complete" inside the
    // calculation journal is not authoritative enough on its own.
    // Do not generate a Parquet history based only on a still-unrepaired WS
    // gap, derived trade completeness or a host-clock guess.
    let (Some(first),Some(last))=(batch.first(),batch.last())else{
        return Err("empty publication candidate".into());
    };
    let end_ms=bar_window(interval,last.open_time_ms)?.1;
    let source=BinanceSource::new()?;
    let remote=source.load_history(
        match product {
            MarketProduct::Spot=>Product::Spot,
            MarketProduct::FuturesUm=>Product::FuturesUm,
            MarketProduct::Options=>return Err("option source cannot use pair journal".into()),
        },symbol,interval,first.open_time_ms,end_ms
    ).await?;
    if remote.len()!=batch.len() ||
       !remote.iter().zip(&batch).all(|(exchange,local)|
           exchange.open_time_ms==local.open_time_ms &&
           exchange.open==local.open&&exchange.high==local.high &&
           exchange.low==local.low&&exchange.close==local.close &&
           exchange.volume==local.volume
       ) {
        return Err("calculated history differs from exchange-confirmed REST candles; repair source journal before publication".into());
    }
    let filepath=catalog.append_bars(
        symbol,product,interval,&batch,price_precision,size_precision
    )?;
    // The source state and Parquet file are two failure domains. Checkpoint
    // after the atomic hard link; if this transaction fails, the next retry
    // verifies the exact historical contents rather than overwriting them.
    let tx=db.transaction()?;
    let after=expected.ok_or("missing publication checkpoint")?;
    tx.execute(
        "INSERT INTO catalog_publications
        (interval,last_end_ms,file_path,acknowledged_at_ms)
        VALUES(?1,?2,?3,unixepoch('subsec')*1000)
        ON CONFLICT(interval) DO UPDATE SET last_end_ms=excluded.last_end_ms,
            file_path=excluded.file_path,
            acknowledged_at_ms=excluded.acknowledged_at_ms",
        params![interval,after,filepath.strip_prefix(catalog.root())?
            .to_string_lossy().to_string()],
    )?;
    tx.commit()?;
    Ok(json!({"published":batch.len(),"symbol":symbol,
        "product":product.as_str(),"interval":interval,
        "checkpoint_end_ms":after,
        "backend":"nautilus_v2_parquet","data_ready":false,
        "exchange_orders":0,"status":"published_unverified"}))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_candles_require_no_unrepaired_trade_ids() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE gaps(
            first_id INTEGER, last_id INTEGER, start_ms INTEGER, end_ms INTEGER,
            PRIMARY KEY(first_id,last_id));").unwrap();
        ensure_no_unresolved_source_gaps(&db).unwrap();
        db.execute("INSERT INTO gaps VALUES (11,12,1000,2000)", []).unwrap();
        assert!(ensure_no_unresolved_source_gaps(&db).is_err());
    }

    #[test]
    fn only_one_catalog_publisher_can_advance_a_pair_checkpoint() {
        let root = std::env::temp_dir().join(format!(
            "seealpha-catalog-lease-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        let first = publication_lease(&root).unwrap();
        assert!(publication_lease(&root).is_err());
        drop(first);
        let reopened = publication_lease(&root).unwrap();
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }
}
#[tokio::main]
async fn main()->std::process::ExitCode{
    match execute().await{
        Ok(result)=>{
            println!("{}",result);
            std::process::ExitCode::SUCCESS
        }
        Err(error)=>{
            eprintln!("Native Catalog publisher failed closed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
