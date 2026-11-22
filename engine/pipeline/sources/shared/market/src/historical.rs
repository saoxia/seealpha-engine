//! Automatic Tick->historical OHLCV reconciliation for finalized native Parquet.
//! Source revisions invalidate the affected time buckets; no exchange Klines.
//! A failure never advances a source fingerprint checkpoint.
use crate::{identity::{self,MarketProduct},tick_bars::INTERVALS};
use rust_decimal::Decimal;
use seealpha_catalog::{bar_window,Catalog,CatalogBar,CatalogError,NANO_PER_MS,RecordKind};
use serde_json::Value;
use serde::{Deserialize,Serialize};
use std::{collections::{BTreeMap,BTreeSet},fs::{self,File,OpenOptions},hash::Hasher,
    io::{Read,Write},path::{Path,PathBuf}};

type Result<T>=std::result::Result<T,Box<dyn std::error::Error>>;
const MINUTE_MS:i64=60_000;
const WORK_MINUTES:usize=10;
const MAX_TICKS:usize=50_000;
const READ_SECONDS:i64=1_800;

#[derive(Clone,Debug)]
struct Revision{
    fingerprint:u64,
    start_ms:i64,
    end_ms:i64,
    cursor_ms:i64,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
struct Completed{fingerprint:u64,start_ms:i64,end_ms:i64}

/// Per-product Pipeline state, with durable per-symbol revision checkpoint.
#[derive(Default)]
pub struct HistoricalTracker{
    completed:BTreeMap<String,Completed>,
    pending:BTreeMap<String,Revision>,
    loaded:BTreeSet<String>,
}
fn file_fingerprint(path:&Path)->Result<u64>{
    let meta=path.symlink_metadata()?;
    if !meta.is_file() || meta.file_type().is_symlink(){
        return Err("untrusted TradeTick Parquet path".into());
    }
    let mut file=File::open(path)?;
    let mut digest=std::collections::hash_map::DefaultHasher::new();
    let mut chunk=[0_u8;64*1024];
    loop {
        let n=file.read(&mut chunk)?;
        if n==0 {break}
        digest.write(&chunk[..n]);
    }
    let after=path.metadata()?;
    if meta.len()!=after.len()||meta.modified()?!=after.modified()?{
        return Err("TradeTick Parquet changed during inventory scan".into());
    }
    Ok(digest.finish())
}
fn ns(record:&Value)->Result<i64>{
    Ok(record.get("ts_event").and_then(Value::as_str)
        .ok_or("TradeTick missing ts_event")?.parse()?)
}
fn decimal(record:&Value,key:&str)->Result<Decimal>{
    let value=record.get(key).and_then(Value::as_str).ok_or("TradeTick missing Decimal")?;
    Ok(value.parse()?)
}
fn read_ticks(source:&Catalog,instrument:&str,start_ms:i64,end_ms:i64)->Result<Vec<Value>>{
    let start=start_ms.checked_mul(NANO_PER_MS).ok_or("Tick start overflow")?;
    let end=end_ms.checked_mul(NANO_PER_MS).ok_or("Tick end overflow")?;
    let rows=source.read_records(&RecordKind::Trades,instrument,start,end,MAX_TICKS)?;
    Ok(rows)
}
fn seconds_for_minute(source:&Catalog,instrument:&str,minute_ms:i64)->Result<Vec<CatalogBar>>{
    let after=minute_ms.checked_add(MINUTE_MS).ok_or("minute overflow")?;
    let mut records=match read_ticks(source,instrument,minute_ms,after){
        Ok(rows)=>rows,
        Err(error) if error.downcast_ref::<CatalogError>()
            .is_some_and(|e|matches!(e,CatalogError::Limit(_)))=>{
            // At exceptionally busy seconds a minute exceeds MAX_TICKS.
            // Page by actual event-time second, never truncate the input.
            let mut collected=Vec::new();
            let mut second=minute_ms;
            while second<after{
                collected.extend(read_ticks(source,instrument,second,second+1_000)?);
                second+=1_000;
            }
            collected
        }
        Err(error)=>return Err(error),
    };
    // Native event timestamps may tie. The original event ordering is
    // authoritative; do not rearrange OHLC open/close within one nanosecond.
    let mut seconds:BTreeMap<i64,CatalogBar>=BTreeMap::new();
    let mut last_ns=None;
    for record in records.drain(..){
        let time=ns(&record)?;
        if last_ns.is_some_and(|prior|time<prior){return Err("out-of-order historical ticks".into())}
        last_ns=Some(time);
        let bucket=time.div_euclid(1_000_000_000)*1_000;
        let price=decimal(&record,"price")?;
        let size=decimal(&record,"size")?;
        if price<=Decimal::ZERO||size<=Decimal::ZERO{return Err("invalid historical Tick".into())}
        match seconds.get_mut(&bucket){
            Some(bar)=>{
                bar.high=bar.high.max(price);
                bar.low=bar.low.min(price);
                bar.close=price;
                bar.volume=bar.volume.checked_add(size).ok_or("historical volume overflow")?;
            }
            None=>{
                seconds.insert(bucket,CatalogBar{
                    open_time_ms:bucket,open:price,high:price,low:price,close:price,volume:size,
                });
            }
        }
    }
    Ok(seconds.into_values().collect())
}
/// Recompute the entire affected minute from all currently visible native
/// TradeTicks, not just the recently imported file (which may overlap peers).
fn rebuild_minute(source:&Catalog,target:&Catalog,instrument:&str,
    minute_ms:i64,precision:(u32,u32))->Result<()>{
    let replacement=seconds_for_minute(source,instrument,minute_ms)?;
    let bar_type=seealpha_catalog::bar_type(instrument,"1s")?;
    // Replace the entire minute, including removed/empty 1s buckets. No
    // synthetic zero-volume bars, no stale historical candles on revisions.
    target.replace_bars_window(&bar_type,"1s",(
        minute_ms,minute_ms.checked_add(MINUTE_MS).ok_or("minute overflow")?
    ),&replacement,precision.0,precision.1)?;
    Ok(())
}
fn higher_bar(target:&Catalog,instrument:&str,interval:&str,
    window_start:i64,window_end:i64)->Result<Option<CatalogBar>>{
    let id=seealpha_catalog::bar_type(instrument,"1s")?;
    // Most instruments/windows are sparse. Try one bounded read first;
    // only chunk exceptionally dense windows if the reader reports Limit.
    let full=target.read_bars(&id,"1s",
        window_start.checked_mul(NANO_PER_MS).ok_or("read start overflow")?,
        window_end.checked_mul(NANO_PER_MS).ok_or("read end overflow")?,50_000);
    let rows=match full {
        Ok(rows)=>rows,
        Err(CatalogError::Limit(_))=>{
            let mut collected=Vec::new();
            let mut cursor=window_start;
            while cursor<window_end{
                let until=cursor.saturating_add(READ_SECONDS*1_000).min(window_end);
                collected.extend(target.read_bars(&id,"1s",
                    cursor.checked_mul(NANO_PER_MS).ok_or("read start overflow")?,
                    until.checked_mul(NANO_PER_MS).ok_or("read end overflow")?,50_000)?);
                cursor=until;
            }
            collected
        }
        Err(error)=>return Err(error.into()),
    };
    let mut merged:Option<CatalogBar>=None;
    for second in rows{
        match merged.as_mut(){
            Some(bar)=>{
                bar.high=bar.high.max(second.high);
                bar.low=bar.low.min(second.low);
                bar.close=second.close;
                bar.volume=bar.volume.checked_add(second.volume).ok_or("higher volume overflow")?;
            }
            None=>{
                merged=Some(CatalogBar{open_time_ms:window_start,
                    open:second.open,high:second.high,low:second.low,
                    close:second.close,volume:second.volume});
            }
        }
    }
    // Validate the same exchange-aligned duration rules used by live TickBars.
    let (_,actual_end)=bar_window(interval,window_start)?;
    if actual_end!=window_end{return Err("historical interval boundary mismatch".into())}
    Ok(merged)
}
fn rebuild_higher(target:&Catalog,instrument:&str,start_ms:i64,end_ms:i64,
    precision:(u32,u32))->Result<()>{
    for &interval in INTERVALS.iter().filter(|&&i|i!="1s"){
        let mut cursor=bar_window(interval,start_ms)?.0;
        while cursor<end_ms{
            let next=bar_window(interval,cursor)?.1;
            let output=higher_bar(target,instrument,interval,cursor,next)?;
            let id=seealpha_catalog::bar_type(instrument,interval)?;
            let bars=output.into_iter().collect::<Vec<_>>();
            target.replace_bars_window(&id,interval,(cursor,next),&bars,precision.0,precision.1)?;
            cursor=next;
        }
    }
    Ok(())
}
fn aligned_start(ms:i64)->i64{ms.div_euclid(MINUTE_MS)*MINUTE_MS}
fn aligned_end(ms:i64)->Result<i64>{
    aligned_start(ms).checked_add(MINUTE_MS).ok_or_else(||"Tick end overflow".into())
}
fn checkpoint_path(root:&Path,instrument:&str)->Result<PathBuf>{
    let directory=root.join(".history-tick-revisions");
    match directory.symlink_metadata(){
        Ok(meta) if meta.is_dir()&&!meta.file_type().is_symlink()=>{},
        Ok(_)=>return Err("untrusted history revision directory".into()),
        Err(error) if error.kind()==std::io::ErrorKind::NotFound=>{
            fs::create_dir(&directory)?;
        }
        Err(error)=>return Err(error.into()),
    }
    Ok(directory.join(format!("{instrument}.json")))
}
fn read_checkpoint(root:&Path,instrument:&str)->Result<BTreeMap<String,Completed>>{
    let path=checkpoint_path(root,instrument)?;
    let raw=match fs::read_to_string(path){
        Ok(raw)=>raw,
        Err(e) if e.kind()==std::io::ErrorKind::NotFound=>return Ok(BTreeMap::new()),
        Err(e)=>return Err(e.into()),
    };
    let state:BTreeMap<String,Completed>=serde_json::from_str(&raw)?;
    if state.keys().any(|key|!key.starts_with(&format!("{instrument}/"))){
        return Err("historical revision checkpoint identity mismatch".into());
    }
    Ok(state)
}
fn write_checkpoint(root:&Path,instrument:&str,state:&BTreeMap<String,Completed>)->Result<()>{
    let path=checkpoint_path(root,instrument)?;
    let parent=path.parent().ok_or("missing revision checkpoint parent")?;
    let stamp=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos();
    let tmp=parent.join(format!(".{}.{}.{stamp}.tmp",instrument,std::process::id()));
    let result=(|| ->Result<()> {
        let mut file=OpenOptions::new().create_new(true).write(true).open(&tmp)?;
        let content=serde_json::to_vec(state)?;
        file.write_all(&content)?;
        file.sync_all()?;
        fs::rename(&tmp,&path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    let _=fs::remove_file(tmp);
    result
}
/// A crash leaves a durable marker before any replacement deletion.
/// Recover marked ranges first; readers otherwise reject the unfinished Bar.
fn recover_marked_windows(source:&Catalog,target:&Catalog,instrument:&str,
    precision:(u32,u32))->Result<()>{
    for &interval in INTERVALS{
        let id=seealpha_catalog::bar_type(instrument,interval)?;
        let marker=target.root().join("data/bars").join(&id).join(".rebuilding");
        let raw=match fs::read_to_string(marker){
            Ok(raw)=>raw,
            Err(e) if e.kind()==std::io::ErrorKind::NotFound=>continue,
            Err(e)=>return Err(e.into()),
        };
        let (start,end)=raw.split_once(',').ok_or("invalid Bar recovery marker")?;
        let start_ms:i64=start.parse()?;
        let end_ms:i64=end.parse()?;
        if interval=="1s"{
            if end_ms!=start_ms+MINUTE_MS{
                return Err("unexpected 1s recovery window".into());
            }
            rebuild_minute(source,target,instrument,start_ms,precision)?;
        }else{
            if bar_window(interval,start_ms)?.1!=end_ms{
                return Err("unexpected historical recovery boundary".into());
            }
            let output=higher_bar(target,instrument,interval,start_ms,end_ms)?;
            let bars=output.into_iter().collect::<Vec<_>>();
            target.replace_bars_window(&id,interval,(start_ms,end_ms),&bars,
                precision.0,precision.1)?;
        }
    }
    Ok(())
}
impl HistoricalTracker {
    /// A single bounded Pipeline poll; native persisted TradeTicks are the
    /// sole input. Only fingerprints committed after all derivative writes.
    pub fn poll(&mut self,source:&Catalog,target:&Catalog,symbol:&str,
        product:MarketProduct,precision:(u32,u32))->Result<usize>{
        let instrument=identity::instrument_id(symbol,product)?;
        if !self.loaded.contains(&instrument){
            // A failed checkpoint load must be retryable on the next poll.
            self.completed.extend(read_checkpoint(target.root(),&instrument)?);
            self.loaded.insert(instrument.clone());
        }
        recover_marked_windows(source,target,&instrument,precision)?;
        let inventory=source.inventory(&RecordKind::Trades,&instrument)?;
        let directory=source.root().join("data/trades").join(&instrument);
        let mut names=BTreeSet::new();
        for row in inventory{
            let filename=row["file"].as_str().ok_or("invalid native file inventory")?;
            let key=format!("{instrument}/{filename}");
            names.insert(key.clone());
            let path=directory.join(filename);
            let fingerprint=file_fingerprint(&path)?;
            if self.completed.get(&key).is_some_and(|c|c.fingerprint==fingerprint)
                || self.pending.get(&key).is_some_and(|r|r.fingerprint==fingerprint)
            {continue}
            let first_ns:i64=row["start_ns"].as_str().ok_or("start missing")?.parse()?;
            let last_ns:i64=row["end_ns"].as_str().ok_or("end missing")?.parse()?;
            if first_ns<0||last_ns<first_ns{return Err("invalid Tick file range".into())}
            let start_ms=aligned_start(first_ns.div_euclid(NANO_PER_MS));
            let end_ms=aligned_end(last_ns.div_euclid(NANO_PER_MS))?;
            self.pending.insert(key,Revision{fingerprint,start_ms,end_ms,cursor_ms:start_ms});
        }
        // A removed native file invalidates its former time range too.
        // Replay that range against REMAINING TradeTicks, deleting obsolete
        // 1s and upper candles rather than keeping stale OHLCV.
        let removed=self.completed.iter()
            .filter(|(key,_)|key.starts_with(&format!("{instrument}/"))&&!names.contains(*key))
            .map(|(key,value)|(key.clone(),value.clone())).collect::<Vec<_>>();
        for (key,entry) in removed{
            if self.pending.get(&key).is_none_or(|task|task.fingerprint!=0){
                self.pending.insert(key,Revision {
                    fingerprint:0,start_ms:entry.start_ms,
                    end_ms:entry.end_ms,cursor_ms:entry.start_ms,
                });
            }
        }
        let mut worked=0;
        let keys=self.pending.keys().filter(|key|key.starts_with(&format!("{instrument}/")))
            .cloned().collect::<Vec<_>>();
        for key in keys{
            while worked<WORK_MINUTES {
                let Some(revision)=self.pending.get(&key).cloned() else {break};
                if revision.cursor_ms>=revision.end_ms{
                    rebuild_higher(target,&instrument,revision.start_ms,revision.end_ms,precision)?;
                    // Persist revision state before declaring this source
                    // done, to avoid full replays after a process restart.
                    let mut next=self.completed.clone();
                    if revision.fingerprint==0{
                        next.remove(&key);
                    }else{
                        next.insert(key.clone(),Completed{
                            fingerprint:revision.fingerprint,
                            start_ms:revision.start_ms,end_ms:revision.end_ms,
                        });
                    }
                    let by_instrument=next.iter()
                        .filter(|(k,_)|k.starts_with(&format!("{instrument}/")))
                        .map(|(k,v)|(k.clone(),v.clone())).collect::<BTreeMap<_,_>>();
                    write_checkpoint(target.root(),&instrument,&by_instrument)?;
                    self.completed=next;
                    self.pending.remove(&key);
                    break;
                }
                rebuild_minute(source,target,&instrument,revision.cursor_ms,precision)?;
                if let Some(task)=self.pending.get_mut(&key){
                    task.cursor_ms=task.cursor_ms.checked_add(MINUTE_MS).ok_or("cursor overflow")?;
                }
                worked+=1;
            }
            if worked>=WORK_MINUTES{break}
        }
        Ok(worked)
    }
}

#[cfg(test)]
mod tests{
    use super::*;

    #[test]
    fn auto_rebuilds_after_native_tick_insert_revision_and_deletion(){
        use arrow_array::{ArrayRef,Decimal128Array,RecordBatch,StringArray,
            builder::TimestampNanosecondBuilder};
        use arrow_schema::{DataType,Field,Schema,TimeUnit};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let root=std::env::temp_dir().join(format!("sa-history-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                .unwrap().as_nanos()));
        let src=root.join("native");
        let out=root.join("derived");
        let instrument="ETHUSDT.BINANCE";
        let trade_dir=src.join("data/trades").join(instrument);
        fs::create_dir_all(&trade_dir).unwrap();
        fs::create_dir_all(&out).unwrap();
        let base=1_735_689_600_000_i64; // UTC 2025-01-01
        let filename="2025-01-01T00-00-00-100000000Z_2025-01-01T00-00-00-500000000Z.parquet";
        let file=trade_dir.join(filename);
        let write_native=|second_price:i128,second_qty:i128|{
            let factor=10_i128.pow(16);
            let price=Arc::new(Decimal128Array::from(vec![100*factor,second_price*factor])
                .with_precision_and_scale(38,16).unwrap()) as ArrayRef;
            let size=Arc::new(Decimal128Array::from(vec![factor,second_qty*factor])
                .with_precision_and_scale(38,16).unwrap()) as ArrayRef;
            let mut events=TimestampNanosecondBuilder::with_capacity(2).with_timezone("UTC");
            let mut init=TimestampNanosecondBuilder::with_capacity(2).with_timezone("UTC");
            for ns in [(base+100)*1_000_000,(base+500)*1_000_000]{
                events.append_value(ns);
                init.append_value(ns);
            }
            let fields=vec![
                Field::new("price",DataType::Decimal128(38,16),false),
                Field::new("size",DataType::Decimal128(38,16),false),
                Field::new("trade_id",DataType::Utf8,false),
                Field::new("aggressor_side",DataType::Utf8,false),
                Field::new("ts_event",DataType::Timestamp(TimeUnit::Nanosecond,Some("UTC".into())),false),
                Field::new("ts_init",DataType::Timestamp(TimeUnit::Nanosecond,Some("UTC".into())),false),
            ];
            let schema=Arc::new(Schema::new_with_metadata(fields,std::collections::HashMap::from([
                ("type_name".into(),"TradeTick".into()),
                ("instrument_id".into(),instrument.into()),
                ("price_precision".into(),"2".into()),
                ("size_precision".into(),"2".into()),
            ])));
            let cols:Vec<ArrayRef>=vec![
                price,size,
                Arc::new(StringArray::from(vec!["1","2"])),
                Arc::new(StringArray::from(vec!["BUYER","SELLER"])),
                Arc::new(events.finish()),Arc::new(init.finish()),
            ];
            let data=RecordBatch::try_new(schema,cols).unwrap();
            let tmp=trade_dir.join("source.parquet.tmp");
            let mut writer=ArrowWriter::try_new(File::create(&tmp).unwrap(),data.schema(),None).unwrap();
            writer.write(&data).unwrap();
            writer.close().unwrap();
            fs::rename(tmp,&file).unwrap();
        };
        let source=Catalog::open_existing(&src).unwrap();
        let target=Catalog::open_existing(&out).unwrap();
        let mut tracker=HistoricalTracker::default();
        write_native(110,2);
        assert_eq!(tracker.poll(&source,&target,"ETHUSDT",MarketProduct::Spot,(2,2)).unwrap(),1);
        let first_id=seealpha_catalog::bar_type(instrument,"1s").unwrap();
        let minute_id=seealpha_catalog::bar_type(instrument,"1m").unwrap();
        let read=|id:&str,interval:&str,end_ms:i64|target.read_bars(id,interval,
            base*1_000_000,end_ms*1_000_000,100).unwrap();
        let seconds=read(&first_id,"1s",base+1_000);
        let minutes=read(&minute_id,"1m",base+60_000);
        assert_eq!(seconds.len(),1);
        assert_eq!(seconds[0].high,Decimal::from(110));
        assert_eq!(minutes[0].volume,Decimal::from(3));
        assert_eq!(tracker.poll(&source,&target,"ETHUSDT",MarketProduct::Spot,(2,2)).unwrap(),0);
        // Same file name and time bounds, corrected underlying Tick values.
        write_native(120,5);
        assert_eq!(tracker.poll(&source,&target,"ETHUSDT",MarketProduct::Spot,(2,2)).unwrap(),1);
        assert_eq!(read(&first_id,"1s",base+1_000)[0].high,Decimal::from(120));
        assert_eq!(read(&minute_id,"1m",base+60_000)[0].volume,Decimal::from(6));
        // Durable checkpoint: a new worker must not repeat finished work.
        let mut restarted=HistoricalTracker::default();
        assert_eq!(restarted.poll(&source,&target,"ETHUSDT",MarketProduct::Spot,(2,2)).unwrap(),0);
        fs::remove_file(file).unwrap();
        assert_eq!(restarted.poll(&source,&target,"ETHUSDT",MarketProduct::Spot,(2,2)).unwrap(),1);
        assert!(read(&first_id,"1s",base+1_000).is_empty());
        assert!(read(&minute_id,"1m",base+60_000).is_empty());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn minute_bounds_include_entire_last_trade_second(){
        assert_eq!(aligned_start(61_999),60_000);
        assert_eq!(aligned_end(61_999).unwrap(),120_000);
    }
}
