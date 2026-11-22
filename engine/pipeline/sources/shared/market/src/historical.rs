//! Automatic Tick->historical OHLCV reconciliation for finalized native Parquet.
//! Source revisions invalidate the affected time buckets; no exchange Klines.
//! A failure never advances a source fingerprint checkpoint.
use crate::{identity::{self,MarketProduct},tick_bars::INTERVALS};
use rust_decimal::Decimal;
use seealpha_catalog::{bar_window,Catalog,CatalogBar,NANO_PER_MS,RecordKind};
use serde_json::Value;
use std::{collections::{BTreeMap,BTreeSet},fs::File,hash::{Hash,Hasher},
    io::Read,path::Path};

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
/// Persisted in this long-running product worker, not a second job scheduler.
/// Restart deliberately rechecks every native source file (idempotent writes).
#[derive(Default)]
pub struct HistoricalTracker{
    completed:BTreeMap<String,u64>,
    pending:BTreeMap<String,Revision>,
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
    let records=read_ticks(source,instrument,minute_ms,after)?;
    let mut seconds:BTreeMap<i64,CatalogBar>=BTreeMap::new();
    let mut last_ns=None;
    for record in records{
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
    // The current append_bars API updates and inserts but cannot delete a
    // timestamp. Never silently leave a stale 1s candle after a deletion.
    let old=target.read_bars(&bar_type,"1s",
        minute_ms*NANO_PER_MS,(minute_ms+MINUTE_MS)*NANO_PER_MS,100)?;
    let new_keys=replacement.iter().map(|bar|bar.open_time_ms).collect::<BTreeSet<_>>();
    if old.iter().any(|bar|!new_keys.contains(&bar.open_time_ms)){
        return Err("Tick deletion requires Bar range-replacement support; existing candle would become stale".into());
    }
    let mut run=Vec::new();
    for bar in replacement{
        if let Some(previous)=run.last().map(|b:&CatalogBar|b.open_time_ms)
            && bar.open_time_ms!=previous+1_000
        {
            target.append_bars(&bar_type,"1s",&run,precision.0,precision.1)?;
            run.clear();
        }
        run.push(bar);
    }
    if !run.is_empty(){
        target.append_bars(&bar_type,"1s",&run,precision.0,precision.1)?;
    }
    Ok(())
}
fn higher_bar(target:&Catalog,instrument:&str,interval:&str,
    window_start:i64,window_end:i64)->Result<Option<CatalogBar>>{
    let id=seealpha_catalog::bar_type(instrument,"1s")?;
    let mut cursor=window_start;
    let mut merged:Option<CatalogBar>=None;
    while cursor<window_end{
        let until=cursor.saturating_add(READ_SECONDS*1_000).min(window_end);
        let rows=target.read_bars(&id,"1s",
            cursor.checked_mul(NANO_PER_MS).ok_or("read start overflow")?,
            until.checked_mul(NANO_PER_MS).ok_or("read end overflow")?,50_000)?;
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
        cursor=until;
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
            if let Some(bar)=higher_bar(target,instrument,interval,cursor,next)?{
                let id=seealpha_catalog::bar_type(instrument,interval)?;
                target.append_bars(&id,interval,&[bar],precision.0,precision.1)?;
            }
            cursor=next;
        }
    }
    Ok(())
}
fn aligned_start(ms:i64)->i64{ms.div_euclid(MINUTE_MS)*MINUTE_MS}
fn aligned_end(ms:i64)->Result<i64>{
    aligned_start(ms).checked_add(MINUTE_MS).ok_or_else(||"Tick end overflow".into())
}
impl HistoricalTracker {
    /// A single bounded Pipeline poll; native persisted TradeTicks are the
    /// sole input. Only fingerprints committed after all derivative writes.
    pub fn poll(&mut self,source:&Catalog,target:&Catalog,symbol:&str,
        product:MarketProduct,precision:(u32,u32))->Result<usize>{
        let instrument=identity::instrument_id(symbol,product)?;
        let inventory=source.inventory(&RecordKind::Trades,&instrument)?;
        let directory=source.root().join("data/trades").join(&instrument);
        let mut names=BTreeSet::new();
        for row in inventory{
            let filename=row["file"].as_str().ok_or("invalid native file inventory")?;
            let key=format!("{instrument}/{filename}");
            names.insert(key.clone());
            let path=directory.join(filename);
            let fingerprint=file_fingerprint(&path)?;
            if self.completed.get(&key)==Some(&fingerprint)
                || self.pending.get(&key).is_some_and(|r|r.fingerprint==fingerprint)
            {continue}
            let first_ns:i64=row["start_ns"].as_str().ok_or("start missing")?.parse()?;
            let last_ns:i64=row["end_ns"].as_str().ok_or("end missing")?.parse()?;
            if first_ns<0||last_ns<first_ns{return Err("invalid Tick file range".into())}
            let start_ms=aligned_start(first_ns.div_euclid(NANO_PER_MS));
            let end_ms=aligned_end(last_ns.div_euclid(NANO_PER_MS))?;
            self.pending.insert(key,Revision{fingerprint,start_ms,end_ms,cursor_ms:start_ms});
        }
        // File removal must not silently leave obsolete historic Bars.
        // Preserve prior state and fail closed until range-replacement is available.
        if self.completed.keys().any(|key|key.starts_with(&format!("{instrument}/"))&&!names.contains(key)){
            return Err("previously indexed TradeTick file disappeared; derived Bar invalidation is required".into());
        }
        let mut worked=0;
        let keys=self.pending.keys().filter(|key|key.starts_with(&format!("{instrument}/")))
            .cloned().collect::<Vec<_>>();
        for key in keys{
            while worked<WORK_MINUTES {
                let Some(revision)=self.pending.get(&key).cloned() else {break};
                if revision.cursor_ms>=revision.end_ms{
                    rebuild_higher(target,&instrument,revision.start_ms,revision.end_ms,precision)?;
                    self.completed.insert(key.clone(),revision.fingerprint);
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
    fn minute_bounds_include_entire_last_trade_second(){
        assert_eq!(aligned_start(61_999),60_000);
        assert_eq!(aligned_end(61_999).unwrap(),120_000);
    }
}
