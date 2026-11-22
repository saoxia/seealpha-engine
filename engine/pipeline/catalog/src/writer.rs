//! Transaction-locked Bar Parquet publisher supporting updates to existing Bars.
//! Parquet objects are rewritten atomically; the Bar identity is not immutable.
use crate::{
    Catalog, CatalogError,
    model::{CatalogBar, parse_bar_type},
    reader::{RecordKind, candidate_files, filename_bounds, inspect_existing_directory},
};
use arrow_array::builder::TimestampNanosecondBuilder;
use arrow_array::{ArrayRef, Decimal128Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use fs2::FileExt;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use rust_decimal::Decimal;
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

const MAX_WRITE: usize = 50_000;
fn decimal(v: Decimal) -> Result<i128, CatalogError> {
    let mut canonical = v;
    canonical.rescale(16);
    if canonical.scale() != 16 || canonical != v {
        return Err(CatalogError::Invalid(
            "K 线 Decimal 超过原生 Parquet 16 位小数精度。".into(),
        ));
    }
    Ok(canonical.mantissa())
}
fn array(values: Vec<i128>) -> Result<ArrayRef, CatalogError> {
    Ok(Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(38, 16)
            .map_err(|e| CatalogError::Arrow(e.to_string()))?,
    ))
}
fn format_ts(ns: i64) -> Result<String, CatalogError> {
    let sec = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000) as u32;
    let time = DateTime::<Utc>::from_timestamp(sec, nanos)
        .ok_or_else(|| CatalogError::Invalid("Catalog 文件时间超出 UTC 范围。".into()))?;
    Ok(format!(
        "{}-{:09}Z",
        time.format("%Y-%m-%dT%H-%M-%S"),
        nanos
    ))
}
pub(crate) fn file_name(first_ns: i64, last_ns: i64) -> Result<String, CatalogError> {
    if first_ns > last_ns {
        return Err(CatalogError::Invalid("Catalog 文件时间倒退。".into()));
    }
    Ok(format!(
        "{}_{}.parquet",
        format_ts(first_ns)?,
        format_ts(last_ns)?
    ))
}
pub(crate) fn safe_mkdir(root: &Path, rel: &Path) -> Result<PathBuf, CatalogError> {
    let mut path = root.to_path_buf();
    for component in rel.components() {
        path.push(component);
        if !inspect_existing_directory(&path)? {
            fs::create_dir(&path)?;
            inspect_existing_directory(&path)?;
        }
    }
    Ok(path)
}
fn schema(
    bar_type: &str,
    instrument_id: &str,
    price_precision: u32,
    size_precision: u32,
) -> Arc<Schema> {
    let columns = ["open", "high", "low", "close", "volume"]
        .into_iter()
        .map(|name| Field::new(name, DataType::Decimal128(38, 16), false))
        .chain(["ts_event", "ts_init"].into_iter().map(|name| {
            Field::new(
                name,
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            )
        }))
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(
        columns,
        HashMap::from([
            ("type_name".to_string(), "Bar".to_string()),
            ("bar_type".to_string(), bar_type.to_owned()),
            ("instrument_id".to_string(), instrument_id.to_owned()),
            ("price_precision".to_string(), price_precision.to_string()),
            ("size_precision".to_string(), size_precision.to_string()),
        ]),
    ))
}
/// Stage a complete immutable Parquet object. The Catalog can update a
/// logical Bar by atomically REPLACING the object at its existing file path.
fn stage_bars(
    path:&Path,kind:&str,instrument:&str,interval:&str,bars:&[CatalogBar],
    price_precision:u32,size_precision:u32,
)->Result<(),CatalogError>{
    let events=bars.iter().map(|b|b.event_ns(interval)).collect::<Result<Vec<_>,_>>()?;
    let mut columns=Vec::with_capacity(7);
    for col in 0..5 {
        let values=bars.iter().map(|bar|decimal(match col {
            0=>bar.open,1=>bar.high,2=>bar.low,3=>bar.close,_=>bar.volume,
        })).collect::<Result<Vec<_>,_>>()?;
        columns.push(array(values)?);
    }
    let mut event_builder=TimestampNanosecondBuilder::with_capacity(events.len()).with_timezone("UTC");
    let mut init_builder=TimestampNanosecondBuilder::with_capacity(events.len()).with_timezone("UTC");
    for ts in &events {
        event_builder.append_value(*ts);
        init_builder.append_value(*ts);
    }
    columns.push(Arc::new(event_builder.finish()));
    columns.push(Arc::new(init_builder.finish()));
    let batch=RecordBatch::try_new(schema(kind,instrument,price_precision,size_precision),columns)
        .map_err(|e|CatalogError::Arrow(e.to_string()))?;
    let file=OpenOptions::new().create_new(true).write(true).open(path)?;
    let options=WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(5_000)).build();
    let mut writer=ArrowWriter::try_new(file,batch.schema(),Some(options))
        .map_err(|e|CatalogError::Parquet(e.to_string()))?;
    writer.write(&batch).map_err(|e|CatalogError::Parquet(e.to_string()))?;
    writer.close().map_err(|e|CatalogError::Parquet(e.to_string()))?;
    File::open(path)?.sync_all()?;
    Ok(())
}
fn validate_batch(interval:&str,rows:&[CatalogBar],price_precision:u32,size_precision:u32)
    ->Result<(),CatalogError>
{
    if rows.is_empty() || rows.len()>MAX_WRITE {
        return Err(CatalogError::Limit("Bar batch exceeds writer bound".into()));
    }
    let mut next=rows[0].open_time_ms;
    for bar in rows {
        bar.validate(interval)?;
        if bar.open_time_ms!=next {
            return Err(CatalogError::Conflict("Bar batch is not contiguous".into()));
        }
        for (value,precision) in [
            (bar.open,price_precision),(bar.high,price_precision),
            (bar.low,price_precision),(bar.close,price_precision),
            (bar.volume,size_precision),
        ] {
            if value.round_dp(precision)!=value {
                return Err(CatalogError::Invalid("Bar exceeds Instrument decimal precision".into()));
            }
        }
        next=crate::bar_window(interval,next)?.1;
    }
    Ok(())
}
impl Catalog {

    /// Replace every Bar in an exact aligned window, including deleting
    /// old timestamps no longer supported by TradeTick. The durable marker
    /// makes incomplete crash-time replacements unreadable until replay.
    pub fn replace_bars_window(
        &self,id:&str,interval:&str,window:(i64,i64),
        bars:&[CatalogBar],price_precision:u32,size_precision:u32,
    )->Result<(),CatalogError>{
        let (start_ms,end_ms)=window;
        if start_ms<0||end_ms<=start_ms||bars.len()>MAX_WRITE
            ||price_precision>16||size_precision>16
            ||crate::bar_window(interval,start_ms)?.0!=start_ms
            ||crate::bar_window(interval,end_ms)?.0!=end_ms
        {
            return Err(CatalogError::Invalid("invalid historical replacement window".into()));
        }
        let (instrument,recorded_interval)=parse_bar_type(id)?;
        if recorded_interval!=interval{
            return Err(CatalogError::Invalid("BarType interval mismatch".into()));
        }
        let mut previous=None;
        for bar in bars{
            bar.validate(interval)?;
            if bar.open_time_ms<start_ms||bar.open_time_ms>=end_ms
                ||previous.is_some_and(|p|bar.open_time_ms<=p)
            {return Err(CatalogError::Invalid("replacement Bar timestamp invalid".into()));}
            for (value,precision) in [
                (bar.open,price_precision),(bar.high,price_precision),
                (bar.low,price_precision),(bar.close,price_precision),(bar.volume,size_precision),
            ]{
                if value.round_dp(precision)!=value{
                    return Err(CatalogError::Invalid("replacement Bar precision invalid".into()));
                }
            }
            previous=Some(bar.open_time_ms);
        }
        let rel=RecordKind::Bars.directory(id)?;
        let directory=safe_mkdir(&self.root,&rel)?;
        let lock=OpenOptions::new().create(true).truncate(false).read(true).write(true)
            .open(directory.join(".writer.lock"))?;
        lock.lock_exclusive()?;
        let marker=directory.join(".rebuilding");
        let expected=format!("{start_ms},{end_ms}");
        match fs::read_to_string(&marker){
            Ok(old) if old!=expected=>{
                return Err(CatalogError::Conflict("different Bar window has pending crash recovery".into()));
            }
            Ok(_)=>{},
            Err(error) if error.kind()==std::io::ErrorKind::NotFound=>{},
            Err(error)=>return Err(error.into()),
        }
        let from=start_ms.checked_mul(crate::NANO_PER_MS)
            .ok_or_else(||CatalogError::Invalid("Bar start overflow".into()))?;
        let to=end_ms.checked_mul(crate::NANO_PER_MS)
            .ok_or_else(||CatalogError::Invalid("Bar end overflow".into()))?;
        let existing=candidate_files(&self.root,&rel,from,to)?;
        for path in &existing{
            let name=path.file_name().and_then(|n|n.to_str())
                .ok_or_else(||CatalogError::Invalid("invalid Bar file".into()))?;
            let (first,last)=filename_bounds(name)?;
            if first<from||last>=to{
                return Err(CatalogError::Conflict("historical replacement crosses an existing file".into()));
            }
        }
        let staging=safe_mkdir(&self.root,Path::new(".staging"))?;
        let timestamp=std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_|CatalogError::Invalid("invalid system timestamp".into()))?.as_nanos();
        let mut staged:Vec<(PathBuf,PathBuf)>=Vec::new();
        let mut run:Vec<CatalogBar>=Vec::new();
        let mut flush=|run:&[CatalogBar]|->Result<(),CatalogError>{
            if run.is_empty(){return Ok(())}
            validate_batch(interval,run,price_precision,size_precision)?;
            let name=file_name(run[0].event_ns(interval)?,run[run.len()-1].event_ns(interval)?)?;
            let dest=directory.join(name);
            let tmp=staging.join(format!("rebuild-{}-{timestamp}-{}.tmp",std::process::id(),staged.len()));
            stage_bars(&tmp,id,&instrument,interval,run,price_precision,size_precision)?;
            staged.push((tmp,dest));
            Ok(())
        };
        let prepare=(||->Result<(),CatalogError>{
            for bar in bars{
                if let Some(prior)=run.last().map(|b|b.open_time_ms)
                    && crate::bar_window(interval,prior)?.1!=bar.open_time_ms
                {
                    flush(&run)?;
                    run.clear();
                }
                run.push(bar.clone());
            }
            flush(&run)
        })();
        if let Err(error)=prepare{
            for (tmp,_) in &staged{let _=fs::remove_file(tmp);}
            return Err(error);
        }
        {
            use std::io::Write;
            let mut file=OpenOptions::new().create(true).truncate(true).write(true).open(&marker)?;
            file.write_all(expected.as_bytes())?;
            file.sync_all()?;
        }
        File::open(&directory)?.sync_all()?;
        let commit=(||->Result<(),CatalogError>{
            for old in &existing{fs::remove_file(old)?;}
            for (tmp,dest) in &staged{fs::hard_link(tmp,dest)?;}
            File::open(&directory)?.sync_all()?;
            fs::remove_file(&marker)?;
            File::open(&directory)?.sync_all()?;
            Ok(())
        })();
        for (tmp,_) in &staged{let _=fs::remove_file(tmp);}
        commit
    }
    /// Insert or update a validated Bar batch. If a timestamp already exists,
    /// the incoming Bar replaces it and untouched rows remain intact.
    /// The writer lock serializes other writers and guarded Catalog readers.
    pub fn append_bars(
        &self,bar_identity:&str,interval:&str,bars:&[CatalogBar],
        price_precision:u32,size_precision:u32,
    )->Result<PathBuf,CatalogError>{
        if bars.is_empty() || bars.len()>MAX_WRITE || price_precision>16 || size_precision>16 {
            return Err(CatalogError::Invalid("invalid Bar batch length or precision".into()));
        }
        let (instrument,recorded_interval)=parse_bar_type(bar_identity)?;
        if recorded_interval!=interval {
            return Err(CatalogError::Invalid("BarType interval mismatch".into()));
        }
        validate_batch(interval,bars,price_precision,size_precision)?;
        let rel=RecordKind::Bars.directory(bar_identity)?;
        let directory=safe_mkdir(&self.root,&rel)?;
        let lease=OpenOptions::new().create(true).truncate(false).read(true).write(true)
            .open(directory.join(".writer.lock"))?;
        lease.lock_exclusive()?;
        if directory.join(".rebuilding").exists(){
            return Err(CatalogError::Conflict("Tick-derived Bar replacement awaiting recovery".into()));
        }
        let first=bars[0].event_ns(interval)?;
        let last=bars.last().ok_or_else(||CatalogError::Invalid("empty batch".into()))?
            .event_ns(interval)?;
        let overlapping=candidate_files(&self.root,&rel,first,last.saturating_add(1))?;
        let idempotent_path=overlapping.first().cloned();
        let mut incoming=bars.iter().cloned()
            .map(|bar|(bar.open_time_ms,bar)).collect::<BTreeMap<_,_>>();
        let mut revisions:Vec<(PathBuf,Vec<CatalogBar>,bool)>=Vec::new();
        for path in overlapping {
            let name=path.file_name().and_then(|n|n.to_str())
                .ok_or_else(||CatalogError::Invalid("invalid Bar filename".into()))?;
            let (begin,end)=filename_bounds(name)?;
            let reader=parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                File::open(&path)?
            ).map_err(|e|CatalogError::Parquet(e.to_string()))?;
            let meta=reader.schema().metadata();
            if meta.get("price_precision").map(String::as_str)!=Some(price_precision.to_string().as_str())
                || meta.get("size_precision").map(String::as_str)!=Some(size_precision.to_string().as_str()) {
                return Err(CatalogError::Conflict("Instrument decimal precision changed".into()));
            }
            let existing=self.read_bars_unlocked(bar_identity,interval,begin,end.saturating_add(1),MAX_WRITE)?;
            if existing.is_empty()
                || existing.first().ok_or_else(||CatalogError::Invalid("missing Bar".into()))?
                    .event_ns(interval)?!=begin
                || existing.last().ok_or_else(||CatalogError::Invalid("missing Bar".into()))?
                    .event_ns(interval)?!=end {
                return Err(CatalogError::Invalid("existing Parquet filename does not match Bar rows".into()));
            }
            let mut merged=existing.iter().cloned()
                .map(|bar|(bar.open_time_ms,bar)).collect::<BTreeMap<_,_>>();
            let keys=incoming.iter()
                .filter_map(|(&key,bar)|match bar.event_ns(interval) {
                    Ok(ts) if (begin..=end).contains(&ts)=>Some(key),
                    _=>None,
                }).collect::<Vec<_>>();
            for key in keys {
                let bar=incoming.remove(&key)
                    .ok_or_else(||CatalogError::Conflict("incoming Bar vanished".into()))?;
                merged.insert(key,bar);
            }
            let updated=merged.into_values().collect::<Vec<_>>();
            validate_batch(interval,&updated,price_precision,size_precision)?;
            if updated!=existing {
                revisions.push((path,updated,true));
            }
        }
        // New time buckets are written as new files. Updating an old Bar
        // rewrites only its existing Parquet file (same name/time bounds),
        // rather than introducing overlapping or duplicate Parquet records.
        let mut pending=Vec::<CatalogBar>::new();
        let mut previous=None;
        for bar in incoming.into_values() {
            if let Some(prev)=previous
                && crate::bar_window(interval,prev)?.1!=bar.open_time_ms
            {
                let first=pending.first().ok_or_else(||CatalogError::Invalid("empty Bar group".into()))?
                    .event_ns(interval)?;
                let last=pending.last().ok_or_else(||CatalogError::Invalid("empty Bar group".into()))?
                    .event_ns(interval)?;
                revisions.push((directory.join(file_name(first,last)?),std::mem::take(&mut pending),false));
            }
            previous=Some(bar.open_time_ms);
            pending.push(bar);
        }
        if !pending.is_empty() {
            let first=pending.first().ok_or_else(||CatalogError::Invalid("empty Bar group".into()))?
                .event_ns(interval)?;
            let last=pending.last().ok_or_else(||CatalogError::Invalid("empty Bar group".into()))?
                .event_ns(interval)?;
            revisions.push((directory.join(file_name(first,last)?),pending,false));
        }
        if revisions.is_empty() {
            return Ok(idempotent_path.unwrap_or(directory.join(file_name(first,last)?)));
        }
        let staging=safe_mkdir(&self.root,Path::new(".staging"))?;
        let stamp=std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_|CatalogError::Invalid("invalid system clock".into()))?
            .as_nanos();
        let mut staged=Vec::new();
        for (index,(destination,rows,replace)) in revisions.iter().enumerate() {
            let tmp=staging.join(format!("bar-{}-{stamp}-{index}.parquet.tmp",std::process::id()));
            if let Err(error)=stage_bars(&tmp,bar_identity,&instrument,interval,rows,price_precision,size_precision) {
                let _=fs::remove_file(&tmp);
                for (prior,_,_) in &staged {
                    let _=fs::remove_file(prior);
                }
                return Err(error);
            }
            staged.push((tmp,destination.clone(),*replace));
        }
        // The complete new content is durable before publication. Each
        // individual object replacement is atomic even across a process crash.
        // A multi-file partial commit remains valid and an identical retry
        // finishes the outstanding files without creating duplicate buckets.
        let mut result=None;
        for (tmp,dest,replace) in &staged {
            let published=if *replace {
                fs::rename(tmp,dest)
            } else {
                fs::hard_link(tmp,dest)
            };
            if let Err(error)=published {
                for (rest,_,_) in &staged {
                    let _=fs::remove_file(rest);
                }
                return Err(CatalogError::Io(error));
            }
            result=Some(dest.clone());
        }
        for (tmp,_,_) in &staged {
            let _=fs::remove_file(tmp);
        }
        File::open(directory)?.sync_all()?;
        result.ok_or_else(||CatalogError::Invalid("no published Bar".into()))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    #[test]
    fn replaces_sparse_bars_and_removes_deleted_trade_second(){
        let path=std::env::temp_dir().join(format!(
            "sa-history-replace-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                .unwrap().as_nanos(),
        ));
        fs::create_dir(&path).unwrap();
        let catalog=Catalog::open_existing(&path).unwrap();
        let id=crate::bar_type("ETHUSDT.BINANCE","1s").unwrap();
        let bar=|time|CatalogBar{
            open_time_ms:time,open:Decimal::from(100),high:Decimal::from(105),
            low:Decimal::from(95),close:Decimal::from(100),volume:Decimal::ONE,
        };
        let rows=[bar(0),bar(1_000),bar(3_000)];
        catalog.replace_bars_window(&id,"1s",(0,60_000),&rows,2,2).unwrap();
        let read=||catalog.read_bars(&id,"1s",0,60_000_000_000,100).unwrap();
        assert_eq!(read(),rows);
        let revised=[bar(0),bar(3_000)];
        catalog.replace_bars_window(&id,"1s",(0,60_000),&revised,2,2).unwrap();
        assert_eq!(read(),revised);
        // Crash after recording the durable replacement marker: readers must
        // reject partial results until the same UTC window is replayed.
        let marker=path.join("data/bars").join(&id).join(".rebuilding");
        fs::write(&marker,"0,60000").unwrap();
        assert!(catalog.read_bars(&id,"1s",0,60_000_000_000,100).is_err());
        catalog.replace_bars_window(&id,"1s",(0,60_000),&revised,2,2).unwrap();
        assert_eq!(read(),revised);
        catalog.replace_bars_window(&id,"1s",(0,60_000),&[],2,2).unwrap();
        assert!(read().is_empty());
        let other=crate::bar_type("ETHUSDT-PERP.BINANCE","1s").unwrap();
        catalog.replace_bars_window(&other,"1s",(0,60_000),&[bar(0)],2,2).unwrap();
        assert_eq!(catalog.read_bars(&other,"1s",0,60_000_000_000,100).unwrap().len(),1);
        let _=fs::remove_dir_all(path);
    }

    #[test]
    fn filename_matches_python_nautilus_v2_exact_nanoseconds() {
        assert_eq!(
            file_name(1795132859999000000, 1795132919999000000).unwrap(),
            "2026-11-20T00-00-59-999000000Z_2026-11-20T00-01-59-999000000Z.parquet"
        );
    }
    #[test]
    fn on_disk_parquet_roundtrip_supports_upsert_and_product_isolation() {
        let path = std::env::temp_dir().join(format!(
            "sa-catalog-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&path).unwrap();
        let catalog = Catalog::open_existing(&path).unwrap();
        let bar = |open_time_ms| CatalogBar {
            open_time_ms,
            open: Decimal::from(100),
            high: Decimal::from(110),
            low: Decimal::from(90),
            close: Decimal::from(105),
            volume: Decimal::ONE,
        };
        let bars = [bar(0), bar(60_000)];
        catalog
            .append_bars("ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL", "1m", &bars, 2, 3)
            .unwrap();
        assert_eq!(
            catalog
                .read_contiguous_bars("ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL", "1m", 0, 120_000, 100)
                .unwrap(),
            bars
        );
        assert!(
            catalog
                .bar_gaps("ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL", "1m", 0, 120_000, 100)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            catalog
                .bar_gaps("ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL", "1m", 0, 180_000, 100)
                .unwrap(),
            vec![crate::BarGap {
                start_ms: 120_000,
                end_ms: 180_000
            }]
        );
        // Exact repeated publication must be safe and idempotent.
        assert!(
            catalog
                .append_bars("ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL", "1m", &bars, 2, 3)
                .is_ok()
        );
        let mut too_precise = bars.clone();
        too_precise[0].close = Decimal::new(105001, 3);
        assert!(
            catalog
                .append_bars("ETHUSDT.BINANCE-1-MINUTE-LAST-EXTERNAL", "1m", &too_precise, 2, 3)
                .is_err()
        );
        // Existing OHLCV is mutable. Updating one incomplete candle does
        // not erase the adjacent historical candle from the same Parquet file.
        let mut revised = bars[1].clone();
        revised.close = Decimal::from(106);
        revised.high = Decimal::from(111);
        revised.volume = Decimal::from(2);
        catalog.append_bars(
            "ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL","1m",
            &[revised.clone()],2,3,
        ).unwrap();
        assert_eq!(
            catalog.read_contiguous_bars(
                "ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL","1m",
                0,120_000,100,
            ).unwrap(),
            vec![bars[0].clone(),revised.clone()],
        );
        // The first partial snapshot of the next minute creates a new
        // Parquet file; later Tick revisions must replace only that minute.
        let mut next = bar(120_000);
        catalog.append_bars(
            "ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL", "1m",
            &[next.clone()], 2, 3,
        ).unwrap();
        next.close=Decimal::from(108);
        next.volume=Decimal::from(3);
        catalog.append_bars(
            "ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL", "1m",
            &[next.clone()], 2, 3,
        ).unwrap();
        assert_eq!(catalog.read_contiguous_bars(
            "ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL","1m",
            0,180_000,100,
        ).unwrap(),vec![bars[0].clone(),revised,next]);
        assert!(
            catalog
                .read_bars(
                    "ETHUSDT.BINANCE-1-MINUTE-LAST-EXTERNAL",
                    "1m",
                    0,
                    120_000_000_000,
                    100
                )
                .unwrap()
                .is_empty()
        );
        let _ = fs::remove_dir_all(path);
    }
}
