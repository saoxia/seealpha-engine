//! Atomic, append-only Nautilus v2 Bar Parquet publisher.
//! The Python Pipeline owns historical repair decisions; this writer never
//! edits an existing Parquet object or imports uncertified partial sessions.
use std::{collections::HashMap,fs::{self,File,OpenOptions},path::{Path,PathBuf},sync::Arc};
use arrow_array::{ArrayRef,RecordBatch,Decimal128Array};
use arrow_array::builder::TimestampNanosecondBuilder;
use arrow_schema::{DataType,Field,Schema,TimeUnit};
use chrono::{DateTime,Utc};
use fs2::FileExt;
use parquet::{arrow::ArrowWriter,basic::Compression,file::properties::WriterProperties};
use rust_decimal::Decimal;
use crate::{Catalog,CatalogError,model::{CatalogBar,MarketProduct,bar_type},reader::{RecordKind,candidate_files,inspect_existing_directory}};

const MAX_WRITE:usize=50_000;
fn decimal(v:Decimal)->Result<i128,CatalogError>{
    let mut canonical=v;
    canonical.rescale(16);
    if canonical!=v {
        return Err(CatalogError::Invalid("K 线 Decimal 超过原生 Parquet 16 位小数精度。".into()));
    }
    Ok(canonical.mantissa())
}
fn array(values:Vec<i128>)->Result<ArrayRef,CatalogError>{
    Ok(Arc::new(Decimal128Array::from(values)
        .with_precision_and_scale(38,16)
        .map_err(|e|CatalogError::Arrow(e.to_string()))?))
}
fn format_ts(ns:i64)->Result<String,CatalogError>{
    let sec=ns.div_euclid(1_000_000_000);
    let nanos=ns.rem_euclid(1_000_000_000) as u32;
    let time=DateTime::<Utc>::from_timestamp(sec,nanos)
        .ok_or_else(||CatalogError::Invalid("Catalog 文件时间超出 UTC 范围。".into()))?;
    Ok(format!("{}-{:09}Z",time.format("%Y-%m-%dT%H-%M-%S"),nanos))
}
pub(crate) fn file_name(first_ns:i64,last_ns:i64)->Result<String,CatalogError>{
    if first_ns>last_ns {return Err(CatalogError::Invalid("Catalog 文件时间倒退。".into()));}
    Ok(format!("{}_{}.parquet",format_ts(first_ns)?,format_ts(last_ns)?))
}
fn safe_mkdir(root:&Path,rel:&Path)->Result<PathBuf,CatalogError>{
    let mut path=root.to_path_buf();
    for component in rel.components(){
        path.push(component);
        if !inspect_existing_directory(&path)? {
            fs::create_dir(&path)?;
            inspect_existing_directory(&path)?;
        }
    }
    Ok(path)
}
fn schema(bar_type:&str,instrument_id:&str,price_precision:u32,size_precision:u32)->Arc<Schema>{
    let columns=["open","high","low","close","volume"].into_iter()
        .map(|name|Field::new(name,DataType::Decimal128(38,16),false))
        .chain(["ts_event","ts_init"].into_iter().map(|name|
            Field::new(name,DataType::Timestamp(TimeUnit::Nanosecond,Some("UTC".into())),false)
        )).collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(columns,HashMap::from([
        ("type_name".to_string(),"Bar".to_string()),
        ("bar_type".to_string(),bar_type.to_owned()),
        ("instrument_id".to_string(),instrument_id.to_owned()),
        ("price_precision".to_string(),price_precision.to_string()),
        ("size_precision".to_string(),size_precision.to_string()),
    ])))
}
impl Catalog{
    /// Publish one already validated, contiguous Bar batch. A concurrent
    /// writer must use the same product lock. A filename collision or
    /// overlapping existing file aborts without changing the old catalog.
    pub fn append_bars(
        &self,symbol:&str,product:MarketProduct,interval:&str,
        bars:&[CatalogBar],price_precision:u32,size_precision:u32,
    )->Result<PathBuf,CatalogError>{
        if bars.is_empty()||bars.len()>MAX_WRITE||price_precision>16||size_precision>16 {
            return Err(CatalogError::Invalid("单次写入数据量或精度超出范围。".into()));
        }
        let kind=bar_type(symbol,product,interval)?;
        let instrument=crate::model::instrument_id(symbol,product)?;
        let dir_rel=RecordKind::Bars.directory(&kind)?;
        let directory=safe_mkdir(&self.root,&dir_rel)?;
        // The lock lives with this one native BarType writer. Readers do
        // not acquire this lock because files become visible atomically.
        let lock_path=directory.join(".writer.lock");
        let lease=OpenOptions::new().create(true).read(true).write(true).open(&lock_path)?;
        lease.lock_exclusive()?;
        let mut expected=bars[0].open_time_ms;
        let mut events=Vec::with_capacity(bars.len());
        for bar in bars {
            bar.validate(interval,product)?;
            if bar.open_time_ms!=expected {
                return Err(CatalogError::Invalid("要写入的 K 线缺失、重复或乱序。".into()));
            }
            events.push(bar.event_ns(interval)?);
            expected=crate::model::bar_window(interval,expected)?.1;
        }
        let first=*events.first().ok_or_else(||CatalogError::Invalid("缺少历史数据。".into()))?;
        let last=*events.last().ok_or_else(||CatalogError::Invalid("缺少历史数据。".into()))?;
        // Include all files that overlap even one millisecond of the new batch.
        let overlapping=candidate_files(
            &self.root,&dir_rel,first,last.saturating_add(1)
        )?;
        if !overlapping.is_empty(){
            // The publisher may crash after the atomic link but before its
            // SQLite publication watermark commits. An *exact* immutable
            // recheck is a safe idempotent retry, not a new Parquet overwrite.
            if overlapping.len()==1 {
                let existing_file=File::open(&overlapping[0])?;
                let reader=parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(existing_file)
                    .map_err(|e|CatalogError::Parquet(e.to_string()))?;
                let metadata=reader.schema().metadata();
                if metadata.get("price_precision").map(String::as_str)
                    ==Some(price_precision.to_string().as_str()) &&
                    metadata.get("size_precision").map(String::as_str)
                    ==Some(size_precision.to_string().as_str()) {
                    let expected_end=crate::model::bar_window(
                        interval,bars.last().ok_or_else(||CatalogError::Invalid("空批次。".into()))?
                            .open_time_ms
                    )?.1;
                    let previous=self.read_contiguous_bars(
                        symbol,product,interval,bars[0].open_time_ms,
                        expected_end,bars.len()
                    );
                    if previous.as_ref().is_ok_and(|rows|rows.as_slice()==bars) {
                        return Ok(overlapping[0].clone());
                    }
                }
            }
            return Err(CatalogError::Conflict(
                "已有 Parquet 文件与新历史时间范围重叠或数据/精度不一致；拒绝覆盖。".into()));
        }
        let name=file_name(first,last)?;
        let destination=directory.join(&name);
        let staging=safe_mkdir(&self.root,Path::new(".staging"))?;
        let nanos=std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_|CatalogError::Invalid("当前时钟无效。".into()))?.as_nanos();
        let tmp=staging.join(format!("bar-{}-{nanos}.parquet.tmp",std::process::id()));
        let write=(||->Result<(),CatalogError>{
            let mut columns=Vec::with_capacity(7);
            for column in 0..5 {
                let data=bars.iter().map(|bar|decimal(match column{
                    0=>bar.open,1=>bar.high,2=>bar.low,
                    3=>bar.close,_=>bar.volume,
                })).collect::<Result<Vec<_>,_>>()?;
                columns.push(array(data)?);
            }
            let mut events_builder=TimestampNanosecondBuilder::with_capacity(events.len())
                .with_timezone("UTC");
            let mut init_builder=TimestampNanosecondBuilder::with_capacity(events.len())
                .with_timezone("UTC");
            for stamp in &events{
                events_builder.append_value(*stamp);
                init_builder.append_value(*stamp);
            }
            columns.push(Arc::new(events_builder.finish()));
            columns.push(Arc::new(init_builder.finish()));
            let data=RecordBatch::try_new(
                schema(&kind,&instrument,price_precision,size_precision),columns
            ).map_err(|e|CatalogError::Arrow(e.to_string()))?;
            let file=OpenOptions::new().create_new(true).write(true).open(&tmp)?;
            let opts=WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_max_row_group_row_count(Some(5_000))
                .build();
            let mut writer=ArrowWriter::try_new(file,data.schema(),Some(opts))
                .map_err(|e|CatalogError::Parquet(e.to_string()))?;
            writer.write(&data).map_err(|e|CatalogError::Parquet(e.to_string()))?;
            writer.close().map_err(|e|CatalogError::Parquet(e.to_string()))?;
            File::open(&tmp)?.sync_all()?;
            // Hard-link is create-if-absent: unlike rename() on Unix it
            // NEVER replaces a historical artifact or races an old writer.
            fs::hard_link(&tmp,&destination)?;
            let _=File::open(&directory).and_then(|d|d.sync_all());
            Ok(())
        })();
        let _=fs::remove_file(&tmp);
        write?;
        drop(lease);
        Ok(destination)
    }
}
#[cfg(test)]
mod tests{
    use super::*;
    use crate::model::MarketProduct;
    use rust_decimal::Decimal;
    #[test]
    fn filename_matches_python_nautilus_v2_exact_nanoseconds(){
        assert_eq!(file_name(1795132859999000000,1795132919999000000).unwrap(),
            "2026-11-20T00-00-59-999000000Z_2026-11-20T00-01-59-999000000Z.parquet");
    }
    #[test]
    fn on_disk_parquet_roundtrip_is_append_only_and_does_not_leak_across_products(){
        let path=std::env::temp_dir().join(format!("sa-catalog-{}-{}",
            std::process::id(),std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        fs::create_dir(&path).unwrap();
        let catalog=Catalog::open_existing(&path).unwrap();
        let bar=|open_time_ms|CatalogBar{
            open_time_ms,open:Decimal::from(100),high:Decimal::from(110),
            low:Decimal::from(90),close:Decimal::from(105),volume:Decimal::ONE,
        };
        let bars=[bar(0),bar(60_000)];
        catalog.append_bars("ETHUSDT",MarketProduct::FuturesUm,"1m",&bars,2,3).unwrap();
        assert_eq!(catalog.read_contiguous_bars("ETHUSDT",MarketProduct::FuturesUm,
            "1m",0,120_000,100).unwrap(),bars);
        // A verified identical retry is safe after a crash between a
        // Parquet hard-link and the publication watermark transaction.
        assert!(catalog.append_bars("ETHUSDT",MarketProduct::FuturesUm,"1m",&bars,2,3).is_ok());
        let mut conflicted=bars.clone();
        conflicted[1].close=Decimal::from(106);
        assert!(catalog.append_bars("ETHUSDT",MarketProduct::FuturesUm,
            "1m",&conflicted,2,3).is_err());
        assert!(catalog.read_bars("ETHUSDT",MarketProduct::Spot,
            "1m",0,120_000_000_000,100).unwrap().is_empty());
        let _=fs::remove_dir_all(path);
    }
}
