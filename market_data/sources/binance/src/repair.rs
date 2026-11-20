//! Offline repair of durable raw Trade IDs from a checksummed Binance USD-M
//! daily trades ZIP. Run ONLY while the live writer is stopped. This does not
//! certify the websocket handover, tail completeness or Catalog readiness.
use std::{fs::{self,File}, path::Path, time::{SystemTime,UNIX_EPOCH}};
use serde_json::{Value,json};
use zip::ZipArchive;
use crate::{history::{ArchiveDay,verify_local_archive},store::PairJournal};

type Result<T> = std::result::Result<T,Box<dyn std::error::Error+Send+Sync>>;

fn in_ranges(id:i64,ranges:&[(i64,i64)]) -> bool {
    ranges.iter().any(|(first,last)|*first<=id && id<=*last)
}
fn parse_record(record:&csv::StringRecord,day:&ArchiveDay) -> Result<(i64,Value)> {
    if record.len()!=6 {return Err("invalid Binance USD-M trades CSV field count".into());}
    let id=record[0].parse::<i64>()?;
    let time=record[4].parse::<i64>()?;
    let day_start=day.day.and_hms_opt(0,0,0).ok_or("invalid day")?
        .and_utc().timestamp_millis();
    let day_end=day_start.checked_add(86_400_000).ok_or("day overflow")?;
    if id<0 || time<day_start || time>=day_end {
        return Err("archive row outside declared UTC day or invalid trade ID".into());
    }
    let maker=match record[5].trim().to_ascii_lowercase().as_str(){
        "true"=>true,"false"=>false,_=>return Err("invalid isBuyerMaker".into())
    };
    // Verify all decimal fields, not just the ones needed by the journal.
    let price=record[1].parse::<rust_decimal::Decimal>()?;
    let size=record[2].parse::<rust_decimal::Decimal>()?;
    let quote=record[3].parse::<rust_decimal::Decimal>()?;
    if price<=rust_decimal::Decimal::ZERO || size<=rust_decimal::Decimal::ZERO ||
       quote<rust_decimal::Decimal::ZERO {
        return Err("invalid trade price/quantity/quote quantity".into());
    }
    Ok((id,json!({"id":id,"time":time,"price":record[1],"qty":record[2],"isBuyerMaker":maker})))
}

/// Fully verify the downloaded source before taking any journal write action.
/// A checksum mismatch must not partially heal gaps.
pub fn repair_day(journal:&mut PairJournal,day:&ArchiveDay,archive_root:&Path)
    -> Result<RepairReceipt>
{
    if journal.symbol()!=day.symbol {return Err("archive/journal symbol mismatch".into());}
    let target=day.local_path(archive_root);
    verify_local_archive(day,archive_root)?;
    let expected_name=format!("{}-trades-{}.csv",day.symbol,day.day);
    let archive=File::open(&target)?;
    let mut zip=ZipArchive::new(archive)?;
    if zip.len()!=1 {return Err("archive must contain exactly one member".into());}
    let entry=zip.by_index(0)?;
    let name=entry.name().map_err(|_|"invalid ZIP member name")?;
    if name!=expected_name {return Err("unexpected ZIP CSV name".into());}
    let mut reader=csv::ReaderBuilder::new().has_headers(false).flexible(true).from_reader(entry);
    let mut rows=Vec::<Value>::with_capacity(2048);
    let mut scanned=0u64;
    let mut restored=0usize;
    let mut previous=None::<i64>;
    let gaps=journal.missing_trade_ids()?;
    if gaps.is_empty() {return Ok(RepairReceipt{scanned:0,restored:0,remaining:0});}
    for item in reader.records() {
        let record=item?;
        if scanned==0 && record.iter().all(|s|s.trim().is_empty()) {continue;}
        if scanned==0 && record.get(0).is_some_and(|s|
            matches!(s.trim().to_ascii_lowercase().as_str(),"id"|"trade id"|"trade_id")) {
            continue;
        }
        let (id,row)=parse_record(&record,day)?;
        if previous.is_some_and(|value|id<value) {
            return Err("archive Trade IDs are not ordered".into());
        }
        previous=Some(id);
        scanned=scanned.checked_add(1).ok_or("row count overflow")?;
        if in_ranges(id,&gaps) {
            rows.push(row);
            if rows.len()>=2048 {
                let now=i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
                restored+=journal.ingest_page(&rows,now)?;
                rows.clear();
            }
        }
    }
    if !rows.is_empty() {
        let now=i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        restored+=journal.ingest_page(&rows,now)?;
    }
    let remaining=journal.unresolved_gap_count()?;
    Ok(RepairReceipt{scanned,restored,remaining})
}
#[derive(Debug)]
pub struct RepairReceipt{
    pub scanned:u64,
    pub restored:usize,
    pub remaining:u64,
}
