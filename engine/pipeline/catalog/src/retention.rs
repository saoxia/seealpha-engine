//! Conservative cutoff enforcement for immutable native Bar parquet files.
use crate::{Catalog,CatalogError,RecordKind,NANO_PER_MS};
use crate::reader::{candidate_files,filename_bounds,inspect_existing_directory};
use fs2::FileExt;
use std::fs::{self,OpenOptions};

impl Catalog {
    /// Remove only files strictly before the requested UTC-day cutoff.
    /// Fail *before deleting anything* when an older file straddles the cutoff.
    /// All writers must honor this BarType's .writer.lock.
    pub fn prune_bars_before(&self,bar_type:&str,cutoff_ms:i64)->Result<usize,CatalogError>{
        if cutoff_ms<0 || cutoff_ms%crate::DAY_MS!=0 {
            return Err(CatalogError::Invalid("retention cutoff must be UTC midnight".into()));
        }
        crate::parse_bar_type(bar_type)?;
        let dir=self.root.join(RecordKind::Bars.directory(bar_type)?);
        if !inspect_existing_directory(&dir)? {return Ok(0)}
        let lock=OpenOptions::new().create(true).read(true).write(true).open(dir.join(".writer.lock"))?;
        lock.lock_exclusive()?;
        let cutoff=cutoff_ms.checked_mul(NANO_PER_MS)
            .ok_or_else(||CatalogError::Invalid("retention cutoff overflow".into()))?;
        let candidates=candidate_files(self.root(),&RecordKind::Bars.directory(bar_type)?,0,i64::MAX)?;
        let mut delete=Vec::new();
        for path in candidates {
            let name=path.file_name().and_then(|n|n.to_str())
                .ok_or_else(||CatalogError::Invalid("invalid parquet filename".into()))?;
            let (first,last)=filename_bounds(name)?;
            if first<cutoff && last>=cutoff {
                return Err(CatalogError::Conflict(
                    "Parquet contains bars on both sides of retention boundary; refusing destructive trim".into()));
            }
            if last<cutoff {delete.push(path);}
        }
        for path in &delete {
            // refuse symlinks and non-files even if a hostile writer replaced an entry.
            let meta=fs::symlink_metadata(path)?;
            if !meta.is_file() || meta.file_type().is_symlink(){
                return Err(CatalogError::Invalid("unsafe retention entry".into()));
            }
            fs::remove_file(path)?;
        }
        if !delete.is_empty(){fs::File::open(&dir)?.sync_all()?;}
        Ok(delete.len())
    }
}
#[cfg(test)]
mod tests{
    use super::*;
    use crate::{CatalogBar,bar_type};
    use rust_decimal::Decimal;
    #[test]
    fn removes_complete_old_day_without_touching_current_day(){
        let dir=std::env::temp_dir().join(format!("pipeline-retention-{}-{:?}",std::process::id(),std::thread::current().id()));
        std::fs::create_dir_all(&dir).unwrap();
        let c=Catalog::open_existing(&dir).unwrap();
        let id=bar_type("ETHUSDT.BINANCE","1d").unwrap();
        let bar=|start|CatalogBar{open_time_ms:start,open:Decimal::ONE,high:Decimal::ONE,
          low:Decimal::ONE,close:Decimal::ONE,volume:Decimal::ONE};
        c.append_bars(&id,"1d",&[bar(1_735_689_600_000)],0,0).unwrap();
        c.append_bars(&id,"1d",&[bar(1_735_776_000_000)],0,0).unwrap();
        assert_eq!(c.prune_bars_before(&id,1_735_776_000_000).unwrap(),1);
        assert_eq!(c.prune_bars_before(&id,1_735_776_000_000).unwrap(),0);
        assert_eq!(c.read_contiguous_bars(&id,"1d",1_735_776_000_000,1_735_862_400_000,1).unwrap().len(),1);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
