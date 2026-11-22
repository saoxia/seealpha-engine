//! Immutable exchange funding-rate observations stored in the market Catalog,
 //! not in the tenant's account Ledger.
use crate::{Catalog, CatalogError, model::NANO_PER_MS, reader::{RecordKind, candidate_files}, writer::{file_name, safe_mkdir}};
use arrow_array::{ArrayRef, Decimal128Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_schema::{Schema, Field, DataType, TimeUnit};
use fs2::FileExt;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use rust_decimal::Decimal;
use std::{collections::HashMap, fs::{self, File, OpenOptions}, path::PathBuf, sync::Arc};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogFunding {
    pub funding_time_ms: i64,
    pub rate_type: String,
    pub funding_rate: Decimal,
    pub mark_price: Decimal,
}
fn decimal_array(values: impl Iterator<Item = Decimal>) -> Result<ArrayRef, CatalogError> {
    let mut mantissas = Vec::new();
    for value in values {
        let mut canonical = value;
        canonical.rescale(16);
        if canonical != value || canonical.scale() != 16 {
            return Err(CatalogError::Invalid("资金费数据超出 Decimal128 精度。".into()));
        }
        mantissas.push(canonical.mantissa());
    }
    Ok(Arc::new(Decimal128Array::from(mantissas)
        .with_precision_and_scale(38, 16).map_err(|e| CatalogError::Arrow(e.to_string()))?))
}
impl Catalog {
    /// Write a verified public exchange observation batch. Neither an estimated
    /// rate nor this parquet artifact proves an account funding payment.
    pub fn append_funding(&self, instrument_id: &str, rows: &[CatalogFunding]) -> Result<Option<PathBuf>, CatalogError> {
        if rows.len() > 5000 { return Err(CatalogError::Limit("资金费批次超出上限。".into())); }
        if rows.is_empty() { return Ok(None); }
        crate::reader::safe_component(instrument_id)?;
        let identity = instrument_id.to_owned();
        let relative = RecordKind::FundingRate.directory(&identity)?;
        let dir = safe_mkdir(&self.root, &relative)?;
        let lease = OpenOptions::new().create(true).read(true).write(true).truncate(false)
            .open(dir.join(".writer.lock"))?;
        lease.lock_exclusive()?;
        let mut previous = None;
        for row in rows {
            if row.funding_time_ms < 0 || row.mark_price <= Decimal::ZERO
                || row.rate_type.is_empty() || row.rate_type.len() > 32
                || previous.is_some_and(|last| row.funding_time_ms <= last) {
                return Err(CatalogError::Invalid("资金费时间或数值无效、重复或倒退。".into()));
            }
            previous = Some(row.funding_time_ms);
        }
        let first = rows[0].funding_time_ms.checked_mul(NANO_PER_MS)
            .ok_or_else(|| CatalogError::Invalid("资金费时间溢出。".into()))?;
        let last = rows[rows.len()-1].funding_time_ms.checked_mul(NANO_PER_MS)
            .ok_or_else(|| CatalogError::Invalid("资金费时间溢出。".into()))?;
        let overlaps = candidate_files(&self.root, &relative, first, last.saturating_add(1))?;
        if !overlaps.is_empty() {
            // Exact replay is safe. Different values, partial intersections or
            // competing publishers must never overwrite immutable evidence.
            let existing = self.read_records(&RecordKind::FundingRate, &identity,
                first, last.saturating_add(1), 5000)?;
            let identical = existing.len() == rows.len() && existing.iter().zip(rows).all(|(value,row)| {
                value["funding_rate"].as_str() == Some(row.funding_rate.to_string().as_str())
                    && value["mark_price"].as_str() == Some(row.mark_price.to_string().as_str())
                    && value["rate_type"].as_str() == Some(row.rate_type.as_str())
                    && value["ts_event"].as_str() == Some((row.funding_time_ms * NANO_PER_MS).to_string().as_str())
            });
            if identical && overlaps.len() == 1 { return Ok(Some(overlaps[0].clone())); }
            return Err(CatalogError::Conflict("资金费 Parquet 已存在重叠时间范围。".into()));
        }
        let times = rows.iter().map(|r| r.funding_time_ms * NANO_PER_MS).collect::<Vec<_>>();
        let schema = Arc::new(Schema::new_with_metadata(vec![
            Field::new("funding_rate", DataType::Decimal128(38,16), false),
            Field::new("mark_price", DataType::Decimal128(38,16), false),
            Field::new("rate_type", DataType::Utf8, false),
            Field::new("ts_event", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
            Field::new("ts_init", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
        ], HashMap::from([
            ("type_name".to_string(), "FundingRateUpdate".to_string()),
            ("instrument_id".to_string(), identity.clone()),
            ("source".to_string(), "exchange_public_funding_history".to_string()),
        ])));
        let batch = RecordBatch::try_new(schema.clone(), vec![
            decimal_array(rows.iter().map(|r| r.funding_rate))?,
            decimal_array(rows.iter().map(|r| r.mark_price))?,
            Arc::new(StringArray::from(rows.iter().map(|r| r.rate_type.as_str()).collect::<Vec<_>>())),
            Arc::new(TimestampNanosecondArray::from(times.clone()).with_timezone("UTC")),
            Arc::new(TimestampNanosecondArray::from(times).with_timezone("UTC")),
        ]).map_err(|e| CatalogError::Arrow(e.to_string()))?;
        let destination = dir.join(file_name(first, last)?);
        let staging = safe_mkdir(&self.root, std::path::Path::new(".staging"))?;
        let stamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| CatalogError::Invalid("时钟不可用。".into()))?.as_nanos();
        let tmp = staging.join(format!("funding-{}-{stamp}.parquet.tmp", std::process::id()));
        let result = (|| -> Result<(), CatalogError> {
            let file = OpenOptions::new().create_new(true).write(true).open(&tmp)?;
            let props = WriterProperties::builder().set_compression(Compression::SNAPPY).build();
            let mut writer = ArrowWriter::try_new(file, schema, Some(props))
                .map_err(|e| CatalogError::Parquet(e.to_string()))?;
            writer.write(&batch).map_err(|e| CatalogError::Parquet(e.to_string()))?;
            writer.close().map_err(|e| CatalogError::Parquet(e.to_string()))?;
            File::open(&tmp)?.sync_all()?;
            fs::hard_link(&tmp, &destination)?;
            File::open(&dir)?.sync_all()?;
            Ok(())
        })();
        let _ = fs::remove_file(&tmp);
        result?;
        drop(lease);
        Ok(Some(destination))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn public_rates_are_catalog_only_and_immutable() {
        let root = std::env::temp_dir().join(format!(
            "seealpha-catalog-funding-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        let catalog = Catalog::open_existing(&root).unwrap();
        let rows = vec![
            CatalogFunding { funding_time_ms: 100, rate_type: "Regular".into(),
                funding_rate: Decimal::new(1,4), mark_price: Decimal::new(2000,0) },
            CatalogFunding { funding_time_ms: 200, rate_type: "Special".into(),
                funding_rate: Decimal::new(-2,4), mark_price: Decimal::new(2100,0) },
        ];
        assert!(catalog.append_funding("ETHUSDT-PERP.BINANCE", &rows).unwrap().is_some());
        assert!(catalog.append_funding("ETHUSDT", &rows).is_ok());
        let identity = "ETHUSDT-PERP.BINANCE";
        let records = catalog.read_records(&RecordKind::FundingRate, &identity,
            0, 300 * NANO_PER_MS, 10).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1]["rate_type"], "Special");
        let mut conflict = rows.clone();
        conflict[0].funding_rate = Decimal::new(3,4);
        assert!(catalog.append_funding("ETHUSDT-PERP.BINANCE", &conflict).is_err());
        assert!(catalog.append_funding("BTCUSDT-PERP.BINANCE", &rows).is_ok());
        let _ = fs::remove_dir_all(root);
    }
}
