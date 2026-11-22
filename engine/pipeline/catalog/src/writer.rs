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
impl Catalog {
    /// Insert or update a validated Bar batch. If a timestamp already exists,
    /// the incoming Bar replaces it and untouched rows remain intact.
    /// The writer lock serializes other writers and guarded Catalog readers.
    pub fn append_bars(
        &self,
        bar_identity: &str,
        interval: &str,
        bars: &[CatalogBar],
        price_precision: u32,
        size_precision: u32,
    ) -> Result<PathBuf, CatalogError> {
        if bars.is_empty() || bars.len() > MAX_WRITE || price_precision > 16 || size_precision > 16
        {
            return Err(CatalogError::Invalid(
                "单次写入数据量或精度超出范围。".into(),
            ));
        }
        let (instrument, recorded_interval) = parse_bar_type(bar_identity)?;
        if recorded_interval != interval {
            return Err(CatalogError::Invalid("BarType 周期与请求不一致。".into()));
        }
        let kind = bar_identity;
        let dir_rel = RecordKind::Bars.directory(kind)?;
        let directory = safe_mkdir(&self.root, &dir_rel)?;
        // The lock lives with this one native BarType writer. Readers do
        // not acquire this lock because files become visible atomically.
        let lock_path = directory.join(".writer.lock");
        let lease = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        lease.lock_exclusive()?;
        let mut expected = bars[0].open_time_ms;
        let mut events = Vec::with_capacity(bars.len());
        for bar in bars {
            bar.validate(interval)?;
            for (value, precision) in [
                (bar.open, price_precision),
                (bar.high, price_precision),
                (bar.low, price_precision),
                (bar.close, price_precision),
                (bar.volume, size_precision),
            ] {
                if value.round_dp(precision) != value {
                    return Err(CatalogError::Invalid(
                        "K 线数值超过声明的价格或数量精度。".into(),
                    ));
                }
            }
            if bar.open_time_ms != expected {
                return Err(CatalogError::Invalid(
                    "要写入的 K 线缺失、重复或乱序。".into(),
                ));
            }
            events.push(bar.event_ns(interval)?);
            expected = crate::model::bar_window(interval, expected)?.1;
        }
        let first = *events
            .first()
            .ok_or_else(|| CatalogError::Invalid("缺少历史数据。".into()))?;
        let last = *events
            .last()
            .ok_or_else(|| CatalogError::Invalid("缺少历史数据。".into()))?;
        let overlapping = candidate_files(&self.root, &dir_rel, first, last.saturating_add(1))?;
        let (published, replaced) = if overlapping.is_empty() {
            (bars.to_vec(), Vec::<PathBuf>::new())
        } else {
            // We cannot mutate Parquet rows in place. Read every original
            // affected file, merge by UTC bucket, then publish a replacement.
            // Readers use a shared lock so they never observe duplicate rows
            // during the replacement transaction.
            let mut first_old = first;
            let mut last_old = last;
            for path in &overlapping {
                let name = path.file_name().and_then(|n| n.to_str())
                    .ok_or_else(|| CatalogError::Invalid("invalid Parquet name".into()))?;
                let (a, b) = filename_bounds(name)?;
                first_old = first_old.min(a);
                last_old = last_old.max(b);
                let metadata = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                    File::open(path)?
                ).map_err(|e| CatalogError::Parquet(e.to_string()))?;
                let meta = metadata.schema().metadata();
                if meta.get("price_precision").map(String::as_str) != Some(price_precision.to_string().as_str())
                    || meta.get("size_precision").map(String::as_str) != Some(size_precision.to_string().as_str())
                {
                    return Err(CatalogError::Conflict(
                        "existing Bar precision differs from requested Instrument precision".into(),
                    ));
                }
            }
            let existing = self.read_bars_unlocked(
                bar_identity, interval, first_old, last_old.saturating_add(1), MAX_WRITE
            )?;
            let mut merged = BTreeMap::new();
            for bar in existing {
                merged.insert(bar.open_time_ms, bar);
            }
            for bar in bars {
                merged.insert(bar.open_time_ms, bar.clone());
            }
            if merged.len() > MAX_WRITE {
                return Err(CatalogError::Limit("merged Bar batch exceeds writer bound".into()));
            }
            (merged.into_values().collect::<Vec<_>>(), overlapping)
        };
        // Preserve all adjacent old rows; do not replace a historical range
        // with only the current Tick's partial Bar.
        let mut expected = published[0].open_time_ms;
        let mut published_events = Vec::with_capacity(published.len());
        for bar in &published {
            bar.validate(interval)?;
            if bar.open_time_ms != expected {
                return Err(CatalogError::Conflict(
                    "overlapping file merge contains a gap; publish smaller intervals".into(),
                ));
            }
            published_events.push(bar.event_ns(interval)?);
            expected = crate::bar_window(interval,expected)?.1;
        }
        let destination = directory.join(file_name(
            *published_events.first().ok_or_else(|| CatalogError::Invalid("no Bar rows".into()))?,
            *published_events.last().ok_or_else(|| CatalogError::Invalid("no Bar rows".into()))?,
        )?);
        let staging = safe_mkdir(&self.root, Path::new(".staging"))?;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| CatalogError::Invalid("当前时钟无效。".into()))?
            .as_nanos();
        let tmp = staging.join(format!("bar-{}-{nanos}.parquet.tmp", std::process::id()));
        let write = (|| -> Result<(), CatalogError> {
            let mut columns = Vec::with_capacity(7);
            for column in 0..5 {
                let data = published
                    .iter()
                    .map(|bar| {
                        decimal(match column {
                            0 => bar.open,
                            1 => bar.high,
                            2 => bar.low,
                            3 => bar.close,
                            _ => bar.volume,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                columns.push(array(data)?);
            }
            let mut events_builder =
                TimestampNanosecondBuilder::with_capacity(published_events.len()).with_timezone("UTC");
            let mut init_builder =
                TimestampNanosecondBuilder::with_capacity(published_events.len()).with_timezone("UTC");
            for stamp in &published_events {
                events_builder.append_value(*stamp);
                init_builder.append_value(*stamp);
            }
            columns.push(Arc::new(events_builder.finish()));
            columns.push(Arc::new(init_builder.finish()));
            let data = RecordBatch::try_new(
                schema(kind, &instrument, price_precision, size_precision),
                columns,
            )
            .map_err(|e| CatalogError::Arrow(e.to_string()))?;
            let file = OpenOptions::new().create_new(true).write(true).open(&tmp)?;
            let opts = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_max_row_group_row_count(Some(5_000))
                .build();
            let mut writer = ArrowWriter::try_new(file, data.schema(), Some(opts))
                .map_err(|e| CatalogError::Parquet(e.to_string()))?;
            writer
                .write(&data)
                .map_err(|e| CatalogError::Parquet(e.to_string()))?;
            writer
                .close()
                .map_err(|e| CatalogError::Parquet(e.to_string()))?;
            File::open(&tmp)?.sync_all()?;
            if replaced.is_empty() {
                fs::hard_link(&tmp, &destination)?;
            } else {
                // Atomic replacement for an existing timestamp range.
                // Reader shared locks prevent observation of intermediate files.
                fs::rename(&tmp, &destination)?;
                for path in &replaced {
                    if *path != destination {
                        fs::remove_file(path)?;
                    }
                }
            }
            File::open(&directory)?.sync_all()?;
            Ok(())
        })();
        let _ = fs::remove_file(&tmp);
        write?;
        drop(lease);
        Ok(destination)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    #[test]
    fn filename_matches_python_nautilus_v2_exact_nanoseconds() {
        assert_eq!(
            file_name(1795132859999000000, 1795132919999000000).unwrap(),
            "2026-11-20T00-00-59-999000000Z_2026-11-20T00-01-59-999000000Z.parquet"
        );
    }
    #[test]
    fn on_disk_parquet_roundtrip_is_append_only_and_does_not_leak_across_products() {
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
        // A verified identical retry is safe after a crash between a
        // Parquet hard-link and the publication watermark transaction.
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
            vec![bars[0].clone(),revised],
        );
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
