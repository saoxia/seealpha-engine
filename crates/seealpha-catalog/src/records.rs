//! Schema-preserving, append-only publisher for existing Nautilus v2 Arrow
//! event batches. This is not an independent definition of Nautilus schemas:
//! producers must supply the authoritative Arrow schema and metadata.
//! Publication is confined to a single immutable time-range file.
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    path::PathBuf,
};
use arrow_array::{Array, Decimal128Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Schema, TimeUnit};
use fs2::FileExt;
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::Compression,
    file::properties::WriterProperties,
};
use rust_decimal::Decimal;
use serde_json::{Map, Value};
use crate::{
    Catalog, CatalogError, RecordKind,
    reader::{candidate_files, get_ts_for_column, row_value},
    writer::{file_name, safe_mkdir},
};

const MAX_ROWS: usize = 50_000;

fn type_name(kind: &RecordKind) -> Option<&'static str> {
    match kind {
        RecordKind::Trades => Some("TradeTick"),
        RecordKind::Quotes => Some("QuoteTick"),
        RecordKind::OrderBookDepth => Some("OrderBookDepth"),
        RecordKind::OptionGreeks => Some("OptionGreeks"),
        RecordKind::FundingRate => Some("FundingRateUpdate"),
        RecordKind::InstrumentStatus => Some("InstrumentStatus"),
        RecordKind::InstrumentClose => Some("InstrumentClose"),
        RecordKind::Bars | RecordKind::Instruments
        | RecordKind::Feature { .. } | RecordKind::Supplement { .. } => None,
    }
}

fn decimal_column(schema: &Schema, name: &str) -> Result<(), CatalogError> {
    let field = schema.field_with_name(name)
        .map_err(|_| CatalogError::Invalid(format!("Nautilus 缺少 {name} 字段。")))?;
    if field.data_type() != &DataType::Decimal128(38, 16) {
        return Err(CatalogError::Invalid(format!(
            "Nautilus {name} 必须使用 decimal128(38,16)。"
        )));
    }
    Ok(())
}

fn schema_check(
    kind: &RecordKind, identifier: &str, schema: &Schema,
) -> Result<(Option<u32>, Option<u32>), CatalogError> {
    if matches!(kind, RecordKind::Bars) {
        return Err(CatalogError::Invalid(
            "Bar 必须使用 append_bars 的窗口和 OHLCV 校验。".into(),
        ));
    }
    if let Some(expected) = type_name(kind) {
        if schema.metadata().get("type_name").map(String::as_str) != Some(expected) {
            return Err(CatalogError::Invalid(
                format!("Nautilus type_name 与 {expected} 不一致。"),
            ));
        }
    }
    // Custom feature identifiers denote the catalog root, not necessarily
    // the instrument to which individual custom events refer.
    if !matches!(kind, RecordKind::Feature { .. } | RecordKind::Supplement { .. }) {
        if let Some(actual) = schema.metadata().get("instrument_id") {
            if actual != identifier {
                return Err(CatalogError::Conflict(
                    "Arrow instrument_id 与目标目录不一致。".into(),
                ));
            }
        } else if !matches!(kind, RecordKind::Instruments) {
            return Err(CatalogError::Invalid("Arrow 缺少 instrument_id 元数据。".into()));
        }
    }
    for column in ["ts_event", "ts_init"] {
        let field = schema.field_with_name(column)
            .map_err(|_| CatalogError::Invalid(format!("Nautilus 缺少 {column}。")))?;
        if !matches!(field.data_type(), DataType::Timestamp(TimeUnit::Nanosecond, _)) {
            return Err(CatalogError::Invalid(
                format!("{column} 必须为纳秒时间戳，不能截断精度。"),
            ));
        }
    }
    match kind {
        RecordKind::Trades => {
            decimal_column(schema, "price")?;
            decimal_column(schema, "size")?;
            schema.field_with_name("trade_id").map_err(|_| CatalogError::Invalid(
                "TradeTick 缺少 trade_id。".into(),
            ))?;
            schema.field_with_name("aggressor_side").map_err(|_| CatalogError::Invalid(
                "TradeTick 缺少 aggressor_side。".into(),
            ))?;
        }
        RecordKind::Quotes => {
            for field in ["bid_price", "ask_price", "bid_size", "ask_size"] {
                decimal_column(schema, field)?;
            }
        }
        _ => {}
    }
    let precision = |name: &str| -> Result<Option<u32>, CatalogError> {
        match schema.metadata().get(name) {
            Some(text) => {
                let value = text.parse::<u32>().map_err(|_| CatalogError::Invalid(
                    format!("{name} 元数据不是整数。"),
                ))?;
                if value > 16 {
                    return Err(CatalogError::Invalid(
                        format!("{name} 超过 Nautilus 固定 16 位小数精度。"),
                    ));
                }
                Ok(Some(value))
            }
            None => Ok(None),
        }
    };
    let pp = precision("price_precision")?;
    let sp = precision("size_precision")?;
    if matches!(kind, RecordKind::Trades | RecordKind::Quotes)
        && (pp.is_none() || sp.is_none())
    {
        return Err(CatalogError::Invalid("Tick 缺少价格或数量精度元数据。".into()));
    }
    Ok((pp, sp))
}

fn record_value(batch: &RecordBatch, row: usize) -> Result<Value, CatalogError> {
    let mut object = Map::new();
    let schema = batch.schema();
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        let value = if matches!(field.name().as_str(), "ts_event" | "ts_init") {
            Value::String(get_ts_for_column(batch, field.name(), row)?.to_string())
        } else {
            row_value(array.as_ref(), row)?
        };
        object.insert(field.name().clone(), value);
    }
    Ok(Value::Object(object))
}

fn validated_rows(
    kind: &RecordKind, batches: &[RecordBatch],
    pp: Option<u32>, sp: Option<u32>,
) -> Result<(i64, i64, Vec<Value>), CatalogError> {
    let mut rows = Vec::new();
    let mut first = None;
    let mut last = None;
    let mut ids = HashSet::new();
    let mut unique_records = HashSet::new();
    for batch in batches {
        let batch_schema = batch.schema();
        for (field, array) in batch_schema.fields().iter().zip(batch.columns()) {
            if !field.is_nullable() && array.null_count() > 0 {
                return Err(CatalogError::Invalid(format!(
                    "Nautilus 非空字段 {} 含有 null。", field.name(),
                )));
            }
            let scale = if let DataType::Decimal128(38, scale) = field.data_type() {
                *scale
            } else {
                continue;
            };
            if scale != 16 {
                return Err(CatalogError::Invalid(
                    "Nautilus Decimal128 必须为 (38,16)。".into(),
                ));
            }
            let decimal = array.as_any().downcast_ref::<Decimal128Array>()
                .ok_or_else(|| CatalogError::Invalid("Arrow Decimal128 类型损坏。".into()))?;
            let declared = match field.name().as_str() {
                "price" | "bid_price" | "ask_price" | "open" | "high" | "low" | "close" => pp,
                "size" | "bid_size" | "ask_size" | "volume" => sp,
                _ => None,
            };
            for index in 0..batch.num_rows() {
                if decimal.is_null(index) {
                    continue;
                }
                let value = Decimal::try_from_i128_with_scale(decimal.value(index), 16)
                    .map_err(|_| CatalogError::Invalid(
                        "Decimal128 数值超出可表示范围。".into(),
                    ))?;
                if declared.is_some_and(|precision| value.round_dp(precision) != value) {
                    return Err(CatalogError::Invalid(
                        "记录数值超过声明的价格或数量精度。".into(),
                    ));
                }
            }
        }
        for row in 0..batch.num_rows() {
            let event = get_ts_for_column(batch, "ts_event", row)?;
            let init = get_ts_for_column(batch, "ts_init", row)?;
            if event < 0 || init < 0 {
                return Err(CatalogError::Invalid("历史时间戳不得为负数。".into()));
            }
            if last.is_some_and(|old| event < old) {
                return Err(CatalogError::Invalid("批次记录时间倒退。".into()));
            }
            if matches!(kind, RecordKind::Trades) {
                let trades = batch.column_by_name("trade_id")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .ok_or_else(|| CatalogError::Invalid(
                        "TradeTick trade_id 必须为 UTF-8 字符串。".into(),
                    ))?;
                if trades.is_null(row) || trades.value(row).is_empty()
                    || !ids.insert(trades.value(row).to_owned())
                {
                    return Err(CatalogError::Conflict(
                        "单次写入包含重复或空 TradeTick trade_id。".into(),
                    ));
                }
            }
            let value = record_value(batch, row)?;
            let fingerprint = serde_json::to_vec(&value).map_err(|e|
                CatalogError::Invalid(format!("记录序列化失败：{e}"))
            )?;
            if !unique_records.insert(fingerprint) {
                return Err(CatalogError::Conflict(
                    "单批次包含完全相同的重复历史记录。".into(),
                ));
            }
            first.get_or_insert(event);
            last = Some(event);
            rows.push(value);
            if rows.len() > MAX_ROWS {
                return Err(CatalogError::Limit("单次写入超过 50,000 条事件。".into()));
            }
        }
    }
    match (first, last) {
        (Some(first), Some(last)) => Ok((first, last, rows)),
        _ => Err(CatalogError::Invalid("禁止写入空事件批次。".into())),
    }
}

impl Catalog {
    /// Persist canonical native Arrow batches for TradeTick, QuoteTick,
    /// OrderBookDepth, OptionGreeks and other non-Bar Nautilus events.
    ///
    /// Schema (including precision and identity metadata) is not synthesized.
    /// The caller must source it from a pinned, compatible Nautilus producer.
    /// Overlaps are rejected, except byte-for-byte equivalent logical retries.
    /// No Python legacy writer is assumed to participate in this lock.
    pub fn append_record_batches(
        &self, kind: &RecordKind, identifier: &str, batches: &[RecordBatch],
    ) -> Result<PathBuf, CatalogError> {
        let rel = kind.directory(identifier)?;
        let first_batch = batches.first()
            .ok_or_else(|| CatalogError::Invalid("禁止写入空批次。".into()))?;
        let schema = first_batch.schema();
        let (pp, sp) = schema_check(kind, identifier, &schema)?;
        if batches.iter().any(|batch| batch.schema() != schema) {
            return Err(CatalogError::Invalid("多个 Arrow 批次的 schema 不一致。".into()));
        }
        let (first, last, values) = validated_rows(kind, batches, pp, sp)?;
        let end = last.checked_add(1)
            .ok_or_else(|| CatalogError::Invalid("纳秒时间戳溢出。".into()))?;
        let directory = safe_mkdir(&self.root, &rel)?;
        let lease = OpenOptions::new().create(true).read(true).write(true)
            .open(directory.join(".writer.lock"))?;
        lease.lock_exclusive()?;
        let overlap = candidate_files(&self.root, &rel, first, end)?;
        if overlap.len() == 1 {
            let name = overlap[0].file_name().and_then(|value| value.to_str())
                .ok_or_else(|| CatalogError::Invalid("Parquet 文件名不是 UTF-8。".into()))?;
            if name == file_name(first, last)? {
                let builder = ParquetRecordBatchReaderBuilder::try_new(
                    File::open(&overlap[0])?,
                ).map_err(|e| CatalogError::Parquet(e.to_string()))?;
                if builder.schema() == &schema {
                    let previous = self.read_records(kind, identifier, first, end, MAX_ROWS)?;
                    if previous == values {
                        return Ok(overlap[0].clone());
                    }
                }
            }
        }
        if !overlap.is_empty() {
            return Err(CatalogError::Conflict(
                "已有 Nautilus Parquet 历史与新文件重叠；不允许覆盖。".into(),
            ));
        }
        let destination = directory.join(file_name(first, last)?);
        let staging = safe_mkdir(&self.root, std::path::Path::new(".staging"))?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| CatalogError::Invalid("系统时钟无效。".into()))?
            .as_nanos();
        let temporary = staging.join(format!(
            "native-{}-{nonce}.parquet.tmp", std::process::id(),
        ));
        let write = (|| -> Result<(), CatalogError> {
            let file = OpenOptions::new().create_new(true).write(true).open(&temporary)?;
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_max_row_group_row_count(Some(5_000))
                .build();
            let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
                .map_err(|e| CatalogError::Parquet(e.to_string()))?;
            for batch in batches {
                writer.write(batch).map_err(|e| CatalogError::Parquet(e.to_string()))?;
            }
            writer.close().map_err(|e| CatalogError::Parquet(e.to_string()))?;
            File::open(&temporary)?.sync_all()?;
            match fs::hard_link(&temporary, &destination) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists =>
                    return Err(CatalogError::Conflict(
                        "目标历史文件已经存在；不允许覆盖。".into(),
                    )),
                Err(e) => return Err(CatalogError::Io(e)),
            }
            File::open(&directory)?.sync_all()?;
            Ok(())
        })();
        let _ = fs::remove_file(&temporary);
        write?;
        drop(lease);
        Ok(destination)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, sync::Arc};
    use arrow_array::{
        ArrayRef, Decimal128Array, StringArray, TimestampNanosecondArray,
    };
    use arrow_schema::{Field, Schema};
    use crate::RecordKind;

    fn fixture(
        kind: &RecordKind, ts: &[i64], ids: Option<&[&str]>,
    ) -> RecordBatch {
        let instrument = "ETHUSDT-PERP.BINANCE";
        let (title, names): (&str, Vec<&str>) = match kind {
            RecordKind::Trades => ("TradeTick", vec!["price", "size"]),
            RecordKind::Quotes => ("QuoteTick",
                vec!["bid_price", "ask_price", "bid_size", "ask_size"]),
            RecordKind::OptionGreeks => ("OptionGreeks", vec!["delta"]),
            RecordKind::OrderBookDepth => ("OrderBookDepth", vec!["sequence"]),
            _ => unreachable!(),
        };
        let mut fields = Vec::new();
        let mut arrays = Vec::<ArrayRef>::new();
        for name in names {
            if name == "sequence" {
                fields.push(Field::new(name, DataType::Int64, false));
                arrays.push(Arc::new(arrow_array::Int64Array::from(
                    ts.iter().map(|v| *v / 1_000).collect::<Vec<_>>(),
                )));
            } else if name == "delta" {
                fields.push(Field::new(name, DataType::Float64, false));
                arrays.push(Arc::new(arrow_array::Float64Array::from(
                    vec![0.42; ts.len()],
                )));
            } else {
                fields.push(Field::new(name, DataType::Decimal128(38, 16), false));
                arrays.push(Arc::new(
                    Decimal128Array::from(vec![200_000_000_000_000_000i128; ts.len()])
                        .with_precision_and_scale(38, 16).unwrap(),
                ));
            }
        }
        if let Some(ids) = ids {
            fields.push(Field::new("aggressor_side", DataType::Utf8, false));
            arrays.push(Arc::new(StringArray::from(vec!["BUY"; ts.len()])));
            fields.push(Field::new("trade_id", DataType::Utf8, false));
            arrays.push(Arc::new(StringArray::from(ids.to_vec())));
        }
        for name in ["ts_event", "ts_init"] {
            fields.push(Field::new(name, DataType::Timestamp(
                TimeUnit::Nanosecond, Some("UTC".into()),
            ), false));
            arrays.push(Arc::new(
                TimestampNanosecondArray::from(ts.to_vec()).with_timezone("UTC"),
            ));
        }
        let metadata = HashMap::from([
            ("type_name".to_string(), title.to_string()),
            ("instrument_id".to_string(), instrument.to_string()),
            ("price_precision".to_string(), "2".to_string()),
            ("size_precision".to_string(), "2".to_string()),
        ]);
        RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(fields, metadata)), arrays,
        ).unwrap()
    }
    fn catalog() -> (Catalog, PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "sa-records-{}-{}", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                .unwrap().as_nanos(),
        ));
        fs::create_dir(&path).unwrap();
        (Catalog::open_existing(&path).unwrap(), path)
    }

    #[test]
    fn native_types_roundtrip_without_timestamp_or_schema_loss() {
        let (catalog, root) = catalog();
        let instrument = "ETHUSDT-PERP.BINANCE";
        for (kind, ids) in [
            (RecordKind::Trades, Some(vec!["41", "42"])),
            (RecordKind::Quotes, None),
            (RecordKind::OptionGreeks, None),
            (RecordKind::OrderBookDepth, None),
        ] {
            let ts = [1_795_132_800_000_000_001, 1_795_132_800_000_000_002];
            let batch = fixture(&kind, &ts, ids.as_deref());
            let first = catalog.append_record_batches(&kind, instrument, &[batch.clone()])
                .unwrap();
            assert_eq!(
                catalog.append_record_batches(&kind, instrument, &[batch]).unwrap(),
                first,
            );
            let rows = catalog.read_records(&kind, instrument, ts[0], ts[1] + 1, 10)
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0]["ts_event"], ts[0].to_string());
            assert_eq!(rows[1]["ts_init"], ts[1].to_string());
            assert!(catalog.append_record_batches(
                &kind, instrument, &[fixture(&kind, &ts, ids.as_deref())],
            ).is_ok());
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_exact_duplicate_quotes_even_at_same_nanosecond() {
        let (catalog, root) = catalog();
        let ts = [1_795_132_800_000_000_001, 1_795_132_800_000_000_001];
        assert!(catalog.append_record_batches(
            &RecordKind::Quotes, "ETHUSDT-PERP.BINANCE",
            &[fixture(&RecordKind::Quotes, &ts, None)],
        ).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_trade_id_duplicates_and_overlapping_evidence() {
        let (catalog, root) = catalog();
        let kind = RecordKind::Trades;
        let instrument = "ETHUSDT-PERP.BINANCE";
        let times = [1_795_132_800_000_000_001, 1_795_132_800_000_000_002];
        assert!(catalog.append_record_batches(
            &kind, instrument, &[fixture(&kind, &times, Some(&["10", "10"]))],
        ).is_err());
        catalog.append_record_batches(
            &kind, instrument, &[fixture(&kind, &times, Some(&["10", "11"]))],
        ).unwrap();
        assert!(catalog.append_record_batches(
            &kind, instrument, &[fixture(&kind, &times, Some(&["12", "13"]))],
        ).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
