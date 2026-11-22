//! Local, read-only Parquet adapter for the Python Engine's Nautilus v2
//! layout. No Python interpreter, SQLite rewrite or implicit directory creation.
use crate::{
    Catalog, CatalogError,
    model::{CatalogBar, MarketProduct, NANO_PER_MS, bar_type, bar_window, instrument_id},
};
use arrow_array::types::{Int8Type, Int16Type, Int32Type, UInt8Type, UInt16Type, UInt32Type};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Decimal128Array, DictionaryArray, FixedSizeListArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, RecordBatch, StringArray, StructArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use base64::Engine;
use chrono::NaiveDateTime;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

fn missing_bar_windows(
    opens_ms: &[i64],
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<Vec<BarGap>, CatalogError> {
    if start_ms < 0 || step_ms <= 0 || end_ms <= start_ms || (end_ms - start_ms) % step_ms != 0 {
        return Err(CatalogError::Invalid(
            "历史修复范围必须非空且按周期对齐。".into(),
        ));
    }
    let count = (end_ms - start_ms) / step_ms;
    if count > 50_000 || opens_ms.len() > 50_000 {
        return Err(CatalogError::Limit("历史修复缺口规划超出单次上限。".into()));
    }
    let mut missing = Vec::new();
    let mut expected = start_ms;
    for &open in opens_ms {
        if open < expected || open < start_ms || open >= end_ms || (open - start_ms) % step_ms != 0
        {
            return Err(CatalogError::Invalid(
                "历史 K 线时间重复、倒退或不对齐。".into(),
            ));
        }
        if open > expected {
            missing.push(BarGap {
                start_ms: expected,
                end_ms: open,
            });
        }
        expected = open
            .checked_add(step_ms)
            .ok_or_else(|| CatalogError::Invalid("历史时间溢出。".into()))?;
    }
    if expected < end_ms {
        missing.push(BarGap {
            start_ms: expected,
            end_ms,
        });
    }
    Ok(missing)
}


const MAX_FILES: usize = 10_000;
const MAX_BATCH: usize = 50_000;

/// A missing interval of *bar opens* on the requested UTC grid.
/// This is a gap report, not evidence that a trade/Tick archive is complete.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct BarGap {
    pub start_ms: i64,
    pub end_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordKind {
    Bars,
    Trades,
    Quotes,
    Instruments,
    FundingRate,
    OptionGreeks,
    OrderBookDepth,
    InstrumentStatus,
    InstrumentClose,
    Feature { identifier: String, name: String },
    Supplement { identifier: String, name: String },
}
impl RecordKind {
    pub fn from_name(name: &str) -> Result<Self, CatalogError> {
        match name {
            "bars" => Ok(Self::Bars),
            "trades" => Ok(Self::Trades),
            "quotes" => Ok(Self::Quotes),
            "instruments" => Ok(Self::Instruments),
            "funding_rate_updates" => Ok(Self::FundingRate),
            "option_greeks" => Ok(Self::OptionGreeks),
            "order_book_depths" => Ok(Self::OrderBookDepth),
            "instrument_statuses" => Ok(Self::InstrumentStatus),
            "instrument_closes" => Ok(Self::InstrumentClose),
            _ => Err(CatalogError::Invalid("未知 Nautilus 历史数据类型。".into())),
        }
    }
    pub fn directory(&self, identity: &str) -> Result<PathBuf, CatalogError> {
        safe_component(identity)?;
        Ok(match self {
            Self::Bars => PathBuf::from("data/bars").join(identity),
            Self::Trades => PathBuf::from("data/trades").join(identity),
            Self::Quotes => PathBuf::from("data/quotes").join(identity),
            Self::Instruments => PathBuf::from("data/instruments").join(identity),
            Self::FundingRate => PathBuf::from("data/funding_rate_updates").join(identity),
            Self::OptionGreeks => PathBuf::from("data/option_greeks").join(identity),
            Self::OrderBookDepth => PathBuf::from("data/order_book_depths").join(identity),
            Self::InstrumentStatus => PathBuf::from("data/instrument_statuses").join(identity),
            Self::InstrumentClose => PathBuf::from("data/instrument_closes").join(identity),
            Self::Feature { identifier, name } => {
                safe_component(identifier)?;
                safe_component(name)?;
                PathBuf::from("features")
                    .join(identifier)
                    .join("data/custom")
                    .join(name)
            }
            Self::Supplement { identifier, name } => {
                safe_component(identifier)?;
                safe_component(name)?;
                PathBuf::from("supplements")
                    .join(identifier)
                    .join("data/custom")
                    .join(name)
            }
        })
    }
}
pub(crate) fn safe_component(value: &str) -> Result<(), CatalogError> {
    if value.is_empty()
        || value.len() > 180
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(CatalogError::Invalid("Catalog 路径标识不安全。".into()));
    }
    Ok(())
}
pub(crate) fn inspect_existing_directory(path: &Path) -> Result<bool, CatalogError> {
    let info = match fs::symlink_metadata(path) {
        Ok(info) => info,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(CatalogError::Io(err)),
    };
    if info.file_type().is_symlink() || !info.is_dir() {
        return Err(CatalogError::Invalid(
            "Catalog 路径存在符号链接或非目录。".into(),
        ));
    }
    Ok(true)
}
pub(crate) fn filename_bounds(name: &str) -> Result<(i64, i64), CatalogError> {
    let stem = name
        .strip_suffix(".parquet")
        .ok_or_else(|| CatalogError::Invalid("Catalog 文件扩展名无效。".into()))?;
    let (a, b) = stem
        .split_once('_')
        .ok_or_else(|| CatalogError::Invalid("Catalog 文件时间范围缺失。".into()))?;
    let parse = |value: &str| {
        if value.len() != 30 {
            return Err(CatalogError::Invalid("Catalog 文件时间命名无效。".into()));
        }
        let at = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H-%M-%S-%fZ")
            .map_err(|_| CatalogError::Invalid("Catalog 文件时间命名无效。".into()))?;
        at.and_utc()
            .timestamp_nanos_opt()
            .ok_or_else(|| CatalogError::Invalid("Catalog 时间溢出。".into()))
    };
    let (first, last) = (parse(a)?, parse(b)?);
    if last < first {
        return Err(CatalogError::Invalid("Catalog 文件时间倒退。".into()));
    }
    Ok((first, last))
}
pub(crate) fn candidate_files(
    root: &Path,
    rel: &Path,
    start_ns: i64,
    end_ns: i64,
) -> Result<Vec<PathBuf>, CatalogError> {
    let mut dir = root.to_path_buf();
    for part in rel.components() {
        dir.push(part);
        if !inspect_existing_directory(&dir)? {
            return Ok(Vec::new());
        }
    }
    let mut files = Vec::new();
    let mut visited = 0usize;
    for entry in fs::read_dir(dir)? {
        visited += 1;
        if visited > MAX_FILES {
            return Err(CatalogError::Limit(
                "Catalog 目录包含过多文件，拒绝无界扫描。".into(),
            ));
        }
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_symlink() || !ty.is_file() {
            return Err(CatalogError::Invalid(
                "Catalog 子目录包含不可信条目。".into(),
            ));
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".metadata.json") || name == ".writer.lock" {
            continue;
        }
        if !name.ends_with(".parquet") {
            return Err(CatalogError::Invalid(format!("未知 Catalog 文件：{name}")));
        }
        let (first, last) = filename_bounds(&name)?;
        if last >= start_ns && first < end_ns {
            files.push(entry.path());
        }
        if files.len() > MAX_FILES {
            return Err(CatalogError::Invalid(
                "单次 Catalog 文件扫描超过安全限制。".into(),
            ));
        }
    }
    files.sort();
    Ok(files)
}
pub(crate) fn get_ts_for_column(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<i64, CatalogError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| CatalogError::Invalid("Parquet 记录缺少时间字段。".into()))?;
    let arr = col
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| CatalogError::Invalid("时间字段不是纳秒 Arrow 类型。".into()))?;
    if arr.is_null(row) {
        return Err(CatalogError::Invalid("时间戳为空。".into()));
    }
    Ok(arr.value(row))
}
fn get_ts(batch: &RecordBatch, row: usize) -> Result<i64, CatalogError> {
    let col = batch
        .column_by_name("ts_event")
        .ok_or_else(|| CatalogError::Invalid("Parquet 记录缺少 ts_event。".into()))?;
    let arr = col
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| CatalogError::Invalid("ts_event 不是 Nautilus 纳秒时间。".into()))?;
    if arr.is_null(row) {
        return Err(CatalogError::Invalid("ts_event 为空。".into()));
    }
    Ok(arr.value(row))
}
fn get_decimal(batch: &RecordBatch, name: &str, row: usize) -> Result<Decimal, CatalogError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| CatalogError::Invalid(format!("Parquet 记录缺少 {name}。")))?;
    let arr = col
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| CatalogError::Invalid(format!("{name} 不是 Decimal128。")))?;
    if arr.is_null(row) {
        return Err(CatalogError::Invalid("Decimal128 为空。".into()));
    }
    let scale = match col.data_type() {
        DataType::Decimal128(_, scale) => u32::try_from(*scale)
            .map_err(|_| CatalogError::Invalid("Decimal128 负精度不支持。".into()))?,
        _ => return Err(CatalogError::Invalid("数值精度元数据缺失。".into())),
    };
    Decimal::try_from_i128_with_scale(arr.value(row), scale)
        .map_err(|_| CatalogError::Invalid("Decimal128 精度超出 Rust Decimal 范围。".into()))
}
pub(crate) fn row_value(array: &dyn Array, i: usize) -> Result<Value, CatalogError> {
    nested_row_value(array, i, 0)
}
fn nested_row_value(array: &dyn Array, i: usize, depth: usize) -> Result<Value, CatalogError> {
    if depth > 6 {
        return Err(CatalogError::Limit("Nautilus Arrow 嵌套层级过深。".into()));
    }
    if array.is_null(i) {
        return Ok(Value::Null);
    }
    macro_rules! primitive {
        ($ty:ty) => {
            if let Some(a) = array.as_any().downcast_ref::<$ty>() {
                return Ok(json!(a.value(i)));
            }
        };
    }
    primitive!(TimestampNanosecondArray);
    primitive!(StringArray);
    primitive!(LargeStringArray);
    primitive!(Int8Array);
    primitive!(UInt8Array);
    primitive!(Int16Array);
    primitive!(UInt16Array);
    for value in [
        array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| a.value(i) as f64),
        array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(i)),
    ]
    .into_iter()
    .flatten()
    {
        if !value.is_finite() {
            return Err(CatalogError::Invalid("Arrow 浮点值为 NaN 或 Inf。".into()));
        }
    }
    primitive!(Float32Array);
    primitive!(Int64Array);
    primitive!(UInt64Array);
    primitive!(Int32Array);
    primitive!(UInt32Array);
    primitive!(Float64Array);
    primitive!(BooleanArray);
    if let Some(binary) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(json!(
            base64::engine::general_purpose::STANDARD.encode(binary.value(i))
        ));
    }
    if let Some(binary) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(json!(
            base64::engine::general_purpose::STANDARD.encode(binary.value(i))
        ));
    }
    if let Some(structure) = array.as_any().downcast_ref::<StructArray>() {
        let mut fields = serde_json::Map::new();
        for (field, column) in structure.fields().iter().zip(structure.columns()) {
            fields.insert(
                field.name().clone(),
                nested_row_value(column.as_ref(), i, depth + 1)?,
            );
        }
        return Ok(Value::Object(fields));
    }
    if let Some(values) = [
        array
            .as_any()
            .downcast_ref::<ListArray>()
            .map(|a| a.value(i)),
        array
            .as_any()
            .downcast_ref::<LargeListArray>()
            .map(|a| a.value(i)),
        array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .map(|a| a.value(i)),
    ]
    .into_iter()
    .flatten()
    .next()
    {
        if values.len() > 50_000 {
            return Err(CatalogError::Limit(
                "单条盘口或特征数组超过 50,000 项。".into(),
            ));
        }
        let mut result = Vec::with_capacity(values.len());
        for offset in 0..values.len() {
            result.push(nested_row_value(values.as_ref(), offset, depth + 1)?);
        }
        return Ok(Value::Array(result));
    }
    if let Some(decimal) = array.as_any().downcast_ref::<Decimal128Array>() {
        let scale = match array.data_type() {
            DataType::Decimal128(_, s) => u32::try_from(*s)
                .map_err(|_| CatalogError::Invalid("Decimal128 负精度。".into()))?,
            _ => return Err(CatalogError::Invalid("数值 schema 无效。".into())),
        };
        return Ok(json!(
            Decimal::try_from_i128_with_scale(decimal.value(i), scale)
                .map_err(|_| CatalogError::Invalid("数值超出精度。".into()))?
                .to_string()
        ));
    }
    macro_rules! dictionary_string {
        ($key:ty) => {
            if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<$key>>() {
                let key = usize::try_from(dict.keys().value(i))
                    .map_err(|_| CatalogError::Invalid("枚举字典索引无效。".into()))?;
                let values = dict
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| CatalogError::Invalid("Nautilus 枚举字典无效。".into()))?;
                if key >= values.len() {
                    return Err(CatalogError::Invalid("枚举索引越界。".into()));
                }
                return Ok(json!(values.value(key)));
            }
        };
    }
    dictionary_string!(Int8Type);
    dictionary_string!(Int16Type);
    dictionary_string!(Int32Type);
    dictionary_string!(UInt8Type);
    dictionary_string!(UInt16Type);
    dictionary_string!(UInt32Type);
    Err(CatalogError::Invalid(format!(
        "Parquet 字段类型未支持：{:?}",
        array.data_type()
    )))
}
/// Decode Python's Nautilus BarType from the *right* so USD-M "-PERP"
/// and option contract hyphens cannot corrupt the symbol identity.
fn parse_bar_folder(folder: &str) -> Result<(String, MarketProduct, String), CatalogError> {
    let mut parts = folder.rsplitn(5, '-');
    let external = parts.next();
    let source = parts.next();
    let unit = parts.next();
    let number = parts.next().and_then(|v| v.parse::<u32>().ok());
    let instrument = parts
        .next()
        .ok_or_else(|| CatalogError::Invalid("BarType 无效。".into()))?;
    if external != Some("EXTERNAL") || source != Some("LAST") {
        return Err(CatalogError::Invalid("不是外部 Nautilus BarType。".into()));
    }
    let interval = match (number, unit) {
        (Some(1), Some("SECOND")) => "1s",
        (Some(1), Some("MINUTE")) => "1m",
        (Some(3), Some("MINUTE")) => "3m",
        (Some(5), Some("MINUTE")) => "5m",
        (Some(15), Some("MINUTE")) => "15m",
        (Some(30), Some("MINUTE")) => "30m",
        (Some(1), Some("HOUR")) => "1h",
        (Some(2), Some("HOUR")) => "2h",
        (Some(4), Some("HOUR")) => "4h",
        (Some(6), Some("HOUR")) => "6h",
        (Some(8), Some("HOUR")) => "8h",
        (Some(12), Some("HOUR")) => "12h",
        (Some(1), Some("DAY")) => "1d",
        (Some(3), Some("DAY")) => "3d",
        (Some(1), Some("WEEK")) => "1w",
        (Some(1), Some("MONTH")) => "1M",
        _ => return Err(CatalogError::Invalid("BarType 周期无效。".into())),
    };
    let (symbol, product) = parse_instrument_folder(instrument)?;
    if bar_type(&symbol, product, interval)? != folder {
        return Err(CatalogError::Invalid("BarType 与产品标识不匹配。".into()));
    }
    Ok((symbol, product, interval.to_owned()))
}
fn parse_instrument_folder(instrument: &str) -> Result<(String, MarketProduct), CatalogError> {
    let (symbol, market) = if let Some(symbol) = instrument.strip_suffix("-PERP.BINANCE") {
        (symbol, MarketProduct::FuturesUm)
    } else if let Some(symbol) = instrument.strip_suffix(".BINANCE") {
        (
            symbol,
            if symbol.contains('-') {
                MarketProduct::Options
            } else {
                MarketProduct::Spot
            },
        )
    } else {
        return Err(CatalogError::Invalid("不是 Binance InstrumentId。".into()));
    };
    if instrument_id(symbol, market)? != instrument {
        return Err(CatalogError::Invalid(
            "目录标识与 InstrumentId 不匹配。".into(),
        ));
    }
    Ok((symbol.to_owned(), market))
}
fn directories(root: &Path) -> Result<Vec<String>, CatalogError> {
    if !inspect_existing_directory(root)? {
        return Ok(Vec::new());
    }
    let mut names = Vec::new();
    for child in fs::read_dir(root)? {
        let child = child?;
        let kind = child.file_type()?;
        if kind.is_symlink() || !kind.is_dir() {
            return Err(CatalogError::Invalid(
                "历史数据目录包含不可信子路径。".into(),
            ));
        }
        let text = child.file_name().to_string_lossy().into_owned();
        safe_component(&text)?;
        names.push(text);
        if names.len() > 5000 {
            return Err(CatalogError::Limit("历史数据标的数量超出限制。".into()));
        }
    }
    names.sort();
    Ok(names)
}

impl Catalog {
    pub fn open_existing(path: &Path) -> Result<Self, CatalogError> {
        if !inspect_existing_directory(path)? {
            return Err(CatalogError::Missing(
                "Catalog 根目录不存在，读取端不自动创建。".into(),
            ));
        }
        if path.join("data/bar").exists() || path.join("data/trade_tick").exists() {
            return Err(CatalogError::Invalid(
                "检测到旧版 Nautilus v1 Catalog，必须明确迁移。".into(),
            ));
        }
        Ok(Self {
            root: path.canonicalize()?,
        })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    /// [start_ns,end_ns), as in the Python HistoricalMarketData adapter.
    /// No filling gaps and no source downloads. An empty existing path yields [].
    pub fn read_bars(
        &self,
        symbol: &str,
        product: MarketProduct,
        interval: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
    ) -> Result<Vec<CatalogBar>, CatalogError> {
        if start_ns >= end_ns || start_ns < 0 || !(1..=MAX_BATCH).contains(&limit) {
            return Err(CatalogError::Invalid("历史 K 线范围或数量无效。".into()));
        }
        let id = bar_type(symbol, product, interval)?;
        let rel = RecordKind::Bars.directory(&id)?;
        let files = candidate_files(&self.root, &rel, start_ns, end_ns)?;
        let mut result = Vec::new();
        let mut seen = BTreeSet::new();
        let mut previous_event = None;
        for path in files {
            let file = File::open(&path)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|err| CatalogError::Parquet(err.to_string()))?;
            let metadata = reader.schema().metadata();
            if metadata.get("type_name").map(String::as_str) != Some("Bar")
                || metadata.get("bar_type").map(String::as_str) != Some(id.as_str())
                || metadata.get("instrument_id").map(String::as_str)
                    != Some(instrument_id(symbol, product)?.as_str())
            {
                return Err(CatalogError::Invalid(
                    "Parquet BarType 或合约标识不一致。".into(),
                ));
            }
            let parse_precision = |name: &str| -> Result<u32, CatalogError> {
                let value = metadata
                    .get(name)
                    .ok_or_else(|| CatalogError::Invalid(format!("Bar 缺少 {name}。")))?
                    .parse::<u32>()
                    .map_err(|_| CatalogError::Invalid(format!("Bar {name} 不是整数。")))?;
                if value > 16 {
                    return Err(CatalogError::Invalid(format!("Bar {name} 超出可用精度。")));
                }
                Ok(value)
            };
            let price_precision = parse_precision("price_precision")?;
            let size_precision = parse_precision("size_precision")?;
            let stream = reader
                .with_batch_size(4096)
                .build()
                .map_err(|err| CatalogError::Parquet(err.to_string()))?;
            for batch in stream {
                let batch = batch.map_err(|err| CatalogError::Parquet(err.to_string()))?;
                for row in 0..batch.num_rows() {
                    let event = get_ts(&batch, row)?;
                    if previous_event.is_some_and(|value| event <= value) {
                        return Err(CatalogError::Invalid("Parquet K 线时间倒退或重复。".into()));
                    }
                    previous_event = Some(event);
                    if event < start_ns || event >= end_ns {
                        continue;
                    }
                    if event % NANO_PER_MS != 0 {
                        return Err(CatalogError::Invalid(
                            "K 线时间不是 close-minus-1ms。".into(),
                        ));
                    }
                    let close_time = event
                        .checked_div(NANO_PER_MS)
                        .and_then(|v| v.checked_add(1))
                        .ok_or_else(|| CatalogError::Invalid("K 线时间溢出。".into()))?;
                    let (open, after) = bar_window(interval, close_time.saturating_sub(1))?;
                    if after != close_time || !seen.insert(open) {
                        return Err(CatalogError::Invalid(
                            "历史 K 线不符合 UTC 收盘时间约定。".into(),
                        ));
                    }
                    let bar = CatalogBar {
                        open_time_ms: open,
                        open: get_decimal(&batch, "open", row)?,
                        high: get_decimal(&batch, "high", row)?,
                        low: get_decimal(&batch, "low", row)?,
                        close: get_decimal(&batch, "close", row)?,
                        volume: get_decimal(&batch, "volume", row)?,
                    };
                    bar.validate(interval, product)?;
                    if [bar.open, bar.high, bar.low, bar.close]
                        .iter()
                        .any(|value| value.round_dp(price_precision) != *value)
                        || bar.volume.round_dp(size_precision) != bar.volume
                    {
                        return Err(CatalogError::Invalid(
                            "Parquet Bar 数值超过声明的合约精度。".into(),
                        ));
                    }
                    result.push(bar);
                    if result.len() > limit {
                        return Err(CatalogError::Limit(
                            "历史 K 线超过调用上限，请改为分批读取。".into(),
                        ));
                    }
                }
            }
        }
        Ok(result)
    }
    /// The strict form used for replay. Historical charts may contain valid
    /// gaps, but a backtest must not turn such gaps into fabricated bars.
    pub fn read_contiguous_bars(
        &self,
        symbol: &str,
        product: MarketProduct,
        interval: &str,
        start_ms: i64,
        end_ms: i64,
        limit: usize,
    ) -> Result<Vec<CatalogBar>, CatalogError> {
        if start_ms < 0 || end_ms <= start_ms {
            return Err(CatalogError::Invalid("回测 K 线范围无效。".into()));
        }
        let mut expected = start_ms;
        let result = self.read_bars(
            symbol,
            product,
            interval,
            start_ms
                .checked_mul(NANO_PER_MS)
                .ok_or_else(|| CatalogError::Invalid("时间溢出。".into()))?,
            end_ms
                .checked_mul(NANO_PER_MS)
                .ok_or_else(|| CatalogError::Invalid("时间溢出。".into()))?,
            limit,
        )?;
        for bar in &result {
            if bar.open_time_ms != expected {
                return Err(CatalogError::Invalid("历史 K 线有缺口或不连续。".into()));
            }
            expected = bar_window(interval, expected)?.1;
        }
        if expected != end_ms {
            return Err(CatalogError::Invalid("历史 K 线不完整，禁止回测。".into()));
        }
        Ok(result)
    }
    /// Report exactly which aligned Bar windows are absent in [start_ms,end_ms).
    /// Consumers can decide whether to repair; a read never starts collection.
    pub fn bar_gaps(
        &self,
        symbol: &str,
        product: MarketProduct,
        interval: &str,
        start_ms: i64,
        end_ms: i64,
        limit: usize,
    ) -> Result<Vec<BarGap>, CatalogError> {
        if start_ms < 0
            || end_ms <= start_ms
            || bar_window(interval, start_ms)?.0 != start_ms
            || bar_window(interval, end_ms)?.0 != end_ms
        {
            return Err(CatalogError::Invalid(
                "历史缺口检查必须使用完整且对齐的 UTC K 线范围。".into(),
            ));
        }
        let start_ns = start_ms
            .checked_mul(NANO_PER_MS)
            .ok_or_else(|| CatalogError::Invalid("开始时间溢出。".into()))?;
        let end_ns = end_ms
            .checked_mul(NANO_PER_MS)
            .ok_or_else(|| CatalogError::Invalid("结束时间溢出。".into()))?;
        let bars = self.read_bars(symbol, product, interval, start_ns, end_ns, limit)?;
        // Monthly windows are calendar-variable; preserve the verified
        // bar_window successor logic instead of assuming a fixed duration.
        if let Some(step_ms) = crate::interval_ms(interval)? {
            let opens = bars.iter().map(|bar| bar.open_time_ms).collect::<Vec<_>>();
            return missing_bar_windows(&opens, start_ms, end_ms, step_ms);
        }
        let mut expected = start_ms;
        let mut missing = Vec::new();
        for bar in bars {
            if bar.open_time_ms > expected {
                missing.push(BarGap {
                    start_ms: expected,
                    end_ms: bar.open_time_ms,
                });
            } else if bar.open_time_ms < expected {
                return Err(CatalogError::Invalid("历史 K 线重复或倒退。".into()));
            }
            expected = bar_window(interval, bar.open_time_ms)?.1;
        }
        if expected < end_ms {
            missing.push(BarGap {
                start_ms: expected,
                end_ms,
            });
        }
        Ok(missing)
    }

    /// Generic bounded scanner for the other native Nautilus types. All
    /// fields retain original schema names; decimals are encoded as strings,
    /// timestamps remain exact nanoseconds (no lossy JSON floats).
    pub fn read_records(
        &self,
        kind: &RecordKind,
        identifier: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
    ) -> Result<Vec<Value>, CatalogError> {
        if start_ns < 0 || end_ns <= start_ns || !(1..=MAX_BATCH).contains(&limit) {
            return Err(CatalogError::Invalid("历史事件范围无效。".into()));
        }
        let rel = kind.directory(identifier)?;
        let paths = candidate_files(&self.root, &rel, start_ns, end_ns)?;
        let mut previous = None;
        let mut out = Vec::new();
        let mut trade_ids = std::collections::HashSet::new();
        let mut record_fingerprints = std::collections::HashSet::new();
        for path in paths {
            let file = File::open(&path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| CatalogError::Parquet(e.to_string()))?;
            let metadata = builder.schema().metadata();
            let declared = match kind {
                RecordKind::Trades => Some("TradeTick"),
                RecordKind::Quotes => Some("QuoteTick"),
                RecordKind::OrderBookDepth => Some("OrderBookDepth"),
                RecordKind::OptionGreeks => Some("OptionGreeks"),
                RecordKind::FundingRate => Some("FundingRateUpdate"),
                RecordKind::InstrumentStatus => Some("InstrumentStatus"),
                RecordKind::InstrumentClose => Some("InstrumentClose"),
                _ => None,
            };
            if declared.is_some_and(|expected| {
                metadata.get("type_name").map(String::as_str) != Some(expected)
            }) || metadata
                .get("instrument_id")
                .is_some_and(|value| value != identifier)
            {
                return Err(CatalogError::Invalid(
                    "Parquet 事件 type_name 或 instrument_id 不一致。".into(),
                ));
            }
            let (file_first, file_last) = filename_bounds(
                path.file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| CatalogError::Invalid("文件名不是 UTF-8。".into()))?,
            )?;
            let reader = builder
                .with_batch_size(4096)
                .build()
                .map_err(|e| CatalogError::Parquet(e.to_string()))?;
            for batch in reader {
                let batch = batch.map_err(|e| CatalogError::Parquet(e.to_string()))?;
                let schema = batch.schema();
                for row in 0..batch.num_rows() {
                    let timestamp = get_ts(&batch, row)?;
                    if timestamp < file_first || timestamp > file_last {
                        return Err(CatalogError::Invalid(
                            "Parquet 事件时间与文件名边界不一致。".into(),
                        ));
                    }
                    get_ts_for_column(&batch, "ts_init", row)?;
                    if matches!(kind, RecordKind::Trades) {
                        let trade_id = batch
                            .column_by_name("trade_id")
                            .and_then(|a| a.as_any().downcast_ref::<StringArray>())
                            .ok_or_else(|| {
                                CatalogError::Invalid("TradeTick trade_id 不是字符串。".into())
                            })?;
                        if trade_id.is_null(row)
                            || trade_id.value(row).is_empty()
                            || !trade_ids.insert(trade_id.value(row).to_owned())
                        {
                            return Err(CatalogError::Conflict(
                                "历史 TradeTick trade_id 重复或无效。".into(),
                            ));
                        }
                    }
                    if previous.is_some_and(|last| timestamp < last) {
                        return Err(CatalogError::Invalid("历史事件时间倒退。".into()));
                    }
                    previous = Some(timestamp);
                    if !(start_ns..end_ns).contains(&timestamp) {
                        continue;
                    }
                    let mut object = serde_json::Map::new();
                    for (field, array) in schema.fields().iter().zip(batch.columns()) {
                        let value = if matches!(field.name().as_str(), "ts_event" | "ts_init") {
                            json!(get_ts_for_column(&batch, field.name(), row)?.to_string())
                        } else {
                            row_value(array.as_ref(), row)?
                        };
                        object.insert(field.name().clone(), value);
                    }
                    let fingerprint = serde_json::to_vec(&object)
                        .map_err(|e| CatalogError::Invalid(format!("历史行序列化失败：{e}")))?;
                    if !record_fingerprints.insert(fingerprint) {
                        return Err(CatalogError::Conflict(
                            "历史 Parquet 含有完全相同的重复记录。".into(),
                        ));
                    }
                    out.push(Value::Object(object));
                    if out.len() > limit {
                        return Err(CatalogError::Limit(
                            "历史事件超过调用上限，必须分页读取。".into(),
                        ));
                    }
                }
            }
        }
        Ok(out)
    }
    /// Match Python dataset_inventory: discover product/interval from
    /// existing Nautilus v2 data directories, not from configured policies.
    /// Per-file event ranges are returned without claiming quality coverage.
    pub fn scan_inventory(&self) -> Result<Vec<Value>, CatalogError> {
        let mut out = Vec::new();
        for folder in directories(&self.root.join("data/bars"))? {
            let (symbol, product, interval) = parse_bar_folder(&folder)?;
            let ranges = self.inventory(&RecordKind::Bars, &folder)?;
            if ranges.is_empty() {
                continue;
            }
            out.push(json!({"symbol":symbol,"product":product.as_str(),
                "instrument_id":instrument_id(&symbol,product)?,
                "level":interval,"data_type":"bars",
                "event_ranges_ns":ranges.iter().map(|r|json!([
                    r["start_ns"],r["end_ns"]
                ])).collect::<Vec<_>>(),
                "quality":"unverified","backend":"nautilus_v2_parquet"}));
        }
        for folder in directories(&self.root.join("data/trades"))? {
            let (symbol, product) = parse_instrument_folder(&folder)?;
            let ranges = self.inventory(&RecordKind::Trades, &folder)?;
            if ranges.is_empty() {
                continue;
            }
            out.push(json!({"symbol":symbol,"product":product.as_str(),
                "instrument_id":folder,"level":"tick","data_type":"trades",
                "event_ranges_ns":ranges.iter().map(|r|json!([
                    r["start_ns"],r["end_ns"]
                ])).collect::<Vec<_>>(),
                "quality":"unverified","backend":"nautilus_v2_parquet"}));
        }
        out.sort_by(|a, b| {
            a["product"]
                .as_str()
                .cmp(&b["product"].as_str())
                .then(a["symbol"].as_str().cmp(&b["symbol"].as_str()))
                .then(a["level"].as_str().cmp(&b["level"].as_str()))
        });
        Ok(out)
    }

    /// Inventory contains only recognized existing files and metadata ranges.
    /// Existence does NOT certify archive coverage or the quality of trades.
    pub fn inventory(
        &self,
        kind: &RecordKind,
        identifier: &str,
    ) -> Result<Vec<Value>, CatalogError> {
        let rel = kind.directory(identifier)?;
        let paths = candidate_files(&self.root, &rel, 0, i64::MAX)?;
        let mut entries = Vec::new();
        for path in paths {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| CatalogError::Invalid("Catalog 文件名编码无效。".into()))?;
            let (first, last) = filename_bounds(name)?;
            entries.push(json!({"file":name,"start_ns":first.to_string(),
                "end_ns":last.to_string(),"quality":"unverified"}));
        }
        Ok(entries)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inventory_reconstructs_option_and_perpetual_bar_types_from_right() {
        assert_eq!(
            parse_bar_folder("ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL").unwrap(),
            (
                "ETHUSDT".to_string(),
                MarketProduct::FuturesUm,
                "1m".to_string()
            )
        );
        assert_eq!(
            parse_bar_folder("BTC-271231-90000-P.BINANCE-5-MINUTE-LAST-EXTERNAL").unwrap(),
            (
                "BTC-271231-90000-P".to_string(),
                MarketProduct::Options,
                "5m".to_string()
            )
        );
        assert!(parse_bar_folder("BTCUSDT-PERP.BINANCE-1-MINUTE-LAST-FAKE").is_err());
    }
    #[test]
    fn path_names_and_windows_are_ported_from_python() {
        let identifier = bar_type("ETHUSDT", MarketProduct::FuturesUm, "1m").unwrap();
        assert_eq!(
            RecordKind::Bars.directory(&identifier).unwrap(),
            PathBuf::from("data/bars/ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL")
        );
        assert!(RecordKind::Bars.directory("../etc/passwd").is_err());
        assert_eq!(
            filename_bounds(
                "2026-11-20T00-00-59-999000000Z_2026-11-20T00-01-59-999000000Z.parquet"
            )
            .unwrap()
            .0,
            1795132859999000000
        );
    }
}
