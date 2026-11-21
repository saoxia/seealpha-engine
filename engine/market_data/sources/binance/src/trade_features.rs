//! Explicit offline Trade ZIP -> native Feature Arrow/Parquet publication.
//! This is a separate versioned feature dataset, NOT the Python BarFeatures
//! schema. No exchange proof, data_ready flag, or live orders are produced.
use std::{collections::HashMap, path::{Path, PathBuf}, sync::Arc};
use arrow_array::{ArrayRef, Float64Array, RecordBatch, TimestampNanosecondArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use rust_decimal::prelude::ToPrimitive;
use seealpha_catalog::{Catalog, RecordKind, DAY_MS};
use seealpha_feed::features::{AggBar, InputBar, indicator_rows};
use crate::trade_archive::{TradeProduct, aggregate_archive};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;
pub const FEATURE_NAME: &str = "NativeTradeFeaturesV1";

fn numeric(value: rust_decimal::Decimal) -> Result<f64> {
    value.to_f64().filter(|v| v.is_finite()).ok_or_else(|| "invalid feature decimal".into())
}
fn times(bars: &[AggBar], interval_ms: i64) -> Result<Vec<i64>> {
    if bars.is_empty() || bars.len() > 50_000 {
        return Err("feature publication requires 1..50000 closed bars".into());
    }
    let first_day = bars[0].open_ms.div_euclid(DAY_MS);
    let mut last = None;
    let mut out = Vec::with_capacity(bars.len());
    for bar in bars {
        if bar.open_ms < 0 || bar.open_ms % interval_ms != 0
            || bar.open_ms.div_euclid(DAY_MS) != first_day
            || last.is_some_and(|prior| prior >= bar.open_ms)
        {
            return Err("feature bars must be strictly ordered within one UTC day".into());
        }
        let end = bar.open_ms.checked_add(interval_ms)
            .ok_or("bar end overflow")?;
        if end.div_euclid(DAY_MS) != first_day && end != (first_day + 1) * DAY_MS {
            return Err("bar crosses UTC day".into());
        }
        let timestamp_ns = end.checked_sub(1).and_then(|v| v.checked_mul(1_000_000))
            .ok_or("feature timestamp overflow")?;
        out.push(timestamp_ns);
        last = Some(bar.open_ms);
    }
    Ok(out)
}
pub fn feature_batch(bars: &[AggBar], interval_ms: i64) -> Result<RecordBatch> {
    if !matches!(interval_ms, 1_000 | 60_000 | 180_000 | 300_000 | 900_000 | 3_600_000) {
        return Err("unsupported interval".into());
    }
    let ts = times(bars, interval_ms)?;
    let inputs = bars.iter().map(|bar| Ok(InputBar {
        open_time_ms: bar.open_ms,
        open: numeric(bar.open)?,
        high: numeric(bar.high)?,
        low: numeric(bar.low)?,
        close: numeric(bar.close)?,
        volume: numeric(bar.volume)?,
        taker_buy: numeric(bar.taker_buy_volume)?,
    })).collect::<Result<Vec<_>>>()?;
    // Deliberately separate V1 indicator parameters from legacy schema.
    // Features are null until warmup, and gaps reset lookback state.
    let rows = indicator_rows(&inputs, interval_ms, 99, 20, 5, 14);
    let mut fields = vec![
        Field::new("ts_event", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("ts_init", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(ts.clone())),
        Arc::new(TimestampNanosecondArray::from(ts)),
    ];
    let mut add_float = |name: &str, values: Vec<Option<f64>>| {
        fields.push(Field::new(name, DataType::Float64, true));
        columns.push(Arc::new(Float64Array::from(values)));
    };
    add_float("open", inputs.iter().map(|v| Some(v.open)).collect());
    add_float("high", inputs.iter().map(|v| Some(v.high)).collect());
    add_float("low", inputs.iter().map(|v| Some(v.low)).collect());
    add_float("close", inputs.iter().map(|v| Some(v.close)).collect());
    add_float("volume", inputs.iter().map(|v| Some(v.volume)).collect());
    add_float("taker_buy_volume", inputs.iter().map(|v| Some(v.taker_buy)).collect());
    add_float("quote_volume", bars.iter().map(|v| numeric(v.quote_volume).map(Some)).collect::<Result<Vec<_>>>()?);
    add_float("taker_buy_quote_volume", bars.iter().map(|v| numeric(v.taker_buy_quote_volume).map(Some)).collect::<Result<Vec<_>>>()?);
    add_float("ma99", rows.iter().map(|v| v.ma).collect());
    add_float("std20", rows.iter().map(|v| v.std).collect());
    add_float("ret_5", rows.iter().map(|v| v.ret).collect());
    add_float("atr14", rows.iter().map(|v| v.atr).collect());
    add_float("natr14", rows.iter().map(|v| v.natr).collect());
    add_float("boll_mid", rows.iter().map(|v| v.boll_mid).collect());
    add_float("boll_upper", rows.iter().map(|v| v.boll_upper).collect());
    add_float("boll_lower", rows.iter().map(|v| v.boll_lower).collect());
    add_float("boll_width", rows.iter().map(|v| v.boll_width).collect());
    add_float("boll_pos", rows.iter().map(|v| v.boll_pos).collect());
    add_float("buy_ratio", rows.iter().map(|v| v.buy_ratio).collect());
    add_float("delta_ratio", rows.iter().map(|v| v.delta_ratio).collect());
    fields.push(Field::new("trade_count", DataType::UInt64, false));
    columns.push(Arc::new(UInt64Array::from(bars.iter().map(|v|v.trade_count).collect::<Vec<_>>())));
    let mut metadata = HashMap::new();
    metadata.insert("type_name".to_owned(), FEATURE_NAME.to_owned());
    metadata.insert("source".to_owned(), "binance-offline-trade-zip".to_owned());
    metadata.insert("feature_version".to_owned(), "1".to_owned());
    metadata.insert("interval_ms".to_owned(), interval_ms.to_string());
    metadata.insert("exchange_verified".to_owned(), "false".to_owned());
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

/// Explicit, bounded, append-only publication under a *new* Feature name.
/// Never claims identical Nautilus Python BarFeatures semantics.
pub fn publish_archive_features(
    archive: &Path, product: TradeProduct, interval_ms: i64,
    catalog_root: &Path, identity: &str,
) -> Result<PathBuf> {
    let bars = aggregate_archive(archive, product, interval_ms)?;
    let batch = feature_batch(&bars, interval_ms)?;
    let catalog = Catalog::open_existing(catalog_root)?;
    Ok(catalog.append_record_batches(
        &RecordKind::Feature { identifier: "rust-v1".into(), name: FEATURE_NAME.into() },
        identity, &[batch],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    fn bar(t: i64) -> AggBar {
        AggBar {
            open_ms: t, open: Decimal::from(10), high: Decimal::from(11),
            low: Decimal::from(9), close: Decimal::from(10),
            volume: Decimal::from(2), quote_volume: Decimal::from(20),
            taker_buy_volume: Decimal::ONE, taker_buy_quote_volume: Decimal::from(10),
            trade_count: 2,
        }
    }
    #[test]
    fn arrow_features_keep_warmup_nulls_and_native_ns_time() {
        let b = feature_batch(&[bar(0),bar(60_000)],60_000).unwrap();
        assert_eq!(b.num_rows(),2);
        let ts = b.column_by_name("ts_event").unwrap()
            .as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
        assert_eq!(ts.value(0),59_999_000_000);
        assert_eq!(b.column_by_name("ma99").unwrap().null_count(),2);
        assert_eq!(b.schema().metadata()["exchange_verified"],"false");
    }
    #[test]
    fn rejects_time_order_and_cross_day() {
        assert!(feature_batch(&[bar(60_000),bar(0)],60_000).is_err());
        assert!(feature_batch(&[bar(0),bar(0)],60_000).is_err());
        assert!(feature_batch(&[bar(0),bar(DAY_MS)],60_000).is_err());
    }
}
