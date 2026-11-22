//! Settled Binance USD-M funding observations.
//!
//! Funding intervals are venue-defined and can change; never synthesize a
//! funding payment every fixed eight hours or confuse mark-price estimates
//! with a settled funding record.
use crate::{MarketError, check_symbol};

use arrow_array::{ArrayRef, Decimal128Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use seealpha_catalog::{Catalog, CatalogError, RecordKind};
use std::{collections::{HashMap, HashSet}, sync::Arc};

/// Public exchange settlements belong to the USD-M market-data pipeline,
/// never to the account/order ledger. The catalog provides atomic immutable
/// Parquet publication; this product module owns the event schema and policy.
fn funding_identifier(symbol: &str) -> Result<String, CatalogError> {
    check_symbol(symbol).map_err(|_| CatalogError::Invalid("invalid USD-M funding symbol".into()))?;
    Ok(format!("{symbol}-PERP.BINANCE"))
}

fn funding_decimal(raw: &str) -> Result<i128, CatalogError> {
    let mut value = raw.parse::<Decimal>()
        .map_err(|_| CatalogError::Invalid("invalid funding decimal".into()))?;
    let original = value;
    value.rescale(16);
    if value != original || value.scale() != 16 {
        return Err(CatalogError::Invalid("funding decimal exceeds 16 fractional digits".into()));
    }
    Ok(value.mantissa())
}

fn funding_batch(symbol: &str, rows: &[FundingObservation]) -> Result<RecordBatch, CatalogError> {
    let identifier = funding_identifier(symbol)?;
    let schema = Arc::new(Schema::new_with_metadata(vec![
        Field::new("funding_rate", DataType::Decimal128(38, 16), false),
        Field::new("mark_price", DataType::Decimal128(38, 16), false),
        Field::new("rate_type", DataType::Utf8, false),
        Field::new("ts_event", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
        Field::new("ts_init", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
    ], HashMap::from([
        ("type_name".to_owned(), "FundingRateUpdate".to_owned()),
        ("instrument_id".to_owned(), identifier),
        ("exchange".to_owned(), "BINANCE".to_owned()),
        ("product".to_owned(), "futures_um".to_owned()),
        ("settlement_source".to_owned(), "official_fundingRate_REST".to_owned()),
    ])));
    let mut rates = Vec::with_capacity(rows.len());
    let mut marks = Vec::with_capacity(rows.len());
    let mut types = Vec::with_capacity(rows.len());
    let mut timestamps = Vec::with_capacity(rows.len());
    let mut previous = None;
    for row in rows {
        let ts = row.funding_time_ms.checked_mul(1_000_000)
            .ok_or_else(|| CatalogError::Invalid("funding timestamp overflow".into()))?;
        if row.symbol != symbol || previous.is_some_and(|p| ts <= p)
            || !matches!(row.rate_type.as_str(), "Regular" | "Special" | "Unknown")
            || row.mark_price.parse::<Decimal>().ok().is_none_or(|x| x <= Decimal::ZERO) {
            return Err(CatalogError::Invalid("invalid or unordered settled funding".into()));
        }
        rates.push(funding_decimal(&row.funding_rate)?);
        marks.push(funding_decimal(&row.mark_price)?);
        types.push(row.rate_type.as_str());
        timestamps.push(ts);
        previous = Some(ts);
    }
    let decimal = |values: Vec<i128>| -> Result<ArrayRef, CatalogError> {
        Ok(Arc::new(Decimal128Array::from(values).with_precision_and_scale(38, 16)
            .map_err(|e| CatalogError::Arrow(e.to_string()))?))
    };
    RecordBatch::try_new(schema, vec![
        decimal(rates)?, decimal(marks)?,
        Arc::new(StringArray::from(types)),
        Arc::new(TimestampNanosecondArray::from(timestamps.clone()).with_timezone("UTC")),
        Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
    ]).map_err(|e| CatalogError::Arrow(e.to_string()))
}

/// Read official, already settled observations. No timestamp-grid interpolation
/// and no assertion that empty intervals prove historical completeness.
pub fn read_funding(
    catalog: &Catalog, symbol: &str, start_ms: i64, end_ms: i64, limit: usize,
) -> Result<Vec<FundingObservation>, CatalogError> {
    let identifier = funding_identifier(symbol)?;
    let start_ns = start_ms.checked_mul(1_000_000)
        .ok_or_else(|| CatalogError::Invalid("start overflow".into()))?;
    let end_ns = end_ms.checked_mul(1_000_000)
        .ok_or_else(|| CatalogError::Invalid("end overflow".into()))?;
    let rows = catalog.read_records(&RecordKind::FundingRate, &identifier, start_ns, end_ns, limit)?;
    rows.into_iter().map(|row| {
        let ts = row["ts_event"].as_str().ok_or_else(|| CatalogError::Invalid("funding timestamp absent".into()))?
            .parse::<i64>().map_err(|_| CatalogError::Invalid("funding timestamp invalid".into()))?;
        if ts % 1_000_000 != 0 {
            return Err(CatalogError::Invalid("funding timestamp not on millisecond".into()));
        }
        let field = |name: &str| -> Result<String, CatalogError> {
            row[name].as_str().map(str::to_owned)
                .ok_or_else(|| CatalogError::Invalid(format!("funding field {name} missing")))
        };
        let observation = FundingObservation {
            symbol: symbol.to_owned(),
            funding_time_ms: ts / 1_000_000,
            funding_rate: field("funding_rate")?,
            mark_price: field("mark_price")?,
            rate_type: field("rate_type")?,
        };
        if !matches!(observation.rate_type.as_str(), "Regular" | "Special" | "Unknown") {
            return Err(CatalogError::Invalid("invalid funding rate type".into()));
        }
        Ok(observation)
    }).collect()
}

/// Publish only unseen settlement events. A repeated observation is idempotent;
/// a different value for the same exchange event is a hard conflict. Split
/// newly discovered events around existing timestamps to avoid partial Parquet
/// overlaps, including when REST replays earlier pages.
pub fn persist_funding(
    catalog: &Catalog, symbol: &str, observations: &[FundingObservation],
) -> Result<usize, CatalogError> {
    if observations.is_empty() { return Ok(0); }
    if observations.len() > 5000 {
        return Err(CatalogError::Limit("funding batch exceeds 5000 events".into()));
    }
    let first = observations.first().unwrap().funding_time_ms;
    let last = observations.last().unwrap().funding_time_ms;
    if first < 0 || last < first {
        return Err(CatalogError::Invalid("invalid funding range".into()));
    }
    let end = last.checked_add(1).ok_or_else(|| CatalogError::Invalid("funding end overflow".into()))?;
    // Read and validate all observations before any mutation.
    let previous = read_funding(catalog, symbol, first, end, 50_000)?;
    let existing: HashMap<i64, FundingObservation> =
        previous.into_iter().map(|r| (r.funding_time_ms, r)).collect();
    let mut seen = HashSet::new();
    let mut new_rows = Vec::new();
    let mut prior = None;
    for row in observations {
        if row.symbol != symbol || !seen.insert(row.funding_time_ms)
            || prior.is_some_and(|p| row.funding_time_ms <= p) {
            return Err(CatalogError::Invalid("duplicate or unordered funding event".into()));
        }
        prior = Some(row.funding_time_ms);
        // Compare Decimal values numerically, since REST and Parquet may
        // serialize equivalent scales differently.
        if let Some(old) = existing.get(&row.funding_time_ms) {
            if old.rate_type != row.rate_type ||
                old.funding_rate.parse::<Decimal>().ok() != row.funding_rate.parse::<Decimal>().ok() ||
                old.mark_price.parse::<Decimal>().ok() != row.mark_price.parse::<Decimal>().ok() {
                return Err(CatalogError::Conflict("stored funding event differs from exchange".into()));
            }
        } else {
            new_rows.push(row.clone());
        }
    }
    if new_rows.is_empty() { return Ok(0); }
    // Every original timestamp boundary already present in Catalog can split
    // a newly published chunk; never write an overlapping file.
    let mut count = 0;
    let mut chunk: Vec<FundingObservation> = Vec::new();
    for row in observations {
        if existing.contains_key(&row.funding_time_ms) {
            if !chunk.is_empty() {
                let batch = funding_batch(symbol, &chunk)?;
                catalog.append_record_batches(&RecordKind::FundingRate, &funding_identifier(symbol)?, &[batch])?;
                count += chunk.len();
                chunk.clear();
            }
        } else {
            chunk.push(row.clone());
        }
    }
    if !chunk.is_empty() {
        let batch = funding_batch(symbol, &chunk)?;
        catalog.append_record_batches(&RecordKind::FundingRate, &funding_identifier(symbol)?, &[batch])?;
        count += chunk.len();
    }
    Ok(count)
}

pub fn latest_funding(
    catalog: &Catalog, symbol: &str, limit: usize,
) -> Result<Vec<FundingObservation>, CatalogError> {
    if !(1..=500).contains(&limit) {
        return Err(CatalogError::Limit("invalid latest funding limit".into()));
    }
    // A bounded historical lookback, not proof of complete history. Request
    // a known supported window and retain only the newest settled events.
    let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| CatalogError::Invalid("system clock invalid".into()))?
        .as_millis() as i64;
    let start = now_ms.saturating_sub(365_i64 * 86_400_000);
    let mut rows = read_funding(catalog, symbol, start, now_ms, 50_000)?;
    if rows.len() > limit { rows.drain(..rows.len()-limit); }
    Ok(rows)
}

use rust_decimal::Decimal;
use serde_json::Value;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingObservation {
    pub symbol: String,
    pub funding_time_ms: i64,
    pub funding_rate: String,
    pub mark_price: String,
    pub rate_type: String,
}

pub fn parse_funding_rows(
    value: &Value,
    requested_symbol: &str,
    start: i64,
    end: i64,
    previous: Option<i64>,
) -> Result<Vec<FundingObservation>, MarketError> {
    let data = value.as_array().ok_or(MarketError::Payload)?;
    if data.len() > 1000 {
        return Err(MarketError::Payload);
    }
    let mut result = Vec::with_capacity(data.len());
    let mut last = previous;
    for row in data {
        let symbol = row["symbol"].as_str().ok_or(MarketError::Payload)?;
        let time = row["fundingTime"].as_i64().ok_or(MarketError::Time)?;
        let rate = row["fundingRate"].as_str().ok_or(MarketError::Decimal)?;
        let mark = row["markPrice"].as_str().ok_or(MarketError::Decimal)?;
        // Older Binance fundingRate rows may not carry an event class; do not
        // infer a regular eight-hour settlement from missing metadata.
        let rate_type = row["rateType"].as_str().unwrap_or("Unknown");
        let rate = rate.parse::<Decimal>().map_err(|_| MarketError::Decimal)?;
        let mark = mark.parse::<Decimal>().map_err(|_| MarketError::Decimal)?;
        if symbol != requested_symbol
            || !(start..end).contains(&time)
            || last.is_some_and(|p| time <= p)
            || mark <= Decimal::ZERO
            || !matches!(rate_type, "Regular" | "Special" | "Unknown")
        {
            return Err(MarketError::Payload);
        }
        result.push(FundingObservation {
            symbol: symbol.to_owned(),
            funding_time_ms: time,
            funding_rate: rate.to_string(),
            mark_price: mark.to_string(),
            rate_type: rate_type.to_owned(),
        });
        last = Some(time);
    }
    Ok(result)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingEstimate {
    pub symbol: String,
    pub event_time_ms: i64,
    pub next_funding_ms: i64,
    pub estimated_rate: Decimal,
}

/// Parse a markPriceUpdate without promoting its mutable projected rate to a
/// settled FundingRateUpdate or an account funding payment.
pub fn parse_funding_estimate(
    value: &Value,
    requested_symbol: &str,
) -> Result<FundingEstimate, MarketError> {
    check_symbol(requested_symbol)?;
    if value["e"].as_str() != Some("markPriceUpdate")
        || value["s"].as_str() != Some(requested_symbol)
    {
        return Err(MarketError::Payload);
    }
    let event = value["E"].as_i64().ok_or(MarketError::Time)?;
    let next = value["T"].as_i64().ok_or(MarketError::Time)?;
    let rate = value["r"]
        .as_str()
        .ok_or(MarketError::Decimal)?
        .parse::<Decimal>()
        .map_err(|_| MarketError::Decimal)?;
    if event < 0 || next <= event {
        return Err(MarketError::Time);
    }
    Ok(FundingEstimate {
        symbol: requested_symbol.to_owned(),
        event_time_ms: event,
        next_funding_ms: next,
        estimated_rate: rate,
    })
}

/// The settled rate is sourced solely from the exchange's public REST
/// fundingRate history. It is not evidence of an account-specific payment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingRateUpdate {
    pub symbol: String,
    pub ts_event_ns: i64,
    pub ts_init_ns: i64,
    pub next_funding_ns: i64,
    pub rate: Decimal,
    pub rate_type: String,
    pub account_payment_confirmed: bool,
}
impl FundingObservation {
    pub fn to_update(&self) -> Result<FundingRateUpdate, MarketError> {
        let nanos = self
            .funding_time_ms
            .checked_mul(1_000_000)
            .ok_or(MarketError::Overflow)?;
        let rate = self
            .funding_rate
            .parse::<Decimal>()
            .map_err(|_| MarketError::Decimal)?;
        Ok(FundingRateUpdate {
            symbol: self.symbol.clone(),
            ts_event_ns: nanos,
            ts_init_ns: nanos,
            next_funding_ns: nanos,
            rate,
            rate_type: self.rate_type.clone(),
            account_payment_confirmed: false,
        })
    }
}

/// An exchange REST scan can establish which rates were observed, not prove
/// that an empty response covers an arbitrary historical period.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingHistoryRead {
    pub observations: Vec<FundingObservation>,
}
impl FundingHistoryRead {
    pub fn historical_complete(&self) -> bool {
        false
    }
    pub fn observed_updates(&self) -> Result<Vec<FundingRateUpdate>, MarketError> {
        self.observations
            .iter()
            .map(FundingObservation::to_update)
            .collect()
    }
}

#[derive(Clone)]
pub struct FundingSource {
    client: reqwest::Client,
}
impl FundingSource {
    pub fn new() -> Result<Self, MarketError> {
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(12))
            .user_agent("seealpha-native-funding/0.1")
            .build()
            .map_err(|_| MarketError::Network)?;
        Ok(Self { client })
    }
    pub async fn history_observed(
        &self,
        symbol: &str,
        start: i64,
        end: i64,
    ) -> Result<FundingHistoryRead, MarketError> {
        Ok(FundingHistoryRead {
            observations: self.history(symbol, start, end).await?,
        })
    }

    pub async fn history(
        &self,
        symbol: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<FundingObservation>, MarketError> {
        check_symbol(symbol)?;
        let mut clock = crate::clock::ServerClock::new();
        clock.sync(&self.client, "futures_um").await?;
        let now = clock.confirmed_ms()?;
        if start < 0 || end <= start || end > now || end - start > 365 * 86_400_000 {
            return Err(MarketError::Time);
        }
        let mut result = Vec::new();
        let mut cursor = start;
        loop {
            let response = self
                .client
                .get("https://fapi.binance.com/fapi/v1/fundingRate")
                .query(&[
                    ("symbol", symbol.to_owned()),
                    ("startTime", cursor.to_string()),
                    ("endTime", (end - 1).to_string()),
                    ("limit", "1000".into()),
                ])
                .send()
                .await
                .map_err(|_| MarketError::Network)?;
            if !response.status().is_success() {
                return Err(MarketError::Status(response.status().as_u16()));
            }
            let value = response
                .json::<Value>()
                .await
                .map_err(|_| MarketError::Payload)?;
            let page = parse_funding_rows(
                &value,
                symbol,
                start,
                end,
                result
                    .last()
                    .map(|v: &FundingObservation| v.funding_time_ms),
            )?;
            let n = page.len();
            if let Some(last) = page.last() {
                cursor = last
                    .funding_time_ms
                    .checked_add(1)
                    .ok_or(MarketError::Overflow)?;
            }
            if result.len() + n > 5000 {
                return Err(MarketError::Payload);
            }
            result.extend(page);
            if n < 1000 || cursor >= end {
                break;
            }
        }
        // These are exchange-supplied settled events, but we do not
        // manufacture missing event timestamps or certify full coverage.
        Ok(result)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn funding_catalog_round_trip_and_conflict_policy() {
        let path = std::env::temp_dir().join(format!("seealpha-um-funding-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                .unwrap().as_nanos()));
        std::fs::create_dir_all(&path).unwrap();
        let catalog = Catalog::open_existing(&path).unwrap();
        let rows = vec![
            FundingObservation { symbol: "BTCUSDT".into(), funding_time_ms: 100,
                funding_rate: "-0.0002".into(), mark_price: "30000".into(),
                rate_type: "Unknown".into() },
            FundingObservation { symbol: "BTCUSDT".into(), funding_time_ms: 150,
                funding_rate: "0.0001".into(), mark_price: "31000".into(),
                rate_type: "Special".into() },
        ];
        assert_eq!(persist_funding(&catalog, "BTCUSDT", &rows).unwrap(), 2);
        assert_eq!(persist_funding(&catalog, "BTCUSDT", &rows).unwrap(), 0);
        let old = read_funding(&catalog, "BTCUSDT", 0, 200, 100).unwrap();
        assert_eq!(old.len(), 2);
        assert_eq!(old[0].rate_type, "Unknown");
        assert_eq!(old[1].funding_rate.parse::<Decimal>().unwrap(), Decimal::new(1, 4));
        let extra = FundingObservation { symbol:"BTCUSDT".into(), funding_time_ms:200,
            funding_rate:"-0.0003".into(), mark_price:"32000".into(),
            rate_type:"Regular".into() };
        assert_eq!(persist_funding(&catalog, "BTCUSDT", &[rows[1].clone(), extra]).unwrap(), 1);
        assert_eq!(read_funding(&catalog, "BTCUSDT", 0, 300, 100).unwrap().len(), 3);
        let changed = FundingObservation { funding_rate:"0.0009".into(), ..rows[0].clone() };
        assert!(persist_funding(&catalog, "BTCUSDT", &[changed]).is_err());
        assert!(read_funding(&catalog, "ETHUSDT", 0, 300, 100).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn settled_funding_parses_without_fabricated_eight_hour_intervals() {
        let raw = json!([{"symbol":"BTCUSDT","fundingTime":11,
            "fundingRate":"-0.0002","markPrice":"30000","rateType":"Regular"},
            {"symbol":"BTCUSDT","fundingTime":20,
            "fundingRate":"0.0001","markPrice":"31000","rateType":"Special"}]);
        let rows = parse_funding_rows(&raw, "BTCUSDT", 10, 30, None).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].funding_rate, "-0.0002");
        let no_type = json!([{"symbol":"BTCUSDT","fundingTime":22,
            "fundingRate":"0.0001","markPrice":"31000"}]);
        assert_eq!(
            parse_funding_rows(&no_type, "BTCUSDT", 10, 30, None).unwrap()[0].rate_type,
            "Unknown"
        );
        assert!(parse_funding_rows(&raw, "BTCUSDT", 10, 30, Some(11)).is_err());
    }

    #[test]
    fn estimate_cannot_be_promoted_to_a_settled_payment() {
        let raw = json!({"e":"markPriceUpdate","s":"ETHUSDT",
            "E":101,"T":180,"r":"-0.00001"});
        let estimate = parse_funding_estimate(&raw, "ETHUSDT").unwrap();
        assert_eq!(estimate.next_funding_ms, 180);
        assert!(estimate.estimated_rate < Decimal::ZERO);
        let mut wrong = raw.clone();
        wrong["T"] = json!(100);
        assert!(parse_funding_estimate(&wrong, "ETHUSDT").is_err());
        let mut wrong_symbol = raw;
        wrong_symbol["s"] = json!("BTCUSDT");
        assert!(parse_funding_estimate(&wrong_symbol, "ETHUSDT").is_err());
    }

    #[test]
    fn special_settlements_preserve_exact_nanoseconds_and_unverified_coverage() {
        let data = json!([
            {"symbol":"ETHUSDT","fundingTime":10,"fundingRate":"-0.0002",
             "markPrice":"3000","rateType":"Special"},
            {"symbol":"ETHUSDT","fundingTime":13,"fundingRate":"0.0001",
             "markPrice":"2990","rateType":"Regular"}
        ]);
        let rows = parse_funding_rows(&data, "ETHUSDT", 0, 20, None).unwrap();
        let history = FundingHistoryRead { observations: rows };
        let updates = history.observed_updates().unwrap();
        assert_eq!(updates[0].ts_event_ns, 10_000_000);
        assert_eq!(updates[0].next_funding_ns, updates[0].ts_event_ns);
        assert_eq!(updates[0].rate_type, "Special");
        assert!(!updates[0].account_payment_confirmed);
        assert!(!history.historical_complete());
        assert!(
            !FundingHistoryRead {
                observations: vec![]
            }
            .historical_complete()
        );
        assert!(parse_funding_rows(&data, "ETHUSDT", 0, 20, Some(10)).is_err());
        let mut duplicate = data;
        duplicate[1]["fundingTime"] = json!(10);
        assert!(parse_funding_rows(&duplicate, "ETHUSDT", 0, 20, None).is_err());
    }
}
