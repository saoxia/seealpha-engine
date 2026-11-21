//! Fail-closed JSON-line bridge for the opt-in legacy Python repair pipeline.
//! The standalone Rust engine uses the library API directly.
use rust_decimal::Decimal;
use seealpha_catalog::{
    CatalogBar, LocalKline, MarketProduct, SavedKline, VenueKline, aggregate_saved_klines,
    validate_venue_klines, venue_kline_differs,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io::{self, Read};
use std::str::FromStr;

#[derive(Deserialize)]
struct Request {
    interval: String,
    product: String,
    start_ms: i64,
    end_ms: i64,
    available_ms: i64,
    rows: Vec<Vec<Value>>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    local: Option<HashMap<String, LocalRow>>,
}

#[derive(Deserialize)]
struct AggregateRequest {
    source_interval: String,
    target_interval: String,
    saved: Vec<SavedRow>,
}

#[derive(Deserialize)]
struct SavedRow {
    event_ns: i64,
    values: [String; 5],
}

#[derive(Serialize)]
struct AggregatedRow {
    values: [String; 5],
    end: i64,
    complete: bool,
}

fn aggregate(request: AggregateRequest) -> Result<HashMap<String, AggregatedRow>, String> {
    if request.saved.len() > 50_000 {
        return Err("本地 K 线记录数量超出限制".into());
    }
    let saved = request
        .saved
        .iter()
        .map(|row| {
            Ok(SavedKline {
                event_ns: row.event_ns,
                values: [
                    decimal(&Value::String(row.values[0].clone()))?,
                    decimal(&Value::String(row.values[1].clone()))?,
                    decimal(&Value::String(row.values[2].clone()))?,
                    decimal(&Value::String(row.values[3].clone()))?,
                    decimal(&Value::String(row.values[4].clone()))?,
                ],
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let buckets = aggregate_saved_klines(
        &saved,
        &request.source_interval,
        &request.target_interval,
    )
    .map_err(|error| error.to_string())?;
    Ok(buckets
        .into_iter()
        .map(|bucket| {
            (
                bucket.start_ms.to_string(),
                AggregatedRow {
                    values: bucket.values.map(|value| value.to_string()),
                    end: bucket.end_ms,
                    complete: bucket.complete,
                },
            )
        })
        .collect())
}

#[derive(Deserialize)]
struct LocalRow {
    values: [String; 5],
    complete: bool,
}

#[derive(Serialize)]
struct Row {
    start_ms: i64,
    end_ms: i64,
    values: [String; 5],
    count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    different: Option<bool>,
}

fn integer(value: &Value) -> Result<i64, String> {
    value.as_i64().ok_or_else(|| "交易所 K 线整数列无效".into())
}

fn decimal(value: &Value) -> Result<Decimal, String> {
    let raw = match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        _ => return Err("交易所 K 线价格或成交量无效".into()),
    };
    Decimal::from_str(&raw).map_err(|_| "交易所 K 线价格或成交量无效".into())
}

fn validate(request: Request) -> Result<Vec<Row>, String> {
    if request.rows.len() > 50_000 {
        return Err("交易所 K 线记录数量超过限制".into());
    }
    let product = MarketProduct::parse(&request.product).map_err(|error| error.to_string())?;
    if request.source.is_some() != request.local.is_some() {
        return Err("原生比较必须同时提供来源与本地 K 线".into());
    }
    if let Some(source) = request.source.as_deref()
        && !matches!(source, "tick" | "1s" | "1m")
    {
        return Err("原生比较来源无效".into());
    }
    let local = request
        .local
        .as_ref()
        .map(|values| {
            values
                .iter()
                .map(|(time, row)| {
                    let timestamp = time.parse::<i64>().map_err(|_| "本地 K 线时间无效")?;
                    let parsed = row
                        .values
                        .iter()
                        .map(|value| {
                            Decimal::from_str(value)
                                .or_else(|_| Decimal::from_scientific(value))
                                .map_err(|_| "本地 K 线数值无效")
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let values: [Decimal; 5] =
                        parsed.try_into().map_err(|_| "本地 K 线数值数量无效")?;
                    Ok((
                        timestamp,
                        LocalKline {
                            values,
                            complete: row.complete,
                        },
                    ))
                })
                .collect::<Result<HashMap<_, _>, &str>>()
        })
        .transpose()?;
    let mut native = Vec::with_capacity(request.rows.len());
    let mut output = Vec::with_capacity(request.rows.len());
    for values in &request.rows {
        if values.len() < 9 {
            return Err("交易所 K 线缺少必需列".into());
        }
        let start_ms = integer(&values[0])?;
        let end_ms = integer(&values[6])?
            .checked_add(1)
            .ok_or("交易所 K 线结束时间溢出")?;
        let price = [
            decimal(&values[1])?,
            decimal(&values[2])?,
            decimal(&values[3])?,
            decimal(&values[4])?,
            decimal(&values[5])?,
        ];
        let count = integer(&values[8])?;
        native.push(VenueKline {
            bar: CatalogBar {
                open_time_ms: start_ms,
                open: price[0],
                high: price[1],
                low: price[2],
                close: price[3],
                volume: price[4],
            },
            end_ms,
            trade_count: count,
        });
        output.push(Row {
            start_ms,
            end_ms,
            values: price.map(|value| value.to_string()),
            count,
            different: None,
        });
    }
    validate_venue_klines(
        &native,
        &request.interval,
        request.start_ms,
        request.end_ms,
        request.available_ms,
        product,
    )
    .map_err(|error| error.to_string())?;
    if let (Some(source), Some(local)) = (request.source.as_deref(), local.as_ref()) {
        for (row, venue) in output.iter_mut().zip(&native) {
            row.different = Some(venue_kline_differs(
                local.get(&venue.bar.open_time_ms),
                venue,
                source,
            ));
        }
    }
    Ok(output)
}

fn run() -> Result<(), String> {
    // Bounded input: never let a remote response exhaust a repair worker.
    let mut payload = String::new();
    io::stdin()
        .take(8 * 1024 * 1024 + 1)
        .read_to_string(&mut payload)
        .map_err(|_| "读取证据失败")?;
    if payload.len() > 8 * 1024 * 1024 {
        return Err("交易所证据超过容量限制".into());
    }
    let value: Value = serde_json::from_str(&payload).map_err(|_| "交易所证据 JSON 无效")?;
    if value.get("mode").and_then(Value::as_str) == Some("aggregate") {
        let request = serde_json::from_value::<AggregateRequest>(value)
            .map_err(|_| "本地聚合请求无效")?;
        let result = aggregate(request)?;
        return serde_json::to_writer(io::stdout(), &result)
            .map_err(|_| "写入聚合结果失败".into());
    }
    let request = serde_json::from_value::<Request>(value)
        .map_err(|_| "交易所证据 JSON 无效")?;
    let result = validate(request)?;
    serde_json::to_writer(io::stdout(), &result).map_err(|_| "写入校验结果失败".into())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(rows: Vec<Vec<Value>>) -> Request {
        Request {
            interval: "1m".into(),
            product: "futures_um".into(),
            start_ms: 0,
            end_ms: 60_000,
            available_ms: 0,
            rows,
            source: None,
            local: None,
        }
    }

    #[test]
    fn aggregates_saved_source_bars_for_legacy_repair() {
        let row = |minute: i64| SavedRow {
            event_ns: (minute * 60_000 + 59_999) * 1_000_000,
            values: [
                "10.00".into(),
                "12.00".into(),
                "9.00".into(),
                "11.00".into(),
                "1.000".into(),
            ],
        };
        let aggregate_result = aggregate(AggregateRequest {
            source_interval: "1m".into(),
            target_interval: "5m".into(),
            saved: vec![row(0), row(1), row(2), row(3), row(4)],
        })
        .unwrap();
        assert_eq!(aggregate_result["0"].end, 300_000);
        assert_eq!(aggregate_result["0"].values[4], "5.000");
        assert!(aggregate_result["0"].complete);
    }

    #[test]
    fn valid_payload_round_trips_exact_decimals() {
        let values = vec![
            json!(0),
            json!("10.00"),
            json!("12.00"),
            json!("9.00"),
            json!("11.00"),
            json!("2.000"),
            json!(59_999),
            json!("0"),
            json!(2),
        ];
        let output = validate(request(vec![values])).unwrap();
        assert_eq!(output[0].values[0], "10.00");
        assert_eq!(output[0].values[4], "2.000");
    }

    #[test]
    fn invalid_remote_values_fail_closed() {
        let values = vec![
            json!(0),
            json!("NaN"),
            json!("12"),
            json!("9"),
            json!("11"),
            json!("2"),
            json!(59_999),
            json!("0"),
            json!(2),
        ];
        assert!(validate(request(vec![values])).is_err());
    }

    #[test]
    fn compare_local_decimals_without_python_fallback() {
        let values = vec![
            json!(0),
            json!("10.00"),
            json!("12.00"),
            json!("9.00"),
            json!("11.00"),
            json!("2.000"),
            json!(59_999),
            json!("0"),
            json!(2),
        ];
        let mut input = request(vec![values]);
        input.source = Some("1m".into());
        input.local = Some(HashMap::from([(
            "0".into(),
            LocalRow {
                values: [
                    "10.0".into(),
                    "12".into(),
                    "9".into(),
                    "11".into(),
                    "2".into(),
                ],
                complete: true,
            },
        )]));
        assert_eq!(validate(input).unwrap()[0].different, Some(false));
    }

    #[test]
    fn comparison_fails_closed_without_local_counterpart() {
        let values = vec![
            json!(0),
            json!("10"),
            json!("12"),
            json!("9"),
            json!("11"),
            json!("2"),
            json!(59_999),
            json!("0"),
            json!(2),
        ];
        let mut input = request(vec![values]);
        input.source = Some("1m".into());
        assert!(validate(input).is_err());
    }
}
