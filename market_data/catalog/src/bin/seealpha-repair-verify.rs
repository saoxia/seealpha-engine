//! Fail-closed JSON-line bridge for the opt-in legacy Python repair pipeline.
//! The standalone Rust engine uses the library API directly.
use rust_decimal::Decimal;
use seealpha_catalog::{CatalogBar, MarketProduct, VenueKline, validate_venue_klines};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
}

#[derive(Serialize)]
struct Row {
    start_ms: i64,
    end_ms: i64,
    values: [String; 5],
    count: i64,
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
    let request = serde_json::from_str::<Request>(&payload).map_err(|_| "交易所证据 JSON 无效")?;
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
        }
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
}
