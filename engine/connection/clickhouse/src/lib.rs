//! Read-only ClickHouse access for external Binance Spot raw trade evidence.
//! No migrations, mutations, producer lifecycle, or implicit catalog writes.
use reqwest::{Client, Url};
use rust_decimal::Decimal;
use serde_json::Value;
use std::time::Duration;
use thiserror::Error;

const MAX_ROWS: usize = 10_000;
const QUERY_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, Error)]
pub enum ClickHouseError {
    #[error("ClickHouse configuration is invalid: {0}")]
    Config(&'static str),
    #[error("invalid bounded trade query: {0}")]
    Query(&'static str),
    #[error("ClickHouse request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("ClickHouse returned invalid trade data: {0}")]
    Data(&'static str),
}

#[derive(Clone)]
pub struct ClickHouseConfig {
    /// HTTP(S) endpoint, e.g. http://127.0.0.1:8123
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: String,
}

impl ClickHouseConfig {
    /// Optional connection: absence is not an error until the caller opts in.
    pub fn from_env() -> Result<Option<Self>, ClickHouseError> {
        let url = match std::env::var("SEEALPHA_CLICKHOUSE_URL") {
            Ok(value) if !value.trim().is_empty() => value,
            _ => return Ok(None),
        };
        let config = Self {
            url,
            database: std::env::var("SEEALPHA_CLICKHOUSE_DATABASE")
                .unwrap_or_else(|_| "default".into()),
            user: std::env::var("SEEALPHA_CLICKHOUSE_USER")
                .unwrap_or_else(|_| "default".into()),
            password: std::env::var("SEEALPHA_CLICKHOUSE_PASSWORD").unwrap_or_default(),
        };
        config.validate()?;
        Ok(Some(config))
    }

    fn validate(&self) -> Result<(), ClickHouseError> {
        let url = Url::parse(&self.url).map_err(|_| ClickHouseError::Config("invalid URL"))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(ClickHouseError::Config("URL must be an HTTP(S) endpoint without embedded credentials or query"));
        }
        if !identifier(&self.database) {
            return Err(ClickHouseError::Config("invalid database identifier"));
        }
        if self.user.is_empty() {
            return Err(ClickHouseError::Config("empty user"));
        }
        Ok(())
    }
}

fn identifier(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn symbol_ok(s: &str) -> bool {
    (2..=20).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

/// Values retain the source's full precision. Decimal(32,12) may exceed
/// rust_decimal's 96-bit range, so no narrowing conversion is automatic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpotTrade {
    pub symbol: String,
    pub timestamp_ms: i64,
    pub trade_id: u64,
    pub price: String,
    pub qty: String,
    pub quote_qty: String,
    pub is_buyer_maker: bool,
}

impl SpotTrade {
    pub fn price_decimal(&self) -> Result<Decimal, ClickHouseError> {
        self.price.parse().map_err(|_| ClickHouseError::Data("price exceeds decimal range"))
    }
    pub fn qty_decimal(&self) -> Result<Decimal, ClickHouseError> {
        self.qty.parse().map_err(|_| ClickHouseError::Data("qty exceeds decimal range"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TradeCursor {
    pub timestamp_ms: i64,
    pub trade_id: u64,
}

#[derive(Clone)]
pub struct ClickHouseConnection {
    client: Client,
    config: ClickHouseConfig,
}

impl ClickHouseConnection {
    pub fn new(config: ClickHouseConfig) -> Result<Self, ClickHouseError> {
        config.validate()?;
        let client = Client::builder()
            .timeout(Duration::from_secs(QUERY_TIMEOUT_SECS))
            .build()?;
        Ok(Self { client, config })
    }

    /// Read a bounded half-open UTC interval [start_ms, end_ms).
    /// Cursor is exclusive; continue from the last returned row.
    /// The source is explicitly Spot raw trade, never USD-M aggTrades.
    pub async fn read_spot_trades(
        &self,
        symbol: &str,
        start_ms: i64,
        end_ms: i64,
        after: Option<TradeCursor>,
        limit: usize,
    ) -> Result<Vec<SpotTrade>, ClickHouseError> {
        if !symbol_ok(symbol) || start_ms < 0 || end_ms <= start_ms {
            return Err(ClickHouseError::Query("invalid symbol or UTC range"));
        }
        if !(1..=MAX_ROWS).contains(&limit) {
            return Err(ClickHouseError::Query("limit must be 1..=10000"));
        }
        if after.is_some_and(|c| c.timestamp_ms < start_ms || c.timestamp_ms >= end_ms) {
            return Err(ClickHouseError::Query("cursor is outside the requested time window"));
        }
        // ClickHouse HTTP query parameters keep user input out of the SQL text.
        // FINAL resolves duplicate ReplacingMergeTree versions by its sort key.
        let query = format!(
            "SELECT symbol, toUnixTimestamp64Milli(dt) AS timestamp_ms, trade_id, \
             toString(price) AS price, toString(qty) AS qty, \
             toString(quote_qty) AS quote_qty, is_buyer_maker \
             FROM binance_trade FINAL \
             WHERE symbol = {{symbol:String}} \
             AND dt >= fromUnixTimestamp64Milli({{start_ms:Int64}}, 'UTC') \
             AND dt < fromUnixTimestamp64Milli({{end_ms:Int64}}, 'UTC') \
             AND ({{has_cursor:UInt8}} = 0 OR (dt, trade_id) > \
                  (fromUnixTimestamp64Milli({{cursor_ms:Int64}}, 'UTC'), {{cursor_id:UInt64}})) \
             ORDER BY dt, trade_id LIMIT {limit} FORMAT JSONEachRow"
        );
        let cursor = after.unwrap_or(TradeCursor { timestamp_ms: start_ms, trade_id: 0 });
        let url = Url::parse(&self.config.url)
            .map_err(|_| ClickHouseError::Config("invalid URL"))?;
        let response = self.client.post(url)
            .basic_auth(&self.config.user, Some(&self.config.password))
            .query(&[
                ("database", self.config.database.as_str()),
                ("param_symbol", symbol),
                ("param_start_ms", &start_ms.to_string()),
                ("param_end_ms", &end_ms.to_string()),
                ("param_has_cursor", if after.is_some() { "1" } else { "0" }),
                ("param_cursor_ms", &cursor.timestamp_ms.to_string()),
                ("param_cursor_id", &cursor.trade_id.to_string()),
                ("readonly", "1"),
                ("max_execution_time", "30"),
                ("output_format_json_quote_64bit_integers", "1"),
            ])
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(query)
            .send().await?
            .error_for_status()?;
        let body = response.text().await?;
        let mut rows = Vec::new();
        let mut last: Option<TradeCursor> = after;
        for line in body.lines() {
            if line.trim().is_empty() { continue; }
            let value: Value = serde_json::from_str(line)
                .map_err(|_| ClickHouseError::Data("invalid JSONEachRow response"))?;
            let row = parse_trade(&value)?;
            if row.symbol != symbol || row.timestamp_ms < start_ms || row.timestamp_ms >= end_ms {
                return Err(ClickHouseError::Data("unexpected symbol or timestamp"));
            }
            let key = TradeCursor { timestamp_ms: row.timestamp_ms, trade_id: row.trade_id };
            if last.is_some_and(|prev| (key.timestamp_ms, key.trade_id) <= (prev.timestamp_ms, prev.trade_id)) {
                return Err(ClickHouseError::Data("duplicate or unordered trade identity"));
            }
            last = Some(key);
            rows.push(row);
            if rows.len() > limit {
                return Err(ClickHouseError::Data("response exceeds requested limit"));
            }
        }
        Ok(rows)
    }
}

fn string<'a>(v: &'a Value, field: &str) -> Result<&'a str, ClickHouseError> {
    v.get(field).and_then(Value::as_str).ok_or(ClickHouseError::Data("missing string field"))
}

fn signed(v: &Value, field: &str) -> Result<i64, ClickHouseError> {
    let item = v.get(field).ok_or(ClickHouseError::Data("missing timestamp"))?;
    item.as_i64().or_else(|| item.as_str().and_then(|s| s.parse().ok()))
        .ok_or(ClickHouseError::Data("invalid timestamp"))
}

fn unsigned(v: &Value, field: &str) -> Result<u64, ClickHouseError> {
    let item = v.get(field).ok_or(ClickHouseError::Data("missing trade id"))?;
    item.as_u64().or_else(|| item.as_str().and_then(|s| s.parse().ok()))
        .ok_or(ClickHouseError::Data("invalid trade id"))
}

fn parse_trade(v: &Value) -> Result<SpotTrade, ClickHouseError> {
    let maker = v.get("is_buyer_maker").ok_or(ClickHouseError::Data("missing maker flag"))?;
    let is_buyer_maker = match maker {
        Value::Bool(b) => *b,
        Value::Number(n) if n.as_u64() == Some(0) => false,
        Value::Number(n) if n.as_u64() == Some(1) => true,
        _ => return Err(ClickHouseError::Data("invalid maker flag")),
    };
    let row = SpotTrade {
        symbol: string(v, "symbol")?.to_owned(),
        timestamp_ms: signed(v, "timestamp_ms")?,
        trade_id: unsigned(v, "trade_id")?,
        price: string(v, "price")?.to_owned(),
        qty: string(v, "qty")?.to_owned(),
        quote_qty: string(v, "quote_qty")?.to_owned(),
        is_buyer_maker,
    };
    if row.price.parse::<Decimal>().ok().is_some_and(|d| d <= Decimal::ZERO)
        || row.qty.parse::<Decimal>().ok().is_some_and(|d| d <= Decimal::ZERO) {
        return Err(ClickHouseError::Data("non-positive price or quantity"));
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejects_invalid_endpoint_database_and_symbol() {
        let config = ClickHouseConfig { url: "http://localhost:8123".into(), database: "default".into(), user: "reader".into(), password: "".into() };
        assert!(config.validate().is_ok());
        let mut invalid = config.clone();
        invalid.database = "default;DROP".into();
        assert!(invalid.validate().is_err());
        invalid = config;
        invalid.url = "http://user:secret@localhost:8123".into();
        assert!(invalid.validate().is_err());
        assert!(!symbol_ok("BTCUSDT' OR 1=1"));
    }

    #[test]
    fn parses_decimal_strings_and_u64_without_float_rounding() {
        let row = parse_trade(&json!({
            "symbol":"BTCUSDT", "timestamp_ms": "1780000000000",
            "trade_id":"18446744073709551615",
            "price":"12345.000000000001", "qty":"0.000000000001",
            "quote_qty":"0.000000012345", "is_buyer_maker":1
        })).unwrap();
        assert_eq!(row.trade_id, u64::MAX);
        assert_eq!(row.price, "12345.000000000001");
        assert!(row.is_buyer_maker);
    }
}
