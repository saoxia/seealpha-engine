//! Isolated public Binance options market-data adapter.
//! Unlike USD-M or Spot, an option symbol includes expiry, strike and side.
//! This module never owns credentials or places orders.
use crate::{Bar, MarketError, interval_ms, parse_binance_klines};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::time::Duration;

const KLINES: &str = "https://eapi.binance.com/eapi/v1/klines";
const EXCHANGE_INFO: &str = "https://eapi.binance.com/eapi/v1/exchangeInfo";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OptionIdentity {
    pub symbol: String,
    pub underlying: String,
    pub expiry: String,
    pub strike: Decimal,
    pub right: OptionRight,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OptionRight {
    Call,
    Put,
}

fn leap(year: u32) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}
fn valid_expiry(date: &str) -> bool {
    if date.len() != 6 || !date.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    let y = 2000 + date[0..2].parse::<u32>().unwrap_or(0);
    let m = date[2..4].parse::<u32>().unwrap_or(0);
    let d = date[4..6].parse::<u32>().unwrap_or(0);
    let last = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap(y) => 29,
        2 => 28,
        _ => return false,
    };
    (1..=last).contains(&d)
}
pub fn parse_option_symbol(raw: &str) -> Result<OptionIdentity, MarketError> {
    if raw.len() > 60 || raw != raw.trim() || raw != raw.to_ascii_uppercase() {
        return Err(MarketError::Symbol);
    }
    let items = raw.split('-').collect::<Vec<_>>();
    if items.len() != 4 {
        return Err(MarketError::Symbol);
    }
    let underlying = items[0];
    if underlying.len() < 2
        || underlying.len() > 15
        || !underlying
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        || !valid_expiry(items[1])
    {
        return Err(MarketError::Symbol);
    }
    let strike = items[2]
        .parse::<Decimal>()
        .map_err(|_| MarketError::Symbol)?;
    if strike <= Decimal::ZERO
        || items[2].starts_with('-')
        || !items[2].bytes().all(|b| b.is_ascii_digit() || b == b'.')
        || items[2].matches('.').count() > 1
    {
        return Err(MarketError::Symbol);
    }
    let right = match items[3] {
        "C" => OptionRight::Call,
        "P" => OptionRight::Put,
        _ => return Err(MarketError::Symbol),
    };
    Ok(OptionIdentity {
        symbol: raw.to_owned(),
        underlying: underlying.to_owned(),
        expiry: items[1].to_owned(),
        strike,
        right,
    })
}

/** Parse exchange-authored, fully closed option candles in either Binance
 * tuple form or the legacy object form. Never synthesize zero-volume gaps. */
pub fn parse_options_klines(value: &Value, step_ms: i64) -> Result<Vec<Bar>, MarketError> {
    let rows = value.as_array().ok_or(MarketError::Payload)?;
    let mut normalized = Vec::with_capacity(rows.len());
    for row in rows {
        if row.is_array() {
            normalized.push(row.clone());
        } else if row.is_object() {
            normalized.push(json!([
                row["openTime"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                row["closeTime"]
            ]));
        } else {
            return Err(MarketError::Payload);
        }
    }
    parse_binance_klines(&Value::Array(normalized), step_ms)
}

/// A recent public trade is an observation, never a synthetic fill.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OptionTrade {
    pub symbol: String,
    pub trade_id: i64,
    pub timestamp_ms: i64,
    pub price: Decimal,
    pub quantity: Decimal,
    pub aggressor_side: i8,
}

/// REST /trades is recent-only; even a nonempty response cannot certify a
/// historical interval. Id gaps are retained as audit evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecentOptionTrades {
    pub observations: Vec<OptionTrade>,
    pub suspected_id_gap: bool,
}
impl RecentOptionTrades {
    pub fn historical_complete(&self) -> bool {
        false
    }
}

pub fn parse_recent_option_trades(
    value: &Value,
    requested_symbol: &str,
) -> Result<RecentOptionTrades, MarketError> {
    parse_option_symbol(requested_symbol)?;
    let rows = value.as_array().ok_or(MarketError::Payload)?;
    if rows.len() > 500 {
        return Err(MarketError::Payload);
    }
    let mut unique = BTreeMap::new();
    for row in rows {
        let symbol = row["symbol"].as_str().unwrap_or(requested_symbol);
        if symbol != requested_symbol {
            return Err(MarketError::Symbol);
        }
        let id = row
            .get("tradeId")
            .or_else(|| row.get("id"))
            .and_then(Value::as_i64)
            .ok_or(MarketError::Payload)?;
        let time = row["time"].as_i64().ok_or(MarketError::Time)?;
        let side = row["side"].as_i64().ok_or(MarketError::Payload)?;
        let price = crate::pipeline::json_decimal(&row["price"])?;
        let quantity = crate::pipeline::json_decimal(&row["qty"])?;
        if id < 0
            || time < 0
            || !matches!(side, -1 | 1)
            || price <= Decimal::ZERO
            || quantity <= Decimal::ZERO
        {
            return Err(MarketError::Trade);
        }
        let trade = OptionTrade {
            symbol: symbol.to_owned(),
            trade_id: id,
            timestamp_ms: time,
            price,
            quantity,
            aggressor_side: side as i8,
        };
        if let Some(previous) = unique.insert(id, trade.clone())
            && previous != trade
        {
            return Err(MarketError::Trade);
        }
    }
    let suspected_id_gap = unique
        .keys()
        .zip(unique.keys().skip(1))
        .any(|(a, b)| a.checked_add(1) != Some(*b));
    let mut observations: Vec<_> = unique.into_values().collect();
    observations.sort_by_key(|trade| (trade.timestamp_ms, trade.trade_id));
    Ok(RecentOptionTrades {
        observations,
        suspected_id_gap,
    })
}

/// EAPI bookTicker is a best-bid/ask observation, not a recoverable historical
/// order book or proof that a disconnected interval contained no changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OptionQuote {
    pub symbol: String,
    pub event_time_ms: i64,
    pub bid_price: Decimal,
    pub ask_price: Decimal,
    pub bid_size: Decimal,
    pub ask_size: Decimal,
}
pub fn parse_option_book_ticker(
    value: &Value,
    requested_symbol: &str,
) -> Result<OptionQuote, MarketError> {
    parse_option_symbol(requested_symbol)?;
    if value["s"].as_str() != Some(requested_symbol) {
        return Err(MarketError::Symbol);
    }
    let time = value["T"].as_i64().ok_or(MarketError::Time)?;
    let bid = crate::pipeline::json_decimal(&value["b"])?;
    let ask = crate::pipeline::json_decimal(&value["a"])?;
    let bid_size = crate::pipeline::json_decimal(&value["B"])?;
    let ask_size = crate::pipeline::json_decimal(&value["A"])?;
    if time < 0
        || [bid, ask, bid_size, ask_size]
            .iter()
            .any(|v| *v < Decimal::ZERO)
        || (bid_size > Decimal::ZERO && bid <= Decimal::ZERO)
        || (ask_size > Decimal::ZERO && ask <= Decimal::ZERO)
        || (bid_size > Decimal::ZERO && ask_size > Decimal::ZERO && bid > ask)
    {
        return Err(MarketError::Payload);
    }
    Ok(OptionQuote {
        symbol: requested_symbol.to_owned(),
        event_time_ms: time,
        bid_price: bid,
        ask_price: ask,
        bid_size,
        ask_size,
    })
}

#[derive(Clone)]
pub struct OptionsSource {
    client: reqwest::Client,
}
impl OptionsSource {
    pub fn new() -> Result<Self, MarketError> {
        let client = reqwest::Client::builder()
            .https_only(true)
            .timeout(Duration::from_secs(12))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("seealpha-native-options/0.1")
            .build()
            .map_err(|_| MarketError::Network)?;
        Ok(Self { client })
    }
    async fn get(&self, url: &str, params: &[(&str, String)]) -> Result<Value, MarketError> {
        let response = self
            .client
            .get(url)
            .query(params)
            .send()
            .await
            .map_err(|_| MarketError::Network)?;
        let status = response.status();
        if !status.is_success() {
            return Err(MarketError::Status(status.as_u16()));
        }
        response
            .json::<Value>()
            .await
            .map_err(|_| MarketError::Payload)
    }
    /// Return exchange-authored contracts only; never infer strike, size or
    /// exercise characteristics from a locally guessed symbol.
    pub async fn contracts(&self) -> Result<Vec<Value>, MarketError> {
        let data = self.get(EXCHANGE_INFO, &[]).await?;
        let rows = data["optionSymbols"]
            .as_array()
            .ok_or(MarketError::Payload)?;
        if rows.len() > 50_000 {
            return Err(MarketError::Payload);
        }
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let symbol = row["symbol"].as_str().ok_or(MarketError::Payload)?;
            let id = parse_option_symbol(symbol)?;
            let expiry = row["expiryDate"].as_i64().ok_or(MarketError::Payload)?;
            let status = row["status"].as_str().ok_or(MarketError::Payload)?;
            let underlying = row["underlying"].as_str().ok_or(MarketError::Payload)?;
            let unit_text = if let Some(value) = row["unit"].as_str() {
                value.to_owned()
            } else if let Some(value) = row["unit"].as_number() {
                value.to_string()
            } else {
                return Err(MarketError::Payload);
            };
            let unit = unit_text
                .parse::<Decimal>()
                .map_err(|_| MarketError::Payload)?;
            if unit <= Decimal::ZERO || expiry <= 0 || underlying.is_empty() {
                return Err(MarketError::Payload);
            }
            items.push(json!({"symbol":id.symbol,"underlying":underlying,"expiry_ms":expiry,
                "strike":id.strike.to_string(),"right":if id.right==OptionRight::Call{"CALL"}else{"PUT"},
                "unit":unit.to_string(),"status":status,"filters":row["filters"]}));
        }
        Ok(items)
    }
    /// Strict [start,end) history. Binance may legitimately have no trades:
    /// a missing bar is not silently filled or declared complete.
    pub async fn history(
        &self,
        symbol: &str,
        interval: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<Bar>, MarketError> {
        parse_option_symbol(symbol)?;
        if !matches!(interval, "1m" | "5m" | "15m") {
            return Err(MarketError::Interval);
        }
        let step = interval_ms(interval)?;
        if start < 0
            || end <= start
            || start % step != 0
            || end % step != 0
            || (end - start) / step > 5000
        {
            return Err(MarketError::Time);
        }
        let mut clock = crate::clock::ServerClock::new();
        clock.sync(&self.client, "options").await?;
        if end > clock.confirmed_ms()? {
            return Err(MarketError::Time);
        }
        let mut out = Vec::new();
        let mut cursor = start;
        while cursor < end {
            let chunk = ((end - cursor) / step).min(1000);
            let payload = self
                .get(
                    KLINES,
                    &[
                        ("symbol", symbol.to_owned()),
                        ("interval", interval.to_owned()),
                        ("startTime", cursor.to_string()),
                        ("endTime", (end - 1).to_string()),
                        ("limit", chunk.to_string()),
                    ],
                )
                .await?;
            let bars = parse_options_klines(&payload, step)?;
            for bar in bars {
                if bar.open_time_ms != cursor || bar.close_time_ms >= end {
                    return Err(MarketError::Time);
                }
                cursor = cursor.checked_add(step).ok_or(MarketError::Overflow)?;
                out.push(bar);
            }
            if out.is_empty()
                || cursor == start
                || (cursor < end && payload.as_array().is_some_and(Vec::is_empty))
            {
                return Err(MarketError::Empty);
            }
        }
        Ok(out)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn options_kline_supports_official_tuple_and_legacy_object_shapes() {
        let tuple = json!([[0, "50", "60", "40", "55", "4", 59999, "0", 1, "0", "0", "0"]]);
        let object = json!([{"openTime":0,"closeTime":59999,"open":"50",
            "high":"60","low":"40","close":"55","volume":"4"}]);
        let tuple_bars = parse_options_klines(&tuple, 60_000).unwrap();
        let object_bars = parse_options_klines(&object, 60_000).unwrap();
        assert_eq!(tuple_bars, object_bars);
        let wrong = json!([{"openTime":0,"closeTime":60_000,
            "open":"50","high":"60","low":"40","close":"55","volume":"4"}]);
        assert!(parse_options_klines(&wrong, 60_000).is_err());
        assert!(parse_options_klines(&json!([tuple[0], object[0]]), 60_000).is_err());
    }

    #[test]
    fn option_contract_identity_is_not_a_generic_spot_symbol() {
        let id = parse_option_symbol("BTC-271231-90000-C").unwrap();
        assert_eq!(id.right, OptionRight::Call);
        assert_eq!(id.strike, Decimal::from(90000));
        for bad in [
            "ALL",
            "BTCUSDT",
            "BTC-270230-90000-C",
            "BTC-271231-0-C",
            "BTC-271231-90000-Q",
            "BTC-271231-90000-C/../../",
            "btc-271231-90000-c",
        ] {
            assert!(parse_option_symbol(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn recent_option_trades_are_not_historical_fills_or_complete_windows() {
        let symbol = "BTC-271231-90000-C";
        let data = json!([
            {"symbol":symbol,"tradeId":12,"time":200,"price":"12.50","qty":"0.2","side":1},
            {"symbol":symbol,"tradeId":10,"time":100,"price":"11","qty":"1","side":-1},
            {"symbol":symbol,"tradeId":10,"time":100,"price":"11","qty":"1","side":-1}
        ]);
        let result = parse_recent_option_trades(&data, symbol).unwrap();
        assert_eq!(result.observations.len(), 2);
        assert_eq!(result.observations[0].trade_id, 10);
        assert!(result.suspected_id_gap);
        assert!(!result.historical_complete());
        assert!(
            !parse_recent_option_trades(&json!([]), symbol)
                .unwrap()
                .historical_complete()
        );
        let mut conflict = data.clone();
        conflict[2]["price"] = json!("99");
        assert!(parse_recent_option_trades(&conflict, symbol).is_err());
        let mut wrong = data;
        wrong[0]["symbol"] = json!("ETH-271231-90000-C");
        assert!(parse_recent_option_trades(&wrong, symbol).is_err());
    }

    #[test]
    fn option_quote_rejects_crossed_books_and_non_exchange_timestamps() {
        let symbol = "BTC-271231-90000-P";
        let row = json!({"s":symbol,"T":120,"b":"10","a":"11","B":"2","A":"3"});
        let quote = parse_option_book_ticker(&row, symbol).unwrap();
        assert_eq!(quote.bid_size, Decimal::from(2));
        let mut crossed = row.clone();
        crossed["b"] = json!("12");
        assert!(parse_option_book_ticker(&crossed, symbol).is_err());
        let mut wrong = row.clone();
        wrong["T"] = json!(-1);
        assert!(parse_option_book_ticker(&wrong, symbol).is_err());
        let mut wrong_symbol = row;
        wrong_symbol["s"] = json!("BTC-271231-90000-C");
        assert!(parse_option_book_ticker(&wrong_symbol, symbol).is_err());
    }
}
