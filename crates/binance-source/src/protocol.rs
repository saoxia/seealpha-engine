//! Strict Binance public combined-stream decoding, independent of network transport.
//!
//! The original exchange identities, timestamps and numeric strings are retained.
//! This module makes no assertion that missed ticks or depth can be backfilled.
use serde_json::{Value, json};
use std::collections::BTreeSet;

use crate::Product;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Channel {
    Trades,
    Mark,
    OptionsBars,
    OptionsPublic,
}

#[derive(Debug, PartialEq)]
pub enum Packet {
    Ack { id: u64 },
    Event { symbol: String, value: Value },
    IgnoredRetired,
}

#[derive(Debug, Eq, PartialEq)]
pub enum DecodeError {
    Oversized,
    Malformed,
    Identity,
    Unsupported,
    Rejected,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DecodeError {}

fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str, DecodeError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or(DecodeError::Malformed)
}
fn integer(value: &Value, key: &str) -> Result<i64, DecodeError> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .filter(|n| *n >= 0)
        .ok_or(DecodeError::Malformed)
}
fn bool_field(value: &Value, key: &str) -> Result<bool, DecodeError> {
    value
        .get(key)
        .and_then(Value::as_bool)
        .ok_or(DecodeError::Malformed)
}
fn value_str_or_number(value: &Value, key: &str) -> Result<String, DecodeError> {
    match value.get(key) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(Value::Number(n)) => Ok(n.to_string()),
        _ => Err(DecodeError::Malformed),
    }
}
fn normalized_public_trade(row: &Value) -> Result<Value, DecodeError> {
    if text(row, "e")? != "trade" {
        return Err(DecodeError::Identity);
    }
    Ok(json!({
        "kind":"trade",
        "row": {
            "id":integer(row,"t")?, "time":integer(row,"T")?,
            "price":value_str_or_number(row,"p")?,
            "qty":value_str_or_number(row,"q")?,
            "isBuyerMaker":bool_field(row,"m")?
        }
    }))
}

/// Inspect one Binance WebSocket text frame. The caller owns subscription
/// request IDs, tracks retired subscriptions and rejects unsupported channels.
/// 1 MiB bound accommodates option depth and rejects abnormal memory pressure.
pub fn decode(
    product: Product,
    channel: Channel,
    bytes: &[u8],
    enabled: &BTreeSet<String>,
    received_ns: u64,
) -> Result<Packet, DecodeError> {
    if bytes.len() > 1_048_576 {
        return Err(DecodeError::Oversized);
    }
    let frame: Value = serde_json::from_slice(bytes).map_err(|_| DecodeError::Malformed)?;
    let object = frame.as_object().ok_or(DecodeError::Malformed)?;
    if let Some(id) = object.get("id") {
        if object.contains_key("code") || object.get("result") != Some(&Value::Null) {
            return Err(DecodeError::Rejected);
        }
        return Ok(Packet::Ack {
            id: id
                .as_u64()
                .or_else(|| id.as_str().and_then(|v| v.parse().ok()))
                .ok_or(DecodeError::Malformed)?,
        });
    }
    let row = frame.get("data").unwrap_or(&frame);
    let symbol = text(row, "s")?.to_owned();
    if !enabled.contains(&symbol) {
        return Ok(Packet::IgnoredRetired);
    }
    let name = object
        .get("stream")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| match (product, channel) {
            (Product::Options, Channel::OptionsBars) => format!(
                "{}@kline_{}",
                symbol.to_ascii_lowercase(),
                row["k"]["i"].as_str().unwrap_or("")
            ),
            (Product::Options, Channel::OptionsPublic) => {
                let suffix = if row["e"] == "trade" {
                    "optionTrade"
                } else if row["e"] == "depthUpdate" {
                    "depth10@100ms"
                } else {
                    "bookTicker"
                };
                format!("{}@{suffix}", symbol.to_ascii_lowercase())
            }
            (_, Channel::Mark) => format!("{}@markPrice@1s", symbol.to_ascii_lowercase()),
            _ => format!("{}@trade", symbol.to_ascii_lowercase()),
        });
    let prefix = format!("{}@", symbol.to_ascii_lowercase());
    if !name.starts_with(&prefix) {
        return Err(DecodeError::Identity);
    }
    let suffix = &name[prefix.len()..];
    let value = match (product, channel, suffix) {
        (Product::Spot | Product::FuturesUm, Channel::Trades, "trade") => {
            normalized_public_trade(row)?
        }
        (Product::FuturesUm, Channel::Mark, "markPrice@1s") => {
            if text(row, "e")? != "markPriceUpdate" {
                return Err(DecodeError::Identity);
            }
            // No synthetic settlement: estimated rate remains separate.
            let _ = (integer(row, "E")?, integer(row, "T")?, text(row, "r")?);
            json!({"kind":"mark","row":row})
        }
        (Product::Options, Channel::OptionsPublic, "optionTrade") => {
            if text(row, "e")? != "trade" || !matches!(text(row, "X")?, "MARKET" | "BLOCK") {
                return Err(DecodeError::Identity);
            }
            let maker = bool_field(row, "m")?;
            json!({"level":"tick","symbol":symbol,"row":{
                "symbol":symbol,"time":integer(row,"T")?,"id":integer(row,"t")?,
                "price":value_str_or_number(row,"p")?,"qty":value_str_or_number(row,"q")?,
                "side":if maker {-1} else {1}
            }})
        }
        (Product::Options, Channel::OptionsPublic, "depth10@100ms") => {
            if text(row, "e")? != "depthUpdate" {
                return Err(DecodeError::Identity);
            }
            let _ = (integer(row, "T")?, integer(row, "u")?);
            let bids = row["b"].as_array().ok_or(DecodeError::Malformed)?;
            let asks = row["a"].as_array().ok_or(DecodeError::Malformed)?;
            if bids.len() > 10 || asks.len() > 10 {
                return Err(DecodeError::Malformed);
            }
            let mut payload = row.clone();
            payload["received_ns"] = json!(received_ns);
            json!({"level":"depth","symbol":symbol,"row":payload})
        }
        (Product::Options, Channel::OptionsPublic, "bookTicker") => {
            let _ = (
                integer(row, "T")?,
                text(row, "b")?,
                text(row, "a")?,
                text(row, "B")?,
                text(row, "A")?,
            );
            let mut payload = row.clone();
            payload["received_ns"] = json!(received_ns);
            json!({"level":"quote","symbol":symbol,"row":payload})
        }
        (Product::Options, Channel::OptionsBars, level) if level.starts_with("kline_") => {
            let interval = &level[6..];
            if !matches!(interval, "1m" | "5m" | "15m") {
                return Err(DecodeError::Unsupported);
            }
            let k = row.get("k").ok_or(DecodeError::Malformed)?;
            if text(k, "s")? != symbol || text(k, "i")? != interval {
                return Err(DecodeError::Identity);
            }
            if !bool_field(k, "x")? {
                return Ok(Packet::IgnoredRetired); // Incomplete K line never claims finality.
            }
            json!({"level":interval,"symbol":symbol,"row":[
                integer(k,"t")?,value_str_or_number(k,"o")?,value_str_or_number(k,"h")?,
                value_str_or_number(k,"l")?,value_str_or_number(k,"c")?,
                value_str_or_number(k,"v")?,integer(k,"T")?
            ]})
        }
        _ => return Err(DecodeError::Unsupported),
    };
    Ok(Packet::Event { symbol, value })
}

pub fn subscription_names(
    symbols: &[String],
    product: Product,
    channel: Channel,
) -> Result<Vec<String>, DecodeError> {
    if (matches!(product, Product::Options)
        != matches!(channel, Channel::OptionsBars | Channel::OptionsPublic))
        || (product == Product::Spot && channel == Channel::Mark)
    {
        return Err(DecodeError::Unsupported);
    }
    let suffixes: &[&str] = match channel {
        Channel::Trades => &["trade"],
        Channel::Mark => &["markPrice@1s"],
        Channel::OptionsBars => &["kline_1m", "kline_5m", "kline_15m"],
        Channel::OptionsPublic => &["optionTrade", "depth10@100ms", "bookTicker"],
    };
    let mut names = Vec::with_capacity(symbols.len() * suffixes.len());
    for symbol in symbols {
        for suffix in suffixes {
            names.push(format!("{}@{suffix}", symbol.to_ascii_lowercase()));
        }
    }
    Ok(names)
}

impl Packet {
    #[cfg(test)]
    fn event_kind(&self) -> &str {
        match self {
            Self::Event { value, .. } => value["kind"].as_str().unwrap_or(""),
            _ => "",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn symbols() -> BTreeSet<String> {
        ["ETHUSDT".into()].into_iter().collect()
    }
    #[test]
    fn perp_and_spot_trade_parity() {
        let packet=br#"{"stream":"ethusdt@trade","data":{"s":"ETHUSDT","e":"trade","t":12,"T":1700000000000,"p":"2100.010","q":"0.2","m":false}}"#;
        for product in [Product::Spot, Product::FuturesUm] {
            let event = decode(product, Channel::Trades, packet, &symbols(), 0).unwrap();
            let Packet::Event { value, .. } = event else {
                panic!("event expected")
            };
            assert_eq!(value["row"]["price"], "2100.010");
            assert_eq!(value["row"]["id"], 12);
            assert_eq!(value["row"]["isBuyerMaker"], false);
        }
    }
    #[test]
    fn rejects_cross_channel_and_bad_confirmation() {
        let packet=br#"{"stream":"ethusdt@markPrice@1s","data":{"s":"ETHUSDT","e":"markPriceUpdate","E":2,"T":5,"r":"0.001"}}"#;
        assert_eq!(
            decode(Product::FuturesUm, Channel::Trades, packet, &symbols(), 0),
            Err(DecodeError::Unsupported)
        );
        assert_eq!(
            decode(Product::Spot, Channel::Mark, packet, &symbols(), 0),
            Err(DecodeError::Unsupported)
        );
        assert_eq!(
            decode(Product::FuturesUm, Channel::Mark, packet, &symbols(), 0)
                .unwrap()
                .event_kind(),
            "mark"
        );
        assert_eq!(
            decode(
                Product::FuturesUm,
                Channel::Trades,
                br#"{"id":"1","result":{"code":1}}"#,
                &symbols(),
                0
            ),
            Err(DecodeError::Rejected)
        );
        assert_eq!(
            decode(
                Product::FuturesUm,
                Channel::Trades,
                br#"{"id":"1","result":null}"#,
                &symbols(),
                0
            ),
            Ok(Packet::Ack { id: 1 })
        );
    }
    #[test]
    fn options_trade_preserves_real_id_and_side() {
        let set = ["BTC-241227-100000-C".into()].into_iter().collect();
        let packet=br#"{"stream":"btc-241227-100000-c@optionTrade","data":{"s":"BTC-241227-100000-C","e":"trade","X":"MARKET","m":true,"T":1700000000000,"t":71,"p":"35.1","q":"0.4"}}"#;
        let Packet::Event { value, .. } =
            decode(Product::Options, Channel::OptionsPublic, packet, &set, 1).unwrap()
        else {
            panic!("not event")
        };
        assert_eq!(value["row"]["side"], -1);
        assert_eq!(value["row"]["id"], 71);
    }
    #[test]
    fn options_closed_only_and_receipt_timestamp() {
        let set = ["BTC-241227-100000-C".into()].into_iter().collect();
        let b=br#"{"stream":"btc-241227-100000-c@kline_1m","data":{"s":"BTC-241227-100000-C","k":{"s":"BTC-241227-100000-C","i":"1m","x":true,"t":1700000000000,"o":"1","h":"2","l":"1","c":"2","v":"5","T":1700000059999}}}"#;
        let Packet::Event { value, .. } =
            decode(Product::Options, Channel::OptionsBars, b, &set, 0).unwrap()
        else {
            panic!("not event")
        };
        assert_eq!(value["row"][0], 1700000000000i64);
        assert_eq!(value["row"][6], 1700000059999i64);
        let d=br#"{"stream":"btc-241227-100000-c@depth10@100ms","data":{"s":"BTC-241227-100000-C","e":"depthUpdate","T":1700000000000,"u":5,"b":[["1","2"]],"a":[["3","4"]]}}"#;
        let Packet::Event { value, .. } =
            decode(Product::Options, Channel::OptionsPublic, d, &set, 99).unwrap()
        else {
            panic!("not event")
        };
        assert_eq!(value["row"]["received_ns"], 99);
        assert_eq!(value["row"]["u"], 5);
    }
    #[test]
    fn subscription_products_stay_separate() {
        assert_eq!(
            subscription_names(&["ETHUSDT".into()], Product::Spot, Channel::Trades).unwrap(),
            vec!["ethusdt@trade"]
        );
        assert!(subscription_names(&["ETHUSDT".into()], Product::Spot, Channel::Mark).is_err());
        assert_eq!(
            subscription_names(
                &["BTC-241227-100000-C".into()],
                Product::Options,
                Channel::OptionsBars
            )
            .unwrap()
            .len(),
            3
        );
    }
}
