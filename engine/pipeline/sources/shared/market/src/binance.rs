use crate::{
    DepthBook, DepthLevel, LoadQuery, MarketError, MarketSnapshot, Product, PublicSource,
    PublicTrade, check_symbol, interval_ms, parse_binance_klines, pipeline::json_decimal,
};
use async_trait::async_trait;
use serde_json::Value;
use std::time::Duration;

pub struct BinanceSource {
    client: reqwest::Client,
}

impl BinanceSource {
    pub fn new() -> Result<Self, MarketError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("seealpha-engine/0.1")
            .https_only(true)
            .build()
            .map_err(|_| MarketError::Network)?;
        Ok(Self { client })
    }

    /// Instrument precision comes from Binance's exchangeInfo filters, not
    /// from observed price samples or a guessed fixed decimal scale.
    pub async fn instrument_precisions(
        &self,product:Product,symbol:&str,
    )->Result<(u32,u32),MarketError>{
        check_symbol(symbol)?;
        let endpoint=match product {
            Product::Spot=>"https://data-api.binance.vision/api/v3/exchangeInfo",
            Product::FuturesUm=>"https://fapi.binance.com/fapi/v1/exchangeInfo",
        };
        let body=self.get_json(endpoint,&[("symbol",symbol.to_string())]).await?;
        let items=body["symbols"].as_array().ok_or(MarketError::Payload)?;
        if items.len()!=1 || items[0]["symbol"].as_str()!=Some(symbol){
            return Err(MarketError::Payload);
        }
        let filters=items[0]["filters"].as_array().ok_or(MarketError::Payload)?;
        let tick=filters.iter().find(|v|v["filterType"]=="PRICE_FILTER")
            .and_then(|v|v["tickSize"].as_str()).ok_or(MarketError::Payload)?;
        let step=filters.iter().find(|v|v["filterType"]=="LOT_SIZE")
            .and_then(|v|v["stepSize"].as_str()).ok_or(MarketError::Payload)?;
        Ok((precision_from_increment(tick)?,precision_from_increment(step)?))
    }

    /// Bounded historical closed-bar reads for the native Rust data manager.
    ///
    /// [start_ms,end_ms) is an exact aligned time range. A missing bar,
    /// partial last bar, duplicate or malformed page fails the entire request;
    /// the caller must not commit a partially verified history window.
    pub async fn load_history(
        &self,
        product: Product,
        symbol: &str,
        interval: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<crate::Bar>, MarketError> {
        check_symbol(symbol)?;
        let step = interval_ms(interval)?;
        if start_ms < 0
            || start_ms >= end_ms
            || start_ms % step != 0
            || end_ms % step != 0
            || end_ms.saturating_sub(start_ms) / step > 5000
        {
            return Err(MarketError::Time);
        }
        let endpoint = match product {
            Product::Spot => "https://data-api.binance.vision/api/v3/klines",
            Product::FuturesUm => "https://fapi.binance.com/fapi/v1/klines",
        };
        let mut result = Vec::new();
        let mut clock = crate::clock::ServerClock::new();
        clock
            .sync(
                &self.client,
                match product {
                    Product::Spot => "spot",
                    Product::FuturesUm => "futures_um",
                },
            )
            .await?;
        let now_ms = clock.confirmed_ms()?;
        // A kline with a future exchange close time is still forming,
        // even if Binance returns a syntactically complete OHLCV tuple.
        if end_ms > now_ms {
            return Err(MarketError::Time);
        }
        let mut cursor = start_ms;
        while cursor < end_ms {
            let take = ((end_ms - cursor) / step).min(1000);
            let value = self
                .get_json(
                    endpoint,
                    &[
                        ("symbol", symbol.to_string()),
                        ("interval", interval.to_string()),
                        ("startTime", cursor.to_string()),
                        ("endTime", (end_ms - 1).to_string()),
                        ("limit", take.to_string()),
                    ],
                )
                .await?;
            let page = parse_binance_klines(&value, step)?;
            if page.is_empty() {
                return Err(MarketError::Empty);
            }
            for bar in page {
                if bar.open_time_ms != cursor || bar.close_time_ms >= end_ms {
                    return Err(MarketError::Time);
                }
                cursor = cursor.checked_add(step).ok_or(MarketError::Overflow)?;
                result.push(bar);
            }
        }
        Ok(result)
    }

    async fn get_json(&self, url: &str, query: &[(&str, String)]) -> Result<Value, MarketError> {
        let mut attempt = 0;
        loop {
            let response = self
                .client
                .get(url)
                .query(query)
                .send()
                .await
                .map_err(|_| MarketError::Network)?;
            let status = response.status();
            if status.is_success() {
                return response.json().await.map_err(|_| MarketError::Payload);
            }
            attempt += 1;
            if attempt >= 2 || !matches!(status.as_u16(), 429 | 500 | 502 | 503) {
                return Err(MarketError::Status(status.as_u16()));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

#[async_trait]
impl PublicSource for BinanceSource {
    async fn load(&self, query: &LoadQuery) -> Result<MarketSnapshot, MarketError> {
        check_symbol(&query.symbol)?;
        let step = interval_ms(&query.interval)?;
        let limit = query.limit.clamp(1, 1000);
        let symbol = query.symbol.clone();
        let interval = query.interval.clone();
        // Spot uses Binance's public market-data host. api.binance.com answers
        // 451 from some regions; data-api.binance.vision serves the same
        // unauthenticated kline, trade, and depth payloads.
        let kline_url = match query.product {
            Product::Spot => "https://data-api.binance.vision/api/v3/klines",
            Product::FuturesUm => "https://fapi.binance.com/fapi/v1/klines",
        };
        let trade_url = match query.product {
            Product::Spot => "https://data-api.binance.vision/api/v3/trades",
            Product::FuturesUm => crate::futures_agg_trades::AGG_TRADES_URL,
        };
        let depth_url = match query.product {
            Product::Spot => "https://data-api.binance.vision/api/v3/depth",
            Product::FuturesUm => "https://fapi.binance.com/fapi/v1/depth",
        };
        let klines_json = self
            .get_json(
                kline_url,
                &[
                    ("symbol", symbol.clone()),
                    ("interval", interval.clone()),
                    ("limit", limit.to_string()),
                ],
            )
            .await?;
        let trades_json = self
            .get_json(
                trade_url,
                &[("symbol", symbol.clone()), ("limit", "500".to_string())],
            )
            .await?;
        let depth_json = self
            .get_json(
                depth_url,
                &[("symbol", symbol.clone()), ("limit", "20".to_string())],
            )
            .await?;
        Ok(MarketSnapshot {
            product: query.product,
            symbol,
            interval,
            klines: parse_binance_klines(&klines_json, step)?,
            trades: match query.product {
                Product::Spot => parse_trades(&trades_json)?,
                Product::FuturesUm => {
                    crate::futures_agg_trades::parse_recent_agg_trades(&trades_json)?
                }
            },
            depth: parse_depth(&depth_json)?,
        })
    }
}

fn precision_from_increment(raw:&str)->Result<u32,MarketError>{
    let number=raw.parse::<rust_decimal::Decimal>().map_err(|_|MarketError::Payload)?;
    if number<=rust_decimal::Decimal::ZERO {return Err(MarketError::Payload);}
    let precision=number.normalize().scale();
    if precision>16 {return Err(MarketError::Payload);}
    // Binance tick/step values must be exact powers of ten for a single
    // decimal-place precision to describe their allowed price/quantity grid.
    let expected=rust_decimal::Decimal::new(1,precision);
    if number!=expected {return Err(MarketError::Payload);}
    Ok(precision)
}

#[cfg(test)]
mod precision_tests {
    use super::*;
    #[test]
    fn instrument_filters_are_exact_not_inferred_from_candle_values(){
        assert_eq!(precision_from_increment("0.01000000").unwrap(),2);
        assert_eq!(precision_from_increment("1.00000000").unwrap(),0);
        assert_eq!(precision_from_increment("0.00010000").unwrap(),4);
        assert!(precision_from_increment("0.00000000").is_err());
        assert!(precision_from_increment("0.25000000").is_err());
    }
}

fn parse_trades(value: &Value) -> Result<Vec<PublicTrade>, MarketError> {
    let rows = value.as_array().ok_or(MarketError::Payload)?;
    let mut trades = Vec::with_capacity(rows.len());
    for row in rows {
        let object = row.as_object().ok_or(MarketError::Payload)?;
        trades.push(PublicTrade {
            id: object
                .get("id")
                .and_then(Value::as_i64)
                .ok_or(MarketError::Payload)?,
            timestamp_ms: object
                .get("time")
                .and_then(Value::as_i64)
                .ok_or(MarketError::Payload)?,
            price: json_decimal(object.get("price").ok_or(MarketError::Payload)?)?,
            quantity: json_decimal(object.get("qty").ok_or(MarketError::Payload)?)?,
            buyer_maker: object
                .get("isBuyerMaker")
                .and_then(Value::as_bool)
                .ok_or(MarketError::Payload)?,
        });
    }
    Ok(trades)
}

fn parse_depth(value: &Value) -> Result<DepthBook, MarketError> {
    let object = value.as_object().ok_or(MarketError::Payload)?;
    Ok(DepthBook {
        bids: parse_levels(object.get("bids").ok_or(MarketError::Payload)?)?,
        asks: parse_levels(object.get("asks").ok_or(MarketError::Payload)?)?,
    })
}

fn parse_levels(value: &Value) -> Result<Vec<DepthLevel>, MarketError> {
    let rows = value.as_array().ok_or(MarketError::Payload)?;
    let mut levels = Vec::with_capacity(rows.len());
    for row in rows {
        let columns = row.as_array().ok_or(MarketError::Payload)?;
        if columns.len() < 2 {
            return Err(MarketError::Payload);
        }
        levels.push(DepthLevel {
            price: json_decimal(&columns[0])?,
            quantity: json_decimal(&columns[1])?,
        });
    }
    Ok(levels)
}
