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
            Product::FuturesUm => "https://fapi.binance.com/fapi/v1/trades",
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
            trades: parse_trades(&trades_json)?,
            depth: parse_depth(&depth_json)?,
        })
    }
}

pub struct MemorySource {
    snapshot: MarketSnapshot,
}

impl MemorySource {
    pub fn new(snapshot: MarketSnapshot) -> Self {
        Self { snapshot }
    }
}

#[async_trait]
impl PublicSource for MemorySource {
    async fn load(&self, query: &LoadQuery) -> Result<MarketSnapshot, MarketError> {
        if query.symbol != self.snapshot.symbol || query.interval != self.snapshot.interval {
            return Err(MarketError::Empty);
        }
        Ok(self.snapshot.clone())
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
