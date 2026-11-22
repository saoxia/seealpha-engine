//! Public market data feed types and the bar pipeline.
//!
//! `feed` reads Binance public market-data endpoints. `pipeline` validates
//! closed klines, aggregates trades into OHLCV bars, and computes decimal
//! features. Neither path starts a collector as a side effect of a read.
mod binance;
pub mod identity;
#[path = "../../../binance_futures_um/agg_trades.rs"]
mod futures_agg_trades;
#[path = "../../../binance_futures_um/repair.rs"]
pub mod futures_repair;
pub mod clock;
#[path = "../../../binance_futures_um/funding.rs"]
pub mod funding;
#[path = "../../../binance_option/option_stream.rs"]
pub mod option_stream;
#[path = "../../../binance_option/options.rs"]
pub mod options;
mod pipeline;
mod repair;
#[path = "../../../binance_spot/repair.rs"]
pub mod spot_repair;

pub use binance::BinanceSource;
/// Read-only external historical raw trade source for future repair orchestration.
pub use seealpha_clickhouse as clickhouse;
pub use pipeline::{
    aggregate_trades, bar_features, interval_ms, parse_binance_klines, validate_bars,
};

use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MarketError {
    #[error("不支持的行情品种")]
    Symbol,
    #[error("不支持的行情周期")]
    Interval,
    #[error("不支持的公开行情产品")]
    UnsupportedProduct,
    #[error("行情为空")]
    Empty,
    #[error("K 线价格或时间无效")]
    Ohlc,
    #[error("K 线时间无效")]
    Time,
    #[error("成交价格或数量无效")]
    Trade,
    #[error("十进制数值无效")]
    Decimal,
    #[error("数值溢出")]
    Overflow,
    #[error("公开行情请求失败")]
    Network,
    #[error("公开行情响应无法解析")]
    Payload,
    #[error("公开行情服务返回 {0}")]
    Status(u16),
    #[error("行情质量检查失败: {0}")]
    Quality(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Product {
    Spot,
    FuturesUm,
}

impl Product {
    pub fn parse(text: &str) -> Result<Self, MarketError> {
        match text {
            "spot" => Ok(Self::Spot),
            "futures_um" => Ok(Self::FuturesUm),
            _ => Err(MarketError::UnsupportedProduct),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::FuturesUm => "futures_um",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bar {
    pub open_time_ms: i64,
    pub close_time_ms: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Product-scoped trade observation: Spot raw trade ID; USD-M aggregate trade ID.
/// Never merge these different identity domains, or infer full coverage from
/// the bounded REST snapshot.
pub struct PublicTrade {
    pub id: i64,
    pub timestamp_ms: i64,
    pub price: Decimal,
    pub quantity: Decimal,
    pub buyer_maker: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepthLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthBook {
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketSnapshot {
    pub product: Product,
    pub symbol: String,
    pub interval: String,
    pub klines: Vec<Bar>,
    pub trades: Vec<PublicTrade>,
    pub depth: DepthBook,
}

#[derive(Debug, Clone)]
pub struct LoadQuery {
    pub product: Product,
    pub symbol: String,
    pub interval: String,
    pub limit: u32,
}

pub fn check_symbol(symbol: &str) -> Result<(), MarketError> {
    let bytes = symbol.as_bytes();
    if (2..=20).contains(&bytes.len())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        Ok(())
    } else {
        Err(MarketError::Symbol)
    }
}

#[async_trait]
pub trait PublicSource: Send + Sync {
    async fn load(&self, query: &LoadQuery) -> Result<MarketSnapshot, MarketError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarFeature {
    pub open_time_ms: i64,
    pub range_ratio: Decimal,
    pub simple_return: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineOutput {
    pub klines: Vec<Bar>,
    pub trade_bars: Vec<Bar>,
    pub features: Vec<BarFeature>,
}

pub fn run_pipeline(snapshot: &MarketSnapshot) -> Result<PipelineOutput, MarketError> {
    let step = interval_ms(&snapshot.interval)?;
    validate_bars(&snapshot.klines, step)?;
    let trade_bars = aggregate_trades(&snapshot.trades, step)?;
    let features = bar_features(&snapshot.klines)?;
    Ok(PipelineOutput {
        klines: snapshot.klines.clone(),
        trade_bars,
        features,
    })
}
