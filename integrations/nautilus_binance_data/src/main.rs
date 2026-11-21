//! Native NautilusTrader Spot / USD-M read-only trade collection experiment.
//!
//! No order client, no Binance REST/WS reimplementation, and no READY claim.
//! This binary must NOT be used as a drop-in replacement for Engine sources
//! until Catalog and WS/REST/ZIP handover parity is proved.
use std::{
    num::NonZeroUsize,
    path::Path,
};

use anyhow::{Context, Result, bail, ensure};
use nautilus_binance::{
    common::enums::{BinanceEnvironment, BinanceProductType},
    config::{
        BinanceDataClientConfig, BinanceInstrumentProviderConfig,
        BinanceSpotMarketDataMode,
    },
    factories::BinanceDataClientFactory,
};
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    enums::Environment,
    nautilus_actor,
};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::{NautilusDataType, TradeTick},
    identifiers::{ActorId, ClientId, InstrumentId, TraderId},
};
use nautilus_persistence::config::{DataCatalogConfig, RotationConfig, StreamingConfig};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Product {
    Spot,
    UsdM,
}

impl Product {
    fn from_arg(raw: &str) -> Result<Self> {
        match raw {
            "spot" => Ok(Self::Spot),
            "futures_um" => Ok(Self::UsdM),
            _ => bail!("market must be 'spot' or 'futures_um'"),
        }
    }

    fn binance(self) -> BinanceProductType {
        match self {
            Self::Spot => BinanceProductType::Spot,
            Self::UsdM => BinanceProductType::UsdM,
        }
    }

    fn instrument(self, symbol: &str) -> InstrumentId {
        match self {
            Self::Spot => InstrumentId::from(format!("{symbol}.BINANCE").as_str()),
            Self::UsdM => InstrumentId::from(format!("{symbol}-PERP.BINANCE").as_str()),
        }
    }
}

fn instrument_ids(product: Product, symbols: &str) -> Result<Vec<InstrumentId>> {
    let raw: Vec<&str> = symbols.split(',').collect();
    ensure!(!raw.is_empty() && raw.len() <= 100, "expected 1..100 Binance symbols");
    let mut ids = Vec::with_capacity(raw.len());
    for symbol in raw {
        ensure!(
            (5..=30).contains(&symbol.len())
                && symbol
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
                ,
            "invalid or unsupported symbol: {symbol:?}"
        );
        let id = product.instrument(symbol);
        ensure!(!ids.contains(&id), "duplicate Binance instrument: {symbol}");
        ids.push(id);
    }
    Ok(ids)
}

struct TradeSubscriber {
    core: DataActorCore,
    instrument_ids: Vec<InstrumentId>,
    client_id: ClientId,
    observed: u64,
}

impl std::fmt::Debug for TradeSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TradeSubscriber")
            .field("instrument_ids", &self.instrument_ids)
            .field("observed", &self.observed)
            .finish()
    }
}

impl TradeSubscriber {
    fn new(ids: Vec<InstrumentId>, client: ClientId) -> Self {
        Self {
            core: DataActorCore::new(DataActorConfig {
                actor_id: Some(ActorId::from("SEEALPHA-MKT-001")),
                ..Default::default()
            }),
            instrument_ids: ids,
            client_id: client,
            observed: 0,
        }
    }
}

nautilus_actor!(TradeSubscriber);

impl DataActor for TradeSubscriber {
    fn on_start(&mut self) -> Result<()> {
        // NautilusTrader multiplexes the subscriptions and manages reconnection.
        self.subscribe_trades(self.instrument_ids.clone(), Some(self.client_id), None);

        // Bounded REST source overlap. This is NOT proof of gap-free handover:
        // downstream validation must compare source identities and IDs.
        let ids = self.instrument_ids.clone();
        for id in ids {
            self.request_trades(
                id,
                None,
                None,
                NonZeroUsize::new(1_000),
                Some(self.client_id),
                None,
            )?;
        }
        Ok(())
    }

    fn on_trade(&mut self, _tick: &TradeTick) -> Result<()> {
        self.observed += 1;
        if self.observed.is_multiple_of(10_000) {
            log::info!("Nautilus trades observed: {}", self.observed);
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    ensure!(
        args.len() == 5,
        "usage: seealpha-nautilus-binance-data <spot|futures_um> <SYMBOL[,SYMBOL...]> <existing-feather-root> <existing-catalog-root>"
    );
    let product = Product::from_arg(&args[1])?;
    let ids = instrument_ids(product, &args[2])?;
    for root in [&args[3], &args[4]] {
        let path = Path::new(root);
        ensure!(path.is_dir(), "directory must already exist: {root}");
        ensure!(path.canonicalize()?.is_dir(), "invalid storage path: {root}");
    }
    let client_name = match product {
        Product::Spot => "SEEALPHA-BINANCE-SPOT-DATA",
        Product::UsdM => "SEEALPHA-BINANCE-UM-DATA",
    };
    let client_id = ClientId::from(client_name);

    let config = BinanceDataClientConfig {
        product_type: product.binance(),
        environment: BinanceEnvironment::Live,
        spot_market_data_mode: BinanceSpotMarketDataMode::Json,
        instrument_provider: BinanceInstrumentProviderConfig {
            load_all: false,
            load_ids: Some(ids.iter().map(ToString::to_string).collect()),
            ..Default::default()
        },
        api_key: None,
        api_secret: None,
        ..Default::default()
    };
    config.validate().context("Nautilus Binance data config rejected")?;

    let mut streaming = StreamingConfig::new(
        args[3].clone(),
        Some(DataCatalogConfig::new(args[4].clone(), None, None)),
        1_000,
        false,
        RotationConfig::NoRotation,
    );
    // Only native trade records, not custom SeeAlpha JSON/SQLite writers.
    streaming.data_types = Some(vec![NautilusDataType::TradeTick]);
    // Stage immediately; promote on orderly close to avoid premature claims
    // of gap-free, queryable coverage during session handover.
    streaming.promotion_interval_ms = None;
    streaming.promote_on_close = true;

    // SeeAlpha jobs and the native live node cannot commit to the same
    // Catalog concurrently. Hold this reservation until LiveNode exits.
    let _catalog_writer = seealpha_nautilus_binance_data::reserve_catalog_writer(
        Path::new(&args[4]),
    )?;

    let mut node = LiveNode::builder(TraderId::from("SEEALPHA-DATA-001"), Environment::Live)?
        .with_name(format!("{client_name}-NODE"))
        .with_streaming_config(streaming)
        .with_delay_post_stop_secs(2)
        .add_data_client(
            Some(client_name.to_owned()),
            Box::new(BinanceDataClientFactory::new()),
            Box::new(config),
        )?
        .build()?;
    node.add_actor(TradeSubscriber::new(ids, client_id))?;

    // No execution clients or strategies are registered. Data is not READY
    // until SeeAlpha separately validates REST/WS/ZIP overlap and Catalog state.
    node.run().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spot_and_um_use_distinct_native_instrument_ids() {
        let spot = instrument_ids(Product::Spot, "BTCUSDT,ETHBTC").unwrap();
        let um = instrument_ids(Product::UsdM, "BTCUSDT,ETHUSDT").unwrap();
        assert_eq!(spot[0].to_string(), "BTCUSDT.BINANCE");
        assert_eq!(spot[1].to_string(), "ETHBTC.BINANCE");
        assert_eq!(um[0].to_string(), "BTCUSDT-PERP.BINANCE");
        assert_ne!(spot, um);
    }

    #[test]
    fn rejects_untrusted_or_duplicate_symbols() {
        for symbols in ["", "btcUSDT", "BTC/USDT", "BTCUSDT,BTCUSDT", ",BTCUSDT", "BTCUSDT,"] {
            assert!(instrument_ids(Product::Spot, symbols).is_err());
        }
    }
}
