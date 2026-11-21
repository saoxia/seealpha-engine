//! Offline contract test for native NautilusTrader 2.0.0rc9 execution wiring.
//!
//! This test deliberately stops at LiveNode::build(). It never calls run(),
//! connects, submits an order, reads a real account, or installs an executor
//! in the SeeAlpha production Runtime.
use nautilus_binance::{
    common::enums::{BinanceEnvironment, BinanceProductType},
    config::{
        BinanceDataClientConfig, BinanceExecutionClientConfig,
        BinanceInstrumentProviderConfig, BinanceSpotMarketDataMode,
    },
    factories::{BinanceDataClientFactory, BinanceExecutionClientFactory},
};
use nautilus_common::enums::Environment;
use nautilus_live::{
    config::{LiveExecutionEngineConfig, LiveRiskEngineConfig},
    node::LiveNode,
};
use nautilus_model::{
    enums::OmsType,
    identifiers::{AccountId, TraderId},
};

#[tokio::test]
async fn rc9_live_node_wires_native_usdm_execution_and_reconciliation_without_connecting() {
    let data_config = BinanceDataClientConfig {
        product_type: BinanceProductType::UsdM,
        environment: BinanceEnvironment::Testnet,
        instrument_provider: BinanceInstrumentProviderConfig {
            load_all: false,
            load_ids: Some(vec!["BTCUSDT-PERP.BINANCE".to_owned()]),
            ..Default::default()
        },
        ..Default::default()
    };
    let exec_config = BinanceExecutionClientConfig {
        account_id: AccountId::from("BINANCE-UM-OFFLINE-001"),
        product_type: BinanceProductType::UsdM,
        environment: BinanceEnvironment::Testnet,
        api_key: Some("offline-fixture-key".into()),
        api_secret: Some("offline-fixture-secret".into()),
        use_ws_trading: false,
        // Dual-side accounts must not be silently modeled as Netting.
        oms_type: Some(OmsType::Hedging),
        ..Default::default()
    };
    data_config.validate().expect("rc9 data config valid");
    exec_config.validate().expect("rc9 execution config valid");

    let node = LiveNode::builder(
        TraderId::from("SEEALPHA-OFFLINE-001"),
        Environment::Live,
    )
    .expect("rc9 native node builder")
    .with_name("SEEALPHA-USD-M-EXEC-OFFLINE".to_owned())
    .with_exec_engine_config(LiveExecutionEngineConfig {
        open_check_interval_secs: Some(10.0),
        position_check_interval_secs: Some(30.0),
        ..Default::default()
    })
    .with_risk_engine_config(LiveRiskEngineConfig::default())
    .add_data_client(
        None,
        Box::new(BinanceDataClientFactory::new()),
        Box::new(data_config),
    )
    .expect("native USD-M data client registration")
    .add_exec_client(
        None,
        Box::new(BinanceExecutionClientFactory::new()),
        Box::new(exec_config),
    )
    .expect("native USD-M execution client registration")
    .with_reconciliation(true)
    .build()
    .expect("rc9 LiveNode must construct without connecting");

    // Only construct and drop. NEVER invoke node.run() or add a trading strategy.
    drop(node);
}
