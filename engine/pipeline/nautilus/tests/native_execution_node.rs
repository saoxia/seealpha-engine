//! Offline contract test for native NautilusTrader 2.0.0rc9 execution wiring.
//!
//! This test deliberately stops at LiveNode::build(). It never calls run(),
//! connects, submits an order, reads a real account, or installs an executor
//! in the SeeAlpha production Runtime.
use nautilus_binance::{
    common::enums::BinanceEnvironment,
    config::{
        BinanceDataClientConfig, BinanceExecutionClientConfig,
        BinanceInstrumentProviderConfig,
    },
    factories::{BinanceDataClientFactory, BinanceExecutionClientFactory},
};
use nautilus_common::enums::Environment;
use nautilus_live::{
    config::{LiveExecutionEngineConfig, LiveRiskEngineConfig},
    node::LiveNode,
};
use nautilus_model::{
    data::{NautilusDataType, NautilusRecordType},
    identifiers::TraderId,
};
use nautilus_persistence::config::{DataCatalogConfig, RotationConfig, StreamingConfig};
use seealpha_nautilus_binance_data::native_execution::{
    resolve_native_target, OrderCapability, PositionMode,
};

#[tokio::test]
async fn rc9_live_node_wires_native_usdm_execution_and_reconciliation_without_connecting() {
    let target = resolve_native_target(
        "futures_um", "BTCUSDT-PERP", "BINANCE-UM-OFFLINE-001",
        PositionMode::Hedge, OrderCapability::StandardSingle,
    ).expect("validated Nautilus USD-M target");
    assert_eq!(target.instrument.to_string(), "BTCUSDT-PERP.BINANCE");
    let data_config = BinanceDataClientConfig {
        product_type: target.product,
        environment: BinanceEnvironment::Testnet,
        instrument_provider: BinanceInstrumentProviderConfig {
            load_all: false,
            load_ids: Some(vec!["BTCUSDT-PERP.BINANCE".to_owned()]),
            ..Default::default()
        },
        ..Default::default()
    };
    let exec_config = BinanceExecutionClientConfig {
        account_id: target.account,
        product_type: target.product,
        environment: BinanceEnvironment::Testnet,
        api_key: Some("offline-fixture-key".into()),
        api_secret: Some("offline-fixture-secret".into()),
        use_ws_trading: false,
        // Dual-side accounts must not be silently modeled as Netting.
        oms_type: Some(target.oms),
        ..Default::default()
    };
    data_config.validate().expect("rc9 data config valid");
    exec_config.validate().expect("rc9 execution config valid");

    // Native execution records belong in Nautilus' own writer and Catalog.
    // SeeAlpha never creates a parallel OrderFilled serialization format.
    // These are offline-only temporary paths, unrelated to Engine tenant data.
    let root = std::env::temp_dir().join(format!(
        "seealpha-nautilus-exec-config-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let feather = root.join("feather");
    let catalog = root.join("catalog");
    std::fs::create_dir_all(&feather).expect("test Feather path");
    std::fs::create_dir_all(&catalog).expect("test Catalog path");

    let mut streaming = StreamingConfig::new(
        feather.to_string_lossy().into_owned(),
        Some(DataCatalogConfig::new(
            catalog.to_string_lossy().into_owned(),
            None,
            None,
        )),
        1_000,
        false,
        RotationConfig::NoRotation,
    );
    streaming.data_types = Some(vec![NautilusDataType::TradeTick]);
    // Order ACK, rejection and fill are separate native event families.
    // OrderFilled alone is not sufficient to mark the SeeAlpha Ledger synced.
    streaming.record_types = Some(vec![
        NautilusRecordType::AccountState,
        NautilusRecordType::OrderSubmitted,
        NautilusRecordType::OrderAccepted,
        NautilusRecordType::OrderRejected,
        NautilusRecordType::OrderCanceled,
        NautilusRecordType::OrderExpired,
        NautilusRecordType::OrderFilled,
        NautilusRecordType::OrderFillVoided,
        NautilusRecordType::OrderStatusReport,
        NautilusRecordType::FillReport,
        NautilusRecordType::PositionStatusReport,
    ]);
    streaming.promotion_interval_ms = None;
    streaming.promote_on_close = true;
    streaming.validate().expect("native event Catalog configuration");

    // LiveNode with streaming enabled currently panics while dropping its
    // Tokio worker runtime in this offline build-only harness (rc9).
    // Verify the native streaming builder API without constructing/disposing
    // a streaming-enabled node. Do not disguise a lifecycle failure.
    let _streaming_builder = LiveNode::builder(
        TraderId::from("SEEALPHA-OFFLINE-STREAM-001"),
        Environment::Live,
    )
    .expect("native streaming builder")
    .with_streaming_config(streaming);

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
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn rc9_live_node_wires_native_spot_execution_without_connecting() {
    // The cutover must reuse Nautilus' own Spot execution factory rather than
    // spawning a second SeeAlpha REST order sender.
    let target = resolve_native_target(
        "spot", "BTCUSDT", "BINANCE-SPOT-OFFLINE-001",
        PositionMode::Spot, OrderCapability::StandardSingle,
    ).expect("validated Nautilus Spot target");
    assert_eq!(target.instrument.to_string(), "BTCUSDT.BINANCE");
    let data_config = BinanceDataClientConfig {
        product_type: target.product,
        environment: BinanceEnvironment::Testnet,
        instrument_provider: BinanceInstrumentProviderConfig {
            load_all: false,
            load_ids: Some(vec!["BTCUSDT.BINANCE".to_owned()]),
            ..Default::default()
        },
        ..Default::default()
    };
    let exec_config = BinanceExecutionClientConfig {
        account_id: target.account,
        product_type: target.product,
        environment: BinanceEnvironment::Testnet,
        api_key: Some("offline-fixture-key".into()),
        api_secret: Some("offline-fixture-secret".into()),
        use_ws_trading: false,
        oms_type: Some(target.oms),
        ..Default::default()
    };
    data_config.validate().expect("native Spot data configuration");
    exec_config.validate().expect("native Spot execution configuration");
    let node = LiveNode::builder(
        TraderId::from("SEEALPHA-SPOT-OFFLINE-001"),
        Environment::Live,
    )
    .expect("native Spot node builder")
    .with_name("SEEALPHA-SPOT-EXEC-OFFLINE".to_owned())
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
    .expect("native Spot data client registration")
    .add_exec_client(
        None,
        Box::new(BinanceExecutionClientFactory::new()),
        Box::new(exec_config),
    )
    .expect("native Spot execution client registration")
    .with_reconciliation(true)
    .build()
    .expect("native Spot LiveNode build without connecting");

    // No run(), connect(), submit_order() or exchange credentials.
    drop(node);
}
