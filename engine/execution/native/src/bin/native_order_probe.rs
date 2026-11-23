//! Configurable manual Binance native-order probe (Live or Testnet).
//!
//! Uses NautilusTrader's own strategy facade, RiskEngine, ExecutionEngine,
//! BinanceExecutionClient and event callbacks. Not a second OMS or REST client.
//! Running normally submits the configured order; --validate-only does not connect.
use std::{fs, path::Path};
use anyhow::{Context, Result, ensure};
use nautilus_binance::{
    config::{BinanceDataClientConfig, BinanceExecutionClientConfig, BinanceInstrumentProviderConfig},
    factories::{BinanceDataClientFactory, BinanceExecutionClientFactory},
};
use nautilus_common::{actor::DataActor, enums::Environment};
use nautilus_live::{
    config::{LiveExecutionEngineConfig, LiveRiskEngineConfig},
    node::LiveNode,
};
use nautilus_model::{
    enums::TimeInForce,
    events::{
        OrderAccepted, OrderCanceled, OrderCancelRejected, OrderDenied, OrderFilled,
        OrderRejected, OrderSubmitted,
    },
    identifiers::{StrategyId, TraderId},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};
use seealpha_nautilus_execution::order_probe::{
    NativeOrderRequest, OrderKind, TradingEnvironment, ValidatedOrder,
};

struct NativeOrderProbe {
    core: StrategyCore,
    order: ValidatedOrder,
    started: bool,
    cancel_requested: bool,
}

impl std::fmt::Debug for NativeOrderProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeOrderProbe")
            .field("instrument", &self.order.route.instrument)
            .field("kind", &self.order.kind)
            .finish()
    }
}

impl NativeOrderProbe {
    fn new(order: ValidatedOrder) -> Self {
        let config = StrategyConfig {
            strategy_id: Some(StrategyId::from("SA-MANUAL-ORDER-932")),
            ..Default::default()
        };
        Self {
            core: StrategyCore::new(config),
            order,
            started: false,
            cancel_requested: false,
        }
    }
}

nautilus_strategy!(NativeOrderProbe, {
    fn on_order_submitted(&mut self, event: OrderSubmitted) {
        log::info!("order submitted: client_order_id={}", event.client_order_id);
    }

    fn on_order_accepted(&mut self, event: OrderAccepted) {
        log::info!(
            "order accepted: client_order_id={} venue_order_id={}",
            event.client_order_id, event.venue_order_id
        );
        // Cancellation is a caller-controlled test action, never automatic.
        if self.order.cancel_on_accept && !self.cancel_requested {
            self.cancel_requested = true;
            if let Err(err) = self.cancel_order(event.client_order_id, None, None) {
                log::error!("native cancel command failed: {err:#}");
            }
        }
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        log::warn!("venue rejected order {}: {}", event.client_order_id, event.reason);
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        log::warn!("native risk/validation denied order: {event:?}");
    }

    fn on_order_cancel_rejected(&mut self, event: OrderCancelRejected) {
        log::warn!("cancel rejected: {event:?}");
    }

    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        log::info!("order canceled: client_order_id={}", event.client_order_id);
    }

    fn on_order_filled(&mut self, event: &OrderFilled) {
        log::info!(
            "order filled: client_order_id={} trade_id={} qty={} price={} (Ledger not posted)",
            event.client_order_id, event.trade_id, event.last_qty, event.last_px,
        );
    }
});

impl DataActor for NativeOrderProbe {
    fn on_start(&mut self) -> Result<()> {
        ensure!(!self.started, "order already submitted by this actor instance");
        self.started = true;
        let o = &self.order;
        let order = match o.kind {
            OrderKind::Market => self.order().market(
                o.route.instrument, o.side, o.quantity,
                None, Some(o.reduce_only), None, None, None, None, None,
            ),
            OrderKind::Limit => self.order().limit(
                o.route.instrument, o.side, o.quantity,
                o.price.expect("validated limit price"), Some(TimeInForce::Gtc),
                None, None, Some(o.reduce_only),
                None, None, None, None, None, None, None, None,
            ),
            OrderKind::TakeProfitMarket => self.order().market_if_touched(
                o.route.instrument, o.side, o.quantity,
                o.price.expect("validated trigger price"),
                None, Some(TimeInForce::Gtc), None, Some(o.reduce_only),
                None, None, None, None, None, None, None,
            ),
        };
        self.submit_order(order, None, None, None)?;
        log::info!("native order command submitted to Nautilus execution pipeline");
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    ensure!(
        args.len() == 2 || (args.len() == 3 && args[2] == "--validate-only"),
        "usage: native_order_probe <config.json> [--validate-only]"
    );
    let config: NativeOrderRequest = serde_json::from_slice(
        &fs::read(Path::new(&args[1])).context("order config not found")?
    ).context("invalid order JSON")?;
    let validated = config.validate()?;
    if args.len() == 3 {
        println!(
            "validated without network: environment={:?} instrument={} side={:?} kind={:?} quantity={} price_or_trigger={:?} reduce_only={} oms={:?} cancel_on_accept={}",
            validated.environment, validated.route.instrument, validated.side,
            validated.kind, validated.quantity, validated.price,
            validated.reduce_only, validated.route.oms, validated.cancel_on_accept,
        );
        return Ok(());
    }

    let (key_name, secret_name) = match validated.environment {
        TradingEnvironment::Live => ("SEEALPHA_BINANCE_API_KEY", "SEEALPHA_BINANCE_API_SECRET"),
        TradingEnvironment::Testnet => (
            "SEEALPHA_BINANCE_TESTNET_KEY", "SEEALPHA_BINANCE_TESTNET_SECRET"
        ),
    };
    let api_key = std::env::var(key_name).with_context(|| format!("{key_name} not set"))?;
    let api_secret = std::env::var(secret_name)
        .with_context(|| format!("{secret_name} not set"))?;
    ensure!(!api_key.is_empty() && !api_secret.is_empty(), "empty exchange credentials");

    let data_config = BinanceDataClientConfig {
        product_type: validated.route.product,
        environment: validated.environment.native(),
        instrument_provider: BinanceInstrumentProviderConfig {
            load_all: false,
            load_ids: Some(vec![validated.route.instrument.to_string()]),
            ..Default::default()
        },
        ..Default::default()
    };
    let exec_config = BinanceExecutionClientConfig {
        account_id: validated.route.account,
        product_type: validated.route.product,
        environment: validated.environment.native(),
        api_key: Some(api_key.into()),
        api_secret: Some(api_secret.into()),
        use_ws_trading: false,
        oms_type: Some(validated.route.oms),
        ..Default::default()
    };
    data_config.validate()?;
    exec_config.validate()?;

    let mut node = LiveNode::builder(
        TraderId::from("SA-MANUAL-932"), Environment::Live,
    )?
    .with_name("SEEALPHA-NATIVE-ORDER-PROBE".into())
    .with_exec_engine_config(LiveExecutionEngineConfig {
        open_check_interval_secs: Some(10.0),
        position_check_interval_secs: Some(30.0),
        ..Default::default()
    })
    .with_risk_engine_config(LiveRiskEngineConfig::default())
    .add_data_client(None, Box::new(BinanceDataClientFactory::new()), Box::new(data_config))?
    .add_exec_client(None, Box::new(BinanceExecutionClientFactory::new()), Box::new(exec_config))?
    .with_reconciliation(true)
    .with_timeout_connection(30)
    .with_delay_post_stop_secs(5)
    .build()?;
    node.add_strategy(NativeOrderProbe::new(validated))?;
    let outcome = node.run().await;
    node.dispose();
    outcome
}
