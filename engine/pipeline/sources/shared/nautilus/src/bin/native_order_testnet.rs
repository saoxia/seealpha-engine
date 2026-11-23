//! One-shot, explicitly armed Binance *testnet* native execution probe.
//!
//! This is NOT a production sender, not a REST fallback, and not a SeeAlpha OMS.
//! Default action is offline validation. No production environment is selectable.
//! A durable, create-new attempt marker blocks accidental replay after crashes.
//! A human must reconcile unknown/pending orders at the exchange before further tests.
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use nautilus_binance::{
    common::enums::BinanceEnvironment,
    config::{
        BinanceDataClientConfig, BinanceExecutionClientConfig, BinanceInstrumentProviderConfig,
    },
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
    orders::Order,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};
use seealpha_nautilus_binance_data::testnet_order_probe::{
    ProbeKind, TestnetOrderProbe, ValidatedProbe,
};

struct OneShotProbe {
    core: StrategyCore,
    validated: ValidatedProbe,
    marker_path: PathBuf,
    started: bool,
    cancel_requested: bool,
}

impl std::fmt::Debug for OneShotProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OneShotProbe")
            .field("instrument", &self.validated.route.instrument)
            .field("kind", &self.validated.kind)
            .finish()
    }
}

impl OneShotProbe {
    fn new(validated: ValidatedProbe, marker_path: PathBuf) -> Self {
        let config = StrategyConfig {
            strategy_id: Some(StrategyId::from("SA-TESTNET-PROBE-932")),
            // Isolated probe: never claim unrelated external orders.
            ..Default::default()
        };
        Self {
            core: StrategyCore::new(config),
            validated,
            marker_path,
            started: false,
            cancel_requested: false,
        }
    }
}

nautilus_strategy!(OneShotProbe, {
    fn on_order_submitted(&mut self, event: OrderSubmitted) {
        log::info!("testnet order submitted: {}", event.client_order_id);
    }

    fn on_order_accepted(&mut self, event: OrderAccepted) {
        log::info!(
            "testnet accepted: client_order_id={} venue_order_id={}",
            event.client_order_id, event.venue_order_id
        );
        // A deliberate immediate cancel validates both native commands.
        // A cancel request is NOT a cancel confirmation.
        if !self.cancel_requested {
            self.cancel_requested = true;
            if let Err(err) = self.cancel_order(event.client_order_id, None, None) {
                log::error!("testnet cancel command failed: {err:#}");
            }
        }
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        log::warn!("testnet venue rejected order {}: {}", event.client_order_id, event.reason);
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        log::warn!("testnet local order denied: {event:?}");
    }

    fn on_order_cancel_rejected(&mut self, event: OrderCancelRejected) {
        log::warn!("testnet cancel rejected: {event:?}; manual reconciliation required");
    }

    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        log::info!("testnet order canceled: {}", event.client_order_id);
    }

    fn on_order_filled(&mut self, event: &OrderFilled) {
        log::info!(
            "testnet fill (NOT a SeeAlpha ledger posting): order={} trade={} qty={} price={}",
            event.client_order_id, event.trade_id, event.last_qty, event.last_px,
        );
    }
});

impl DataActor for OneShotProbe {
    fn on_start(&mut self) -> Result<()> {
        ensure!(!self.started, "already attempted testnet submission");
        self.started = true;
        let o = &self.validated;
        let order = match o.kind {
            ProbeKind::Limit => self.order().limit(
                o.route.instrument, o.side, o.quantity, o.price,
                Some(TimeInForce::Gtc), None, None, Some(o.reduce_only),
                None, None, None, None, None, None, None, None,
            ),
            ProbeKind::TakeProfitMarket => self.order().market_if_touched(
                o.route.instrument, o.side, o.quantity, o.price,
                None, Some(TimeInForce::Gtc), None, Some(o.reduce_only),
                None, None, None, None, None, None, None,
            ),
        };

        // Record Nautilus's real client ID and sync it BEFORE the first submission.
        // If a crash occurs anywhere afterward, this request ID is permanently
        // considered attempted; never submit it again without manual reconciliation.
        let mut file = OpenOptions::new().append(true).open(&self.marker_path)
            .context("attempt marker disappeared before submit")?;
        writeln!(file, "client_order_id={}", order.client_order_id())?;
        file.sync_all().context("could not persist client order identity")?;
        self.submit_order(order, None, None, None)?;
        log::warn!("testnet submission attempted: cancel is best effort; verify venue state");
        Ok(())
    }
}

/// The file reservation must occur before connecting the execution client.
/// Reuse of the same request_id is unconditionally rejected, even if the
/// previous attempt never reached Binance. No automatic cleanup or replay.
fn reserve_attempt(spec: &TestnetOrderProbe, root: &Path) -> Result<(fs::File, PathBuf)> {
    let meta = fs::symlink_metadata(root).context("existing state directory required")?;
    ensure!(meta.is_dir() && !meta.file_type().is_symlink(),
        "state directory must be a real directory");
    #[cfg(unix)]
    ensure!(meta.permissions().mode() & 0o077 == 0,
        "testnet state directory must be private (0700)");
    let lock_path = root.join(format!("{}.lock", spec.account_id));
    if let Ok(meta) = fs::symlink_metadata(&lock_path) {
        ensure!(meta.is_file() && !meta.file_type().is_symlink(), "unsafe account lock path");
    }
    let lock = OpenOptions::new().read(true).write(true).create(true)
        .truncate(false).open(lock_path)?;
    lock.try_lock_exclusive().context("another probe owns this testnet account")?;

    let marker_path = root.join(format!("{}.attempt", spec.request_id));
    let mut marker = OpenOptions::new().write(true).create_new(true)
        .open(&marker_path)
        .context("request_id already attempted: reconcile at venue; do not retry")?;
    #[cfg(unix)]
    fs::set_permissions(&marker_path, fs::Permissions::from_mode(0o600))?;
    writeln!(marker, "TESTNET ONE-SHOT RESERVED BEFORE CONNECTION")?;
    writeln!(marker, "request_id={}", spec.request_id)?;
    writeln!(marker, "account_id={}", spec.account_id)?;
    writeln!(marker, "product={}", spec.product)?;
    writeln!(marker, "instrument={}", spec.symbol)?;
    marker.sync_all()?;
    Ok((lock, marker_path))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    ensure!(args.len() == 3 || args.len() == 4,
        "usage: native_order_testnet <config.json> <existing-state-dir> [--submit-testnet-once]; absent flag = validate only");
    let config_path = Path::new(&args[1]);
    let meta = fs::symlink_metadata(config_path).context("config file not found")?;
    ensure!(meta.is_file() && !meta.file_type().is_symlink(),
        "config must be a regular file (no symlink)");
    ensure!(meta.len() <= 4096, "testnet probe config too large");
    let config: TestnetOrderProbe = serde_json::from_slice(&fs::read(config_path)?)
        .context("invalid probe JSON")?;
    let validated = config.validate()?;
    if args.len() == 3 {
        println!("VALIDATED OFFLINE; NO connection, NO order, NO account state changed");
        println!(
            "instrument={} side={:?} kind={:?} qty={} price_or_trigger={} reduce_only={} oms={:?}",
            validated.route.instrument, validated.side, validated.kind,
            validated.quantity, validated.price, validated.reduce_only, validated.route.oms,
        );
        return Ok(());
    }
    ensure!(args[3] == "--submit-testnet-once",
        "unknown action (only --submit-testnet-once is supported)");
    // Hard-coded Testnet: no mainnet parameter, no prod credentials.
    let api_key = std::env::var("SEEALPHA_BINANCE_TESTNET_KEY")
        .context("testnet key not set")?;
    let api_secret = std::env::var("SEEALPHA_BINANCE_TESTNET_SECRET")
        .context("testnet secret not set")?;
    ensure!(!api_key.is_empty() && !api_secret.is_empty(),
        "empty testnet credentials");

    let data_config = BinanceDataClientConfig {
        product_type: validated.route.product,
        environment: BinanceEnvironment::Testnet,
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
        environment: BinanceEnvironment::Testnet,
        api_key: Some(api_key.into()),
        api_secret: Some(api_secret.into()),
        use_ws_trading: false,
        oms_type: Some(validated.route.oms),
        ..Default::default()
    };
    data_config.validate()?;
    exec_config.validate()?;
    let (_account_lock, marker_path) = reserve_attempt(&config, Path::new(&args[2]))?;

    let mut node = LiveNode::builder(
        TraderId::from("SA-TESTNET-932"), Environment::Live,
    )?
    .with_name("SEEALPHA-TESTNET-ORDER-PROBE".into())
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
    node.add_strategy(OneShotProbe::new(validated, marker_path))?;
    let outcome = node.run().await;
    node.dispose();
    outcome?;
    Ok(())
}
