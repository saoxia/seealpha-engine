//! Explicit testnet-only, read-only Nautilus execution account observer.
//!
//! No strategies, no trade commands, no account ledger writes, no fallback
//! REST client. Use isolated process and separate account cursor per product.
use std::{cell::RefCell, path::Path, rc::Rc, time::{SystemTime, UNIX_EPOCH}};

use anyhow::{Context, Result, ensure};
use nautilus_binance::{
    common::enums::{BinanceEnvironment, BinanceProductType},
    config::{BinanceExecutionClientConfig, BinanceInstrumentProviderConfig},
    factories::BinanceExecutionClientFactory,
};
use nautilus_common::enums::Environment;
use nautilus_live::{
    config::{LiveExecutionEngineConfig, LiveRiskEngineConfig},
    node::LiveNode,
};
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{AccountId, TraderId},
};
use seealpha_nautilus_binance_data::native_account_session::{
    NativeAccountSession, NativeAccountSubscription,
};

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_nanos()).ok())
        .unwrap_or(0)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    ensure!(
        args.len() == 4,
        "usage: native_account_observer <spot|futures_um> <NATIVE_ACCOUNT_ID> <existing-state-dir>"
    );
    // The observer supports only Binance testnet. Production access and
    // any trading authority require a separately reviewed integration.
    let (product, account_type, oms) = match args[1].as_str() {
        "spot" => (BinanceProductType::Spot, AccountType::Cash, OmsType::Hedging),
        "futures_um" => (BinanceProductType::UsdM, AccountType::Margin, OmsType::Netting),
        _ => anyhow::bail!("only spot or futures_um testnet is supported"),
    };
    let account_id = &args[2];
    let root = Path::new(&args[3]);
    ensure!(root.is_dir(), "state directory must exist");
    ensure!(!root.symlink_metadata()?.file_type().is_symlink(), "symlink state directory forbidden");
    let checkpoint = root.join(format!("{}-{}.cursor.json", args[1], account_id));
    let session = Rc::new(RefCell::new(NativeAccountSession::open(
        account_id, account_type, &checkpoint,
    )?));

    let api_key = std::env::var("SEEALPHA_BINANCE_TESTNET_KEY")
        .context("SEEALPHA_BINANCE_TESTNET_KEY must be set")?;
    let api_secret = std::env::var("SEEALPHA_BINANCE_TESTNET_SECRET")
        .context("SEEALPHA_BINANCE_TESTNET_SECRET must be set")?;
    ensure!(!api_key.is_empty() && !api_secret.is_empty(), "testnet credentials missing");
    let exec = BinanceExecutionClientConfig {
        account_id: AccountId::from(account_id.as_str()),
        product_type: product,
        environment: BinanceEnvironment::Testnet,
        api_key: Some(api_key.into()),
        api_secret: Some(api_secret.into()),
        use_ws_trading: false,
        oms_type: Some(oms),
        instrument_provider: BinanceInstrumentProviderConfig {
            load_all: false,
            ..Default::default()
        },
        ..Default::default()
    };
    exec.validate().context("native Binance execution account config invalid")?;

    let mut node = LiveNode::builder(
        TraderId::from("SEEALPHA-ACCT-OBSERVER-001"), Environment::Live,
    )?
    .with_name("SEEALPHA-READONLY-ACCOUNT-TESTNET".to_owned())
    .with_exec_engine_config(LiveExecutionEngineConfig {
        open_check_interval_secs: Some(10.0),
        position_check_interval_secs: Some(30.0),
        ..Default::default()
    })
    .with_risk_engine_config(LiveRiskEngineConfig::default())
    .add_exec_client(None, Box::new(BinanceExecutionClientFactory::new()), Box::new(exec))?
    .with_reconciliation(true)
    .with_timeout_connection(30)
    .with_delay_post_stop_secs(2)
    .build()?;

    // Typed event-bus subscription; account and type checked again per event.
    // No Ledger::save_account_snapshot call is possible from this binary.
    let _subscription = NativeAccountSubscription::attach(
        Rc::clone(&session), now_ns, 60_000_000_000,
    );
    let result = node.run().await;
    session.borrow_mut().mark_disconnected();
    result?;
    Ok(())
}
