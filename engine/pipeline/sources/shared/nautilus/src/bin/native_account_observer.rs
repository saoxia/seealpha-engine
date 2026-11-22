//! Explicit testnet-only, read-only Nautilus execution account observer.
//!
//! No strategies, no trade commands, no account ledger writes, no fallback
//! REST client. Use isolated process and separate account cursor per product.
use std::{
    cell::RefCell,
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    rc::Rc,
    time::{SystemTime, UNIX_EPOCH},
};
use fs2::FileExt;
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
#[cfg(unix)]
use tokio::{io::AsyncWriteExt, net::UnixListener, task::LocalSet};

use anyhow::{Context, Result, ensure};
use nautilus_binance::{
    common::enums::{BinanceEnvironment, BinanceProductType},
    config::{BinanceExecutionClientConfig, BinanceInstrumentProviderConfig},
    factories::BinanceExecutionClientFactory,
};
use nautilus_common::{cache::CacheConfig, enums::Environment};
use nautilus_infrastructure::sql::cache::PostgresCacheConfig;
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

/// An explicitly enabled native PostgreSQL Cache. The password is read from a
/// mounted local file and never accepted in command-line arguments or logs.
fn postgres_cache_from_env() -> Result<Option<PostgresCacheConfig>> {
    match std::env::var("SEEALPHA_ENABLE_POSTGRES_CACHE") {
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Ok(value) if value == "0" => return Ok(None),
        Ok(value) if value == "1" => {}
        _ => anyhow::bail!("SEEALPHA_ENABLE_POSTGRES_CACHE must be 0 or 1"),
    }
    let password_file = std::env::var("SEEALPHA_POSTGRES_CACHE_PASSWORD_FILE")
        .context("PostgreSQL cache enabled without password file")?;
    let password_file = Path::new(&password_file);
    let meta = fs::symlink_metadata(password_file)
        .context("PostgreSQL cache password file unavailable")?;
    ensure!(meta.is_file() && !meta.file_type().is_symlink(),
        "PostgreSQL credential must be a regular file");
    let password = fs::read_to_string(password_file)
        .context("cannot read PostgreSQL cache password")?;
    ensure!(!password.is_empty() && password.len() <= 512
        && password.bytes().all(|b| b.is_ascii_alphanumeric()),
        "invalid PostgreSQL cache password");
    Ok(Some(PostgresCacheConfig {
        host: Some("127.0.0.1".to_owned()),
        port: Some(5432),
        username: Some("seealpha_cache".to_owned()),
        password: Some(password),
        database: Some("seealpha_cache".to_owned()),
    }))
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_nanos()).ok())
        .unwrap_or(0)
}

// The socket is deliberately read-only. Filesystem permissions and a pinned
// tenant/account binding are required; it is not a trading authority.
#[cfg(unix)]
fn account_socket_path() -> Result<Option<(PathBuf, String, String)>> {
    let Ok(dir) = std::env::var("SEEALPHA_ACCOUNT_SOCKET_DIR") else {
        return Ok(None);
    };
    let tenant = std::env::var("SEEALPHA_TENANT_ID")
        .context("native account socket requires SEEALPHA_TENANT_ID")?;
    let account = std::env::var("SEEALPHA_ENGINE_ACCOUNT_ID")
        .context("native account socket requires SEEALPHA_ENGINE_ACCOUNT_ID")?;
    for id in [&tenant, &account] {
        ensure!(!id.is_empty() && id.len() <= 64
            && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "invalid tenant or engine account identity");
    }
    let dir = PathBuf::from(dir);
    ensure!(dir.is_absolute(), "native account socket dir must be absolute");
    let meta = fs::symlink_metadata(&dir).context("native account socket directory missing")?;
    ensure!(meta.is_dir() && !meta.file_type().is_symlink()
        && meta.permissions().mode() & 0o077 == 0,
        "native account socket directory must be private (0700)");
    let path = dir.join(format!("{tenant}--{account}--sandbox.sock"));
    ensure!(!path.exists() && fs::symlink_metadata(&path).is_err(),
        "native account socket path is occupied; refuse to replace it");
    Ok(Some((path, tenant, account)))
}

#[cfg(unix)]
async fn serve_account_socket(
    listener: UnixListener,
    session: Rc<RefCell<NativeAccountSession>>,
    tenant: String,
    account: String,
) {
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            break;
        };
        let view = session.borrow().fresh(now_ns(), 60_000_000_000);
        let payload = match view {
            Some(view) => serde_json::json!({
                "tenant_id": tenant, "account_id": account,
                "environment": "sandbox", "source": "nautilus_testnet_read_only",
                "native_account": view,
            }),
            None => serde_json::json!({"error": "native account report unavailable or stale"}),
        };
        if let Ok(mut data) = serde_json::to_vec(&payload) {
            data.push(b'\n');
            let _ = stream.write_all(&data).await;
        }
    }
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
    // Validate before composing any on-disk lock or cursor filename.
    ensure!(
        !account_id.is_empty()
            && account_id.len() <= 128
            && account_id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "invalid native account ID"
    );
    let root = Path::new(&args[3]);
    ensure!(root.is_dir(), "state directory must exist");
    ensure!(!root.symlink_metadata()?.file_type().is_symlink(), "symlink state directory forbidden");
    let lock = root.join(format!("{}-{}.cursor.lock", args[1], account_id));
    if let Ok(meta) = fs::symlink_metadata(&lock) {
        ensure!(meta.is_file() && !meta.file_type().is_symlink(), "invalid cursor lock file");
    }
    // Keep the file handle alive while the node runs: a second instance must
    // not update the same recovery cursor concurrently.
    let _cursor_lock = OpenOptions::new()
        .read(true).write(true).create(true).truncate(false).open(&lock)?;
    _cursor_lock.try_lock_exclusive().context("native account observer already active")?;
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

    let pg_cache = postgres_cache_from_env()?;
    let mut builder = LiveNode::builder(
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
    .with_delay_post_stop_secs(2);
    if let Some(postgres) = pg_cache {
        // Keep saved events on restart; never clear persisted state at startup.
        builder = builder.with_cache_config(CacheConfig {
            persist_account_events: true,
            flush_on_start: false,
            ..Default::default()
        })
        .with_cache_database_factory(Box::new(postgres));
    }
    let mut node = builder.build()?;

    // Typed event-bus subscription; account and type checked again per event.
    // No Ledger::save_account_snapshot call is possible from this binary.
    let _subscription = NativeAccountSubscription::attach(
        Rc::clone(&session), now_ns, 60_000_000_000,
    );
    // Optional local read-only Runtime bridge. No credentials or order commands
    // cross this socket, and the native node remains testnet-only.
    #[cfg(unix)]
    let socket_config = account_socket_path()?;
    #[cfg(unix)]
    let result = if let Some((socket_path, tenant, account)) = socket_config {
        let listener = UnixListener::bind(&socket_path)
            .context("cannot bind private native account socket")?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
        let local = LocalSet::new();
        let outcome = local.run_until(async {
            let server = tokio::task::spawn_local(
                serve_account_socket(listener, Rc::clone(&session), tenant, account)
            );
            let outcome = node.run().await;
            server.abort();
            outcome
        }).await;
        let _ = fs::remove_file(&socket_path);
        outcome
    } else {
        node.run().await
    };
    #[cfg(not(unix))]
    let result = node.run().await;
    // Release the native Postgres cache and drain queued writes before exit.
    // LiveNode::run() alone does not guarantee cache adapter disposal.
    node.dispose();
    session.borrow_mut().mark_disconnected();
    result?;
    Ok(())
}
