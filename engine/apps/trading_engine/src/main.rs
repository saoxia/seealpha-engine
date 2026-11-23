//! Native SeeAlpha Engine process; no Runtime facade.
use seealpha_http::{AppState,Config,serve};
use seealpha_market::{BinanceSource,LoadQuery,Product,run_pipeline};
use seealpha_catalog::ControlStore;
use seealpha_database::{Connection,bootstrap_engine_schema};
use seealpha_ledger::Ledger;
use seealpha_strategy::store::StrategyStore;
use serde_json::json;
use std::{process::ExitCode,sync::Arc};

fn main() -> ExitCode {
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => { eprintln!("{error}"); return ExitCode::from(1); }
    };
    let state = match open_state(config) {
        Ok(state) => state,
        Err(error) => { eprintln!("{error}"); return ExitCode::from(1); }
    };
    let smoke = std::env::args().nth(1).as_deref() == Some("smoke");
    let result = if smoke {
        tokio::runtime::Builder::new_multi_thread().enable_all().build()
            .and_then(|runtime| runtime.block_on(run_smoke(state)))
    } else {
        tokio::runtime::Builder::new_multi_thread().enable_all().build()
            .and_then(|runtime| runtime.block_on(serve(state)))
    };
    if let Err(error) = result { eprintln!("{error}"); return ExitCode::from(1); }
    ExitCode::SUCCESS
}

fn open_state(config:Config)->Result<AppState,String>{
    config.ensure_state_dir().map_err(|e|e.to_string())?;
    let book=config.book_path();
    let db=Connection::open(&book).map_err(|e|e.to_string())?;
    bootstrap_engine_schema(&db).map_err(|e|e.to_string())?;
    drop(db);
    let ledger=Ledger::open(&book).map_err(|e|e.to_string())?;
    // Preserve historical synthetic-fill quarantine before serving reads.
    ledger.quarantine_synthetic_fills().map_err(|e|e.to_string())?;
    let strategy_store=StrategyStore::open(&book.with_file_name("strategies"))
        .map_err(|e|e.to_string())?;
    let control=ControlStore::open(&book.with_file_name("control-state"))
        .map_err(|e|e.to_string())?;
    let market=BinanceSource::new().map_err(|e|e.to_string())?;
    Ok(AppState {
        config:Arc::new(config),
        ledger:Arc::new(ledger),
        strategy_store:Arc::new(strategy_store),
        control:Arc::new(control),
        market:Arc::new(market),
    })
}

async fn run_smoke(state: AppState) -> Result<(), std::io::Error> {
    let symbol = std::env::var("ENGINE_SMOKE_SYMBOL").unwrap_or_else(|_| "ETHUSDT".into());
    let interval = std::env::var("ENGINE_SMOKE_INTERVAL").unwrap_or_else(|_| "1m".into());
    let limit = std::env::var("ENGINE_SMOKE_LIMIT").ok().and_then(|value|value.parse().ok()).unwrap_or(120);
    let product = Product::parse(&std::env::var("ENGINE_SMOKE_PRODUCT").unwrap_or_else(|_|"spot".into())).map_err(io_error)?;
    let snapshot = state.market.load(&LoadQuery{product,symbol,interval,limit}).await.map_err(io_error)?;
    let pipeline = run_pipeline(&snapshot).map_err(io_error)?;
    if pipeline.klines.is_empty(){return Err(std::io::Error::other("smoke data contains no closed bars"));}
    println!("{}",json!({"mode":"read_only_market_smoke","kline_count":pipeline.klines.len(),"exchange_orders":0}));
    Ok(())
}
fn io_error(error:impl std::fmt::Display)->std::io::Error {
    std::io::Error::other(error.to_string())
}
