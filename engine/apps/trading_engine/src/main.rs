//! Native SeeAlpha Engine process.
//!
//! `seealpha-engine` serves HTTP v2. `seealpha-engine smoke` reads public
//! public Binance market data and validates its read-only bar pipeline.
use seealpha_http::{AppState, serve};
use seealpha_market::{BinanceSource, LoadQuery, Product, run_pipeline};
use seealpha_runtime::{Config, Runtime, ensure_state_dir};
use serde_json::json;
use std::process::ExitCode;
use std::sync::Arc;

fn main() -> ExitCode {
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(1);
        }
    };
    if let Err(error) = ensure_state_dir(&config.state_dir) {
        eprintln!("{error}");
        return ExitCode::from(1);
    }
    let runtime = match Runtime::open(&config) {
        Ok(runtime) => Arc::new(runtime),
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(1);
        }
    };
    let market = match BinanceSource::new() {
        Ok(source) => Arc::new(source),
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(1);
        }
    };
    let state = AppState {
        config: Arc::new(config),
        runtime,
        market,
    };
    let smoke = std::env::args().nth(1).as_deref() == Some("smoke");
    let result = if smoke {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .and_then(|runtime| runtime.block_on(run_smoke(state)))
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .and_then(|runtime| runtime.block_on(serve(state)))
    };
    if let Err(error) = result {
        eprintln!("{error}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

async fn run_smoke(state: AppState) -> Result<(), std::io::Error> {
    let symbol = std::env::var("ENGINE_SMOKE_SYMBOL").unwrap_or_else(|_| "ETHUSDT".into());
    let interval = std::env::var("ENGINE_SMOKE_INTERVAL").unwrap_or_else(|_| "1m".into());
    let limit = std::env::var("ENGINE_SMOKE_LIMIT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(120);
    let product =
        Product::parse(&std::env::var("ENGINE_SMOKE_PRODUCT").unwrap_or_else(|_| "spot".into()))
            .map_err(io_error)?;
    let snapshot = state
        .market
        .load(&LoadQuery {
            product,
            symbol,
            interval,
            limit,
        })
        .await
        .map_err(io_error)?;
    let pipeline = run_pipeline(&snapshot).map_err(io_error)?;
    if pipeline.klines.is_empty() {
        return Err(std::io::Error::other("smoke data contains no closed bars"));
    }
    println!("{}", json!({
        "mode": "read_only_market_smoke",
        "kline_count": pipeline.klines.len(),
        "exchange_orders": 0,
    }));
    Ok(())
}

fn io_error(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(error.to_string())
}
