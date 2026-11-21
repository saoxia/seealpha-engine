//! Native SeeAlpha Engine process.
//!
//! `seealpha-engine` serves HTTP v2. `seealpha-engine smoke` reads public
//! Binance market data and runs the sample Python strategy on a paper book.
use seealpha_http::{AppState, serve};
use seealpha_market::{BinanceSource, LoadQuery, Product};
use seealpha_runtime::{
    Config, PackageBody, PaperRequest, Runtime, StrategyInput, ensure_state_dir,
};
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
    let tenant = state
        .config
        .tenant_id
        .clone()
        .unwrap_or_else(|| "smoke".into());
    let purpose = state
        .config
        .tenant_purpose
        .clone()
        .unwrap_or_else(|| "development".into());
    if purpose == "production" {
        return Err(std::io::Error::other("smoke refuses a production tenant"));
    }
    state
        .runtime
        .ensure_tenant(&tenant, &purpose, "active")
        .map_err(io_error)?;
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
    let report = state
        .runtime
        .run_paper(PaperRequest {
            tenant_id: tenant,
            account_id: "paper".into(),
            snapshot,
            strategies: vec![StrategyInput {
                member_id: "ema_crossover".into(),
                package: PackageBody::Directory(state.config.sample_package()),
                parameters: json!({}),
            }],
            request_id: None,
        })
        .map_err(io_error)?;
    if report["exchange_orders"] != 0
        || report["venue"] != "paper"
        || report["kline_count"].as_u64().unwrap_or(0) == 0
    {
        return Err(std::io::Error::other(
            "paper smoke did not keep the exchange path closed",
        ));
    }
    println!("{report}");
    Ok(())
}

fn io_error(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(error.to_string())
}
