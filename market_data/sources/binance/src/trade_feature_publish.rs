//! Explicit native offline Trade ZIP -> feature Parquet publisher.
use std::{path::Path, process::ExitCode};
use seealpha_binance_market_data_source::{
    trade_archive::TradeProduct,
    trade_features::publish_archive_features,
};
fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 6 {
        return Err("usage: binance-trade-feature-publish <spot|futures_um> <archive.zip> <interval_ms> <catalog-root> <safe-identity>".into());
    }
    let product = TradeProduct::parse(&args[1])?;
    let step = args[3].parse::<i64>()?;
    let file = publish_archive_features(
        Path::new(&args[2]), product, step, Path::new(&args[4]), &args[5],
    )?;
    // Never leak an absolute local filesystem path to a client.
    println!("{}",serde_json::json!({
        "published":true,
        "file":file.file_name().and_then(|s|s.to_str()),
        "exchange_verified":false,"data_ready":false,"exchange_orders":0,
        "feature_name":"NativeTradeFeaturesV1"
    }));
    Ok(())
}
fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => { eprintln!("offline feature publication rejected: {err}"); ExitCode::FAILURE }
    }
}
