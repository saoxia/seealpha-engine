//! Offline Trade ZIP -> OHLCV JSON; never writes Catalog or sends orders.
use seealpha_binance_market_data_source::trade_archive::{aggregate_archive, TradeProduct};
use serde_json::json;
use std::path::Path;
use std::process::ExitCode;

fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: binance-trade-archive-bars <spot|futures_um> <archive.zip> <interval_ms>".into());
    }
    let product = TradeProduct::parse(&args[1])?;
    let interval_ms: i64 = args[3].parse()?;
    let bars = aggregate_archive(Path::new(&args[2]), product, interval_ms)?;
    let output: Vec<_> = bars.iter().map(|bar| json!({
        "open_ms": bar.open_ms,
        "open": bar.open.to_string(),
        "high": bar.high.to_string(),
        "low": bar.low.to_string(),
        "close": bar.close.to_string(),
        "volume": bar.volume.to_string(),
        "quote_volume": bar.quote_volume.to_string(),
        "taker_buy_volume": bar.taker_buy_volume.to_string(),
        "taker_buy_quote_volume": bar.taker_buy_quote_volume.to_string(),
        "trade_count": bar.trade_count,
        "verified_against_exchange": false,
        "allow_new_risk": false
    })).collect();
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}
fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => { eprintln!("Native trade archive conversion failed: {err}"); ExitCode::FAILURE }
    }
}
