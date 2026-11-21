//! Explicit offline Binance public archive download; no live market-data path.
//!
//! binance-trade-archive-fetch <spot|futures_um> <trades|aggTrades>
//!     <SYMBOL> <YYYY-MM-DD> <existing-root>
use std::{path::Path, process::ExitCode};
use seealpha_binance_market_data_source::archive_fetch::ArchiveSpec;
use serde_json::json;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Binance historical archive download failed: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 6 {
        return Err(
            "usage: binance-trade-archive-fetch <spot|futures_um> <trades|aggTrades> <SYMBOL> <YYYY-MM-DD> <existing-root>"
                .into(),
        );
    }
    let archive = ArchiveSpec::new(&args[1], &args[2], &args[3], &args[4])?;
    let downloaded = archive.download(Path::new(&args[5])).await?;
    println!("{}", json!({
        "path": downloaded.path,
        "sha256": downloaded.sha256,
        "bytes": downloaded.bytes,
        "reused": downloaded.reused,
        "source": "https://data.binance.vision",
        "archive": archive.remote_path(),
        "verified": true,
        "catalog_committed": false
    }));
    Ok(())
}
