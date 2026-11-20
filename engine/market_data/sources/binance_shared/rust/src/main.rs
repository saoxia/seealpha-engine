//! Read-only inspection of existing source-owned runtime manifests.
//! This binary does not start collection or change storage.
use std::fs;
use std::process::ExitCode;

use seealpha_binance_market_data_source::{
    MAX_MANIFEST_BYTES, Product, cold_start_shards, parse_manifest,
};

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        return Err("usage: binance-manifest-inspect <futures_um|spot|options> <manifest.json>".into());
    }
    let product = Product::parse(&args[1]).ok_or("invalid product")?;
    let size = fs::metadata(&args[2])?.len();
    if size > MAX_MANIFEST_BYTES as u64 {
        return Err("manifest exceeds 5 MiB".into());
    }
    let raw = fs::read(&args[2])?;
    let policies = parse_manifest(&raw, product)?;
    let active = policies.iter().filter(|p| p.realtime_enabled).count();
    let shards = cold_start_shards(&policies, product);
    let response = serde_json::json!({
        "product": product.as_str(),
        "manifest_version": 1,
        "policies": policies.len(),
        "active": active,
        "socket_capacity": product.socket_capacity(),
        "cold_start_shard_sizes": shards.iter().map(Vec::len).collect::<Vec<_>>(),
        "no_side_effects": true,
    });
    println!("{}", response);
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
