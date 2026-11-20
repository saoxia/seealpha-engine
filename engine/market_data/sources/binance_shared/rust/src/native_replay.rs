//! Offline native journal replay for Python/Rust parity. No network or accounts.
use std::io::{self,BufRead};
use std::path::Path;

use serde_json::{Value,json};
use seealpha_binance_market_data_source::store::{PairJournal,WriteReceipt};

fn run()->Result<(),Box<dyn std::error::Error+Send+Sync>> {
    let args:Vec<String>=std::env::args().collect();
    if args.len()!=4 {
        return Err("usage: binance-journal-replay <symbol> <base_level> <existing-root>".into());
    }
    let mut journal=PairJournal::open(Path::new(&args[3]),&args[1],&args[2])?;
    let mut accepted=0_u64;
    let mut duplicates=0_u64;
    let mut filtered=0_u64;
    for raw in io::stdin().lock().lines() {
        let line=raw?;
        if line.trim().is_empty(){continue;}
        let entry:Value=serde_json::from_str(&line)?;
        let now_ms=entry["now_ms"].as_i64().ok_or("missing now_ms")?;
        let row=entry.get("row").ok_or("missing raw row")?;
        match journal.write_trade(row,now_ms)? {
            WriteReceipt::Accepted=>accepted+=1,
            WriteReceipt::Duplicate=>duplicates+=1,
            WriteReceipt::FilteredZero=>filtered+=1,
        }
    }
    println!("{}",json!({
        "accepted":accepted,"duplicates":duplicates,
        "filtered_zero":filtered,"trade_count":journal.trade_count()?,
        "pending_count":journal.pending_count()?,"conflicted":journal.conflicted()?
    }));
    Ok(())
}
fn main()->std::process::ExitCode {
    match run() {
        Ok(())=>std::process::ExitCode::SUCCESS,
        Err(error)=>{eprintln!("{error}");std::process::ExitCode::FAILURE}
    }
}
