//! Offline repair requires the live source writer to be stopped.
use std::{env,path::PathBuf,process::ExitCode};
use chrono::NaiveDate;
use seealpha_binance_market_data_source::{history::ArchiveDay,repair,store::PairJournal};

fn run()->Result<(),Box<dyn std::error::Error+Send+Sync>>{
    let args:Vec<String>=env::args().skip(1).collect();
    if args.len()!=5 {
        return Err("usage: binance-um-gap-repair <symbol> <YYYY-MM-DD> <pair-root> <archive-root> <base-level> (stop live writer first)".into());
    }
    let symbol=seealpha_binance_market_data_source::normalize_symbol(
        &args[0],seealpha_binance_market_data_source::Product::FuturesUm)
        .ok_or("invalid symbol")?;
    let day=NaiveDate::parse_from_str(&args[1],"%Y-%m-%d")?;
    let pair=PathBuf::from(&args[2]).canonicalize()?;
    let archive=PathBuf::from(&args[3]).canonicalize()?;
    let mut journal=PairJournal::open(&pair,&symbol,&args[4])?;
    let report=repair::repair_day(&mut journal,&ArchiveDay{symbol,day},&archive)?;
    println!("scanned={} restored={} remaining_gap_ranges={} data_ready=false",
        report.scanned,report.restored,report.remaining);
    Ok(())
}
fn main()->ExitCode{
    match run(){
        Ok(())=>ExitCode::SUCCESS,
        Err(e)=>{eprintln!("gap repair rejected: {e}");ExitCode::FAILURE}
    }
}
