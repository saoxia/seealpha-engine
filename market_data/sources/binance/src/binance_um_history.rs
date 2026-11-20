//! Strict public-data-only USD-M archive CLI.
//! Example:
//! cargo run -p seealpha-binance-market-data-source --bin binance-um-history --
//!   --symbols ETHUSDT,BTCUSDT --start 2026-11-18 --end 2026-11-20
use std::{env, path::PathBuf, process::ExitCode};
use chrono::NaiveDate;
use seealpha_binance_market_data_source::history;

fn option(args:&[String],name:&str)->Result<String,String>{
    let index=args.iter().position(|arg|arg==name)
        .ok_or_else(||format!("missing {name}"))?;
    args.get(index+1).filter(|value|!value.starts_with("--"))
        .cloned().ok_or_else(||format!("missing value for {name}"))
}

#[tokio::main]
async fn main()->ExitCode {
    match run().await {
        Ok(())=>ExitCode::SUCCESS,
        Err(e)=>{eprintln!("binance-um-history: {e}");ExitCode::FAILURE}
    }
}
async fn run()->Result<(),String>{
    let args:Vec<String>=env::args().skip(1).collect();
    if args.iter().any(|s|s=="--help"||s=="-h") {
        println!("binance-um-history --symbols ETHUSDT,BTCUSDT --start YYYY-MM-DD --end YYYY-MM-DD --output-dir PATH [--plan-only] [--refresh]");
        return Ok(());
    }
    let symbols:Vec<String>=option(&args,"--symbols")?.split(',')
        .map(str::to_owned).collect();
    let parse_date=|key|NaiveDate::parse_from_str(&option(&args,key)?, "%Y-%m-%d")
        .map_err(|e|format!("{key}: {e}"));
    let days=history::plan(&symbols,parse_date("--start")?,
        parse_date("--end")?,history::today_utc())?;
    let plan_only=args.iter().any(|arg|arg=="--plan-only");
    if plan_only {
        for day in days {println!("{}",day.remote_path());}
        return Ok(());
    }
    let output=PathBuf::from(option(&args,"--output-dir")?);
    let refresh=args.iter().any(|arg|arg=="--refresh");
    let client=reqwest::Client::builder().connect_timeout(std::time::Duration::from_secs(30))
        .user_agent("SeeAlpha-Rust-Binance-History/0.1")
        .redirect(reqwest::redirect::Policy::none())
        .build().map_err(|e|e.to_string())?;
    let mut failed=Vec::new();
    for (index,day) in days.iter().enumerate() {
        match history::sync(&client,day,&output,refresh).await {
            Ok(path)=>println!("[{}/{}] verified {}",index+1,days.len(),path.display()),
            Err(error)=>failed.push(error),
        }
    }
    if failed.is_empty(){Ok(())}else{Err(failed.join("\n"))}
}
