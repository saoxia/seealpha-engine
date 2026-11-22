//! One autonomous reconciliation process per Binance product.
//! Database config is desired state; data is never declared READY here.
use seealpha_catalog::{Catalog,ControlStore,PipelineSubscription,DAY_MS,validate_subscription};
use seealpha_market::{Product,options::{OptionsSource,parse_option_symbol},identity::{self,MarketProduct}};
use std::{collections::{BTreeMap,BTreeSet},env,error::Error,path::{Path,PathBuf},process::{Child,Command,Stdio},time::{Duration,SystemTime,UNIX_EPOCH}};

type Result<T> = std::result::Result<T,Box<dyn Error>>;

fn catalog_path()->PathBuf {
    env::var_os("ENGINE_CATALOG_DIR").map(PathBuf::from)
        .unwrap_or_else(||env::current_dir().unwrap_or_default().join("data/catalog"))
}

fn utc_today_ms()->Result<i64> {
    let now=SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok((i64::try_from(now.as_millis())?/DAY_MS)*DAY_MS)
}

struct Desired {retain_from:i64,fetch_from:Option<i64>,precision:Option<(u32,u32)>}

fn desired(rows:&[(String,PipelineSubscription)])
    ->Result<BTreeMap<String,Desired>>
{
    let mut out=BTreeMap::new();
    for (_,row) in rows {
        validate_subscription(row)?;
        let start=row.start_ms()?;
        let entry=out.entry(row.symbol.clone())
            .or_insert(Desired{retain_from:start,fetch_from:None,precision:None});
        entry.retain_from=entry.retain_from.min(start);
        if row.enabled {
            entry.fetch_from=Some(entry.fetch_from.map_or(start,|earliest|earliest.min(start)));
            if let (Some(price),Some(size))=(row.price_precision,row.size_precision) {
                if entry.precision.is_some_and(|previous|previous!=(price,size)) {
                    return Err(format!("conflicting instrument precision: {}",row.symbol).into());
                }
                entry.precision=Some((price,size));
            }
        }
    }
    Ok(out)
}

struct Collector {child:Child,symbols:String}
impl Collector {
    fn stop(&mut self)->Result<()> {
        let pid=self.child.id();
        let status=Command::new("kill").arg("-TERM").arg(pid.to_string()).status()?;
        if !status.success(){return Err("cannot signal native collector".into())}
        for _ in 0..100 {
            if self.child.try_wait()?.is_some(){return Ok(())}
            std::thread::sleep(Duration::from_millis(100));
        }
        Err("native collector did not stop; refusing concurrent replacement".into())
    }
}
impl Drop for Collector {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none(){
            let _=Command::new("kill").arg("-TERM").arg(self.child.id().to_string()).status();
        }
    }
}

fn native_root(key:&str,product:&str)->Result<PathBuf>{
    let base=env::var_os(key).map(PathBuf::from)
        .unwrap_or_else(||env::current_dir().unwrap_or_default().join(
            if key=="PIPELINE_FEATHER_ROOT"{"data/feather"}else{"data/native-catalog"}
        ));
    let path=base.join(product);
    std::fs::create_dir_all(&path)?;
    if path.symlink_metadata()?.file_type().is_symlink(){return Err("native source root is a symlink".into())}
    Ok(path.canonicalize()?)
}

fn sync_collector(product:&str,desired_symbols:&BTreeSet<String>,active:&mut Option<Collector>)->Result<()>{
    let joined=desired_symbols.iter().cloned().collect::<Vec<_>>().join(",");
    let exited=match active.as_mut(){
        Some(child)=>child.child.try_wait()?.is_some(),
        None=>false,
    };
    if exited{*active=None;}
    if active.as_ref().is_some_and(|child|child.symbols==joined){return Ok(())}
    if let Some(child)=active.as_mut(){child.stop()?;}
    *active=None;
    if joined.is_empty(){return Ok(())}
    let bin=env::var_os("PIPELINE_NATIVE_BIN")
        .unwrap_or_else(||"seealpha-nautilus-binance-data".into());
    let feather=native_root("PIPELINE_FEATHER_ROOT",product)?;
    let catalog=native_root("PIPELINE_NATIVE_CATALOG_ROOT",product)?;
    let child=Command::new(bin).args([product,&joined])
        .arg(feather).arg(catalog)
        .stdin(Stdio::null()).spawn()?;
    eprintln!("pipeline {product}: native realtime subscribed {}",desired_symbols.len());
    *active=Some(Collector{child,symbols:joined});
    Ok(())
}

/// Spot and USD-M OHLCV must come exclusively from validated TradeTicks.
/// Never call Binance Kline REST or write exchange-authored OHLCV to Catalog.
/// Historical Tick ZIP continuity and native live/ZIP handoff still need
/// independent attestation before immutable Bars can be safely published.
async fn reconcile_history(
    catalog:&Catalog,product:Product,
    symbol:&str,retain_from:i64,_fetch_from:i64,
)->Result<()>{
    let instrument=identity::instrument_id(symbol,match product {
        Product::Spot=>MarketProduct::Spot,Product::FuturesUm=>MarketProduct::FuturesUm
    })?;
    let removed=catalog.prune_instrument_bars_before(&instrument,retain_from)?;
    if removed>0 {
        eprintln!("pipeline {} {symbol}: removed {removed} expired Bar files",product.as_str());
    }
    // Do not backfill using Kline API and do not manufacture immutable Bar
    // coverage from unverified partial Tick files.
    Ok(())
}

/// Options never use Spot / USD-M native trade client. A 15m Bar is the
/// finest daily-bounded unit supported by the existing OptionsSource.
async fn reconcile_options(
    catalog:&Catalog,source:&OptionsSource,symbol:&str,
    retain_from:i64,fetch_from:i64,precision:Option<(u32,u32)>,
)->Result<()>{
    let contract=parse_option_symbol(symbol)?;
    let id=identity::bar_type(symbol,MarketProduct::Options,"15m")?;
    let instrument=identity::instrument_id(symbol,MarketProduct::Options)?;
    catalog.prune_instrument_bars_before(&instrument,retain_from)?;
    // Do not query a contract past its expiry date or fabricate bars on a
    // nonexistent history window. Expiry-day partial history needs separate
    // exchange-calendar reconciliation; only closed prior UTC days are used.
    let expiry=chrono::NaiveDate::parse_from_str(&format!("20{}",contract.expiry),"%Y%m%d")?
        .and_hms_opt(0,0,0).ok_or("invalid expiry")?.and_utc().timestamp_millis();
    let end=utc_today_ms()?.min(expiry);
    if fetch_from>=end{return Ok(())}
    let (price_precision,size_precision)=precision.ok_or(
        "Options Bar publication requires verified Instrument price/size precision in subscription"
    )?;
    let mut day=fetch_from;
    let mut scanned=0;
    while day<end && scanned<31 {
        let after=day.checked_add(DAY_MS).ok_or("date overflow")?;
        let gaps=catalog.bar_gaps(&id,"15m",day,after,100)?;
        if !gaps.is_empty(){
            let bars=source.history(symbol,"15m",day,after).await?;
            let mapped=bars.into_iter().map(|bar|seealpha_catalog::CatalogBar{
                open_time_ms:bar.open_time_ms,open:bar.open,high:bar.high,
                low:bar.low,close:bar.close,volume:bar.volume,
            }).collect::<Vec<_>>();
            // Never overwrite a partial existing day or manufacture missing
            // bars. Existing contract requires strict contiguous full-day history.
            if gaps.len()!=1 || gaps[0].start_ms!=day || gaps[0].end_ms!=after {
                eprintln!("options {symbol}: partial 15m day requires explicit safe repair");
            } else if mapped.len()==96 {
                catalog.append_bars(&id,"15m",&mapped,price_precision,size_precision)?;
            }
        }
        scanned+=1;
        day=after;
    }
    Ok(())
}

async fn cycle_options(
    control:&ControlStore,catalog:&Catalog,source:&OptionsSource,
)->Result<()>{
    let rows=control.product_subscriptions("options")?;
    let expected=desired(&rows)?;
    if rows.iter().any(|(_,row)|row.enabled&&row.realtime_enabled) {
        eprintln!("options realtime requested but WebSocket connector is not implemented; no subscription started");
    }
    for (symbol,settings) in expected {
        let Some(fetch_from)=settings.fetch_from else{continue};
        if let Err(error)=reconcile_options(
            catalog,source,&symbol,settings.retain_from,fetch_from,settings.precision
        ).await {
            eprintln!("options {symbol}: {error}");
        }
    }
    Ok(())
}

async fn cycle(
    control:&ControlStore,catalog:&Catalog,product:Product,
    active:&mut Option<Collector>,
)->Result<()>{
    let rows=control.product_subscriptions(product.as_str())?;
    let expected=desired(&rows)?;
    let live=rows.iter().filter(|(_,row)|row.enabled&&row.realtime_enabled)
        .map(|(_,row)|row.symbol.clone()).collect::<BTreeSet<_>>();
    // One native LiveNode per PRODUCT, never a separate WebSocket for each symbol.
    if let Err(error)=sync_collector(product.as_str(),&live,active) {
        eprintln!("pipeline {} realtime: {error}",product.as_str());
    }
    for (symbol,settings) in expected {
        let Some(fetch_from)=settings.fetch_from else{continue};
        if let Err(error)=reconcile_history(catalog,product,&symbol,settings.retain_from,fetch_from).await{
            eprintln!("pipeline {} {symbol} history: {error}",product.as_str());
        }
    }
    Ok(())
}

#[tokio::main]
async fn main()->Result<()>{
    let name=env::args().nth(1).ok_or("usage: pipeline_worker <spot|futures_um|options>")?;
    let product=MarketProduct::parse(&name)?;
    let control=ControlStore::open(Path::new("pipeline-control"))?;
    let options=if product==MarketProduct::Options {Some(OptionsSource::new()?)} else {None};
    let mut native=None;
    loop {
        match Catalog::open_existing(&catalog_path()) {
            Ok(catalog)=>{
                let result=match product {
                    MarketProduct::Spot|MarketProduct::FuturesUm=>{
                        let parsed=Product::parse(&name)?;
                        cycle(&control,&catalog,parsed,&mut native).await
                    }
                    MarketProduct::Options=>cycle_options(
                        &control,&catalog,options.as_ref().ok_or("missing options source")?
                    ).await,
                };
                if let Err(error)=result {
                    eprintln!("pipeline {name} cycle: {error}");
                }
            }
            Err(error)=>eprintln!("pipeline {name} catalog: {error}"),
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

#[cfg(test)]
mod tests{
    use super::*;
    #[test] fn earlier_tenant_start_wins_and_disabled_prevents_deletion(){
        let make=|start:&str,enabled:bool|PipelineSubscription{
            product:"spot".into(),symbol:"BTCUSDT".into(),history_start:start.into(),
            realtime_enabled:enabled,enabled,price_precision:Some(8),size_precision:Some(8)};
        let rows=vec![("a".into(),make("2025-06-01",true)),
                      ("b".into(),make("2025-01-01",false))];
        let spec=desired(&rows).unwrap();
        assert_eq!(spec["BTCUSDT"].retain_from,make("2025-01-01",false).start_ms().unwrap());
        assert_eq!(spec["BTCUSDT"].fetch_from,Some(make("2025-06-01",true).start_ms().unwrap()));
    }
}
