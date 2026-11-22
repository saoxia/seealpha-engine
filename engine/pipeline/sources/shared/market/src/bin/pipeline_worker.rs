//! One autonomous reconciliation process per Binance product.
//! Database config is desired state; data is never declared READY here.
use seealpha_catalog::{Catalog,ControlStore,PipelineSubscription,DAY_MS,validate_subscription};
use seealpha_market::{BinanceSource,Product,identity::{self,MarketProduct}};
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

fn desired(rows:&[(String,PipelineSubscription)])
    ->Result<BTreeMap<String,(i64,bool,Option<(u32,u32)>)>>
{
    let mut out=BTreeMap::new();
    for (_,row) in rows {
        validate_subscription(row)?;
        let start=row.start_ms()?;
        let entry=out.entry(row.symbol.clone())
            .or_insert((start,false,None::<(u32,u32)>));
        entry.0=entry.0.min(start);
        if row.enabled {
            entry.1=true;
            if let (Some(price),Some(size))=(row.price_precision,row.size_precision) {
                if entry.2.is_some_and(|previous|previous!=(price,size)) {
                    return Err(format!("conflicting instrument precision: {}",row.symbol).into());
                }
                entry.2=Some((price,size));
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
    if let Some(child)=active.as_mut(){
        if child.child.try_wait()?.is_some(){*active=None}
    }
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

async fn reconcile_history(
    catalog:&Catalog,product:Product,source:&BinanceSource,
    symbol:&str,start:i64,precision:Option<(u32,u32)>,
)->Result<()>{
    let id=identity::bar_type(symbol,match product {
        Product::Spot=>MarketProduct::Spot,Product::FuturesUm=>MarketProduct::FuturesUm
    },"1d")?;
    // A shared public Catalog retains the earliest date requested by ANY
    // tenant, including disabled subscriptions that must not delete history.
    let instrument=identity::instrument_id(symbol,match product {
        Product::Spot=>MarketProduct::Spot,Product::FuturesUm=>MarketProduct::FuturesUm
    })?;
    let removed=catalog.prune_instrument_bars_before(&instrument,start)?;
    if removed>0{eprintln!("pipeline {} {symbol}: removed {removed} expired Bar files",product.as_str())}
    let today=utc_today_ms()?;
    if start>=today{return Ok(())}
    let gaps=catalog.bar_gaps(&id,"1d",start,today,50_000)?;
    if gaps.is_empty(){return Ok(())}
    let Some((price_precision,size_precision))=precision else{
        return Err(format!("missing verified instrument precision for {symbol}; refusing guessed Parquet precision").into());
    };
    let mut done=0;
    for gap in gaps {
        let mut day=gap.start_ms;
        while day<gap.end_ms && done<31 {
            let until=day.checked_add(DAY_MS).ok_or("date overflow")?;
            let bars=source.load_history(product,symbol,"1d",day,until).await?;
            let mapped=bars.into_iter().map(|bar|seealpha_catalog::CatalogBar{
                open_time_ms:bar.open_time_ms,open:bar.open,high:bar.high,
                low:bar.low,close:bar.close,volume:bar.volume
            }).collect::<Vec<_>>();
            catalog.append_bars(&id,"1d",&mapped,price_precision,size_precision)?;
            done+=1;day=until;
        }
        if done>=31 {break}
    }
    if done>0 {eprintln!("pipeline {} {symbol}: committed {done} verified daily bars",product.as_str())}
    Ok(())
}

async fn cycle(
    control:&ControlStore,catalog:&Catalog,product:Product,source:&BinanceSource,
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
    for (symbol,(start,enabled,precision)) in expected {
        if !enabled {continue}
        if let Err(error)=reconcile_history(catalog,product,source,&symbol,start,precision).await{
            eprintln!("pipeline {} {symbol} history: {error}",product.as_str());
        }
    }
    Ok(())
}

#[tokio::main]
async fn main()->Result<()>{
    let product=env::args().nth(1).ok_or("usage: seealpha-pipeline <spot|futures_um>")?;
    let product=Product::parse(&product)?;
    let control=ControlStore::open(Path::new("pipeline-control"))?;
    let source=BinanceSource::new()?;
    let mut native=None;
    loop {
        match Catalog::open_existing(&catalog_path()) {
            Ok(catalog)=>{
                if let Err(error)=cycle(&control,&catalog,product,&source,&mut native).await {
                    eprintln!("pipeline {} cycle: {error}",product.as_str());
                }
            }
            Err(error)=>eprintln!("pipeline {} catalog: {error}",product.as_str()),
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
        assert_eq!(spec["BTCUSDT"].0,make("2025-01-01",false).start_ms().unwrap());
        assert!(spec["BTCUSDT"].1);
    }
}
