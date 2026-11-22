//! Tick-exclusive OHLCV engine. A snapshot is emitted for EVERY valid new tick,
//! including an unfinished bar. Never fabricate empty bars or infer coverage.
use chrono::{Datelike, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::BTreeMap;

pub const INTERVALS: &[&str] = &[
    "1s","1m","3m","5m","15m","30m","1h","2h","4h","6h","8h",
    "12h","1d","3d","1w","1M",
];

#[derive(Clone,Debug,PartialEq,Eq,Serialize)]
pub struct TickBar {
    pub interval:String,
    pub open_time_ms:i64,
    pub close_time_ms:i64,
    pub open:Decimal,
    pub high:Decimal,
    pub low:Decimal,
    pub close:Decimal,
    pub volume:Decimal,
    /// False on every in-progress tick update. This is a snapshot, not
    /// a claim that the full period has already been observed.
    pub closed:bool,
}

#[derive(Clone,Debug)]
pub struct MarketTick {
    pub id:String,
    pub ts_ns:u64,
    pub price:Decimal,
    pub size:Decimal,
}

#[derive(Debug,Default)]
pub struct TickBars {
    last:(Option<u64>,Option<String>),
    states:BTreeMap<&'static str,TickBar>,
}

fn window(interval:&str, timestamp_ms:i64)->Result<(i64,i64),String>{
    if timestamp_ms<0 {return Err("negative timestamp".into())}
    if interval=="1M" {
        let ts=Utc.timestamp_millis_opt(timestamp_ms).single()
            .ok_or("invalid calendar timestamp")?;
        let (year,month)=(ts.year(),ts.month());
        let from=Utc.with_ymd_and_hms(year,month,1,0,0,0).single()
            .ok_or("invalid month start")?.timestamp_millis();
        let (next_year,next_month)=if month==12 {(year+1,1)}else{(year,month+1)};
        let to=Utc.with_ymd_and_hms(next_year,next_month,1,0,0,0).single()
            .ok_or("invalid month end")?.timestamp_millis();
        return Ok((from,to));
    }
    let width=match interval{
        "1s"=>1_000,"1m"=>60_000,"3m"=>180_000,"5m"=>300_000,
        "15m"=>900_000,"30m"=>1_800_000,"1h"=>3_600_000,
        "2h"=>7_200_000,"4h"=>14_400_000,"6h"=>21_600_000,
        "8h"=>28_800_000,"12h"=>43_200_000,"1d"=>86_400_000,
        "3d"=>259_200_000,"1w"=>604_800_000,
        _=>return Err("unsupported bar interval".into()),
    };
    // Binance 1w opens Monday UTC; 3d uses exchange-compatible epoch offset.
    let offset=match interval{"1w"=>4*86_400_000,"3d"=>86_400_000,_=>0};
    let from=(timestamp_ms-offset).div_euclid(width)*width+offset;
    Ok((from,from.checked_add(width).ok_or("bar time overflow")?))
}

// Tick drives 1s/1m/3m/5m directly. Higher timeframes MUST consume
// the evolving 1s OHLCV snapshot, not the raw TradeTick input.
const TICK_INTERVALS: &[&str] = &["1s","1m","3m","5m"];
const SECOND_INTERVALS: &[&str] = &[
    "15m","30m","1h","2h","4h","6h","8h","12h","1d","3d","1w","1M",
];

impl TickBars {
    /// Produces a fresh *open* Bar snapshot for every supported period on
    /// each valid TradeTick. Higher bars aggregate the latest 1s snapshot
    /// incrementally. Revisions within the SAME 1s bucket contribute only
    /// the volume DELTA, never that second's cumulative volume twice.
    pub fn on_tick(&mut self, tick:&MarketTick)->Result<Vec<TickBar>,String>{
        if tick.id.is_empty() || tick.price<=Decimal::ZERO || tick.size<=Decimal::ZERO {
            return Err("invalid Tick identity/price/size".into());
        }
        if self.last.0.is_some_and(|prior|tick.ts_ns<prior) {
            return Err("out-of-order Tick: requires historical reconciliation".into());
        }
        if self.last.0==Some(tick.ts_ns) && self.last.1.as_deref()==Some(tick.id.as_str()) {
            return Ok(Vec::new());
        }
        let ms=i64::try_from(tick.ts_ns/1_000_000).map_err(|_|"tick time overflow")?;
        // Check all UTC boundary rules before modifying any aggregation state.
        let windows=INTERVALS.iter()
            .map(|i|window(i,ms))
            .collect::<Result<Vec<_>,_>>()?;
        let mut expected=BTreeMap::new();
        for (&interval,bounds) in INTERVALS.iter().zip(windows) {
            expected.insert(interval,bounds);
        }

        // Work on a copy so a Decimal overflow or malformed window cannot
        // publish partially advanced states and poison subsequent replay.
        let mut next=self.states.clone();
        let previous_second=next.get("1s").cloned();
        let mut updates=Vec::with_capacity(INTERVALS.len()*2);
        for &interval in TICK_INTERVALS {
            let (start,end)=expected[interval];
            let source=TickBar {
                interval:interval.into(), open_time_ms:start,close_time_ms:end-1,
                open:tick.price,high:tick.price,low:tick.price,close:tick.price,
                volume:tick.size,closed:false,
            };
            update_bucket(&mut next,interval,&source,tick.size,(start,end),&mut updates)?;
        }
        let second=next.get("1s").ok_or("missing updated 1s bar")?.clone();
        let delta=if previous_second.as_ref().is_some_and(|old|old.open_time_ms==second.open_time_ms) {
            second.volume.checked_sub(previous_second.as_ref().ok_or("missing prior 1s")?.volume)
                .ok_or("1s volume underflow")?
        } else {
            second.volume
        };
        if delta<=Decimal::ZERO {
            return Err("1s volume did not advance".into());
        }
        for &interval in SECOND_INTERVALS {
            update_bucket(&mut next,interval,&second,delta,expected[interval],&mut updates)?;
        }
        self.states=next;
        self.last=(Some(tick.ts_ns),Some(tick.id.clone()));
        Ok(updates)
    }
}

/// Apply either one raw Tick (<=5m) or the CURRENT 1s Bar (>5m). The
/// input volume is an incremental delta; the OHLC values are snapshots.
fn update_bucket(
    states:&mut BTreeMap<&'static str,TickBar>,
    interval:&'static str,
    source:&TickBar,
    delta:Decimal,
    (start,end):(i64,i64),
    updates:&mut Vec<TickBar>,
)->Result<(),String>{
    match states.get_mut(interval){
        Some(old) if old.open_time_ms==start=>{
            old.high=old.high.max(source.high);
            old.low=old.low.min(source.low);
            old.close=source.close;
            old.volume=old.volume.checked_add(delta).ok_or("volume overflow")?;
            updates.push(old.clone());
        }
        Some(old)=>{
            if old.open_time_ms>start {return Err("late Bar interval regression".into())}
            let mut complete=old.clone();
            complete.closed=true;
            updates.push(complete);
            let fresh=TickBar{
                interval:interval.into(),open_time_ms:start,close_time_ms:end-1,
                open:source.open,high:source.high,low:source.low,close:source.close,
                volume:delta,closed:false,
            };
            states.insert(interval,fresh.clone());
            updates.push(fresh);
        }
        None=>{
            let fresh=TickBar{
                interval:interval.into(),open_time_ms:start,close_time_ms:end-1,
                open:source.open,high:source.high,low:source.low,close:source.close,
                volume:delta,closed:false,
            };
            states.insert(interval,fresh.clone());
            updates.push(fresh);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests{
    use super::*;
    fn tick(id:&str,ms:u64,price:i64,size:i64)->MarketTick{
        MarketTick{id:id.into(),ts_ns:ms*1_000_000,
            price:Decimal::from(price),size:Decimal::from(size)}
    }
    #[test]
    fn open_bars_update_on_every_tick_even_before_close(){
        let mut a=TickBars::default();
        let first=a.on_tick(&tick("1",1200,100,2)).unwrap();
        assert_eq!(first.len(),INTERVALS.len());
        assert!(first.iter().all(|b| !b.closed));
        let second=a.on_tick(&tick("2",1400,110,3)).unwrap();
        assert_eq!(second.len(),INTERVALS.len());
        let s=second.iter().find(|b|b.interval=="1s").unwrap();
        assert_eq!((s.open,s.high,s.low,s.close,s.volume),
            (Decimal::from(100),Decimal::from(110),Decimal::from(100),Decimal::from(110),Decimal::from(5)));
        assert!(!s.closed);
        assert!(a.on_tick(&tick("2",1400,110,3)).unwrap().is_empty());
        let next=a.on_tick(&tick("3",2000,105,1)).unwrap();
        assert_eq!(next.iter().filter(|b|b.interval=="1s").count(),2);
        assert!(next.iter().any(|b|b.interval=="1s"&&b.closed));
        assert!(next.iter().any(|b|b.interval=="1s"&&!b.closed&&b.close==Decimal::from(105)));
    }
    #[test]
    fn no_ticks_means_no_synthetic_bars(){
        let mut a=TickBars::default();
        a.on_tick(&tick("1",0,10,1)).unwrap();
        let result=a.on_tick(&tick("2",5_000,12,2)).unwrap();
        assert_eq!(result.iter().filter(|b|b.interval=="1s").count(),2);
        assert_eq!(result.iter().filter(|b|b.interval=="1s"&&b.open_time_ms==1_000).count(),0);
        assert!(a.on_tick(&tick("late",200,11,1)).is_err());
    }
    #[test]
    fn above_five_minutes_aggregates_latest_unclosed_one_second_bar_without_double_volume(){
        let mut a=TickBars::default();
        let updates=a.on_tick(&tick("1",1_000,100,2)).unwrap();
        assert_eq!(updates.len(),INTERVALS.len());
        let updates=a.on_tick(&tick("2",1_500,110,3)).unwrap();
        let second=updates.iter().find(|b|b.interval=="1s").unwrap();
        assert_eq!(second.volume,Decimal::from(5));
        // 15m+ must use the updated 1s snapshot; this second contributes
        // only +3, not its full cumulative 5 again.
        for name in SECOND_INTERVALS {
            let higher=updates.iter().find(|b|b.interval==*name).unwrap();
            assert_eq!(higher.volume,Decimal::from(5),"{name}");
            assert_eq!(higher.high,Decimal::from(110),"{name}");
            assert_eq!(higher.close,Decimal::from(110),"{name}");
            assert!(!higher.closed);
        }
        let updates=a.on_tick(&tick("3",2_000,90,4)).unwrap();
        let second=updates.iter().find(|b|b.interval=="1s"&&!b.closed).unwrap();
        assert_eq!(second.volume,Decimal::from(4));
        for name in SECOND_INTERVALS {
            let higher=updates.iter().find(|b|b.interval==*name).unwrap();
            assert_eq!(higher.volume,Decimal::from(9),"{name}");
            assert_eq!(higher.low,Decimal::from(90),"{name}");
            assert_eq!(higher.open,Decimal::from(100),"{name}");
        }
    }
    #[test]
    fn five_minute_is_tick_driven_but_larger_buckets_roll_from_one_second(){
        let mut a=TickBars::default();
        a.on_tick(&tick("1",299_999,100,2)).unwrap();
        let updates=a.on_tick(&tick("2",300_000,110,3)).unwrap();
        assert!(updates.iter().any(|b|b.interval=="5m"&&b.closed));
        assert!(updates.iter().any(|b|b.interval=="5m"&&!b.closed&&b.volume==Decimal::from(3)));
        let fifteen=updates.iter().find(|b|b.interval=="15m").unwrap();
        assert_eq!(fifteen.volume,Decimal::from(5));
        assert!(!fifteen.closed);
        let updates=a.on_tick(&tick("3",900_000,120,4)).unwrap();
        assert!(updates.iter().any(|b|b.interval=="15m"&&b.closed&&b.volume==Decimal::from(5)));
        assert!(updates.iter().any(|b|b.interval=="15m"&&!b.closed&&b.volume==Decimal::from(4)));
    }
    #[test]
    fn only_one_second_snapshot_is_needed_for_monthly_intrabar_revision(){
        let mut a=TickBars::default();
        let feb=Utc.with_ymd_and_hms(2026,2,28,23,59,59).single().unwrap().timestamp_millis();
        a.on_tick(&tick("1",feb as u64,100,2)).unwrap();
        let result=a.on_tick(&tick("2",(feb+100) as u64,111,3)).unwrap();
        let month=result.iter().find(|b|b.interval=="1M").unwrap();
        assert!(!month.closed);
        assert_eq!(month.volume,Decimal::from(5));
        assert_eq!(month.high,Decimal::from(111));
        let result=a.on_tick(&tick("3",(feb+1_000) as u64,120,4)).unwrap();
        assert!(result.iter().any(|b|b.interval=="1M"&&b.closed&&b.volume==Decimal::from(5)));
        assert!(result.iter().any(|b|b.interval=="1M"&&!b.closed&&b.volume==Decimal::from(4)));
    }
    #[test]
    fn windows_match_week_and_calendar_month(){
        assert_eq!(window("1w",0).unwrap().0,-3*86_400_000);
        assert_eq!(window("3d",86_400_000).unwrap(),(86_400_000,4*86_400_000));
        let feb=Utc.with_ymd_and_hms(2026,2,20,0,0,0).single().unwrap().timestamp_millis();
        assert_eq!(window("1M",feb).unwrap().1-window("1M",feb).unwrap().0,28*86_400_000);
    }
}
