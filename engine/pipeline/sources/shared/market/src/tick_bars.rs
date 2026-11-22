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

/// Build historical OHLCV from already final, validated 1s Bar observations.
/// Unlike the live path, this reads each 1s Bar ONCE instead of replaying
/// every trade into every target timeframe. Empty seconds are not synthesized.
///
/// covered_until_ms is the *verified* exclusive end of the underlying 1s
/// dataset. A Bar is final only when its entire window is covered; the last
/// window may be returned open if the input ends before its close.
// Compiled into native live collector through its shared TickBars module,
// but called only by the market historical-reconstruction library.
#[allow(dead_code)]
pub fn aggregate_historical_seconds(
    seconds: &[TickBar],
    interval: &str,
    covered_until_ms: i64,
) -> Result<Vec<TickBar>, String> {
    if interval == "1s" || !INTERVALS.contains(&interval) {
        return Err("historical 1s source requires a larger supported interval".into());
    }
    if covered_until_ms < 0 {
        return Err("invalid verified coverage end".into());
    }
    let mut output: Vec<TickBar> = Vec::new();
    let mut previous_start = None;
    for second in seconds {
        if second.interval != "1s"
            || !second.closed
            || second.open_time_ms < 0
            || second.open_time_ms % 1_000 != 0
            || second.close_time_ms != second.open_time_ms + 999
            || second.close_time_ms >= covered_until_ms
            || second.open <= Decimal::ZERO
            || second.high < second.open
            || second.high < second.close
            || second.low > second.open
            || second.low > second.close
            || second.volume <= Decimal::ZERO
            || previous_start.is_some_and(|last| second.open_time_ms <= last)
        {
            return Err("invalid, duplicated or unordered historical 1s Bar".into());
        }
        previous_start = Some(second.open_time_ms);
        let (start, end) = window(interval, second.open_time_ms)?;
        if let Some(current) = output.last_mut()
            && current.open_time_ms == start
        {
            current.high = current.high.max(second.high);
            current.low = current.low.min(second.low);
            current.close = second.close;
            current.volume = current.volume.checked_add(second.volume)
                .ok_or("historical Bar volume overflow")?;
            continue;
        }
        output.push(TickBar {
            interval: interval.into(),
            open_time_ms: start,
            close_time_ms: end - 1,
            open: second.open,
            high: second.high,
            low: second.low,
            close: second.close,
            volume: second.volume,
            closed: false,
        });
    }
    for bar in &mut output {
        bar.closed = bar.close_time_ms < covered_until_ms;
    }
    Ok(output)
}

impl TickBars {
    /// Returns one current snapshot for EACH supported period, immediately.
    /// A closed final snapshot for the previous bucket is additionally
    /// emitted when the first tick of the next bucket arrives.
    pub fn on_tick(&mut self,tick:&MarketTick)->Result<Vec<TickBar>,String>{
        if tick.id.is_empty()||tick.price<=Decimal::ZERO||tick.size<=Decimal::ZERO {
            return Err("invalid Tick identity/price/size".into());
        }
        if self.last.0.is_some_and(|prior|tick.ts_ns<prior) {
            return Err("out-of-order Tick: requires historical reconciliation".into());
        }
        if self.last.0==Some(tick.ts_ns)&&self.last.1.as_deref()==Some(&tick.id){
            return Ok(Vec::new());
        }
        let ms=i64::try_from(tick.ts_ns/1_000_000).map_err(|_|"tick time overflow")?;
        let mut updates=Vec::with_capacity(INTERVALS.len()*2);
        // Compute all boundary rules before modifying any aggregation state.
        let windows=INTERVALS.iter().map(|i|window(i,ms)).collect::<Result<Vec<_>,_>>()?;
        for (&interval,(start,end)) in INTERVALS.iter().zip(windows) {
            let current=self.states.get_mut(interval);
            match current{
                Some(old) if old.open_time_ms==start=>{
                    old.high=old.high.max(tick.price);
                    old.low=old.low.min(tick.price);
                    old.close=tick.price;
                    old.volume=old.volume.checked_add(tick.size).ok_or("volume overflow")?;
                    updates.push(old.clone());
                }
                Some(old)=>{
                    if old.open_time_ms>start{return Err("late Tick interval regression".into());}
                    let mut complete=old.clone();
                    complete.closed=true;
                    updates.push(complete);
                    let fresh=TickBar{
                        interval:interval.into(),open_time_ms:start,close_time_ms:end-1,
                        open:tick.price,high:tick.price,low:tick.price,close:tick.price,
                        volume:tick.size,closed:false,
                    };
                    self.states.insert(interval,fresh.clone());
                    updates.push(fresh);
                }
                None=>{
                    let fresh=TickBar{
                        interval:interval.into(),open_time_ms:start,close_time_ms:end-1,
                        open:tick.price,high:tick.price,low:tick.price,close:tick.price,
                        volume:tick.size,closed:false,
                    };
                    self.states.insert(interval,fresh.clone());
                    updates.push(fresh);
                }
            }
        }
        self.last=(Some(tick.ts_ns),Some(tick.id.clone()));
        Ok(updates)
    }
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

    fn second(start:i64,open:i64,high:i64,low:i64,close:i64,volume:i64)->TickBar{
        TickBar {
            interval:"1s".into(),open_time_ms:start,close_time_ms:start+999,
            open:Decimal::from(open),high:Decimal::from(high),
            low:Decimal::from(low),close:Decimal::from(close),
            volume:Decimal::from(volume),closed:true,
        }
    }

    #[test]
    fn historical_second_bars_aggregate_ohlcv_once_without_fabricated_empty_windows(){
        let rows = vec![
            second(0,100,110,100,105,2),
            second(1_000,105,120,104,119,3),
            second(901_000,90,95,89,92,4),
        ];
        let result=aggregate_historical_seconds(&rows,"15m",1_800_000).unwrap();
        assert_eq!(result.len(),2);
        assert_eq!((result[0].open,result[0].high,result[0].low,
            result[0].close,result[0].volume),
            (Decimal::from(100),Decimal::from(120),Decimal::from(100),
             Decimal::from(119),Decimal::from(5)));
        assert!(result.iter().all(|bar|bar.closed));
        assert_eq!(result[1].open_time_ms,900_000);
        assert_eq!(result[1].volume,Decimal::from(4));
        let partial=aggregate_historical_seconds(&rows,"15m",1_000_000).unwrap();
        assert!(partial[0].closed);
        assert!(!partial[1].closed);
    }

    #[test]
    fn historical_second_bars_validate_source_and_calendar_boundaries(){
        let rows=[second(0,100,100,100,100,1)];
        assert!(aggregate_historical_seconds(&rows,"1s",60_000).is_err());
        assert!(aggregate_historical_seconds(&rows,"15m",1_000).is_ok());
        assert!(aggregate_historical_seconds(&rows,"15m",500).is_err());
        assert!(aggregate_historical_seconds(&[rows[0].clone(),rows[0].clone()],"15m",2_000).is_err());
        let mut unclosed=rows[0].clone();
        unclosed.closed=false;
        assert!(aggregate_historical_seconds(&[unclosed],"1m",2_000).is_err());
        let feb=Utc.with_ymd_and_hms(2026,2,28,23,59,59).single().unwrap().timestamp_millis();
        let march=Utc.with_ymd_and_hms(2026,3,1,0,0,0).single().unwrap().timestamp_millis();
        let result=aggregate_historical_seconds(
            &[second(feb,100,100,100,100,2),second(march,101,101,101,101,3)],
            "1M",march+1_000,
        ).unwrap();
        assert_eq!(result.len(),2);
        assert!(result[0].closed);
        assert!(!result[1].closed);
    }
    #[test]
    fn windows_match_week_and_calendar_month(){
        assert_eq!(window("1w",0).unwrap().0,-3*86_400_000);
        assert_eq!(window("3d",86_400_000).unwrap(),(86_400_000,4*86_400_000));
        let feb=Utc.with_ymd_and_hms(2026,2,20,0,0,0).single().unwrap().timestamp_millis();
        assert_eq!(window("1M",feb).unwrap().1-window("1M",feb).unwrap().0,28*86_400_000);
    }
}
