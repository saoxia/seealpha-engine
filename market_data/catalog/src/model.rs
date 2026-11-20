//! The Python Engine's native market identity and UTC bar-time conventions.
//! This module does not depend on NautilusTrader Python or a private API.
use chrono::{Datelike,TimeZone,Utc};
use rust_decimal::Decimal;
use serde::{Deserialize,Serialize};
use crate::CatalogError;

pub const NANO_PER_MS:i64=1_000_000;
pub const DAY_MS:i64=86_400_000;
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
pub enum MarketProduct { Spot, FuturesUm, Options }
impl MarketProduct{
    pub fn parse(raw:&str)->Result<Self,CatalogError>{
        match raw {
            "spot"=>Ok(Self::Spot),
            "futures_um"=>Ok(Self::FuturesUm),
            "options"=>Ok(Self::Options),
            _=>Err(CatalogError::Invalid("行情产品必须为 spot、futures_um 或 options。".into()))
        }
    }
    pub fn as_str(self)->&'static str{
        match self {Self::Spot=>"spot",Self::FuturesUm=>"futures_um",Self::Options=>"options"}
    }
}
fn valid_option(symbol:&str)->bool{
    let parts:Vec<_>=symbol.split('-').collect();
    if parts.len()!=4 || parts[0].len()<2 || parts[0].len()>15 ||
       !parts[0].bytes().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit()){
        return false;
    }
    let date=parts[1];
    if date.len()!=6 || !date.bytes().all(|c|c.is_ascii_digit()) {return false;}
    let yy=date[0..2].parse::<i32>().ok();
    let mm=date[2..4].parse::<u32>().ok();
    let dd=date[4..6].parse::<u32>().ok();
    if let (Some(yy),Some(mm),Some(dd))=(yy,mm,dd) {
        if chrono::NaiveDate::from_ymd_opt(2000+yy,mm,dd).is_none() {return false;}
    }else{return false;}
    if parts[3]!="C"&&parts[3]!="P" {return false;}
    if parts[2].matches('.').count()>1 ||
        !parts[2].bytes().all(|c|c.is_ascii_digit()||c==b'.') {
        return false;
    }
    parts[2].parse::<Decimal>().is_ok_and(|strike|strike>Decimal::ZERO)
}
pub fn instrument_id(symbol:&str,product:MarketProduct)->Result<String,CatalogError>{
    if symbol.len()>64 || symbol.is_empty() ||
       symbol!=symbol.trim() || symbol!=symbol.to_ascii_uppercase() {
        return Err(CatalogError::Invalid("行情代码未规范化。".into()));
    }
    let valid=match product {
        MarketProduct::Spot=>
            (3..=30).contains(&symbol.len())&&
            symbol.bytes().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit()),
        MarketProduct::FuturesUm=>
            (5..=30).contains(&symbol.len())&&
            (symbol.ends_with("USDT")||symbol.ends_with("USDC"))&&
            symbol.bytes().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit()),
        MarketProduct::Options=>valid_option(symbol),
    };
    if !valid {return Err(CatalogError::Invalid("行情代码无效。".into()));}
    Ok(match product {
        MarketProduct::FuturesUm=>format!("{symbol}-PERP.BINANCE"),
        _=>format!("{symbol}.BINANCE"),
    })
}
pub fn bar_type(symbol:&str,product:MarketProduct,interval:&str)->Result<String,CatalogError>{
    let instrument=instrument_id(symbol,product)?;
    let (multiple,unit)=match interval{
        "1s"=>(1,"SECOND"),
        "1m"=>(1,"MINUTE"),"3m"=>(3,"MINUTE"),"5m"=>(5,"MINUTE"),
        "15m"=>(15,"MINUTE"),"30m"=>(30,"MINUTE"),
        "1h"=>(1,"HOUR"),"2h"=>(2,"HOUR"),"4h"=>(4,"HOUR"),
        "6h"=>(6,"HOUR"),"8h"=>(8,"HOUR"),"12h"=>(12,"HOUR"),
        "1d"=>(1,"DAY"),"3d"=>(3,"DAY"),"1w"=>(1,"WEEK"),
        "1M"=>(1,"MONTH"),
        _=>return Err(CatalogError::Invalid("不支持的 Nautilus K 线周期。".into())),
    };
    Ok(format!("{instrument}-{multiple}-{unit}-LAST-EXTERNAL"))
}
pub fn interval_ms(interval:&str)->Result<Option<i64>,CatalogError>{
    Ok(match interval {
        "1s"=>Some(1_000),"1m"=>Some(60_000),"3m"=>Some(180_000),
        "5m"=>Some(300_000),"15m"=>Some(900_000),"30m"=>Some(1_800_000),
        "1h"=>Some(3_600_000),"2h"=>Some(7_200_000),"4h"=>Some(14_400_000),
        "6h"=>Some(21_600_000),"8h"=>Some(28_800_000),"12h"=>Some(43_200_000),
        "1d"=>Some(DAY_MS),"3d"=>Some(3*DAY_MS),"1w"=>Some(7*DAY_MS),
        "1M"=>None,
        _=>return Err(CatalogError::Invalid("不支持的 Nautilus K 线周期。".into())),
    })
}
/// Matches core.market.instruments.bar_window:
/// 3d windows start one UTC day after 1970-01-01; weeks start Monday.
pub fn bar_window(interval:&str,ts_ms:i64)->Result<(i64,i64),CatalogError>{
    if ts_ms<0 {return Err(CatalogError::Invalid("K 线时间必须为正 UTC 毫秒。".into()));}
    if let Some(width)=interval_ms(interval)? {
        let offset=match interval {"1w"=>4*DAY_MS,"3d"=>DAY_MS,_=>0};
        let start=(ts_ms-offset).div_euclid(width)*width+offset;
        return Ok((start,start.checked_add(width)
            .ok_or_else(||CatalogError::Invalid("K 线结束时间溢出。".into()))?));
    }
    let at=Utc.timestamp_millis_opt(ts_ms).single()
        .ok_or_else(||CatalogError::Invalid("K 线月份时间无效。".into()))?;
    let year=at.year();let month=at.month();
    let first=Utc.with_ymd_and_hms(year,month,1,0,0,0).single()
        .ok_or_else(||CatalogError::Invalid("K 线月份边界无效。".into()))?;
    let (next_y,next_m)=if month==12 {(year+1,1)} else {(year,month+1)};
    let after=Utc.with_ymd_and_hms(next_y,next_m,1,0,0,0).single()
        .ok_or_else(||CatalogError::Invalid("K 线月份边界无效。".into()))?;
    Ok((first.timestamp_millis(),after.timestamp_millis()))
}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
pub struct CatalogBar {
    pub open_time_ms:i64,
    pub open:Decimal,
    pub high:Decimal,
    pub low:Decimal,
    pub close:Decimal,
    pub volume:Decimal,
}
impl CatalogBar {
    pub fn event_ns(&self,interval:&str)->Result<i64,CatalogError>{
        let (start,end)=bar_window(interval,self.open_time_ms)?;
        if start!=self.open_time_ms {
            return Err(CatalogError::Invalid("K 线没有对齐 Binance/UTC 边界。".into()));
        }
        end.checked_sub(1)
            .and_then(|ms|ms.checked_mul(NANO_PER_MS))
            .ok_or_else(||CatalogError::Invalid("K 线纳秒时间溢出。".into()))
    }
    pub fn validate(&self,interval:&str,product:MarketProduct)->Result<(),CatalogError>{
        self.event_ns(interval)?;
        if self.open<Decimal::ZERO||self.high<Decimal::ZERO||self.low<Decimal::ZERO||
            self.close<Decimal::ZERO||self.volume<Decimal::ZERO ||
            self.high<self.open||self.high<self.close||
            self.low>self.open||self.low>self.close ||
            (product!=MarketProduct::Options &&
            (self.open.is_zero()||self.high.is_zero()||self.low.is_zero()||self.close.is_zero())) {
            return Err(CatalogError::Invalid("K 线 OHLCV 质量校验失败。".into()));
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn identifiers_match_python_bar_type_for(){
        assert_eq!(instrument_id("ETHUSDT",MarketProduct::FuturesUm).unwrap(),
            "ETHUSDT-PERP.BINANCE");
        assert_eq!(bar_type("ETHUSDT",MarketProduct::FuturesUm,"1m").unwrap(),
            "ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL");
        assert_eq!(bar_type("BTC-271231-90000-P",MarketProduct::Options,"5m").unwrap(),
            "BTC-271231-90000-P.BINANCE-5-MINUTE-LAST-EXTERNAL");
        assert!(instrument_id("../ETHUSDT",MarketProduct::Spot).is_err());
    }
    #[test]
    fn utc_windows_match_python_three_day_week_and_month(){
        assert_eq!(bar_window("1w",0).unwrap().0,-3*DAY_MS);
        assert_eq!(bar_window("3d",DAY_MS).unwrap(),(DAY_MS,4*DAY_MS));
        let at=Utc.with_ymd_and_hms(2026,2,15,1,2,3).single().unwrap().timestamp_millis();
        let (from,until)=bar_window("1M",at).unwrap();
        assert_eq!(until-from,28*DAY_MS);
        assert_eq!(from,Utc.with_ymd_and_hms(2026,2,1,0,0,0)
            .single().unwrap().timestamp_millis());
    }
}
