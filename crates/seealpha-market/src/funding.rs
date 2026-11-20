//! Settled Binance USD-M funding observations.
//!
//! Funding intervals are venue-defined and can change; never synthesize a
//! funding payment every fixed eight hours or confuse mark-price estimates
//! with a settled funding record.
use crate::{MarketError,check_symbol};
use rust_decimal::Decimal;
use serde_json::Value;
use std::time::Duration;

#[derive(Debug,Clone,PartialEq,Eq)]
pub struct FundingObservation {
    pub symbol:String,
    pub funding_time_ms:i64,
    pub funding_rate:String,
    pub mark_price:String,
    pub rate_type:String,
}

pub fn parse_funding_rows(
    value:&Value,requested_symbol:&str,start:i64,end:i64,previous:Option<i64>,
)->Result<Vec<FundingObservation>,MarketError>{
    let data=value.as_array().ok_or(MarketError::Payload)?;
    if data.len()>1000 {return Err(MarketError::Payload);}
    let mut result=Vec::with_capacity(data.len());
    let mut last=previous;
    for row in data {
        let symbol=row["symbol"].as_str().ok_or(MarketError::Payload)?;
        let time=row["fundingTime"].as_i64().ok_or(MarketError::Time)?;
        let rate=row["fundingRate"].as_str().ok_or(MarketError::Decimal)?;
        let mark=row["markPrice"].as_str().ok_or(MarketError::Decimal)?;
        let rate_type=row["rateType"].as_str().unwrap_or("Regular");
        let rate=rate.parse::<Decimal>().map_err(|_|MarketError::Decimal)?;
        let mark=mark.parse::<Decimal>().map_err(|_|MarketError::Decimal)?;
        if symbol!=requested_symbol || !(start..end).contains(&time) ||
            last.is_some_and(|p|time<=p) ||
            mark<=Decimal::ZERO || !matches!(rate_type,"Regular"|"Special") {
            return Err(MarketError::Payload);
        }
        result.push(FundingObservation{symbol:symbol.to_owned(),funding_time_ms:time,
            funding_rate:rate.to_string(),mark_price:mark.to_string(),
            rate_type:rate_type.to_owned()});
        last=Some(time);
    }
    Ok(result)
}
#[derive(Clone)]
pub struct FundingSource {client:reqwest::Client}
impl FundingSource {
    pub fn new()->Result<Self,MarketError>{
        let client=reqwest::Client::builder()
            .https_only(true).redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(12))
            .user_agent("seealpha-native-funding/0.1")
            .build().map_err(|_|MarketError::Network)?;
        Ok(Self{client})
    }
    pub async fn history(
        &self,symbol:&str,start:i64,end:i64,
    )->Result<Vec<FundingObservation>,MarketError>{
        check_symbol(symbol)?;
        let mut clock=crate::clock::ServerClock::new();
        clock.sync(&self.client,"futures_um").await?;
        let now=clock.confirmed_ms()?;
        if start<0 || end<=start || end>now || end-start>365*86_400_000 {
            return Err(MarketError::Time);
        }
        let mut result=Vec::new();
        let mut cursor=start;
        loop {
            let response=self.client.get("https://fapi.binance.com/fapi/v1/fundingRate")
                .query(&[("symbol",symbol.to_owned()),
                    ("startTime",cursor.to_string()),
                    ("endTime",(end-1).to_string()),
                    ("limit","1000".into())])
                .send().await.map_err(|_|MarketError::Network)?;
            if !response.status().is_success() {
                return Err(MarketError::Status(response.status().as_u16()));
            }
            let value=response.json::<Value>().await.map_err(|_|MarketError::Payload)?;
            let page=parse_funding_rows(&value,symbol,start,end,
                result.last().map(|v:&FundingObservation|v.funding_time_ms))?;
            let n=page.len();
            if let Some(last)=page.last() {
                cursor=last.funding_time_ms.checked_add(1).ok_or(MarketError::Overflow)?;
            }
            if result.len()+n>5000 {return Err(MarketError::Payload);}
            result.extend(page);
            if n<1000 || cursor>=end {break;}
        }
        // These are exchange-supplied settled events, but we do not
        // manufacture missing event timestamps or certify full coverage.
        Ok(result)
    }
}
#[cfg(test)]
mod tests{
    use super::*;
    use serde_json::json;
    #[test]
    fn settled_funding_parses_without_fabricated_eight_hour_intervals(){
        let raw=json!([{"symbol":"BTCUSDT","fundingTime":11,
            "fundingRate":"-0.0002","markPrice":"30000","rateType":"Regular"},
            {"symbol":"BTCUSDT","fundingTime":20,
            "fundingRate":"0.0001","markPrice":"31000","rateType":"Special"}]);
        let rows=parse_funding_rows(&raw,"BTCUSDT",10,30,None).unwrap();
        assert_eq!(rows.len(),2);
        assert_eq!(rows[0].funding_rate,"-0.0002");
        assert!(parse_funding_rows(&raw,"BTCUSDT",10,30,Some(11)).is_err());
    }
}
