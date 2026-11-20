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
        // Older Binance fundingRate rows may not carry an event class; do not
        // infer a regular eight-hour settlement from missing metadata.
        let rate_type=row["rateType"].as_str().unwrap_or("Unknown");
        let rate=rate.parse::<Decimal>().map_err(|_|MarketError::Decimal)?;
        let mark=mark.parse::<Decimal>().map_err(|_|MarketError::Decimal)?;
        if symbol!=requested_symbol || !(start..end).contains(&time) ||
            last.is_some_and(|p|time<=p) ||
            mark<=Decimal::ZERO || !matches!(rate_type,"Regular"|"Special"|"Unknown") {
            return Err(MarketError::Payload);
        }
        result.push(FundingObservation{symbol:symbol.to_owned(),funding_time_ms:time,
            funding_rate:rate.to_string(),mark_price:mark.to_string(),
            rate_type:rate_type.to_owned()});
        last=Some(time);
    }
    Ok(result)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingEstimate {
    pub symbol: String,
    pub event_time_ms: i64,
    pub next_funding_ms: i64,
    pub estimated_rate: Decimal,
}

/// Parse a markPriceUpdate without promoting its mutable projected rate to a
/// settled FundingRateUpdate or an account funding payment.
pub fn parse_funding_estimate(
    value: &Value, requested_symbol: &str,
) -> Result<FundingEstimate, MarketError> {
    check_symbol(requested_symbol)?;
    if value["e"].as_str() != Some("markPriceUpdate")
        || value["s"].as_str() != Some(requested_symbol) {
        return Err(MarketError::Payload);
    }
    let event = value["E"].as_i64().ok_or(MarketError::Time)?;
    let next = value["T"].as_i64().ok_or(MarketError::Time)?;
    let rate = value["r"].as_str().ok_or(MarketError::Decimal)?
        .parse::<Decimal>().map_err(|_| MarketError::Decimal)?;
    if event < 0 || next <= event { return Err(MarketError::Time); }
    Ok(FundingEstimate { symbol: requested_symbol.to_owned(),
        event_time_ms: event, next_funding_ms: next, estimated_rate: rate })
}

/// The settled rate is sourced solely from the exchange's public REST
/// fundingRate history. It is not evidence of an account-specific payment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingRateUpdate {
    pub symbol: String,
    pub ts_event_ns: i64,
    pub ts_init_ns: i64,
    pub next_funding_ns: i64,
    pub rate: Decimal,
    pub rate_type: String,
    pub account_payment_confirmed: bool,
}
impl FundingObservation {
    pub fn to_update(&self) -> Result<FundingRateUpdate, MarketError> {
        let nanos = self.funding_time_ms.checked_mul(1_000_000)
            .ok_or(MarketError::Overflow)?;
        let rate = self.funding_rate.parse::<Decimal>()
            .map_err(|_| MarketError::Decimal)?;
        Ok(FundingRateUpdate {
            symbol: self.symbol.clone(), ts_event_ns: nanos, ts_init_ns: nanos,
            next_funding_ns: nanos, rate, rate_type: self.rate_type.clone(),
            account_payment_confirmed: false,
        })
    }
}

/// An exchange REST scan can establish which rates were observed, not prove
/// that an empty response covers an arbitrary historical period.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingHistoryRead {
    pub observations: Vec<FundingObservation>,
}
impl FundingHistoryRead {
    pub fn historical_complete(&self) -> bool { false }
    pub fn observed_updates(&self) -> Result<Vec<FundingRateUpdate>, MarketError> {
        self.observations.iter().map(FundingObservation::to_update).collect()
    }
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
    pub async fn history_observed(
        &self, symbol: &str, start: i64, end: i64,
    ) -> Result<FundingHistoryRead, MarketError> {
        Ok(FundingHistoryRead { observations: self.history(symbol, start, end).await? })
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
        let no_type = json!([{"symbol":"BTCUSDT","fundingTime":22,
            "fundingRate":"0.0001","markPrice":"31000"}]);
        assert_eq!(parse_funding_rows(&no_type,"BTCUSDT",10,30,None)
            .unwrap()[0].rate_type,"Unknown");
        assert!(parse_funding_rows(&raw,"BTCUSDT",10,30,Some(11)).is_err());
    }

    #[test]
    fn estimate_cannot_be_promoted_to_a_settled_payment() {
        let raw = json!({"e":"markPriceUpdate","s":"ETHUSDT",
            "E":101,"T":180,"r":"-0.00001"});
        let estimate = parse_funding_estimate(&raw, "ETHUSDT").unwrap();
        assert_eq!(estimate.next_funding_ms, 180);
        assert!(estimate.estimated_rate < Decimal::ZERO);
        let mut wrong = raw.clone();
        wrong["T"] = json!(100);
        assert!(parse_funding_estimate(&wrong, "ETHUSDT").is_err());
        let mut wrong_symbol = raw;
        wrong_symbol["s"] = json!("BTCUSDT");
        assert!(parse_funding_estimate(&wrong_symbol, "ETHUSDT").is_err());
    }

    #[test]
    fn special_settlements_preserve_exact_nanoseconds_and_unverified_coverage() {
        let data = json!([
            {"symbol":"ETHUSDT","fundingTime":10,"fundingRate":"-0.0002",
             "markPrice":"3000","rateType":"Special"},
            {"symbol":"ETHUSDT","fundingTime":13,"fundingRate":"0.0001",
             "markPrice":"2990","rateType":"Regular"}
        ]);
        let rows = parse_funding_rows(&data, "ETHUSDT", 0, 20, None).unwrap();
        let history = FundingHistoryRead { observations: rows };
        let updates = history.observed_updates().unwrap();
        assert_eq!(updates[0].ts_event_ns, 10_000_000);
        assert_eq!(updates[0].next_funding_ns, updates[0].ts_event_ns);
        assert_eq!(updates[0].rate_type, "Special");
        assert!(!updates[0].account_payment_confirmed);
        assert!(!history.historical_complete());
        assert!(!FundingHistoryRead { observations: vec![] }.historical_complete());
        assert!(parse_funding_rows(&data, "ETHUSDT", 0, 20, Some(10)).is_err());
        let mut duplicate = data;
        duplicate[1]["fundingTime"] = json!(10);
        assert!(parse_funding_rows(&duplicate, "ETHUSDT", 0, 20, None).is_err());
    }

}
