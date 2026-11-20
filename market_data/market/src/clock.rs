//! Python ServerClock port: exchange time is authoritative for finalized
//! bars. The host wall clock can lag/lead; it must not certify a candle.
use crate::MarketError;
use serde_json::Value;
use std::time::{Duration,Instant};

const MAX_RTT_MS:i64=2_000;
const MAX_DRIFT_MS:i64=5_000;
const MAX_AGE:Duration=Duration::from_secs(120);
#[derive(Debug)]
pub struct ServerClock{
    base_ms:i64,
    uncertainty_ms:i64,
    observed_at:Option<Instant>,
    valid:bool,
}
impl Default for ServerClock{
    fn default()->Self{Self::new()}
}
impl ServerClock {
    pub fn new()->Self{Self{base_ms:0,uncertainty_ms:0,observed_at:None,valid:false}}
    pub fn observe(&mut self,server_ms:i64,rtt_ms:i64,now:Instant)
        ->Result<(),MarketError>{
        self.valid=false;
        if server_ms<=0||!(0..=MAX_RTT_MS).contains(&rtt_ms){
            return Err(MarketError::Time);
        }
        let estimate=server_ms.checked_add(rtt_ms/2).ok_or(MarketError::Time)?;
        if self.base_ms>0 {
            let previous=self.current_ms(now)?;
            if estimate.abs_diff(previous)>MAX_DRIFT_MS as u64 {
                return Err(MarketError::Time);
            }
        }
        self.base_ms=estimate;
        self.uncertainty_ms=rtt_ms/2+1;
        self.observed_at=Some(now);
        self.valid=true;
        Ok(())
    }
    fn current_ms(&self,now:Instant)->Result<i64,MarketError>{
        let since=self.observed_at.ok_or(MarketError::Time)?;
        self.base_ms.checked_add(i64::try_from(now.saturating_duration_since(since)
            .as_millis()).map_err(|_|MarketError::Time)?).ok_or(MarketError::Time)
    }
    pub fn healthy(&self)->bool{
        self.valid && self.base_ms>0 &&
            self.observed_at.is_some_and(|value|value.elapsed()<MAX_AGE)
    }
    pub fn confirmed_ms(&self)->Result<i64,MarketError>{
        if !self.healthy(){return Err(MarketError::Time);}
        self.current_ms(Instant::now())?.checked_sub(self.uncertainty_ms)
            .ok_or(MarketError::Time)
    }
    pub async fn sync(&mut self,client:&reqwest::Client,scope:&str)
        ->Result<(),MarketError>{
        self.valid=false;
        let endpoint=match scope {
            "spot"=>"https://api.binance.com/api/v3/time",
            "futures_um"=>"https://fapi.binance.com/fapi/v1/time",
            "options"=>"https://eapi.binance.com/eapi/v1/time",
            _=>return Err(MarketError::Time),
        };
        let before=Instant::now();
        let response=client.get(endpoint).send().await.map_err(|_|MarketError::Network)?;
        if !response.status().is_success(){
            return Err(MarketError::Status(response.status().as_u16()));
        }
        let body=response.json::<Value>().await.map_err(|_|MarketError::Payload)?;
        let now=Instant::now();
        let elapsed=i64::try_from(now.duration_since(before).as_millis())
            .map_err(|_|MarketError::Time)?;
        let server=body["serverTime"].as_i64().ok_or(MarketError::Time)?;
        self.observe(server,elapsed,now)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn python_clock_rejects_long_roundtrips_and_large_jumps(){
        let start=Instant::now();
        let mut clock=ServerClock::new();
        assert!(clock.observe(100_000,2_001,start).is_err());
        clock.observe(100_000,100,start).unwrap();
        assert!(clock.healthy());
        assert!(clock.observe(110_000,100,start+Duration::from_secs(1)).is_err());
        assert!(!clock.healthy());
        clock.observe(101_000,100,start+Duration::from_secs(1)).unwrap();
    }
}
