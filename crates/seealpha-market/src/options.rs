//! Isolated public Binance options market-data adapter.
//! Unlike USD-M or Spot, an option symbol includes expiry, strike and side.
//! This module never owns credentials or places orders.
use crate::{Bar, MarketError, interval_ms, parse_binance_klines};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const KLINES: &str = "https://eapi.binance.com/eapi/v1/klines";
const EXCHANGE_INFO: &str = "https://eapi.binance.com/eapi/v1/exchangeInfo";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OptionIdentity {
    pub symbol: String,
    pub underlying: String,
    pub expiry: String,
    pub strike: Decimal,
    pub right: OptionRight,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OptionRight { Call, Put }

fn leap(year:u32)->bool { year.is_multiple_of(4) && (!year.is_multiple_of(100)||year.is_multiple_of(400)) }
fn valid_expiry(date:&str)->bool {
    if date.len()!=6 || !date.bytes().all(|b|b.is_ascii_digit()) {return false;}
    let y=2000+date[0..2].parse::<u32>().unwrap_or(0);
    let m=date[2..4].parse::<u32>().unwrap_or(0);
    let d=date[4..6].parse::<u32>().unwrap_or(0);
    let last=match m { 1|3|5|7|8|10|12=>31,4|6|9|11=>30,2 if leap(y)=>29,2=>28,_=>return false };
    (1..=last).contains(&d)
}
pub fn parse_option_symbol(raw:&str)->Result<OptionIdentity,MarketError>{
    if raw.len()>60 || raw!=raw.trim() || raw!=raw.to_ascii_uppercase() {
        return Err(MarketError::Symbol);
    }
    let items=raw.split('-').collect::<Vec<_>>();
    if items.len()!=4 {return Err(MarketError::Symbol);}
    let underlying=items[0];
    if underlying.len()<2 || underlying.len()>15 ||
        !underlying.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit()) ||
        !valid_expiry(items[1]) {
        return Err(MarketError::Symbol);
    }
    let strike=items[2].parse::<Decimal>().map_err(|_|MarketError::Symbol)?;
    if strike<=Decimal::ZERO || items[2].starts_with('-') ||
        !items[2].bytes().all(|b|b.is_ascii_digit()||b==b'.') ||
        items[2].matches('.').count()>1 {
        return Err(MarketError::Symbol);
    }
    let right=match items[3] {"C"=>OptionRight::Call,"P"=>OptionRight::Put,_=>return Err(MarketError::Symbol)};
    Ok(OptionIdentity{symbol:raw.to_owned(),underlying:underlying.to_owned(),
        expiry:items[1].to_owned(),strike,right})
}
fn now_ms()->Result<i64,MarketError>{
    SystemTime::now().duration_since(UNIX_EPOCH).ok()
        .and_then(|d|i64::try_from(d.as_millis()).ok()).ok_or(MarketError::Time)
}
#[derive(Clone)]
pub struct OptionsSource { client:reqwest::Client }
impl OptionsSource {
    pub fn new()->Result<Self,MarketError>{
        let client=reqwest::Client::builder().https_only(true)
            .timeout(Duration::from_secs(12)).redirect(reqwest::redirect::Policy::none())
            .user_agent("seealpha-native-options/0.1").build()
            .map_err(|_|MarketError::Network)?;
        Ok(Self{client})
    }
    async fn get(&self,url:&str,params:&[(&str,String)])->Result<Value,MarketError>{
        let response=self.client.get(url).query(params).send().await.map_err(|_|MarketError::Network)?;
        let status=response.status();
        if !status.is_success() {return Err(MarketError::Status(status.as_u16()));}
        response.json::<Value>().await.map_err(|_|MarketError::Payload)
    }
    /// Return exchange-authored contracts only; never infer strike, size or
    /// exercise characteristics from a locally guessed symbol.
    pub async fn contracts(&self)->Result<Vec<Value>,MarketError>{
        let data=self.get(EXCHANGE_INFO,&[]).await?;
        let rows=data["optionSymbols"].as_array().ok_or(MarketError::Payload)?;
        if rows.len()>50_000 {return Err(MarketError::Payload);}
        let mut items=Vec::with_capacity(rows.len());
        for row in rows {
            let symbol=row["symbol"].as_str().ok_or(MarketError::Payload)?;
            let id=parse_option_symbol(symbol)?;
            let expiry=row["expiryDate"].as_i64().ok_or(MarketError::Payload)?;
            let status=row["status"].as_str().ok_or(MarketError::Payload)?;
            let underlying=row["underlying"].as_str().ok_or(MarketError::Payload)?;
            let unit=row["unit"].as_i64().ok_or(MarketError::Payload)?;
            if unit<=0 || expiry<=0 || underlying.is_empty() {
                return Err(MarketError::Payload);
            }
            items.push(json!({"symbol":id.symbol,"underlying":underlying,"expiry_ms":expiry,
                "strike":id.strike.to_string(),"right":if id.right==OptionRight::Call{"CALL"}else{"PUT"},
                "unit":unit,"status":status,"filters":row["filters"]}));
        }
        Ok(items)
    }
    /// Strict [start,end) history. Binance may legitimately have no trades:
    /// a missing bar is not silently filled or declared complete.
    pub async fn history(&self,symbol:&str,interval:&str,start:i64,end:i64)->Result<Vec<Bar>,MarketError>{
        parse_option_symbol(symbol)?;
        if !matches!(interval,"1m"|"5m"|"15m") {return Err(MarketError::Interval);}
        let step=interval_ms(interval)?;
        if start<0 || end<=start || start%step!=0 || end%step!=0 ||
            end>now_ms()? || (end-start)/step>5000 {
            return Err(MarketError::Time);
        }
        let mut out=Vec::new();
        let mut cursor=start;
        while cursor<end {
            let chunk=((end-cursor)/step).min(1000);
            let payload=self.get(KLINES,&[
                ("symbol",symbol.to_owned()),
                ("interval",interval.to_owned()),
                ("startTime",cursor.to_string()),
                ("endTime",(end-1).to_string()),
                ("limit",chunk.to_string()),
            ]).await?;
            let bars=parse_binance_klines(&payload,step)?;
            for bar in bars {
                if bar.open_time_ms!=cursor || bar.close_time_ms>=end {
                    return Err(MarketError::Time);
                }
                cursor=cursor.checked_add(step).ok_or(MarketError::Overflow)?;
                out.push(bar);
            }
            if out.is_empty() || cursor<end && chunk>0 && payload.as_array().is_some_and(Vec::is_empty) {
                return Err(MarketError::Empty);
            }
        }
        Ok(out)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn option_contract_identity_is_not_a_generic_spot_symbol(){
        let id=parse_option_symbol("BTC-271231-90000-C").unwrap();
        assert_eq!(id.right,OptionRight::Call);
        assert_eq!(id.strike,Decimal::from(90000));
        for bad in ["ALL","BTCUSDT","BTC-270230-90000-C",
            "BTC-271231-0-C","BTC-271231-90000-Q","BTC-271231-90000-C/../../",
            "btc-271231-90000-c"] {
            assert!(parse_option_symbol(bad).is_err(),"{bad}");
        }
    }
}
