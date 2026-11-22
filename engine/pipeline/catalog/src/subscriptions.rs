//! Durable desired subscription state; Pipeline reconciles it without Runtime jobs.
use crate::control::{ControlError,ControlStore,db_error,now_ms};
use chrono::NaiveDate;
use seealpha_database::params;
use serde::{Serialize,Deserialize};

#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSubscription {
    pub product:String,
    pub symbol:String,
    pub history_start:String,
    pub realtime_enabled:bool,
    pub enabled:bool,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub price_precision:Option<u32>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub size_precision:Option<u32>,
}

impl PipelineSubscription {
    pub fn start_ms(&self)->Result<i64,ControlError>{
        NaiveDate::parse_from_str(&self.history_start,"%Y-%m-%d")
            .map_err(|_|ControlError::Conflict("invalid history_start".into()))?
            .and_hms_opt(0,0,0).ok_or_else(||ControlError::Conflict("invalid midnight".into()))
            .map(|dt|dt.and_utc().timestamp_millis())
    }
}

pub fn validate_subscription(value:&PipelineSubscription)->Result<(),ControlError>{
    if !matches!(value.product.as_str(),"spot"|"futures_um") {
        return Err(ControlError::Conflict("only spot and futures_um supported".into()));
    }
    if !(5..=30).contains(&value.symbol.len())
       || !value.symbol.bytes().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit())
       || (value.product=="futures_um" && !value.symbol.ends_with("USDT") && !value.symbol.ends_with("USDC")) {
       return Err(ControlError::Conflict("invalid Binance symbol".into()));
    }
    let date=NaiveDate::parse_from_str(&value.history_start,"%Y-%m-%d")
        .map_err(|_|ControlError::Conflict("history_start must be YYYY-MM-DD".into()))?;
    if date.format("%Y-%m-%d").to_string()!=value.history_start
       || date < NaiveDate::from_ymd_opt(2017,1,1).unwrap() {
        return Err(ControlError::Conflict("history_start is invalid or unsupported".into()));
    }
    if [value.price_precision,value.size_precision].iter().flatten().any(|p|*p>16)
        || value.price_precision.is_some()!=value.size_precision.is_some() {
        return Err(ControlError::Conflict("price and size precision must both be 0..16".into()));
    }
    Ok(())
}

impl ControlStore {
    pub fn upsert_subscription(&self,tenant:&str,value:&PipelineSubscription)->Result<(),ControlError>{
        validate_subscription(value)?;
        if tenant.is_empty() || tenant.len()>128
            || !tenant.bytes().all(|c|c.is_ascii_alphanumeric()||c==b'_'||c==b'-') {
            return Err(ControlError::Conflict("invalid tenant".into()));
        }
        let conn=self.lock()?;
        conn.execute("INSERT INTO pipeline_subscriptions
          (tenant_id,product,symbol,history_start,realtime_enabled,enabled,price_precision,size_precision,updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
          ON CONFLICT(tenant_id,product,symbol) DO UPDATE SET
          history_start=EXCLUDED.history_start,realtime_enabled=EXCLUDED.realtime_enabled,
          enabled=EXCLUDED.enabled,price_precision=EXCLUDED.price_precision,
          size_precision=EXCLUDED.size_precision,updated_at=EXCLUDED.updated_at",
          params![tenant,value.product,value.symbol,value.history_start,value.realtime_enabled,
            value.enabled,value.price_precision.map(|p|p as i32),
            value.size_precision.map(|p|p as i32),now_ms()]).map_err(db_error)?;
        Ok(())
    }
    pub fn subscriptions(&self,tenant:&str,product:Option<&str>)->Result<Vec<PipelineSubscription>,ControlError>{
        if product.is_some_and(|p| !matches!(p,"spot"|"futures_um")) {
            return Err(ControlError::Conflict("invalid product".into()));
        }
        let conn=self.lock()?;
        let mut q=conn.prepare("SELECT product,symbol,history_start,realtime_enabled,enabled,
              price_precision,size_precision FROM pipeline_subscriptions
              WHERE tenant_id=$1 AND ($2::text IS NULL OR product=$2) ORDER BY product,symbol")
              .map_err(db_error)?;
        q.query_map(params![tenant,product],|r|{
            let price:Option<i32>=r.get(5)?; let size:Option<i32>=r.get(6)?;
            Ok(PipelineSubscription {
                product:r.get(0)?,symbol:r.get(1)?,history_start:r.get(2)?,
                realtime_enabled:r.get(3)?,enabled:r.get(4)?,
                price_precision:price.map(|v|v as u32),size_precision:size.map(|v|v as u32)
            })
        }).map_err(db_error)?.collect::<Result<Vec<_>,_>>().map_err(db_error)
    }
    pub fn disable_subscription(&self,tenant:&str,product:&str,symbol:&str)->Result<bool,ControlError>{
        let conn=self.lock()?;
        Ok(conn.execute("UPDATE pipeline_subscriptions SET enabled=FALSE,realtime_enabled=FALSE,
              updated_at=$4 WHERE tenant_id=$1 AND product=$2 AND symbol=$3",
              params![tenant,product,symbol,now_ms()]).map_err(db_error)?==1)
    }
    pub fn enabled_subscriptions(&self,product:&str)->Result<Vec<(String,PipelineSubscription)>,ControlError>{
        if !matches!(product,"spot"|"futures_um") {
            return Err(ControlError::Conflict("invalid product".into()));
        }
        let conn=self.lock()?;
        let mut q=conn.prepare("SELECT tenant_id,product,symbol,history_start,realtime_enabled,enabled,
              price_precision,size_precision FROM pipeline_subscriptions
              WHERE product=$1 AND enabled=TRUE ORDER BY tenant_id,symbol").map_err(db_error)?;
        q.query_map(params![product],|r|{
            let price:Option<i32>=r.get(6)?;let size:Option<i32>=r.get(7)?;
            Ok((r.get(0)?,PipelineSubscription{
                product:r.get(1)?,symbol:r.get(2)?,history_start:r.get(3)?,
                realtime_enabled:r.get(4)?,enabled:r.get(5)?,
                price_precision:price.map(|v|v as u32),size_precision:size.map(|v|v as u32),
            }))
        }).map_err(db_error)?.collect::<Result<Vec<_>,_>>().map_err(db_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn item(start:&str)->PipelineSubscription {
        PipelineSubscription {product:"spot".into(),symbol:"ETHUSDT".into(),
            history_start:start.into(),realtime_enabled:true,enabled:true,
            price_precision:None,size_precision:None}
    }
    #[test] fn strict_utc_dates_and_schema(){
        assert_eq!(item("2025-01-01").start_ms().unwrap(),1735689600000);
        for bad in ["2025-02-30","2025-1-1","2016-01-01","bad"] {
            assert!(validate_subscription(&item(bad)).is_err());
        }
        assert!(serde_json::from_str::<PipelineSubscription>(
            r#"{"product":"spot","symbol":"ETHUSDT","history_start":"2025-01-01",
                "enabled":true,"realtime_enabled":false,"token":"unsafe"}"#).is_err());
    }
}
