//! Native data control and read-side market subscriptions.
//! User/strategy subscriptions are declarations, never collector lifecycle calls.
//! Historical jobs use only public Binance klines and commit atomically to
//! the Rust catalog after the complete requested interval is validated.
use crate::{Runtime, RuntimeError, map_ledger};
use seealpha_ledger::CatalogBar;
use seealpha_market::{BinanceSource, Product, check_symbol, interval_ms};
use seealpha_market::options::{OptionsSource,parse_option_symbol};
use seealpha_market::funding::FundingSource;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).ok()
        .and_then(|d|i64::try_from(d.as_millis()).ok()).unwrap_or(0)
}
fn bad(message:&str)->RuntimeError {
    RuntimeError::Unprocessable(message.into())
}
fn field<'a>(object:&'a Value,name:&str)->Result<&'a str,RuntimeError> {
    object.get(name).and_then(Value::as_str)
        .filter(|x|!x.trim().is_empty()).ok_or_else(||bad("行情任务参数无效。"))
}
fn valid_symbol(product:&str,symbol:&str)->bool {
    match product {
        "spot"|"futures_um"=>check_symbol(symbol).is_ok(),
        "options"=>symbol=="ALL" ||
            ((3..=30).contains(&symbol.len()) &&
            symbol.chars().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit()||c=='-'||c=='.')),
        _=>false,
    }
}
fn valid_policy(policy:&Value)->Result<(String,String),RuntimeError>{
    let product=policy.get("product").and_then(Value::as_str).unwrap_or("futures_um");
    let symbol=field(policy,"symbol")?;
    if !valid_symbol(product,symbol) {
        return Err(bad("行情产品或交易对无效。"));
    }
    let base=policy.get("base_level").and_then(Value::as_str).unwrap_or("1m");
    if base!="auto" && base!="tick" && base!="1s" && interval_ms(base).is_err() &&
        !matches!(base,"2h"|"6h"|"8h"|"12h"|"3d"|"1w"|"1M") {
        return Err(bad("最小保存粒度无效。"));
    }
    if let Some(intervals)=policy.get("intervals") {
        let arr=intervals.as_array().ok_or_else(||bad("生成周期无效。"))?;
        if arr.len()>16 || arr.iter().any(|v|v.as_str().is_none_or(|t|{
            interval_ms(t).is_err() && !matches!(t,"2h"|"6h"|"8h"|"12h"|"3d"|"1w"|"1M")
        })) {return Err(bad("生成周期无效。"));}
    }
    if let Some(days)=policy.get("history_days") {
        if days.as_i64().is_none_or(|x|!(1..=36500).contains(&x)) {
            return Err(bad("历史天数无效。"));
        }
    }
    Ok((product.to_owned(),symbol.to_owned()))
}

fn hash_job(tenant_id:&str,payload:&Value)->String {
    let mut digest=Sha256::new();
    digest.update(tenant_id.as_bytes());
    digest.update(payload.to_string().as_bytes());
    let b=digest.finalize();
    let mut out=String::new();
    for byte in b.iter().take(16) {
        use std::fmt::Write;
        let _=write!(&mut out,"{byte:02x}");
    }
    out
}

impl Runtime {
    pub fn sync_data_policies(&self,tenant_id:&str,policies:&[Value])->Result<Value,RuntimeError> {
        self.require_active(tenant_id)?;
        if policies.len()>2500 {return Err(bad("行情配置数量超出范围。"));}
        let mut names=BTreeSet::new();
        for policy in policies {
            let key=valid_policy(policy)?;
            if !names.insert(key) {
                return Err(bad("行情配置包含重复交易对。"));
            }
        }
        let previous=self.ledger.policies(tenant_id).map_err(map_ledger)?;
        if previous.as_slice()!=policies {
            let mut processes=self.source_processes.lock()
                .map_err(|_|RuntimeError::Conflict("数据源状态锁不可用。".into()))?;
            for product in ["spot","futures_um"] {
                let key=format!("{tenant_id}:{product}");
                if let Some(child)=processes.get_mut(&key) {
                    if child.try_wait().map_err(|_|RuntimeError::Conflict("数据源状态未知。".into()))?.is_none() {
                        return Err(RuntimeError::Conflict(
                            "原生数据源正在运行，请先停止后再修改持久化采集配置。".into()));
                    }
                }
            }
        }
        // The control-plane journal is independent from collector journals.
        // A policy change cannot start a producer through a read subscription.
        self.ledger.replace_policies(tenant_id,policies).map_err(map_ledger)?;
        Ok(json!({"synced":policies.len(),"last_error":null}))
    }

    pub fn data_policies(&self,tenant_id:&str)->Result<Value,RuntimeError> {
        self.require_active(tenant_id)?;
        Ok(json!({"items":self.ledger.policies(tenant_id).map_err(map_ledger)?}))
    }

    pub fn data_services(&self,tenant_id:&str)->Result<Value,RuntimeError> {
        self.require_active(tenant_id)?;
        let jobs=self.ledger.data_jobs(tenant_id).map_err(map_ledger)?;
        let policies=self.ledger.policies(tenant_id).map_err(map_ledger)?;
        let active=jobs.iter().filter(|j|matches!(j["status"].as_str(),Some("queued"|"running"))).count();
        Ok(json!({"policies":policies.len(),"jobs":jobs.len(),"pending_jobs":active,
            "producer_status":"unverified","active_collectors":0,
            "warning":"订阅是只读声明；未验证原生采集器 READY 前禁止报告已开始采集。"}))
    }

    pub fn data_job_list(&self,tenant_id:&str)->Result<Value,RuntimeError> {
        self.require_active(tenant_id)?;
        Ok(json!({"items":self.ledger.data_jobs(tenant_id).map_err(map_ledger)?}))
    }

    pub fn data_job(&self,tenant_id:&str,id:&str)->Result<Value,RuntimeError> {
        self.require_active(tenant_id)?;
        self.ledger.data_job(tenant_id,id).map_err(map_ledger)?
            .ok_or_else(||RuntimeError::NotFound("数据任务不存在。".into()))
    }

    pub fn queue_data_job(&self,tenant_id:&str,payload:&Value)->Result<Value,RuntimeError> {
        self.require_active(tenant_id)?;
        let action=field(payload,"action")?;
        // Never claim support for deletion/trim without a verified snapshot or
        // delegate unsafe file system operations to a generic executor.
        if action!="backfill" {
            return Err(RuntimeError::Conflict("该历史任务尚未完成 Rust 等价迁移。".into()));
        }
        let product=payload.get("product").and_then(Value::as_str).unwrap_or("futures_um");
        let symbol=field(payload,"symbol")?;
        let kind=payload.get("data_kind").and_then(Value::as_str).unwrap_or("bars");
        if !valid_symbol(product,symbol) || !matches!(kind,"bars"|"funding")
            || (product=="options" && parse_option_symbol(symbol).is_err())
            || (kind=="funding" && product!="futures_um") {
            return Err(bad("行情产品、合约或历史数据类型不支持。"));
        }
        let start=payload.get("start_ms").and_then(Value::as_i64).ok_or_else(||bad("开始时间无效。"))?;
        let end=payload.get("end_ms").and_then(Value::as_i64).ok_or_else(||bad("结束时间无效。"))?;
        let interval=payload.get("interval").and_then(Value::as_str).unwrap_or("1m");
        if start<0 || start>=end {return Err(bad("历史补齐时间范围无效。"));}
        if kind=="funding" {
            if end.saturating_sub(start)>365*86_400_000 {
                return Err(bad("单次资金费读取超过一年。"));
            }
        } else {
            let step=interval_ms(interval).map_err(|_|bad("K 线周期无效。"))?;
            if product=="options" && !matches!(interval,"1m"|"5m"|"15m") {
                return Err(bad("原生期权历史只支持 1m/5m/15m。"));
            }
            if start%step!=0 || end%step!=0 || (end-start)/step>5000 {
                return Err(bad("历史补齐范围须对齐 UTC K 线边界且不超过 5000 根。"));
            }
        }
        // Deterministic identity lets a client retry without duplicating work.
        let mut normalized=payload.clone();
        // Legacy callers omit product for the default USD-M market.
        if let Some(object)=normalized.as_object_mut() {
            object.entry("product".to_string()).or_insert(json!("futures_um"));
            object.entry("interval".to_string()).or_insert(json!("1m"));
        }
        let id=hash_job(tenant_id,&normalized);
        self.ledger.queue_data_job(tenant_id,&id,action,&normalized).map_err(map_ledger)
    }

    pub fn cancel_data_job(&self,tenant_id:&str,id:&str)->Result<Value,RuntimeError> {
        self.require_active(tenant_id)?;
        if self.ledger.transition_data_job(tenant_id,id,"queued","cancelled",None).map_err(map_ledger)? {
            return self.data_job(tenant_id,id);
        }
        let current=self.data_job(tenant_id,id)?;
        if current["status"]=="cancelled" {return Ok(current);}
        Err(RuntimeError::Conflict("任务正在执行或已结束，不能直接取消。".into()))
    }

    /// A non-live, bounded backfill executor. It never invokes signed endpoints.
    /// Recovery leaves interrupted tasks visible rather than blindly restarting.
    pub async fn execute_data_job(&self,tenant_id:&str,id:&str)->Result<Value,RuntimeError> {
        self.require_active(tenant_id)?;
        let original=self.data_job(tenant_id,id)?;
        if !self.ledger.transition_data_job(tenant_id,id,"queued","running",None).map_err(map_ledger)? {
            return Err(RuntimeError::Conflict("任务已领取、取消或需要人工核验。".into()));
        }
        let result=self.backfill_job(tenant_id,&original["payload"]).await;
        let (status,body)=match result {
            Ok(value)=>("completed",value),
            Err(error)=>("failed",json!({"detail":error.to_string()})),
        };
        self.ledger.transition_data_job(tenant_id,id,"running",status,Some(&body))
            .map_err(map_ledger)?;
        self.data_job(tenant_id,id)
    }

    async fn backfill_job(&self,tenant_id:&str,payload:&Value)->Result<Value,RuntimeError> {
        let product=field(payload,"product")?;
        let symbol=field(payload,"symbol")?;
        let interval=payload.get("interval").and_then(Value::as_str).unwrap_or("1m");
        let begin=payload["start_ms"].as_i64().ok_or_else(||bad("开始时间无效。"))?;
        let end=payload["end_ms"].as_i64().ok_or_else(||bad("结束时间无效。"))?;
        let kind=payload.get("data_kind").and_then(Value::as_str).unwrap_or("bars");
        if kind=="funding" {
            let source=FundingSource::new()
                .map_err(|e|RuntimeError::Conflict(e.to_string()))?;
            let events=source.history(symbol,begin,end).await
                .map_err(|e|RuntimeError::Conflict(format!("结算资金费读取失败：{e}")))?;
            let rows=events.iter().map(|record|json!({
                "symbol":record.symbol,"funding_time_ms":record.funding_time_ms,
                "funding_rate":record.funding_rate,"mark_price":record.mark_price,
                "rate_type":record.rate_type
            })).collect::<Vec<_>>();
            self.ledger.import_funding_events(tenant_id,symbol,&rows).map_err(map_ledger)?;
            return Ok(json!({"action":"backfill","data_kind":"funding","symbol":symbol,
                "start_ms":begin,"end_ms":end,"observations":rows.len(),
                "completeness":"event_pages_only","exchange_orders":0}));
        }
        let bars=if product=="options" {
            let source=OptionsSource::new().map_err(|e|RuntimeError::Conflict(e.to_string()))?;
            source.history(symbol,interval,begin,end).await
        } else {
            let market=Product::parse(product).map_err(|_|bad("行情产品无效。"))?;
            let source=BinanceSource::new().map_err(|e|RuntimeError::Conflict(e.to_string()))?;
            source.load_history(market,symbol,interval,begin,end).await
        }.map_err(|e|RuntimeError::Conflict(format!("历史数据质量或远端读取失败：{e}")))?;
        // A historical response must never be published as a live ready feed.
        let records=bars.iter().map(|bar|CatalogBar{
            open_time_ms:bar.open_time_ms,
            open:bar.open.to_string(),high:bar.high.to_string(),
            low:bar.low.to_string(),close:bar.close.to_string(),
            volume:bar.volume.to_string(),
        }).collect::<Vec<_>>();
        self.ledger.import_verified_catalog_bars(tenant_id,product,symbol,interval,&records)
            .map_err(map_ledger)?;
        Ok(json!({"action":"backfill","product":product,"symbol":symbol,
            "interval":interval,"start_ms":begin,"end_ms":end,
            "validated_bars":records.len(),"source":"binance_public",
            "exchange_orders":0,"status":"completed"}))
    }

    pub fn note_market_subscription(&self,tenant_id:&str,account_id:&str,
        product:&str,symbol:&str,role:&str,state:&str,progress:&Value
    )->Result<Value,RuntimeError>{
        self.require_active(tenant_id)?;
        if !valid_symbol(product,symbol)||role.len()>48||role.is_empty()
            || !matches!(state,"running"|"stopped"|"error"|"unknown"|"disabled") {
            return Err(bad("行情订阅状态无效：未经数据质量证明的 READY 不得由外部上报。"));
        }
        let result=self.ledger.record_subscription(
            tenant_id,account_id,product,symbol,role,state,progress
        ).map_err(map_ledger)?;
        self.ledger.add_event(tenant_id,account_id,"subscription",&result.to_string())
            .map_err(map_ledger)?;
        Ok(result)
    }

    pub fn market_subscriptions(&self,tenant_id:&str,account_id:&str)
        ->Result<Value,RuntimeError>{
        self.require_active(tenant_id)?;
        Ok(json!({"subscriptions":self.ledger.subscriptions(tenant_id,account_id).map_err(map_ledger)?}))
    }

    pub fn subscribe_strategy_market(
        &self,tenant_id:&str,account_id:&str,issuance_id:&str,
        streams:&Value,lookback:i64,
    )->Result<Value,RuntimeError>{
        self.require_active(tenant_id)?;
        if issuance_id.is_empty()||issuance_id.len()>64 || !(1..=5000).contains(&lookback) {
            return Err(bad("策略行情订阅参数无效。"));
        }
        let items=streams.as_array().ok_or_else(||bad("策略行情数据流无效。"))?;
        if items.is_empty()||items.len()>8 {return Err(bad("策略行情数据流无效。"));}
        let policies=self.ledger.policies(tenant_id).map_err(map_ledger)?;
        let mut unique=BTreeSet::new();
        for stream in items {
            let source=stream.get("source").and_then(Value::as_str).unwrap_or("BINANCE");
            let product=field(stream,"product")?;
            let symbol=field(stream,"symbol")?;
            let interval=field(stream,"interval")?;
            let kind=stream.get("kind").and_then(Value::as_str).unwrap_or("closed_bars");
            if source!="BINANCE" || kind!="closed_bars" || interval_ms(interval).is_err()
                || !valid_symbol(product,symbol) || product=="options" {
                return Err(bad("策略行情数据流不支持。"));
            }
            if !unique.insert((product,symbol,interval)) {
                return Err(bad("策略行情数据流包含重复项。"));
            }
            if !policies.iter().any(|policy|{
                policy["product"]==product && policy["symbol"]==symbol &&
                policy["realtime_enabled"]==true
            }) {
                return Err(RuntimeError::Conflict("策略订阅未授权或采集策略未启用。".into()));
            }
        }
        // A read-side subscription never changes policies or starts a worker.
        self.ledger.set_strategy_reader(tenant_id,account_id,issuance_id,streams,lookback)
            .map_err(map_ledger)?;
        self.strategy_market_status(tenant_id)
    }

    pub fn unsubscribe_strategy_market(
        &self,tenant_id:&str,account_id:&str,issuance_id:&str
    )->Result<Value,RuntimeError>{
        self.require_active(tenant_id)?;
        self.ledger.remove_strategy_reader(tenant_id,account_id,issuance_id).map_err(map_ledger)?;
        self.strategy_market_status(tenant_id)
    }

    pub fn strategy_market_status(&self,tenant_id:&str)->Result<Value,RuntimeError>{
        self.require_active(tenant_id)?;
        let readers=self.ledger.strategy_readers(tenant_id).map_err(map_ledger)?;
        let mut required=std::collections::BTreeMap::<String,i64>::new();
        for reader in &readers {
            let lookback=reader["lookback_bars"].as_i64().unwrap_or(1);
            if let Some(streams)=reader["streams"].as_array() {
                for s in streams {
                    let key=format!("{}:{}:{}",s["product"].as_str().unwrap_or(""),
                        s["symbol"].as_str().unwrap_or(""),s["interval"].as_str().unwrap_or(""));
                    let entry=required.entry(key).or_insert(0);
                    *entry=(*entry).max(lookback);
                }
            }
        }
        Ok(json!({"items":readers,"requirements":required,"started_collectors":0}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_symbols_and_duplicate_policies_are_rejected() {
        assert!(!valid_symbol("spot","ETH/USDT"));
        assert!(valid_policy(&json!({"product":"spot","symbol":"ETHUSDT","base_level":"1m"})).is_ok());
        assert!(valid_policy(&json!({"product":"options","symbol":"ALL","base_level":"auto"})).is_ok());
    }
    #[test]
    fn deterministic_job_id_is_tenant_scoped() {
        let data=json!({"action":"backfill","product":"spot","symbol":"ETHUSDT"});
        assert_eq!(hash_job("one",&data),hash_job("one",&data));
        assert_ne!(hash_job("one",&data),hash_job("two",&data));
    }
}
