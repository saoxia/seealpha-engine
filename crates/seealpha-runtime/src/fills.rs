//! Reconcile real exchange trade history into the ledger. A signed GET can
//! update local evidence; it can never retry a trade POST or infer a fill from
//! an accepted NEW order.
use crate::{Runtime,RuntimeError,map_ledger};
use rust_decimal::Decimal;
use seealpha_venue::{Credentials,Product,VenueClient};
use seealpha_venue::lifecycle::VerifiedOrderState;
use serde_json::{json,Value};

fn error(msg:&str)->RuntimeError {RuntimeError::Conflict(msg.into())}
fn market_product(stored:&Value,symbol:&str)->Result<Product,RuntimeError>{
    let product=stored["product"].as_str();
    match product {
        Some("spot")=>Ok(Product::Spot),
        Some("futures_um")=>Ok(Product::FuturesUm),
        Some("options")=>Ok(Product::Option),
        None if symbol.ends_with("-PERP")=>Ok(Product::FuturesUm),
        None if seealpha_market::options::parse_option_symbol(symbol).is_ok()=>Ok(Product::Option),
        None if seealpha_market::check_symbol(symbol).is_ok()=>Ok(Product::Spot),
        _=>Err(error("订单产品标识无法核实，拒绝猜测交易所品种。")),
    }
}

impl Runtime {
    #[allow(clippy::too_many_arguments)]
    pub fn sync_verified_order_fills(
        &self,tenant:&str,account:&str,environment:&str,request_id:&str,
        api_key:&str,secret:&str,venue_base:&str,
    )->Result<Value,RuntimeError>{
        let t=self.require_active(tenant)?;
        if t.purpose!="production"||!matches!(environment,"production"|"sandbox") {
            return Err(error("非正式租户或账户环境无效。"));
        }
        if api_key.is_empty()||secret.is_empty()||api_key==secret {
            return Err(error("交易所凭据无效。"));
        }
        let stored=self.ledger.get_order_request(tenant,account,request_id)
            .map_err(map_ledger)?.ok_or_else(||RuntimeError::NotFound("没有该订单请求。".into()))?;
        let mut record=serde_json::from_str::<Value>(&stored)
            .map_err(|_|error("订单账本损坏，拒绝推断执行状态。"))?;
        if record["environment"]!=environment {
            return Err(error("订单和账户环境不匹配。"));
        }
        let symbol=record["symbol"].as_str().ok_or_else(||error("订单合约无效。"))?.to_owned();
        let client_id=record["response"]["client_order_id"].as_str()
            .ok_or_else(||error("订单客户端编号缺失。"))?.to_owned();
        let product=market_product(&record,&symbol)?;
        if product==Product::Option {
            return Err(error("期权真实成交列表尚未通过鉴权与分页验收。"));
        }
        let client=VenueClient::new(venue_base)
            .map_err(|_|error("交易网关配置无效。"))?;
        let credentials=Credentials{api_key:api_key.to_owned(),secret:secret.to_owned()};
        let Some(order)=client.query_order(&credentials,product,&symbol,&client_id)
            .map_err(|_|error("交易所订单查询失败，保持原状态且禁止重试委托。"))? else {
            return Err(error("交易所尚未确认此订单，保持 unknown 状态并人工核验。"));
        };
        let next=VerifiedOrderState::from_exchange(&order)
            .map_err(|_|error("交易所订单状态或累计成交量不可信，拒绝覆盖账本。"))?;
        if !record["response"]["order"].is_null() {
            let prev=VerifiedOrderState::from_exchange(&record["response"]["order"])
                .map_err(|_|error("已有订单证据损坏，拒绝覆盖。"))?;
            prev.advance(&next).map_err(|_|error(
                "交易所订单状态回退、终态变化或累计成交量冲突，需人工核验。"))?;
        }
        let order_id=order["order_id"].as_str().ok_or_else(||error("交易所订单 ID 缺失。"))?;
        let qty=order["filled"].as_str().ok_or_else(||error("交易所累计成交量缺失。"))?
            .parse::<Decimal>().map_err(|_|error("交易所累计成交量无效。"))?;
        if qty<Decimal::ZERO{return Err(error("交易所累计成交量为负。"));}
        let trades=client.query_order_trades(&credentials,product,&symbol,order_id)
            .map_err(|_|error("真实成交明细查询未完成，拒绝虚构成交。"))?;
        let mut observed=Decimal::ZERO;
        for item in &trades {
            let amount=item["quantity"].as_str().ok_or_else(||error("成交数量缺失。"))?
                .parse::<Decimal>().map_err(|_|error("成交数量无效。"))?;
            observed=observed.checked_add(amount).ok_or_else(||error("成交数量溢出。"))?;
        }
        if observed>qty {return Err(error("成交明细超过交易所订单累计成交量。"));}
        self.ledger.import_verified_fills(
            tenant,account,environment,order_id,&trades
        ).map_err(map_ledger)?;
        self.ledger.upsert_order_row(
            tenant,account,environment,&client_id,&order.to_string()
        ).map_err(map_ledger)?;
        let terminal=matches!(
            order["status"].as_str(),Some("FILLED"|"CANCELED"|"EXPIRED"|"EXPIRED_IN_MATCH"|"REJECTED")
        );
        let fills_complete=terminal&&observed==qty;
        record["response"]["status"]=json!("completed");
        record["response"]["order"]=order.clone();
        record["records"]=json!(trades);
        record["ledger_synced"]=json!(fills_complete);
        record["verified_fill_count"]=json!(trades.len());
        record["verified_filled_quantity"]=json!(observed.to_string());
        record["order_terminal"]=json!(terminal);
        record["fills_complete"]=json!(fills_complete);
        self.ledger.save_order_request(tenant,account,request_id,&record.to_string())
            .map_err(map_ledger)?;
        Ok(json!({"request_id":request_id,"client_order_id":client_id,
            "order":order,"verified_fills":trades,
            "observed_filled_quantity":observed.to_string(),
            "reported_filled_quantity":qty.to_string(),
            "fills_complete":fills_complete,"order_terminal":terminal,
            "ledger_synced":fills_complete,
            "repost_order":false}))
    }
}
