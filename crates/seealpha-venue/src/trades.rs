//! Canonical exchange-authored fill records, never derived from order ACKs.
//! User trade history is a signed *read* and must not trigger another order.
use crate::{Product,VenueError};
use rust_decimal::Decimal;
use serde_json::{Value,json};
use std::collections::BTreeSet;

fn id(value:&Value)->Option<String>{
    value.as_u64().map(|v|v.to_string())
        .or_else(||value.as_str().filter(|s|
            !s.is_empty()&&s.bytes().all(|b|b.is_ascii_digit())
        ).map(str::to_owned))
}
fn dec<'a>(row:&'a Value,name:&str,positive:bool)->Result<&'a str,VenueError>{
    let raw=row[name].as_str().ok_or(VenueError::Unconfirmed)?;
    let n=raw.parse::<Decimal>().map_err(|_|VenueError::Unconfirmed)?;
    if positive && n<=Decimal::ZERO {return Err(VenueError::Unconfirmed);}
    Ok(raw)
}
pub fn normalize_order_trades(
    product:Product,symbol:&str,order_id:&str,payload:&Value,
)->Result<Vec<Value>,VenueError>{
    if product==Product::Option || order_id.is_empty() ||
        !order_id.bytes().all(|b|b.is_ascii_digit()) {
        return Err(VenueError::Unconfirmed);
    }
    let rows=payload.as_array().ok_or(VenueError::Unconfirmed)?;
    // Full pagination is required before we can certify more than one page.
    if rows.len()>=1000 {return Err(VenueError::Unconfirmed);}
    let mut trades=Vec::with_capacity(rows.len());
    let mut seen=BTreeSet::new();
    for row in rows {
        if row["symbol"].as_str()!=Some(symbol) ||
            id(&row["orderId"]).as_deref()!=Some(order_id) {
            return Err(VenueError::Unconfirmed);
        }
        let trade_id=id(&row["id"]).ok_or(VenueError::Unconfirmed)?;
        if !seen.insert(trade_id.clone()) {
            return Err(VenueError::Unconfirmed);
        }
        let quantity=dec(row,"qty",true)?;
        let price=dec(row,"price",true)?;
        let time=row["time"].as_i64().ok_or(VenueError::Unconfirmed)?;
        if time<0 {return Err(VenueError::Unconfirmed);}
        let side=if let Some(buyer)=row["buyer"].as_bool()
            .or_else(||row["isBuyer"].as_bool()) {
            if buyer{"BUY"}else{"SELL"}
        }else{
            match row["side"].as_str() {
                Some("BUY")=>"BUY",Some("SELL")=>"SELL",
                _=>return Err(VenueError::Unconfirmed),
            }
        };
        let commission=if row["commission"].is_null() {"0"}else{dec(row,"commission",false)?};
        let realized=if row["realizedPnl"].is_null(){"0"}else{dec(row,"realizedPnl",false)?};
        trades.push(json!({
            "trade_id":format!("binance:{}:{}:{trade_id}",
                match product{Product::Spot=>"spot",Product::FuturesUm=>"futures_um",
                    Product::Option=>"options"},symbol),
            "venue_trade_id":trade_id,
            "venue_order_id":order_id,
            "symbol":symbol,"side":side,
            "quantity":quantity,"price":price,
            "commission":commission,
            "commission_asset":row["commissionAsset"],
            "realized_pnl":realized,
            "trade_time_ms":time,
            "source":"signed_exchange_user_trades"
        }));
    }
    Ok(trades)
}
#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn trade_rows_require_real_exchange_ids_and_matching_order(){
        let raw=json!([{"id":77,"orderId":8,"symbol":"ETHUSDT",
            "qty":"0.5","price":"2000","time":100,
            "buyer":true,"commission":"0.02","commissionAsset":"USDT"}]);
        let rows=normalize_order_trades(Product::FuturesUm,"ETHUSDT","8",&raw).unwrap();
        assert_eq!(rows[0]["trade_id"],"binance:futures_um:ETHUSDT:77");
        assert_eq!(rows[0]["side"],"BUY");
        assert!(normalize_order_trades(Product::FuturesUm,"ETHUSDT","9",&raw).is_err());
        assert!(normalize_order_trades(Product::Option,"ETHUSDT","8",&raw).is_err());
    }
}
