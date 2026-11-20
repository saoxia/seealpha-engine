//! Exchange user-stream order observations: status != execution evidence.
//! A disconnected or gapped stream freezes risk until complete REST trade proof.
use crate::{Product, VenueError, lifecycle::VerifiedOrderState};
use rust_decimal::Decimal;
use serde_json::{Value,json};
use std::collections::BTreeMap;
use std::str::FromStr;

#[derive(Clone,Debug,PartialEq,Eq)]
pub struct TradeEvidence {
    pub venue_trade_id:String,
    pub venue_order_id:String,
    pub symbol:String,
    pub quantity:Decimal,
    pub price:Decimal,
    pub trade_time_ms:i64,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct OrderEvent {
    pub state:VerifiedOrderState,
    pub execution_type:String,
    pub event_time_ms:i64,
    pub trade:Option<TradeEvidence>,
}
fn numeric_id(v:&Value)->Result<String,VenueError>{
    let raw=v.as_str().map(str::to_owned).or_else(||v.as_u64().map(|n|n.to_string()))
        .ok_or(VenueError::Unconfirmed)?;
    if raw.is_empty() || !raw.bytes().all(|b|b.is_ascii_digit()){
        return Err(VenueError::Unconfirmed);
    }
    Ok(raw)
}
fn dec(v:&Value)->Result<Decimal,VenueError>{
    Decimal::from_str(v.as_str().ok_or(VenueError::Unconfirmed)?)
        .map_err(|_|VenueError::Unconfirmed)
}
pub fn parse_order_event(product:Product,raw:&Value)->Result<OrderEvent,VenueError>{
    let (row,et)=match product {
        Product::Spot if raw["e"]=="executionReport" => (raw,raw.get("E")),
        Product::FuturesUm if raw["e"]=="ORDER_TRADE_UPDATE" =>
            (raw.get("o").ok_or(VenueError::Unconfirmed)?,raw.get("E")),
        // Options and Algo messages have distinct schemas. Reconcile with GET.
        _ => return Err(VenueError::Unconfirmed),
    };
    let keys=["s","c","i","q","z","l","L","t","T","x","X"];
    let symbol=row[keys[0]].as_str().filter(|s|!s.is_empty()).ok_or(VenueError::Unconfirmed)?;
    let client=row[keys[1]].as_str().filter(|s|!s.is_empty()).ok_or(VenueError::Unconfirmed)?;
    let order_id=numeric_id(&row[keys[2]])?;
    let original=dec(&row[keys[3]])?;
    let filled=dec(&row[keys[4]])?;
    let last=dec(&row[keys[5]])?;
    if last<Decimal::ZERO || last>filled {return Err(VenueError::Unconfirmed);}
    let exec=row[keys[9]].as_str().ok_or(VenueError::Unconfirmed)?;
    let status=row[keys[10]].as_str().ok_or(VenueError::Unconfirmed)?;
    let time=et.and_then(Value::as_i64).filter(|t|*t>=0).ok_or(VenueError::Unconfirmed)?;
    let state=VerifiedOrderState::from_exchange(&json!({
        "client_order_id":client,"order_id":order_id,
        "quantity":original.to_string(),"filled":filled.to_string(),"status":status,
    }))?;
    let trade=if exec=="TRADE" {
        if last<=Decimal::ZERO{return Err(VenueError::Unconfirmed);}
        let price=dec(&row[keys[6]])?;
        if price<=Decimal::ZERO{return Err(VenueError::Unconfirmed);}
        let id=numeric_id(&row[keys[7]])?;
        let ts=row[keys[8]].as_i64().filter(|n|*n>0).ok_or(VenueError::Unconfirmed)?;
        Some(TradeEvidence{venue_trade_id:id,venue_order_id:order_id,symbol:symbol.into(),
            quantity:last,price,trade_time_ms:ts})
    }else{
        if !last.is_zero(){return Err(VenueError::Unconfirmed);}
        None
    };
    Ok(OrderEvent{state,execution_type:exec.into(),event_time_ms:time,trade})
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum AppliedEvent { Duplicate, StatusOnly, Trade(TradeEvidence) }
/// A checkpoint must be persisted atomically with its trade IDs by Runtime.
#[derive(Clone,Debug,Default)]
pub struct StreamReconciler {
    pub state:Option<VerifiedOrderState>,
    pub frozen:bool,
    pub last_event_time_ms:Option<i64>,
    trades:BTreeMap<String,TradeEvidence>,
}
impl StreamReconciler {
    pub fn on_disconnect(&mut self){self.frozen=true;}
    pub fn trades(&self)->&BTreeMap<String,TradeEvidence>{&self.trades}
    pub fn apply(&mut self,event:OrderEvent)->Result<AppliedEvent,VenueError>{
        if self.frozen{return Err(VenueError::Unconfirmed);}
        if self.last_event_time_ms.is_some_and(|seen|event.event_time_ms<seen){
            self.frozen=true;return Err(VenueError::Unconfirmed);
        }
        if let Some(prev)=&self.state {
            if prev.advance(&event.state).is_err(){
                self.frozen=true;return Err(VenueError::Unconfirmed);
            }
        }else if !event.state.filled_quantity.is_zero(){
            self.frozen=true;return Err(VenueError::Unconfirmed);
        }
        let prior=self.state.as_ref().map_or(Decimal::ZERO,|s|s.filled_quantity);
        let delta=event.state.filled_quantity-prior;
        if let Some(trade)=event.trade {
            if let Some(existing)=self.trades.get(&trade.venue_trade_id){
                if existing!=&trade || !delta.is_zero(){
                    self.frozen=true;return Err(VenueError::Unconfirmed);
                }
                self.last_event_time_ms=Some(event.event_time_ms);
                return Ok(AppliedEvent::Duplicate);
            }
            if delta!=trade.quantity{
                self.frozen=true;return Err(VenueError::Unconfirmed);
            }
            self.trades.insert(trade.venue_trade_id.clone(),trade.clone());
            self.state=Some(event.state);
            self.last_event_time_ms=Some(event.event_time_ms);
            Ok(AppliedEvent::Trade(trade))
        }else{
            if !delta.is_zero(){
                self.frozen=true;return Err(VenueError::Unconfirmed);
            }
            let duplicate=self.state.as_ref()==Some(&event.state);
            self.state=Some(event.state);
            self.last_event_time_ms=Some(event.event_time_ms);
            if duplicate{Ok(AppliedEvent::Duplicate)}else{Ok(AppliedEvent::StatusOnly)}
        }
    }
    /// Complete signed GET order + all real userTrades, never synthesized ACK fills.
    pub fn recover_from_rest(
        &mut self, verified:VerifiedOrderState, normalized_fills:&[Value],page_complete:bool,
    )->Result<(),VenueError>{
        if !page_complete{self.frozen=true;return Err(VenueError::Unconfirmed);}
        let mut trades=BTreeMap::new();
        let mut total=Decimal::ZERO;
        for fill in normalized_fills {
            let tid=fill["venue_trade_id"].as_str().filter(|v|!v.is_empty())
                .ok_or(VenueError::Unconfirmed)?;
            let oid=fill["venue_order_id"].as_str().ok_or(VenueError::Unconfirmed)?;
            if oid!=verified.exchange_order_id{return Err(VenueError::Unconfirmed);}
            let quantity=dec(&fill["quantity"])?;
            let price=dec(&fill["price"])?;
            let time=fill["trade_time_ms"].as_i64().filter(|t|*t>=0)
                .ok_or(VenueError::Unconfirmed)?;
            let symbol=fill["symbol"].as_str().filter(|s|!s.is_empty())
                .ok_or(VenueError::Unconfirmed)?;
            if quantity<=Decimal::ZERO || price<=Decimal::ZERO{return Err(VenueError::Unconfirmed);}
            total+=quantity;
            if trades.insert(tid.to_string(),TradeEvidence {
                venue_trade_id:tid.into(),venue_order_id:oid.into(),symbol:symbol.into(),
                quantity,price,trade_time_ms:time,
            }).is_some(){return Err(VenueError::Unconfirmed);}
        }
        if total!=verified.filled_quantity{self.frozen=true;return Err(VenueError::Unconfirmed);}
        for (id,trade) in &trades {
            if self.trades.get(id).is_some_and(|previous|previous!=trade) {
                self.frozen=true;return Err(VenueError::Unconfirmed);
            }
        }
        if let Some(old)=&self.state {old.advance(&verified)?;}
        self.trades=trades;
        self.state=Some(verified);
        self.last_event_time_ms=None; // REST has its own authoritative timestamp
        self.frozen=false;
        Ok(())
    }
}
#[cfg(test)]
mod tests{
    use super::*;
    fn spot(x:&str,status:&str,z:&str,l:&str,tid:u64)->Value{
        json!({"e":"executionReport","E":100,"T":100,
            "s":"ETHUSDT","c":"cid","i":1,"q":"2","x":x,"X":status,
            "z":z,"l":l,"L":"2000","t":tid})
    }
    #[test]
    fn ack_partial_cancel_only_real_trade_creates_fill(){
        let mut r=StreamReconciler::default();
        let a=parse_order_event(Product::Spot,&spot("NEW","NEW","0","0",0)).unwrap();
        assert_eq!(r.apply(a).unwrap(),AppliedEvent::StatusOnly);
        let b=parse_order_event(Product::Spot,&spot("TRADE","PARTIALLY_FILLED","1","1",42)).unwrap();
        assert!(matches!(r.apply(b),Ok(AppliedEvent::Trade(_))));
        let c=parse_order_event(Product::Spot,&spot("CANCELED","CANCELED","1","0",0)).unwrap();
        assert_eq!(r.apply(c).unwrap(),AppliedEvent::StatusOnly);
        assert_eq!(r.trades().len(),1);
    }
    #[test]
    fn gap_freezes_and_requires_all_rest_fills(){
        let mut r=StreamReconciler::default();
        let a=parse_order_event(Product::Spot,&spot("NEW","NEW","0","0",0)).unwrap();
        r.apply(a).unwrap();
        let gap=parse_order_event(Product::Spot,
            &spot("TRADE","PARTIALLY_FILLED","1.5","0.5",3)).unwrap();
        assert!(r.apply(gap).is_err());
        assert!(r.frozen);
        let s=VerifiedOrderState::from_exchange(&json!({
            "client_order_id":"cid","order_id":"1","status":"PARTIALLY_FILLED",
            "quantity":"2","filled":"1"})).unwrap();
        let fills=vec![json!({"venue_trade_id":"42","venue_order_id":"1",
            "symbol":"ETHUSDT","quantity":"1","price":"2000","trade_time_ms":100})];
        assert!(r.recover_from_rest(s.clone(),&[],true).is_err());
        assert!(r.recover_from_rest(s.clone(),&fills,false).is_err());
        r.recover_from_rest(s,&fills,true).unwrap();
        assert!(!r.frozen);
    }
    #[test]
    fn older_events_and_conflicting_trade_replays_freeze() {
        let mut r=StreamReconciler::default();
        r.apply(parse_order_event(Product::Spot,
            &spot("NEW","NEW","0","0",0)).unwrap()).unwrap();
        let mut stale=spot("NEW","NEW","0","0",0);
        stale["E"]=json!(99);
        assert!(r.apply(parse_order_event(Product::Spot,&stale).unwrap()).is_err());
        assert!(r.frozen);
    }
    #[test]
    fn algo_trigger_and_um_amendment_are_not_trades(){
        assert!(parse_order_event(Product::FuturesUm,&json!({"e":"ALGO_UPDATE"})).is_err());
        let um=json!({"e":"ORDER_TRADE_UPDATE","E":100,"o":{
            "s":"ETHUSDT","c":"cid","i":1,"q":"2","z":"0","l":"0",
            "x":"AMENDMENT","X":"NEW","L":"0","t":0}});
        assert!(parse_order_event(Product::FuturesUm,&um).unwrap().trade.is_none());
    }
}
