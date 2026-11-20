//! Offline reconciliation of authenticated USDT option-expiry cash records.
use std::collections::{BTreeMap,BTreeSet};
use rust_decimal::Decimal;
use crate::basket::{BasketError,LegId,Product};

#[derive(Debug,Clone,PartialEq,Eq)]
pub struct ExpiringContract {
    pub leg: LegId,
    pub expiry_ns: u64,
    pub strike: Decimal,
    pub unit: Decimal,
    pub increment: Decimal,
    pub settle_asset: String,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct VerifiedSettlement {
    pub leg: LegId,
    pub settlement_id: String,
    pub settled_at_ns: u64,
    pub index_price: Decimal,
    pub settled_quantity: Decimal,
    pub gross_credit: Decimal,
    pub fee: Decimal,
    pub net_credit: Decimal,
    pub source: String,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct ExpiryReconciliation {
    pub expiry_ns: u64,
    pub index_price: Decimal,
    pub gross_option_credit: Decimal,
    pub fees: Decimal,
    pub net_credit: Decimal,
    pub cash_before: Decimal,
    pub cash_after: Decimal,
    pub event_ids: Vec<String>,
    pub verified: bool,
}
fn checked_add(a:Decimal,b:Decimal)->Result<Decimal,BasketError>{
    a.checked_add(b).ok_or(BasketError::Overflow)
}
pub fn reconcile_expiry(
    contracts:&[ExpiringContract],
    positions_before:&BTreeMap<LegId,Decimal>,
    records:&[VerifiedSettlement],
    perpetual_at_expiry:Decimal,
    cash_before:Decimal,cash_after:Decimal,now_ns:u64,
)->Result<ExpiryReconciliation,BasketError>{
    if contracts.is_empty()||contracts.len()>16||records.len()!=contracts.len()||
       now_ns==0||!perpetual_at_expiry.is_zero()||cash_before<Decimal::ZERO||
       cash_after<Decimal::ZERO{return Err(BasketError::Evidence);}
    let mut checked=BTreeMap::new();
    let mut scope=None;
    for contract in contracts {
        let leg=&contract.leg;
        let p:Vec<_>=leg.symbol.split('-').collect();
        if leg.product!=Product::Option||p.len()!=4||!matches!(p[3],"C"|"P")||
           contract.expiry_ns==0||contract.expiry_ns>now_ns||
           contract.settle_asset!="USDT"||contract.unit<=Decimal::ZERO||
           contract.increment<=Decimal::ZERO||contract.strike<=Decimal::ZERO||
           p[2].parse::<Decimal>().ok()!=Some(contract.strike)||
           checked.insert(leg.clone(),contract).is_some() {
            return Err(BasketError::Evidence);
        }
        let identity=(leg.tenant.clone(),leg.account.clone(),p[0].to_owned(),p[1].to_owned(),contract.expiry_ns);
        if scope.as_ref().is_some_and(|x|x!=&identity) {return Err(BasketError::Evidence);}
        scope=Some(identity);
    }
    if checked.keys().ne(positions_before.keys()){return Err(BasketError::Evidence);}
    for (leg,q) in positions_before {
        if *q<Decimal::ZERO||!(*q%checked[leg].increment).is_zero(){return Err(BasketError::Evidence);}
    }
    let mut seen_legs=BTreeSet::new();
    let mut seen_ids=BTreeSet::new();
    let mut index=None;
    let (mut gross,mut fees,mut net)=(Decimal::ZERO,Decimal::ZERO,Decimal::ZERO);
    for row in records {
        let contract=checked.get(&row.leg).ok_or(BasketError::Evidence)?;
        if !seen_legs.insert(row.leg.clone())||row.settlement_id.is_empty()||
           row.settlement_id.len()>128||
           !row.settlement_id.bytes().all(|b|b.is_ascii_alphanumeric()||b"._:-".contains(&b))||
           !seen_ids.insert(row.settlement_id.clone())||
           row.source!="authenticated_exchange_exercise_record"||
           row.settled_at_ns<contract.expiry_ns||row.settled_at_ns>now_ns||
           row.settled_quantity!=positions_before[&row.leg]||
           row.index_price<=Decimal::ZERO||row.gross_credit<Decimal::ZERO||
           row.fee<Decimal::ZERO||row.net_credit<Decimal::ZERO {
            return Err(BasketError::Evidence);
        }
        if index.is_some_and(|p|p!=row.index_price){return Err(BasketError::Evidence);}
        index=Some(row.index_price);
        let diff=if row.leg.symbol.ends_with("-C"){
            (row.index_price-contract.strike).max(Decimal::ZERO)
        }else{(contract.strike-row.index_price).max(Decimal::ZERO)};
        let payoff=diff.checked_mul(contract.unit)
            .and_then(|x|x.checked_mul(row.settled_quantity)).ok_or(BasketError::Overflow)?;
        if row.gross_credit!=payoff||row.fee>payoff||row.net_credit!=payoff-row.fee {
            return Err(BasketError::Evidence);
        }
        gross=checked_add(gross,payoff)?;
        fees=checked_add(fees,row.fee)?;
        net=checked_add(net,row.net_credit)?;
    }
    if checked_add(cash_before,net)?!=cash_after{return Err(BasketError::Evidence);}
    let mut ids:Vec<_>=seen_ids.into_iter().collect();ids.sort();
    Ok(ExpiryReconciliation{expiry_ns:contracts[0].expiry_ns,index_price:index.ok_or(BasketError::Evidence)?,
        gross_option_credit:gross,fees,net_credit:net,cash_before,cash_after,event_ids:ids,verified:true})
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn refuses_settlement_with_open_perpetual(){
        let l=LegId{tenant:"t".into(),account:"a".into(),product:Product::Option,
            symbol:"ETH-261225-3000-C".into()};
        let c=ExpiringContract{leg:l.clone(),expiry_ns:1,strike:Decimal::from(3000),
            unit:Decimal::ONE,increment:Decimal::ONE,settle_asset:"USDT".into()};
        assert_eq!(reconcile_expiry(&[c],&[(l,Decimal::ONE)].into(),&[],
            Decimal::ONE,Decimal::ZERO,Decimal::ZERO,2),Err(BasketError::Evidence));
    }
}
