//! Research-only multi-product basket planning and durable-journal state transitions.
//! This module never submits an order. Persistence and evidence authentication
//! are obligations of the embedding runtime.
use std::collections::{BTreeMap, BTreeSet};
use rust_decimal::Decimal;
use thiserror::Error;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Product { Spot, FuturesUm, Option }
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LegId { pub tenant: String, pub account: String, pub product: Product, pub symbol: String }
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegTarget { pub member_id: String, pub leg: LegId, pub signed_quantity: Decimal }
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evidence { pub price: Decimal, pub increment: Decimal, pub multiplier: Decimal, pub timestamp_ns: u64 }
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Phase { pub name: String, pub changes: BTreeMap<LegId, Decimal> }
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan { pub targets: BTreeMap<LegId, Decimal>, pub phases: Vec<Phase>, pub gross_notional: Decimal, pub executable: bool }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy { Generic, FundingCarry, OptionProtected }
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State { Ready, AwaitingConfirmation, Frozen, Completed }
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalEvent {
    Begin { request_id: String },
    Fill { request_id: String, event_id: String, leg: LegId, cumulative: Decimal },
    Confirm { request_id: String, positions: BTreeMap<LegId,Decimal> },
    Freeze { reason: String },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub plan: Plan,
    pub initial: BTreeMap<LegId,Decimal>,
    pub positions: BTreeMap<LegId,Decimal>,
    pub phase_index: usize,
    pub state: State,
    pub request_id: Option<String>,
    pub fills: BTreeMap<LegId,Decimal>,
    pub used_requests: BTreeSet<String>,
    pub seen_events: BTreeMap<String,(String,LegId,Decimal)>,
    pub events: Vec<JournalEvent>,
    pub reason: Option<String>,
}
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BasketError {
    #[error("invalid or incomplete basket evidence")] Evidence,
    #[error("basket risk budget or hedge coverage violation")] Risk,
    #[error("basket phase or request state violation")] State,
    #[error("basket event conflict or cumulative quantity violation")] Fill,
    #[error("unresolved external execution; freeze and reconcile")] Frozen,
    #[error("decimal overflow")] Overflow,
}
fn add(a:Decimal,b:Decimal)->Result<Decimal,BasketError>{a.checked_add(b).ok_or(BasketError::Overflow)}
fn mul(a:Decimal,b:Decimal)->Result<Decimal,BasketError>{a.checked_mul(b).ok_or(BasketError::Overflow)}
fn gross(p:&BTreeMap<LegId,Decimal>,e:&BTreeMap<LegId,Evidence>)->Result<Decimal,BasketError>{
    p.iter().try_fold(Decimal::ZERO,|sum,(leg,q)| {
        let x=e.get(leg).ok_or(BasketError::Evidence)?;
        add(sum,mul(mul(q.abs(),x.price)?,x.multiplier)?)
    })
}
fn valid_event_id(id:&str)->bool {
    !id.is_empty()&&id.len()<=128&&id.bytes().all(|b|
        b.is_ascii_alphanumeric()||b==b'_'||b==b'-'||b==b'.'||b==b':')
}
fn change_phase(name:&str, rows:impl IntoIterator<Item=(LegId,Decimal)>)->Option<Phase>{
    let changes:BTreeMap<_,_>=rows.into_iter().filter(|(_,q)|!q.is_zero()).collect();
    if changes.is_empty(){None}else{Some(Phase{name:name.into(),changes})}
}
/// Explicitly conservative phase planning; never inferred venue atomicity.
pub fn plan(targets:&[LegTarget],positions:&BTreeMap<LegId,Decimal>,
    evidence:&BTreeMap<LegId,Evidence>,now_ns:u64,max_age_ns:u64,
    max_gross:Decimal,policy:Policy,emergency_unwind:bool)->Result<Plan,BasketError>{
    plan_with_constraints(targets,positions,evidence,now_ns,max_age_ns,max_gross,policy,
        Decimal::ZERO,&[],emergency_unwind)
}
pub fn plan_with_constraints(targets:&[LegTarget],positions:&BTreeMap<LegId,Decimal>,
    evidence:&BTreeMap<LegId,Evidence>,now_ns:u64,max_age_ns:u64,
    max_gross:Decimal,policy:Policy,max_leg_drift:Decimal,
    hedge_pairs:&[(LegId,LegId)],emergency_unwind:bool)->Result<Plan,BasketError>{
    if max_leg_drift<Decimal::ZERO||(policy!=Policy::Generic&&!hedge_pairs.is_empty()){
        return Err(BasketError::Risk);
    }
    if targets.is_empty()||targets.len()>64||now_ns==0||max_age_ns==0||max_gross<=Decimal::ZERO{return Err(BasketError::Evidence);}
    let mut net=BTreeMap::<LegId,Decimal>::new();
    let mut unique=BTreeSet::new();
    for target in targets {
        let l=&target.leg;
        if target.member_id.trim().is_empty()||l.tenant.trim().is_empty()||l.account.trim().is_empty()||
            l.symbol.is_empty()||!l.symbol.chars().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit()||c=='-'||c=='_')||
            !unique.insert((target.member_id.clone(),l.clone())) {return Err(BasketError::Evidence);}
        let entry=net.entry(l.clone()).or_default();*entry=add(*entry,target.signed_quantity)?;
    }
    if net.len()>16||positions.len()!=net.len()||net.keys().ne(positions.keys())||
       net.keys().any(|l|l.tenant!=net.keys().next().expect("not empty").tenant||
           l.account!=net.keys().next().expect("not empty").account){return Err(BasketError::Evidence);}
    for (leg,target) in &net {
        let quote=evidence.get(leg).ok_or(BasketError::Evidence)?;
        let current=positions[leg];
        if quote.price<=Decimal::ZERO||quote.increment<=Decimal::ZERO||quote.multiplier<=Decimal::ZERO||
           quote.timestamp_ns>now_ns||now_ns-quote.timestamp_ns>max_age_ns||
           !(*target%quote.increment).is_zero()||!(current%quote.increment).is_zero()||
           (leg.product!=Product::FuturesUm&&(*target<Decimal::ZERO||current<Decimal::ZERO)) {
            return Err(BasketError::Evidence);
        }
    }
    if emergency_unwind && (net.values().any(|v|!v.is_zero())||positions.values().all(Decimal::is_zero)) {
        return Err(BasketError::Risk);
    }
    let futures:Vec<_>=net.keys().filter(|l|l.product==Product::FuturesUm).cloned().collect();
    if policy!=Policy::Generic&&futures.len()!=1{return Err(BasketError::Risk);}
    let mut hedged=BTreeSet::new();
    for (spot,perpetual) in hedge_pairs {
        if spot.product!=Product::Spot||perpetual.product!=Product::FuturesUm||
           spot.symbol!=perpetual.symbol||!net.contains_key(spot)||!net.contains_key(perpetual)||
           !hedged.insert(spot.clone())||!hedged.insert(perpetual.clone())||
           add(net[spot],net[perpetual])?!=Decimal::ZERO||positions[perpetual]>Decimal::ZERO {
            return Err(BasketError::Risk);
        }
    }
    let mut phases=Vec::<Phase>::new();
    match policy {
        Policy::Generic=>{
            let mut reduce=Vec::new();let mut increase=Vec::new();
            for (l,desired) in net.iter().filter(|(l,_)|!hedged.contains(*l)) {
                let old=positions[l];
                if old!=Decimal::ZERO&&((old.is_sign_negative()!=desired.is_sign_negative() || desired.is_zero())||desired.abs()<old.abs()){
                    let to=if (old.is_sign_negative()!=desired.is_sign_negative() || desired.is_zero()){Decimal::ZERO}else{*desired};
                    reduce.push((l.clone(),to-old));
                }
                if *desired!=Decimal::ZERO&&((old.is_sign_negative()!=desired.is_sign_negative() || desired.is_zero())||desired.abs()>old.abs()){
                    let from=if (old.is_sign_negative()!=desired.is_sign_negative() || desired.is_zero()){Decimal::ZERO}else{old};
                    increase.push((l.clone(),*desired-from));
                }
            }
            if let Some(p)=change_phase("reduce_existing_exposure",reduce){phases.push(p);}
            let mut reduce_perp=Vec::new();
            let mut reduce_spot=Vec::new();
            let mut acquire_spot=Vec::new();
            let mut increase_perp=Vec::new();
            for (spot,perpetual) in hedge_pairs {
                if net[perpetual]>positions[perpetual] {
                    reduce_perp.push((perpetual.clone(),net[perpetual]-positions[perpetual]));
                }
                if net[spot]<positions[spot] {
                    reduce_spot.push((spot.clone(),net[spot]-positions[spot]));
                }
                if net[spot]>positions[spot] {
                    acquire_spot.push((spot.clone(),net[spot]-positions[spot]));
                }
                if net[perpetual]<positions[perpetual] {
                    increase_perp.push((perpetual.clone(),net[perpetual]-positions[perpetual]));
                }
            }
            for (name,rows) in [
                ("reduce_hedge_perpetual",reduce_perp),
                ("reduce_hedge_spot",reduce_spot),
                ("acquire_hedge_spot",acquire_spot),
                ("increase_hedged_perpetual",increase_perp),
            ] {
                if let Some(p)=change_phase(name,rows){phases.push(p);}
            }
            if let Some(p)=change_phase("increase_new_exposure",increase){phases.push(p);}
        }
        Policy::FundingCarry=>{
            if net.len()!=2{return Err(BasketError::Risk);}
            let perp=&futures[0];
            let spot=net.keys().find(|l|l.product==Product::Spot).ok_or(BasketError::Risk)?;
            if perp.symbol!=spot.symbol||net[spot]<Decimal::ZERO||net[perp]>Decimal::ZERO||
              add(net[spot],net[perp])?!=Decimal::ZERO {return Err(BasketError::Risk);}
            if !emergency_unwind &&
               add(positions[spot],positions[perp])?.abs()>max_leg_drift {
                return Err(BasketError::Risk);
            }
            if let Some(p)=change_phase(if emergency_unwind{"emergency_coordinated_unwind"}else{"coordinated_spot_perpetual_required"},
                net.iter().map(|(l,q)|(l.clone(),*q-positions[l]))){phases.push(p);}
        }
        Policy::OptionProtected=>{
            let perp=&futures[0];
            let options:Vec<_>=net.keys().filter(|l|l.product==Product::Option).cloned().collect();
            if net.len()!=3||options.len()!=2{return Err(BasketError::Risk);}
            let base=perp.symbol.strip_suffix("USDT").ok_or(BasketError::Risk)?;
            let mut suffixes=BTreeSet::new();let mut expiry=None;
            for l in &options {
                let pieces:Vec<_>=l.symbol.split('-').collect();
                if pieces.len()!=4||pieces[0]!=base||pieces[1].len()!=6||
                   !pieces[1].bytes().all(|c|c.is_ascii_digit())||
                   !pieces[2].bytes().all(|c|c.is_ascii_digit())||
                   !matches!(pieces[3],"C"|"P"){return Err(BasketError::Risk);}
                if expiry.is_some_and(|x|x!=pieces[1]){return Err(BasketError::Risk);}
                expiry=Some(pieces[1]);suffixes.insert(pieces[3]);
                if mul(net[l],evidence[l].multiplier)?<net[perp].abs() ||
                  (!emergency_unwind&&mul(positions[l],evidence[l].multiplier)?<positions[perp].abs()){
                    return Err(BasketError::Risk);
                }
            }
            if suffixes.len()!=2{return Err(BasketError::Risk);}
            let old=positions[perp];let new=net[perp];
            let reduce=if old!=Decimal::ZERO&&((old.is_sign_negative()!=new.is_sign_negative() || new.is_zero())||new.abs()<old.abs()){
                Some((perp.clone(),if (old.is_sign_negative()!=new.is_sign_negative() || new.is_zero()){-old}else{new-old}))
            }else{None};
            let increase=if new!=Decimal::ZERO&&((old.is_sign_negative()!=new.is_sign_negative() || new.is_zero())||new.abs()>old.abs()){
                Some((perp.clone(),if (old.is_sign_negative()!=new.is_sign_negative() || new.is_zero()){new}else{new-old}))
            }else{None};
            let prefix=if emergency_unwind{"emergency_"}else{""};
            for (name,rows) in [
                ("reduce_directional_risk",reduce.into_iter().collect()),
                ("acquire_and_confirm_protection",options.iter().filter_map(|l|{
                    let q=net[l]-positions[l];(q>Decimal::ZERO).then(||(l.clone(),q))}).collect()),
                ("increase_directional_risk_after_protection",increase.into_iter().collect()),
                ("release_protection_after_risk_reduction",options.iter().filter_map(|l|{
                    let q=net[l]-positions[l];(q<Decimal::ZERO).then(||(l.clone(),q))}).collect()),
            ]{
                if let Some(p)=change_phase(&format!("{prefix}{name}"),rows){phases.push(p);}
            }
        }
    }
    let mut projected=positions.clone();
    let mut current=gross(&projected,evidence)?;
    for phase in &phases {
        let mut after=projected.clone();
        for (leg,delta) in &phase.changes {let value=after.get_mut(leg).ok_or(BasketError::Evidence)?;*value=add(*value,*delta)?;}
        let peak=projected.iter().try_fold(Decimal::ZERO,|sum,(leg,qty)|{
            let unit=mul(evidence[leg].price,evidence[leg].multiplier)?;
            add(sum,mul(qty.abs().max(after[leg].abs()),unit)?)
        })?;
        let next=gross(&after,evidence)?;
        if (current>max_gross&&(peak>current||next>=current))||
           (current<=max_gross&&peak>max_gross){return Err(BasketError::Risk);}
        current=next;projected=after;
    }
    let total=gross(&net,evidence)?;
    if projected!=net||total>max_gross{return Err(BasketError::Risk);}
    Ok(Plan{targets:net,phases,gross_notional:total,executable:false})
}
impl Checkpoint {
    pub fn new(plan:Plan,starting:BTreeMap<LegId,Decimal>)->Result<Self,BasketError>{
        if plan.executable||plan.targets.is_empty()||plan.targets.keys().ne(starting.keys())||plan.phases.len()>32{return Err(BasketError::Evidence);}
        let mut final_positions=starting.clone();
        for phase in &plan.phases {
            if phase.name.is_empty()||phase.changes.is_empty(){return Err(BasketError::Evidence);}
            for (l,q) in &phase.changes {
                let p=final_positions.get_mut(l).ok_or(BasketError::Evidence)?;
                *p=add(*p,*q)?;
            }
        }
        if final_positions!=plan.targets{return Err(BasketError::Evidence);}
        let state=if plan.phases.is_empty(){State::Completed}else{State::Ready};
        Ok(Self{plan,initial:starting.clone(),positions:starting,phase_index:0,state,
            request_id:None,fills:BTreeMap::new(),used_requests:BTreeSet::new(),
            seen_events:BTreeMap::new(),events:Vec::new(),reason:None})
    }
    pub fn begin(&mut self,id:&str)->Result<(),BasketError>{
        if self.state==State::AwaitingConfirmation&&self.request_id.as_deref()==Some(id){return Ok(());}
        if self.state!=State::Ready||!valid_event_id(id)||self.used_requests.contains(id)||self.events.len()>=4096{return Err(BasketError::State);}
        self.used_requests.insert(id.into());self.request_id=Some(id.into());self.fills.clear();
        self.state=State::AwaitingConfirmation;self.events.push(JournalEvent::Begin{request_id:id.into()});Ok(())
    }
    pub fn fill(&mut self,request:&str,event:&str,leg:&LegId,cumulative:Decimal)->Result<(),BasketError>{
        if self.state!=State::AwaitingConfirmation||self.request_id.as_deref()!=Some(request)||!valid_event_id(event)||self.events.len()>=4096{return Err(BasketError::State);}
        if let Some(previous)=self.seen_events.get(event){
            if previous==&(request.to_owned(),leg.clone(),cumulative){return Ok(());}
            return Err(BasketError::Fill);
        }
        let phase=&self.plan.phases[self.phase_index];
        let expected=*phase.changes.get(leg).ok_or(BasketError::Fill)?;
        let old=self.fills.get(leg).copied().unwrap_or_default();
        if expected.is_zero()||(!cumulative.is_zero() && cumulative.is_sign_negative()!=expected.is_sign_negative())||cumulative.abs()>expected.abs()||
           cumulative.abs()<old.abs(){return Err(BasketError::Fill);}
        self.fills.insert(leg.clone(),cumulative);
        self.seen_events.insert(event.into(),(request.into(),leg.clone(),cumulative));
        self.events.push(JournalEvent::Fill{request_id:request.into(),event_id:event.into(),leg:leg.clone(),cumulative});Ok(())
    }
    pub fn confirm(&mut self,request:&str,terminal:&BTreeMap<LegId,bool>,
        observed:BTreeMap<LegId,Decimal>)->Result<(),BasketError>{
        if self.state!=State::AwaitingConfirmation||self.request_id.as_deref()!=Some(request)||self.events.len()>=4096{return Err(BasketError::State);}
        let phase=&self.plan.phases[self.phase_index];
        if terminal.keys().ne(phase.changes.keys())||terminal.values().any(|filled|!*filled)||
           phase.changes.iter().any(|(leg,q)|self.fills.get(leg)!=Some(q)){return Err(BasketError::Fill);}
        let mut expected=self.positions.clone();
        for (leg,change) in &phase.changes {
            let q=expected.get_mut(leg).ok_or(BasketError::Evidence)?;*q=add(*q,*change)?;
        }
        if observed!=expected{return Err(BasketError::Evidence);}
        self.phase_index+=1;self.positions=observed.clone();
        self.state=if self.phase_index==self.plan.phases.len(){State::Completed}else{State::Ready};
        self.request_id=None;self.fills.clear();
        self.events.push(JournalEvent::Confirm{request_id:request.into(),positions:observed});Ok(())
    }
    pub fn freeze(&mut self,reason:&str)->Result<(),BasketError>{
        if self.state==State::Completed||reason.is_empty()||reason.len()>512||self.events.len()>=4096{return Err(BasketError::State);}
        if self.state==State::Frozen{return Ok(());}
        self.state=State::Frozen;self.reason=Some(reason.into());
        self.events.push(JournalEvent::Freeze{reason:reason.into()});Ok(())
    }
    /// Replays events from immutable initial state; pending commands stay pending.
    pub fn replay(plan:Plan,initial:BTreeMap<LegId,Decimal>,events:&[JournalEvent])
        ->Result<Self,BasketError>{
        if events.len()>4096{return Err(BasketError::Evidence);}
        let mut state=Self::new(plan,initial)?;
        for e in events {
            match e {
                JournalEvent::Begin{request_id}=>state.begin(request_id)?,
                JournalEvent::Fill{request_id,event_id,leg,cumulative}=>state.fill(request_id,event_id,leg,*cumulative)?,
                JournalEvent::Confirm{request_id,positions}=>{
                    let phase=state.plan.phases.get(state.phase_index).ok_or(BasketError::State)?;
                    let terminal=phase.changes.keys().map(|l|(l.clone(),true)).collect();
                    state.confirm(request_id,&terminal,positions.clone())?;
                }
                JournalEvent::Freeze{reason}=>state.freeze(reason)?,
            }
        }
        if state.events!=events{return Err(BasketError::Evidence);}
        Ok(state)
    }
}
#[cfg(test)]
mod tests{
    use super::*;
    fn d(x:i64)->Decimal{Decimal::from(x)}
    fn leg(product:Product,symbol:&str)->LegId{LegId{tenant:"t".into(),account:"a".into(),product,symbol:symbol.into()}}
    #[test] fn funding_hedge_needs_confirmation(){
        let s=leg(Product::Spot,"ETHUSDT");let f=leg(Product::FuturesUm,"ETHUSDT");
        let target=vec![LegTarget{member_id:"a".into(),leg:s.clone(),signed_quantity:d(1)},
            LegTarget{member_id:"b".into(),leg:f.clone(),signed_quantity:d(-1)}];
        let positions=[(s.clone(),d(0)),(f.clone(),d(0))].into();
        let ev=[(s.clone(),Evidence{price:d(100),increment:d(1),multiplier:d(1),timestamp_ns:10}),
            (f.clone(),Evidence{price:d(100),increment:d(1),multiplier:d(1),timestamp_ns:10})].into();
        let p=plan(&target,&positions,&ev,11,5,d(200),Policy::FundingCarry,false).unwrap();
        let mut c=Checkpoint::new(p.clone(),positions.clone()).unwrap();c.begin("request").unwrap();
        assert_eq!(c.begin("request"),Ok(()));
        c.fill("request","e1",&s,d(1)).unwrap();c.fill("request","e2",&f,d(-1)).unwrap();
        assert_eq!(c.fill("request","e1",&s,d(1)),Ok(()));
        let terminal=[(s.clone(),true),(f.clone(),true)].into();
        let observed=[(s.clone(),d(1)),(f.clone(),d(-1))].into();
        c.confirm("request",&terminal,observed).unwrap();
        assert_eq!(c.state,State::Completed);
        let recovered=Checkpoint::replay(p,positions,&c.events).unwrap();
        assert_eq!(recovered,c);
    }
    #[test] fn declared_hedge_acquires_spot_before_increasing_perpetual(){
        let s=leg(Product::Spot,"ETHUSDT");
        let f=leg(Product::FuturesUm,"ETHUSDT");
        let targets=vec![
            LegTarget{member_id:"spot".into(),leg:s.clone(),signed_quantity:d(1)},
            LegTarget{member_id:"perp".into(),leg:f.clone(),signed_quantity:d(-1)},
        ];
        let start=[(s.clone(),d(0)),(f.clone(),d(0))].into();
        let e=[(s.clone(),Evidence{price:d(100),increment:d(1),
            multiplier:d(1),timestamp_ns:10}),
            (f.clone(),Evidence{price:d(100),increment:d(1),
            multiplier:d(1),timestamp_ns:10})].into();
        let plan=plan_with_constraints(&targets,&start,&e,11,5,d(200),
            Policy::Generic,d(0),&[(s,f)],false).unwrap();
        assert_eq!(plan.phases.len(),2);
        assert_eq!(plan.phases[0].name,"acquire_hedge_spot");
        assert_eq!(plan.phases[1].name,"increase_hedged_perpetual");
    }
    #[test] fn incomplete_fill_does_not_advance(){
        let s=leg(Product::Spot,"ETHUSDT");
        let p=Plan{targets:[(s.clone(),d(1))].into(),phases:vec![Phase{name:"buy".into(),
            changes:[(s.clone(),d(1))].into()}],gross_notional:d(100),executable:false};
        let mut c=Checkpoint::new(p,[(s.clone(),d(0))].into()).unwrap();c.begin("x").unwrap();
        assert!(c.confirm("x",&[(s.clone(),true)].into(),[(s.clone(),d(1))].into()).is_err());
        c.freeze("unknown external outcome").unwrap();assert_eq!(c.state,State::Frozen);
    }
}
