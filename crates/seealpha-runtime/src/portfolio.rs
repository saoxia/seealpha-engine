//! Rust-owned coordinator lifecycle, persisted before handing out any order intent.
//! Commands are never sent to the venue by this adapter.
use crate::{Runtime,RuntimeError,map_ledger};
use rust_decimal::Decimal;
use seealpha_portfolio::{
    coordinator::{Bar,Command,Coordinator,VirtualFill,VirtualOrder,VirtualKind},
    coordinator_snapshot::{decode_coordinator,encode_coordinator},
};
/// All events are applied to an isolated snapshot and atomically persisted.
pub enum NativePortfolioEvent {
    RegisterMember(String),
    PublishTarget { member:String, quantity:Decimal, risk_exit:bool },
    AcceptOrder { order:VirtualOrder, armed_at:Option<u64> },
    ModifyOrder { id:String, quantity:Option<Decimal>, kind:Option<VirtualKind> },
    CancelOrder(String),
    FlattenMember(String),
    ConfirmTerminal { order_id:String, canceled:bool, denied_at:Option<u64> },
    Freeze(String),
}
pub struct NativePortfolioState {
    pub coordinator:Coordinator,
    pub revision:String,
}
pub struct NativePortfolioResult {
    pub state:NativePortfolioState,
    pub fills:Vec<VirtualFill>,
    pub command:Option<Command>,
}
fn scope(tenant:&str,account:&str,instrument:&str)->Result<String,RuntimeError>{
    if [tenant,account,instrument].iter().any(|s|s.is_empty()||s.len()>40||
        !s.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'_'||b==b'-')) {
        return Err(RuntimeError::Conflict("invalid native portfolio scope".into()));
    }
    Ok(format!("{tenant}:{account}:{instrument}"))
}
impl Runtime {
    /// Reads a durable state; unresolved flight remains pending.
    pub fn recover_native_portfolio(
        &self,tenant:&str,account:&str,instrument:&str,
    )->Result<Option<NativePortfolioState>,RuntimeError>{
        self.require_active(tenant)?;
        let key=scope(tenant,account,instrument)?;
        let Some(row)=self.ledger.load_coordinator_state(tenant,account,instrument)
            .map_err(map_ledger)? else {return Ok(None)};
        let (coordinator,revision)=decode_coordinator(&key,&row.json)
            .map_err(|e|RuntimeError::Conflict(e.to_string()))?;
        if revision!=row.revision {
            return Err(RuntimeError::Conflict("native portfolio revision mismatch".into()));
        }
        Ok(Some(NativePortfolioState{coordinator,revision}))
    }
    pub fn save_native_portfolio(
        &self,tenant:&str,account:&str,instrument:&str,
        coordinator:&Coordinator,expected:Option<&str>,
    )->Result<NativePortfolioState,RuntimeError>{
        self.require_active(tenant)?;
        let key=scope(tenant,account,instrument)?;
        let encoded=encode_coordinator(&key,coordinator)
            .map_err(|e|RuntimeError::Conflict(e.to_string()))?;
        self.ledger.save_coordinator_state(tenant,account,instrument,
            &encoded.json,&encoded.revision,expected).map_err(map_ledger)?;
        Ok(NativePortfolioState{coordinator:coordinator.clone(),revision:encoded.revision})
    }
    pub fn apply_native_portfolio_event(
        &self,tenant:&str,account:&str,instrument:&str,
        expected_revision:&str,event:NativePortfolioEvent,
    )->Result<NativePortfolioState,RuntimeError>{
        let state=self.recover_native_portfolio(tenant,account,instrument)?
            .ok_or_else(||RuntimeError::NotFound("portfolio state missing".into()))?;
        if state.revision!=expected_revision{
            return Err(RuntimeError::Conflict("stale portfolio state".into()));
        }
        let mut coordinator=state.coordinator;
        let result=match event {
            NativePortfolioEvent::RegisterMember(id)=>coordinator.register(&id),
            NativePortfolioEvent::PublishTarget{member,quantity,risk_exit}=>
                coordinator.report_target(&member,quantity,risk_exit),
            NativePortfolioEvent::AcceptOrder{order,armed_at}=>
                coordinator.accept_order(order,armed_at),
            NativePortfolioEvent::ModifyOrder{id,quantity,kind}=>
                coordinator.modify(&id,quantity,kind),
            NativePortfolioEvent::CancelOrder(id)=>coordinator.cancel(&id),
            NativePortfolioEvent::FlattenMember(member)=>coordinator.flatten(&member),
            NativePortfolioEvent::ConfirmTerminal{order_id,canceled,denied_at}=>
                coordinator.confirm_terminal(&order_id,canceled,denied_at),
            NativePortfolioEvent::Freeze(reason)=>{coordinator.freeze(&reason);Ok(())},
        };
        if let Err(error)=result {
            if coordinator.frozen {
                self.save_native_portfolio(tenant,account,instrument,&coordinator,Some(expected_revision))?;
            }
            return Err(RuntimeError::Conflict(error.to_string()));
        }
        self.save_native_portfolio(tenant,account,instrument,&coordinator,Some(expected_revision))
    }
    /// Caller supplies authoritative net position; persisted flight is a prerequisite
    /// for interpreting Submit as a request eligible for subsequent risk checks.
    pub fn reconcile_native_portfolio(
        &self,tenant:&str,account:&str,instrument:&str,
        expected_revision:&str,bar:Option<Bar>,account_position:Decimal,request_id:&str,
    )->Result<NativePortfolioResult,RuntimeError>{
        let state=self.recover_native_portfolio(tenant,account,instrument)?
            .ok_or_else(||RuntimeError::NotFound("portfolio state missing".into()))?;
        if state.revision!=expected_revision{
            return Err(RuntimeError::Conflict("stale portfolio state".into()));
        }
        let mut coordinator=state.coordinator;
        let (fills,command)=coordinator.reconcile(bar,account_position,request_id)
            .map_err(|e|RuntimeError::Conflict(e.to_string()))?;
        let updated=self.save_native_portfolio(tenant,account,instrument,
            &coordinator,Some(expected_revision))?;
        Ok(NativePortfolioResult{state:updated,fills,command})
    }
    pub fn apply_native_portfolio_fill(
        &self,tenant:&str,account:&str,instrument:&str,
        expected_revision:&str,order_id:&str,event_id:&str,cumulative:Decimal,
        price:Decimal,bar:Option<Bar>,
    )->Result<NativePortfolioResult,RuntimeError>{
        let state=self.recover_native_portfolio(tenant,account,instrument)?
            .ok_or_else(||RuntimeError::NotFound("portfolio state missing".into()))?;
        if state.revision!=expected_revision{
            return Err(RuntimeError::Conflict("stale portfolio state".into()));
        }
        let mut coordinator=state.coordinator;
        let fills=match coordinator.apply_venue_fill(order_id,event_id,cumulative,price,bar){
            Ok(fills)=>fills,
            Err(error)=>{
                if coordinator.frozen {
                    self.save_native_portfolio(tenant,account,instrument,&coordinator,Some(expected_revision))?;
                }
                return Err(RuntimeError::Conflict(error.to_string()));
            }
        };
        let updated=self.save_native_portfolio(tenant,account,instrument,
            &coordinator,Some(expected_revision))?;
        Ok(NativePortfolioResult{state:updated,fills,command:None})
    }
}
