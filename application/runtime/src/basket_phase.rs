//! Persist a cross-leg risk decision and an unresolved basket phase before
//! any potential Venue side effect. Only cfg(test) exercises a mock transport.
use crate::{map_ledger, Runtime, RuntimeError};
use rust_decimal::Decimal;
#[cfg(test)]
use seealpha_venue::order_guard::{PreparedWrite, SignedTransport};
#[cfg(test)]
use crate::order_journal::{BasketReservation, LedgerIntentJournal};
use seealpha_ledger::{BasketAdmission, IntentStart};
use seealpha_portfolio::{
    basket::{Evidence, LegId, Product},
    journal::{decode_checkpoint, encode_checkpoint},
    phase_gate::{gate_next_phase, PhaseGateLimits},
};
use serde_json::{json, Value};
use std::collections::BTreeMap;

impl Runtime {
    /// Register one already risk-reserved basket leg without sending an order.
    /// The sole-leg restriction is deliberate: a multi-leg phase needs an
    /// explicit coordinated dispatch/recovery protocol before admission.
    #[allow(clippy::too_many_arguments)]
    fn validate_reserved_basket_leg(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        basket_id: &str,
        expected_basket_revision: &str,
        expected_account_revision: i64,
        request_id: &str,
        client_order_id: &str,
        fingerprint: &str,
        intent: &Value,
    ) -> Result<(), RuntimeError> {
        self.require_active(tenant)?;
        let stored = self.ledger.load_basket_journal(tenant, account, basket_id)
            .map_err(map_ledger)?
            .ok_or_else(|| RuntimeError::Conflict("篮子风控阶段不存在。".into()))?;
        if stored.revision != expected_basket_revision {
            return Err(RuntimeError::Conflict("篮子风控阶段版本已变化。".into()));
        }
        let verified = decode_checkpoint(basket_id, &stored.body_json)
            .map_err(|_| RuntimeError::Conflict("篮子风控检查点无法验证。".into()))?;
        if verified.revision != expected_basket_revision
            || verified.checkpoint.request_id.as_deref() != Some(request_id)
            || verified.checkpoint.state != seealpha_portfolio::basket::State::AwaitingConfirmation
        {
            return Err(RuntimeError::Conflict("篮子没有预留当前订单请求。".into()));
        }
        let phase = verified.checkpoint.plan.phases.get(verified.checkpoint.phase_index)
            .ok_or_else(|| RuntimeError::Conflict("篮子没有待执行阶段。".into()))?;
        if phase.changes.len() != 1 {
            return Err(RuntimeError::Conflict("多腿阶段尚无独立授权的发送协议。".into()));
        }
        let (leg, delta) = phase.changes.iter().next()
            .ok_or_else(|| RuntimeError::Conflict("缺少待执行的篮子腿。".into()))?;
        let product = match &leg.product {
            Product::Spot => "spot",
            Product::FuturesUm => "futures_um",
            Product::Option => "options",
        };
        let side = if *delta > Decimal::ZERO { "BUY" } else { "SELL" };
        let requested = intent["quantity"].as_str().and_then(|v| v.parse::<Decimal>().ok());
        if delta.is_zero() || leg.tenant != tenant || leg.account != account
            || intent["action"] != "NEW"
            || intent["environment"].as_str() != Some(environment)
            || intent["product"].as_str() != Some(product)
            || intent["symbol"].as_str() != Some(leg.symbol.as_str())
            || intent["side"].as_str() != Some(side)
            || requested != Some(delta.abs())
            || intent["client_order_id"].as_str() != Some(client_order_id)
        {
            return Err(RuntimeError::Conflict("订单与已预留的风控阶段不一致。".into()));
        }
        // The Ledger checks the account and basket CAS revisions in its own
        // IMMEDIATE transaction alongside the insert. This Runtime read is
        // for semantic validation, not an atomic authorization by itself.
        Ok(())
    }

    pub fn preregister_reserved_basket_leg(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        basket_id: &str,
        expected_basket_revision: &str,
        expected_account_revision: i64,
        request_id: &str,
        client_order_id: &str,
        fingerprint: &str,
        intent: &Value,
    ) -> Result<IntentStart, RuntimeError> {
        self.validate_reserved_basket_leg(
            tenant, account, environment, basket_id, expected_basket_revision,
            expected_account_revision, request_id, client_order_id, fingerprint, intent,
        )?;
        self.ledger.preregister_basket_order_intent(
            tenant, account, environment, request_id, client_order_id,
            fingerprint, intent, BasketAdmission {
                basket_id,
                basket_revision: expected_basket_revision,
                account_revision: expected_account_revision,
            },
        ).map_err(map_ledger)
    }

    /// Offline-only wiring of the reserved risk stage to Venue one-shot
    /// admission. No signed exchange transport is constructed by Runtime.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn dispatch_reserved_basket_leg_fixture<T: SignedTransport>(
        &self,
        tenant: &str, account: &str, environment: &str,
        basket_id: &str, basket_revision: &str, account_revision: i64,
        request_id: &str, client_order_id: &str,
        intent: &Value, mut prepared: PreparedWrite, transport: &mut T,
    ) -> Result<Value, RuntimeError> {
        let record = prepared.record();
        if record.request_id != request_id || record.client_order_id != client_order_id {
            return Err(RuntimeError::Conflict("Venue 请求身份与风控预留不一致。".into()));
        }
        self.validate_reserved_basket_leg(
            tenant, account, environment, basket_id, basket_revision,
            account_revision, request_id, client_order_id,
            &record.payload_sha256, intent,
        )?;
        let mut journal = LedgerIntentJournal::new_basket(
            &self.ledger, tenant, account, environment, intent.clone(),
            BasketReservation {
                basket_id: basket_id.into(),
                basket_revision: basket_revision.into(),
                account_revision,
            },
        );
        prepared.dispatch(&mut journal, transport)
            .map_err(|_| RuntimeError::Conflict(
                "Venue 模拟发送失败或状态未知，请按订单身份对账，禁止重复提交。".into(),
            ))
    }

    /// Read the trusted, replay-verified basket checkpoint, validate all legs,
    /// then CAS-persist AwaitingConfirmation; no VENUE permission is returned.
    pub fn reserve_basket_phase(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        expected_account_revision: i64,
        basket_id: &str,
        expected_revision: &str,
        request_id: &str,
        positions: &BTreeMap<LegId, Decimal>,
        evidence: &BTreeMap<LegId, Evidence>,
        limits: PhaseGateLimits,
        hedge_pairs: &[(LegId, LegId)],
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        let stored = self.ledger.load_basket_journal(tenant, account, basket_id)
            .map_err(map_ledger)?
            .ok_or_else(|| RuntimeError::NotFound("篮子持久化记录不存在。".into()))?;
        if stored.revision != expected_revision {
            return Err(RuntimeError::Conflict("篮子版本过期，请先恢复状态。".into()));
        }
        let decoded = decode_checkpoint(basket_id, &stored.body_json)
            .map_err(|e| RuntimeError::Conflict(e.to_string()))?;
        if decoded.revision != stored.revision {
            return Err(RuntimeError::Conflict("篮子持久化校验失败。".into()));
        }
        if decoded.checkpoint.plan.targets.keys().any(|leg|
            leg.tenant != tenant || leg.account != account
        ) || positions.keys().any(|leg|
            leg.tenant != tenant || leg.account != account
        ) {
            return Err(RuntimeError::Conflict("篮子仓位与租户账户作用域不一致。".into()));
        }
        // Bind supplied positions to one immutable, fresh, CAS-versioned
        // authoritative account observation. Caller-provided positions alone
        // are never an authority for a multi-leg admission.
        if !matches!(environment, "production" | "sandbox" | "paper") {
            return Err(RuntimeError::Conflict("账户环境无效。".into()));
        }
        let account_state = self.ledger.account_snapshot(tenant, account, environment)
            .map_err(map_ledger)?
            .ok_or_else(|| RuntimeError::Conflict("缺少已核验账户仓位快照。".into()))?;
        if account_state.revision != expected_account_revision
            || (environment == "paper" && account_state.source != "paper_simulator")
            || (environment != "paper"
                && account_state.source != "signed_exchange_account_query")
        {
            return Err(RuntimeError::Conflict("账户快照来源或版本不可信。".into()));
        }
        let snapshot_ns = u64::try_from(account_state.observed_at_ms)
            .ok().and_then(|v| v.checked_mul(1_000_000))
            .ok_or_else(|| RuntimeError::Conflict("账户快照时间溢出。".into()))?;
        if snapshot_ns > limits.now_ns
            || limits.now_ns - snapshot_ns > limits.max_age_ns
        {
            return Err(RuntimeError::Conflict("账户快照已过期，禁止篮子阶段登记。".into()));
        }
        let reported = account_state.state["positions"].as_object()
            .ok_or_else(|| RuntimeError::Conflict("账户仓位证据缺失。".into()))?;
        for (leg, desired) in positions {
            let product = match &leg.product {
                Product::Spot => "spot",
                Product::FuturesUm => "futures_um",
                Product::Option => "options",
            };
            let key = format!("{product}:{}", leg.symbol);
            let amount = reported.get(&key).and_then(Value::as_str)
                .ok_or_else(|| RuntimeError::Conflict("账户快照未覆盖篮子全部腿。".into()))?
                .parse::<Decimal>()
                .map_err(|_| RuntimeError::Conflict("账户仓位精度或数值无效。".into()))?;
            if amount != *desired {
                return Err(RuntimeError::Conflict("账户快照与篮子持仓不一致。".into()));
            }
        }
        let gate = gate_next_phase(
            &decoded.checkpoint, positions, evidence, limits, hedge_pairs,
        ).map_err(|e| RuntimeError::Conflict(e.to_string()))?;
        if !gate.admitted || gate.executable {
            return Err(RuntimeError::Conflict(
                gate.block_reason.unwrap_or_else(|| "篮子阶段风险门禁拒绝。".into()),
            ));
        }
        let mut checkpoint = decoded.checkpoint;
        checkpoint.begin(request_id)
            .map_err(|e| RuntimeError::Conflict(e.to_string()))?;
        let encoded = encode_checkpoint(basket_id, &checkpoint)
            .map_err(|e| RuntimeError::Conflict(e.to_string()))?;
        let saved = self.ledger.save_basket_journal(
            tenant, account, basket_id, &encoded.json, &encoded.revision,
            Some(expected_revision),
        ).map_err(map_ledger)?;
        Ok(json!({
            "basket_id": basket_id,
            "revision": saved.revision,
            "request_id": request_id,
            "status": "awaiting_confirmation",
            "worst_case_gross": gate.worst_case_gross.to_string(),
            "worst_case_leg_drift": gate.worst_case_leg_drift.map(|v|v.to_string()),
            "executable": false,
            "exchange_orders": 0,
        }))
    }
}

#[cfg(test)]
mod offline_admission_tests {
    use super::*;
    use crate::config::{AuthMode, Config};
    use seealpha_portfolio::basket::{Checkpoint, Phase, Plan};
    use seealpha_venue::{Product as VenueProduct, VenueError};
    use seealpha_venue::order_guard::WriteAction;

    struct FakeWire { attempts: usize, acknowledge: bool }
    impl SignedTransport for FakeWire {
        fn send(&mut self, method: &str, path: &str, _: &[(String, String)])
            -> Result<Value, VenueError> {
            assert_eq!(method, "POST");
            assert_eq!(path, "/api/v3/order");
            self.attempts += 1;
            if self.acknowledge {
                Ok(json!({"clientOrderId":"client1","symbol":"ETHUSDT",
                    "orderId":42,"status":"NEW"}))
            } else {
                Err(VenueError::Unconfirmed)
            }
        }
    }

    fn config(path: &std::path::Path) -> Config {
        Config {
            host:"127.0.0.1".into(), port:8766, auth_mode:AuthMode::None,
            token:String::new(), state_dir:path.into(), project:path.into(),
            tenant_id:None, tenant_purpose:Some("development".into()),
            research_signing_key:None, research_mcp_enabled:false,
            venue_base:None, python:"python3".into(),
        }
    }

    fn prepared() -> PreparedWrite {
        PreparedWrite::new("t:a:paper", "req1", VenueProduct::Spot,
            WriteAction::NewOrder, "ETHUSDT", "client1",
            vec![
                ("symbol".into(),"ETHUSDT".into()),
                ("newClientOrderId".into(),"client1".into()),
                ("side".into(),"BUY".into()),
                ("quantity".into(),"0.1".into()),
                ("price".into(),"2000".into()),
                ("type".into(),"LIMIT".into()),
            ]).unwrap()
    }

    fn setup(runtime: &Runtime) -> (String, Value) {
        runtime.ensure_tenant("t", "development", "active").unwrap();
        let leg = LegId {
            tenant:"t".into(), account:"a".into(),
            product:Product::Spot, symbol:"ETHUSDT".into(),
        };
        let changes = [(leg.clone(),Decimal::new(1,1))].into();
        let plan = Plan {
            targets:[(leg.clone(),Decimal::new(1,1))].into(),
            phases:vec![Phase{name:"buy".into(),changes}],
            gross_notional:Decimal::new(200,0), executable:false,
        };
        let mut cp = Checkpoint::new(plan,[(leg,Decimal::ZERO)].into()).unwrap();
        cp.begin("req1").unwrap();
        let encoded = encode_checkpoint("basket1",&cp).unwrap();
        runtime.ledger.save_basket_journal(
            "t","a","basket1",&encoded.json,&encoded.revision,None,
        ).unwrap();
        runtime.ledger.save_account_snapshot(
            "t","a","paper",None,"paper1",1,"paper_simulator",
            &json!({"balances":{"USDT":"1000"},"positions":{"spot:ETHUSDT":"0"}}),
        ).unwrap();
        let intent = json!({
            "action":"NEW","environment":"paper","product":"spot",
            "client_order_id":"client1","symbol":"ETHUSDT",
            "side":"BUY","quantity":"0.1","price":"2000","order_type":"LIMIT",
            "basket_id":"basket1","basket_revision":encoded.revision,
            "basket_request_id":"req1","account_revision":1,
            "wire_parameters":[
                ["symbol","ETHUSDT"],["newClientOrderId","client1"],
                ["side","BUY"],["quantity","0.1"],["price","2000"],
                ["type","LIMIT"]
            ]
        });
        (encoded.revision,intent)
    }

    #[test]
    fn timeout_then_restart_never_reissues_and_never_invents_fill() {
        let root = std::env::temp_dir().join(format!(
            "seealpha-basket-wire-{}-{:?}",
            std::process::id(), std::thread::current().id(),
        ));
        let _ = std::fs::remove_dir_all(&root);
        let cfg=config(&root);
        let runtime=Runtime::open(&cfg).unwrap();
        let (revision,intent)=setup(&runtime);
        let mut wire=FakeWire{attempts:0,acknowledge:false};
        assert!(runtime.dispatch_reserved_basket_leg_fixture(
            "t","a","paper","basket1",&revision,1,"req1","client1",
            &intent,prepared(),&mut wire,
        ).is_err());
        assert_eq!(wire.attempts,1);
        assert!(runtime.ledger.fills("t","a","paper").unwrap().is_empty());
        drop(runtime);
        let resumed=Runtime::open(&cfg).unwrap();
        assert!(resumed.dispatch_reserved_basket_leg_fixture(
            "t","a","paper","basket1",&revision,1,"req1","client1",
            &intent,prepared(),&mut wire,
        ).is_err());
        assert_eq!(wire.attempts,1);
        assert!(resumed.ledger.request("t","a","req1").unwrap().is_some());
        drop(resumed);
        let _=std::fs::remove_dir_all(root);
    }

    #[test]
    fn accepted_ack_has_no_fill_and_stale_snapshot_cannot_send() {
        let root = std::env::temp_dir().join(format!(
            "seealpha-basket-ack-{}-{:?}",
            std::process::id(), std::thread::current().id(),
        ));
        let _=std::fs::remove_dir_all(&root);
        let runtime=Runtime::open(&config(&root)).unwrap();
        let (revision,intent)=setup(&runtime);
        runtime.ledger.save_account_snapshot(
            "t","a","paper",Some(1),"paper2",2,"paper_simulator",
            &json!({"balances":{"USDT":"999"},"positions":{"spot:ETHUSDT":"0"}}),
        ).unwrap();
        let mut wire=FakeWire{attempts:0,acknowledge:true};
        assert!(runtime.dispatch_reserved_basket_leg_fixture(
            "t","a","paper","basket1",&revision,1,"req1","client1",
            &intent,prepared(),&mut wire,
        ).is_err());
        assert_eq!(wire.attempts,0);
        // Current revision is valid; the exchange ACK does not prove a fill.
        runtime.dispatch_reserved_basket_leg_fixture(
            "t","a","paper","basket1",&revision,2,"req1","client1",
            &{ let mut updated=intent.clone(); updated["account_revision"]=json!(2); updated },
            prepared(),&mut wire,
        ).unwrap();
        assert_eq!(wire.attempts,1);
        assert!(runtime.ledger.fills("t","a","paper").unwrap().is_empty());
        drop(runtime);
        let _=std::fs::remove_dir_all(root);
    }
}
