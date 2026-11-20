//! Deterministic fault-injection regressions against the Python coordinator,
//! basket_order_evidence and basket_risk_monitor behavioral contracts.
//! Synthetic evidence only: there is no network, venue, or order transmitter.
use rust_decimal::Decimal;
use std::collections::BTreeMap;

use seealpha_portfolio::basket::{
    Checkpoint, Evidence, LegId, Phase, Plan, Product, State,
};
use seealpha_portfolio::coordinator::{
    Bar, Command, Coordinator, CoordinatorError, Side, VirtualKind, VirtualOrder,
};
use seealpha_portfolio::coordinator_snapshot::{
    decode_coordinator, encode_coordinator,
};
use seealpha_portfolio::journal::{decode_checkpoint, encode_checkpoint};
use seealpha_portfolio::order_evidence::{
    bind_phase_orders, reconcile_order_evidence, OrderEvidence, VenueOrderStatus,
};
use seealpha_portfolio::phase_gate::{
    gate_next_phase, PhaseGateLimits,
};
use seealpha_portfolio::risk::{observe_phase, RiskLimits};

fn d(x: i64) -> Decimal {
    Decimal::from(x)
}
fn bar(t: u64) -> Bar {
    Bar { time: t, low: d(90), high: d(110), close: d(100) }
}
fn virtual_order(id: &str, member: &str, side: Side, qty: i64) -> VirtualOrder {
    VirtualOrder {
        id: id.into(), member_id: member.into(), side,
        quantity: d(qty), filled: Decimal::ZERO, kind: VirtualKind::Market,
        risk_exit: false, armed_at: None,
    }
}
fn leg(product: Product) -> LegId {
    LegId {
        tenant: "tenant".into(), account: "account".into(),
        product, symbol: "ETHUSDT".into(),
    }
}
fn quote() -> Evidence {
    Evidence { price: d(100), increment: d(1), multiplier: d(1), timestamp_ns: 10 }
}
fn basket() -> (Checkpoint, LegId, LegId, BTreeMap<LegId, Evidence>) {
    let spot = leg(Product::Spot);
    let future = leg(Product::FuturesUm);
    let start = [(spot.clone(), d(0)), (future.clone(), d(0))].into();
    let target = [(spot.clone(), d(1)), (future.clone(), d(-1))].into();
    let phases = vec![Phase {
        name: "coordinated_venue_orders".into(),
        changes: [(spot.clone(), d(1)), (future.clone(), d(-1))].into(),
    }];
    let checkpoint = Checkpoint::new(Plan {
        targets: target, phases, gross_notional: d(200), executable: false,
    }, start).unwrap();
    let evidence = [(spot.clone(), quote()), (future.clone(), quote())].into();
    (checkpoint, spot, future, evidence)
}

#[test]
fn crossing_rolls_back_if_risk_budget_denies_the_net_position() {
    let mut c = Coordinator::new(d(1), d(1)).unwrap();
    c.register("buyer").unwrap();
    c.register("seller").unwrap();
    c.accept_order(virtual_order("buy", "buyer", Side::Buy, 3), None).unwrap();
    c.accept_order(virtual_order("sell", "seller", Side::Sell, 1), None).unwrap();
    assert_eq!(
        c.reconcile(Some(bar(10)), d(0), "request").unwrap_err(),
        CoordinatorError::RiskExit,
    );
    assert_eq!(c.member_position("buyer"), Some(d(0)));
    assert_eq!(c.member_position("seller"), Some(d(0)));
    assert_eq!(c.virtual_order("buy").unwrap().filled, d(0));
    assert_eq!(c.virtual_order("sell").unwrap().filled, d(0));
    assert!(c.flight.is_none());
}

#[test]
fn external_fill_after_virtual_cancel_freezes_without_phantom_allocation() {
    let mut c = Coordinator::new(d(1), d(10)).unwrap();
    c.register("buyer").unwrap();
    c.accept_order(virtual_order("virtual", "buyer", Side::Buy, 2), None).unwrap();
    assert_eq!(
        c.reconcile(Some(bar(10)), d(0), "venue-request").unwrap().1,
        Some(Command::Submit { id: "venue-request".into(), signed_quantity: d(2) }),
    );
    c.cancel("virtual").unwrap();
    assert_eq!(
        c.apply_venue_fill("venue-request", "execution-1", d(1), d(100), Some(bar(11)))
            .unwrap_err(),
        CoordinatorError::Evidence,
    );
    assert!(c.frozen);
    assert_eq!(c.member_position("buyer"), Some(d(0)));
    assert_eq!(c.flight.as_ref().unwrap().filled, d(0));
    assert_eq!(
        c.reconcile(Some(bar(12)), d(0), "must-not-retry").unwrap_err(),
        CoordinatorError::Frozen,
    );
    let raw = encode_coordinator("tenant:account:ETHUSDT", &c).unwrap();
    let (restored, _) = decode_coordinator("tenant:account:ETHUSDT", &raw.json).unwrap();
    assert!(restored.frozen);
    assert_eq!(restored.flight.as_ref().unwrap().id, "venue-request");
}

#[test]
fn cancel_rejection_keeps_original_order_in_flight_and_frozen() {
    let mut c = Coordinator::new(d(1), d(10)).unwrap();
    c.register("buyer").unwrap();
    c.accept_order(virtual_order("virtual", "buyer", Side::Buy, 2), None).unwrap();
    c.reconcile(Some(bar(10)), d(0), "venue-request").unwrap();
    c.report_target("buyer", d(0), true).unwrap();
    assert_eq!(
        c.reconcile(Some(bar(11)), d(0), "new-request").unwrap().1,
        Some(Command::Cancel { id: "venue-request".into() }),
    );
    c.cancel_denied("venue-request", Some(11)).unwrap();
    assert!(c.frozen);
    assert_eq!(c.flight.as_ref().unwrap().id, "venue-request");
    assert_eq!(
        c.reconcile(Some(bar(12)), d(0), "replacement").unwrap_err(),
        CoordinatorError::Frozen,
    );
}

#[test]
fn risk_exit_cannot_reverse_a_members_position() {
    let mut c = Coordinator::new(d(1), d(10)).unwrap();
    c.register("a").unwrap();
    c.report_target("a", d(1), false).unwrap();
    assert_eq!(
        c.report_target("a", d(-1), true).unwrap_err(),
        CoordinatorError::RiskExit,
    );
}

#[test]
fn invalid_modify_is_atomic_and_valid_stop_amend_rearms() {
    let mut c = Coordinator::new(d(1), d(10)).unwrap();
    c.register("a").unwrap();
    let mut stop = virtual_order("stop", "a", Side::Buy, 2);
    stop.kind = VirtualKind::StopLimit {
        trigger: d(100), price: d(100), triggered: true,
    };
    c.accept_order(stop, Some(10)).unwrap();
    assert!(matches!(
        &c.virtual_order("stop").unwrap().kind,
        VirtualKind::StopLimit { triggered: false, .. }
    ));
    assert_eq!(
        c.modify("stop", Some(d(3)), Some(VirtualKind::Limit { price: d(0) }))
            .unwrap_err(),
        CoordinatorError::Invalid,
    );
    assert_eq!(c.virtual_order("stop").unwrap().quantity, d(2));
    c.modify("stop", None, Some(VirtualKind::StopMarket { trigger: d(100) }))
        .unwrap();
    assert_eq!(c.virtual_order("stop").unwrap().armed_at, None);
    assert!(c.reconcile(Some(bar(11)), d(0), "same-bar").unwrap().1.is_none());
    assert!(c.reconcile(Some(bar(12)), d(0), "next-bar").unwrap().1.is_some());
}

#[test]
fn one_leg_fills_and_other_leg_unknown_freezes_journal_and_replay() {
    let (mut cp, spot, future, quotes) = basket();
    let denied = gate_next_phase(
        &cp, &cp.positions, &quotes,
        PhaseGateLimits {
            now_ns: 11, max_age_ns: 5, max_gross_notional: d(300),
            max_leg_drift: d(0),
        },
        &[],
    ).unwrap();
    assert!(!denied.admitted);
    // With an explicitly larger transient budget, the research phase can be
    // registered, but a failed counter-leg still prevents any following phase.
    cp.begin("request").unwrap();
    let manifest = bind_phase_orders(&cp).unwrap();
    let order_id = |leg: &LegId| manifest.orders.iter()
        .find(|o| o.leg == *leg).unwrap().client_order_id.clone();
    let rows = vec![
        OrderEvidence {
            leg: spot.clone(), client_order_id: order_id(&spot),
            venue_order_id: "spot1001".into(), execution_id: "exec2001".into(),
            filled_quantity: d(1), status: VenueOrderStatus::Filled, as_of_ns: 11,
        },
        OrderEvidence {
            leg: future.clone(), client_order_id: order_id(&future),
            venue_order_id: "perp1002".into(), execution_id: "".into(),
            filled_quantity: d(0), status: VenueOrderStatus::Unknown, as_of_ns: 11,
        },
    ];
    let observed = [(spot.clone(), d(1)), (future.clone(), d(0))].into();
    let frozen = reconcile_order_evidence(
        cp, &manifest, &rows, &observed, 11, 11, 5,
    ).unwrap();
    assert_eq!(frozen.state, State::Frozen);
    assert_eq!(frozen.fills[&spot], d(1));
    let risk = observe_phase(
        frozen.clone(), &observed, &quotes,
        RiskLimits {
            now_ns: 11, max_age_ns: 5, max_pending_ns: 5,
            max_gross_notional: d(300), max_leg_drift: d(0),
        },
        Some(10),
    ).unwrap();
    assert!(risk.frozen);
    let encoded = encode_checkpoint("basket1", &frozen).unwrap();
    let decoded = decode_checkpoint("basket1", &encoded.json).unwrap();
    assert_eq!(decoded.checkpoint.state, State::Frozen);
    assert!(gate_next_phase(
        &decoded.checkpoint, &observed, &quotes,
        PhaseGateLimits {
            now_ns: 11, max_age_ns: 5, max_gross_notional: d(300),
            max_leg_drift: d(1),
        },
        &[],
    ).unwrap().block_reason.is_some());
}
