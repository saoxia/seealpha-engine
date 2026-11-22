//! Deterministic fault-injection regressions against the Python coordinator,
//! basket_order_evidence and basket_risk_monitor behavioral contracts.
//! Synthetic evidence only: there is no network, venue, or order transmitter.
use rust_decimal::Decimal;
use std::collections::BTreeMap;

use seealpha_execution::basket::{Checkpoint, Evidence, LegId, Phase, Plan, Product, State};
use seealpha_execution::coordinator::{
    Bar, Command, Coordinator, CoordinatorError, Side, VirtualKind, VirtualOrder,
};
use seealpha_execution::coordinator_snapshot::{decode_coordinator, encode_coordinator};
use seealpha_execution::journal::{decode_checkpoint, encode_checkpoint};
use seealpha_execution::order_evidence::{
    OrderEvidence, VenueOrderStatus, bind_phase_orders, reconcile_order_evidence,
};
use seealpha_execution::phase_gate::{PhaseGateLimits, gate_next_phase};
use seealpha_execution::risk::{RiskLimits, observe_phase};

fn d(x: i64) -> Decimal {
    Decimal::from(x)
}
fn bar(t: u64) -> Bar {
    Bar {
        time: t,
        low: d(90),
        high: d(110),
        close: d(100),
    }
}
fn virtual_order(id: &str, member: &str, side: Side, qty: i64) -> VirtualOrder {
    VirtualOrder {
        id: id.into(),
        member_id: member.into(),
        side,
        quantity: d(qty),
        filled: Decimal::ZERO,
        kind: VirtualKind::Market,
        risk_exit: false,
        armed_at: None,
    }
}
fn leg(product: Product) -> LegId {
    LegId {
        tenant: "tenant".into(),
        account: "account".into(),
        product,
        symbol: "ETHUSDT".into(),
    }
}
fn quote() -> Evidence {
    Evidence {
        price: d(100),
        increment: d(1),
        multiplier: d(1),
        timestamp_ns: 10,
    }
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
    let checkpoint = Checkpoint::new(
        Plan {
            targets: target,
            phases,
            gross_notional: d(200),
            executable: false,
        },
        start,
    )
    .unwrap();
    let evidence = [(spot.clone(), quote()), (future.clone(), quote())].into();
    (checkpoint, spot, future, evidence)
}

#[test]
fn crossing_rolls_back_if_risk_budget_denies_the_net_position() {
    let mut c = Coordinator::new(d(1), d(1)).unwrap();
    c.register("buyer").unwrap();
    c.register("seller").unwrap();
    c.accept_order(virtual_order("buy", "buyer", Side::Buy, 3), None)
        .unwrap();
    c.accept_order(virtual_order("sell", "seller", Side::Sell, 1), None)
        .unwrap();
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
    c.accept_order(virtual_order("virtual", "buyer", Side::Buy, 2), None)
        .unwrap();
    assert_eq!(
        c.reconcile(Some(bar(10)), d(0), "venue-request").unwrap().1,
        Some(Command::Submit {
            id: "venue-request".into(),
            signed_quantity: d(2)
        }),
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
        c.reconcile(Some(bar(12)), d(0), "must-not-retry")
            .unwrap_err(),
        CoordinatorError::Frozen,
    );
    let raw = encode_coordinator("tenant:account:ETHUSDT", &c).unwrap();
    let (restored, _) = decode_coordinator("tenant:account:ETHUSDT", &raw.json).unwrap();
    assert!(restored.frozen);
    assert_eq!(restored.flight.as_ref().unwrap().id, "venue-request");
}

#[test]
fn partially_attributable_venue_fill_freezes_without_committing_phantom_member_fills() {
    let mut coordinator = Coordinator::new(d(1), d(10)).unwrap();
    coordinator.register("buyer").unwrap();
    coordinator
        .accept_order(virtual_order("original", "buyer", Side::Buy, 2), None)
        .unwrap();
    coordinator.reconcile(Some(bar(10)), d(0), "venue-request").unwrap();

    // A strategy changes its virtual orders while the external request is in
    // flight. Only one unit can now be attributed to a two-unit venue fill.
    coordinator.cancel("original").unwrap();
    coordinator
        .accept_order(virtual_order("remaining", "buyer", Side::Buy, 1), None)
        .unwrap();
    let before_position = coordinator.member_position("buyer");
    let before_order = coordinator.virtual_order("remaining").unwrap().clone();

    assert_eq!(
        coordinator.apply_venue_fill(
            "venue-request", "exec-1", d(2), d(100), Some(bar(11))
        ),
        Err(CoordinatorError::Evidence)
    );
    assert!(coordinator.frozen);
    assert_eq!(coordinator.member_position("buyer"), before_position);
    assert_eq!(coordinator.virtual_order("remaining"), Some(&before_order));
    assert_eq!(coordinator.flight.as_ref().unwrap().filled, Decimal::ZERO);

    // The frozen state, original in-flight identity, and unaffected member
    // book must survive restart until independent account reconciliation.
    let stored = encode_coordinator("tenant:account:ETHUSDT", &coordinator).unwrap();
    let (restored, _) = decode_coordinator("tenant:account:ETHUSDT", &stored.json).unwrap();
    assert!(restored.frozen);
    assert_eq!(restored.member_position("buyer"), before_position);
    assert_eq!(restored.virtual_order("remaining"), Some(&before_order));
    assert_eq!(restored.flight.as_ref().unwrap().filled, Decimal::ZERO);
}

#[test]
fn cancel_rejection_keeps_original_order_in_flight_and_frozen() {
    let mut c = Coordinator::new(d(1), d(10)).unwrap();
    c.register("buyer").unwrap();
    c.accept_order(virtual_order("virtual", "buyer", Side::Buy, 2), None)
        .unwrap();
    c.reconcile(Some(bar(10)), d(0), "venue-request").unwrap();
    c.report_target("buyer", d(0), true).unwrap();
    assert_eq!(
        c.reconcile(Some(bar(11)), d(0), "new-request").unwrap().1,
        Some(Command::Cancel {
            id: "venue-request".into()
        }),
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
        trigger: d(100),
        price: d(100),
        triggered: true,
    };
    c.accept_order(stop, Some(10)).unwrap();
    assert!(matches!(
        &c.virtual_order("stop").unwrap().kind,
        VirtualKind::StopLimit {
            triggered: false,
            ..
        }
    ));
    assert_eq!(
        c.modify("stop", Some(d(3)), Some(VirtualKind::Limit { price: d(0) }))
            .unwrap_err(),
        CoordinatorError::Invalid,
    );
    assert_eq!(c.virtual_order("stop").unwrap().quantity, d(2));
    c.modify(
        "stop",
        None,
        Some(VirtualKind::StopMarket { trigger: d(100) }),
    )
    .unwrap();
    assert_eq!(c.virtual_order("stop").unwrap().armed_at, None);
    assert!(
        c.reconcile(Some(bar(11)), d(0), "same-bar")
            .unwrap()
            .1
            .is_none()
    );
    assert!(
        c.reconcile(Some(bar(12)), d(0), "next-bar")
            .unwrap()
            .1
            .is_some()
    );
}

#[test]
fn one_leg_fills_and_other_leg_unknown_freezes_journal_and_replay() {
    let (mut cp, spot, future, quotes) = basket();
    let denied = gate_next_phase(
        &cp,
        &cp.positions,
        &quotes,
        PhaseGateLimits {
            now_ns: 11,
            max_age_ns: 5,
            max_gross_notional: d(300),
            max_leg_drift: d(0),
        },
        &[],
    )
    .unwrap();
    assert!(!denied.admitted);
    // With an explicitly larger transient budget, the research phase can be
    // registered, but a failed counter-leg still prevents any following phase.
    cp.begin("request").unwrap();
    let manifest = bind_phase_orders(&cp).unwrap();
    let order_id = |leg: &LegId| {
        manifest
            .orders
            .iter()
            .find(|o| o.leg == *leg)
            .unwrap()
            .client_order_id
            .clone()
    };
    let rows = vec![
        OrderEvidence {
            leg: spot.clone(),
            client_order_id: order_id(&spot),
            venue_order_id: "spot1001".into(),
            execution_id: "exec2001".into(),
            filled_quantity: d(1),
            status: VenueOrderStatus::Filled,
            as_of_ns: 11,
        },
        OrderEvidence {
            leg: future.clone(),
            client_order_id: order_id(&future),
            venue_order_id: "perp1002".into(),
            execution_id: "".into(),
            filled_quantity: d(0),
            status: VenueOrderStatus::Unknown,
            as_of_ns: 11,
        },
    ];
    let observed = [(spot.clone(), d(1)), (future.clone(), d(0))].into();
    let frozen = reconcile_order_evidence(cp, &manifest, &rows, &observed, 11, 11, 5).unwrap();
    assert_eq!(frozen.state, State::Frozen);
    assert_eq!(frozen.fills[&spot], d(1));
    let risk = observe_phase(
        frozen.clone(),
        &observed,
        &quotes,
        RiskLimits {
            now_ns: 11,
            max_age_ns: 5,
            max_pending_ns: 5,
            max_gross_notional: d(300),
            max_leg_drift: d(0),
        },
        Some(10),
    )
    .unwrap();
    assert!(risk.frozen);
    let encoded = encode_checkpoint("basket1", &frozen).unwrap();
    let decoded = decode_checkpoint("basket1", &encoded.json).unwrap();
    assert_eq!(decoded.checkpoint.state, State::Frozen);
    assert!(
        gate_next_phase(
            &decoded.checkpoint,
            &observed,
            &quotes,
            PhaseGateLimits {
                now_ns: 11,
                max_age_ns: 5,
                max_gross_notional: d(300),
                max_leg_drift: d(1),
            },
            &[],
        )
        .unwrap()
        .block_reason
        .is_some()
    );
}

#[test]
fn partial_fill_with_unchanged_account_snapshot_is_not_accepted() {
    let (mut cp, spot, future, _) = basket();
    cp.begin("request").unwrap();
    let manifest = bind_phase_orders(&cp).unwrap();
    let client = |leg: &LegId| {
        manifest
            .orders
            .iter()
            .find(|order| order.leg == *leg)
            .unwrap()
            .client_order_id
            .clone()
    };
    let rows = vec![
        OrderEvidence {
            leg: spot.clone(),
            client_order_id: client(&spot),
            venue_order_id: "spot1001".into(),
            execution_id: "exec2001".into(),
            filled_quantity: d(1),
            status: VenueOrderStatus::Filled,
            as_of_ns: 11,
        },
        OrderEvidence {
            leg: future.clone(),
            client_order_id: client(&future),
            venue_order_id: "perp1002".into(),
            execution_id: "".into(),
            filled_quantity: d(0),
            status: VenueOrderStatus::Unknown,
            as_of_ns: 11,
        },
    ];
    // A FILLED exchange row without the matching account position is a
    // conflicting snapshot, not evidence of successfully hedged exposure.
    let unchanged = [(spot, d(0)), (future, d(0))].into();
    let frozen = reconcile_order_evidence(cp, &manifest, &rows, &unchanged, 11, 11, 5).unwrap();
    assert_eq!(frozen.state, State::Frozen);
    assert!(
        frozen
            .reason
            .as_deref()
            .unwrap()
            .contains("positions disagree")
    );
}

#[test]
fn completed_order_id_is_never_reused_after_snapshot_restore() {
    let mut c = Coordinator::new(d(1), d(10)).unwrap();
    c.register("buyer").unwrap();
    c.accept_order(virtual_order("virtual", "buyer", Side::Buy, 1), None)
        .unwrap();
    c.reconcile(Some(bar(10)), d(0), "request-once").unwrap();
    let fills = c
        .apply_venue_fill("request-once", "execution-1", d(1), d(100), Some(bar(11)))
        .unwrap();
    assert_eq!(fills.len(), 1);
    assert!(c.flight.is_none());
    c.report_target("buyer", d(2), false).unwrap();
    let raw = encode_coordinator("tenant:account:ETHUSDT", &c).unwrap();
    let (mut restored, _) = decode_coordinator("tenant:account:ETHUSDT", &raw.json).unwrap();
    assert_eq!(
        restored
            .reconcile(Some(bar(12)), d(1), "request-once")
            .unwrap_err(),
        CoordinatorError::Duplicate,
    );
    assert!(restored.flight.is_none());
    assert_eq!(
        restored
            .reconcile(Some(bar(12)), d(1), "request-two")
            .unwrap()
            .1,
        Some(Command::Submit {
            id: "request-two".into(),
            signed_quantity: d(1),
        }),
    );
}

#[test]
fn missing_pair_declaration_and_cross_account_scope_fail_closed() {
    let (cp, spot, future, quotes) = basket();
    let another = LegId {
        symbol: "BTCUSDT".into(),
        ..spot.clone()
    };
    let positions = [
        (spot.clone(), d(0)),
        (future.clone(), d(0)),
        (another.clone(), d(0)),
    ]
    .into();
    let changes = [
        (spot.clone(), d(1)),
        (future.clone(), d(-1)),
        (another.clone(), d(1)),
    ]
    .into();
    let target = [
        (spot.clone(), d(1)),
        (future.clone(), d(-1)),
        (another.clone(), d(1)),
    ]
    .into();
    let multi = Checkpoint::new(
        Plan {
            targets: target,
            phases: vec![Phase {
                name: "mixed".into(),
                changes,
            }],
            gross_notional: d(300),
            executable: false,
        },
        positions,
    )
    .unwrap();
    let mut evidence = quotes.clone();
    evidence.insert(another, quote());
    let limits = PhaseGateLimits {
        now_ns: 11,
        max_age_ns: 5,
        max_gross_notional: d(500),
        max_leg_drift: d(1),
    };
    assert!(gate_next_phase(&multi, &multi.positions, &evidence, limits, &[],).is_err());
    let mut foreign = future.clone();
    foreign.account = "unrelated-account".into();
    let mismatched_positions = [(spot.clone(), d(0)), (foreign.clone(), d(0))].into();
    let mismatched = Checkpoint::new(
        Plan {
            targets: [(spot.clone(), d(1)), (foreign.clone(), d(-1))].into(),
            phases: vec![Phase {
                name: "cross_account".into(),
                changes: [(spot.clone(), d(1)), (foreign.clone(), d(-1))].into(),
            }],
            gross_notional: d(200),
            executable: false,
        },
        mismatched_positions,
    )
    .unwrap();
    let mixed_quotes = [(spot, quote()), (foreign, quote())].into();
    assert!(
        gate_next_phase(
            &mismatched,
            &mismatched.positions,
            &mixed_quotes,
            limits,
            &[],
        )
        .is_err()
    );
    assert_eq!(cp.state, State::Ready);
}
