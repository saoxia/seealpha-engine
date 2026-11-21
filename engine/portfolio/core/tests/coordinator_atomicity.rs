//! Coordinator failure atomicity and explicit flatten semantics.
use rust_decimal::Decimal;
use seealpha_portfolio::coordinator::{
    Coordinator, CoordinatorError, Side, VirtualKind, VirtualOrder,
};

fn resting(id: &str, member: &str) -> VirtualOrder {
    VirtualOrder {
        id: id.into(),
        member_id: member.into(),
        side: Side::Buy,
        quantity: Decimal::from(2),
        filled: Decimal::ZERO,
        kind: VirtualKind::Limit {
            price: Decimal::from(100),
        },
        risk_exit: false,
        armed_at: None,
    }
}

#[test]
fn flatten_unknown_member_is_rejected_without_state_mutation() {
    let mut coordinator = Coordinator::new(Decimal::ONE, Decimal::from(10)).unwrap();
    coordinator.register("member-a").unwrap();
    coordinator.accept_order(resting("order-a", "member-a"), Some(1)).unwrap();
    let before = serde_json::to_value(&coordinator).unwrap();

    assert_eq!(
        coordinator.flatten("missing-member"),
        Err(CoordinatorError::NotFound)
    );
    assert_eq!(serde_json::to_value(&coordinator).unwrap(), before);
    assert!(coordinator.virtual_order("order-a").is_some());
}

#[test]
fn flatten_cancels_only_member_resting_orders_and_sets_zero_target() {
    let mut coordinator = Coordinator::new(Decimal::ONE, Decimal::from(10)).unwrap();
    coordinator.register("member-a").unwrap();
    coordinator.register("member-b").unwrap();
    coordinator.accept_order(resting("order-a", "member-a"), Some(1)).unwrap();
    coordinator.accept_order(resting("order-b", "member-b"), Some(1)).unwrap();

    coordinator.flatten("member-a").unwrap();
    assert!(coordinator.virtual_order("order-a").is_none());
    assert!(coordinator.virtual_order("order-b").is_some());
    let (_, command) = coordinator.reconcile(None, Decimal::ZERO, "cmd-a").unwrap();
    assert!(command.is_none());
}

#[test]
fn frozen_coordinator_rejects_flatten_without_mutation() {
    let mut coordinator = Coordinator::new(Decimal::ONE, Decimal::from(10)).unwrap();
    coordinator.register("member-a").unwrap();
    coordinator.accept_order(resting("order-a", "member-a"), Some(1)).unwrap();
    coordinator.freeze("unresolved external evidence");
    let before = serde_json::to_value(&coordinator).unwrap();

    assert_eq!(coordinator.flatten("member-a"), Err(CoordinatorError::Frozen));
    assert_eq!(serde_json::to_value(&coordinator).unwrap(), before);
}
