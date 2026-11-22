//! Native target publication through the portfolio coordinator.
//! Validates the strategy holding contract without involving Python sizing.
use rust_decimal::Decimal;
use seealpha_execution::coordinator::{Command, Coordinator, CoordinatorError};
use seealpha_execution::{TargetHolding, TargetHoldingError};

fn holding(q: Option<Decimal>, w: Option<Decimal>) -> TargetHolding {
    TargetHolding::new("ETHUSDT", q, w).unwrap()
}

#[test]
fn weighted_publications_resolve_and_net_inside_rust() {
    let mut c = Coordinator::new(Decimal::ONE, Decimal::from(20)).unwrap();
    c.register("long").unwrap();
    c.register("short").unwrap();
    c.report_holding(
        "long",
        &holding(None, Some(Decimal::new(75, 2))),
        Decimal::from(8),
        false,
    ).unwrap();
    c.report_holding(
        "short",
        &holding(None, Some(Decimal::new(-25, 2))),
        Decimal::from(8),
        false,
    ).unwrap();

    let (_, command) = c.reconcile(None, Decimal::ZERO, "target-1").unwrap();
    assert_eq!(
        command,
        Some(Command::Submit {
            id: "target-1".into(),
            signed_quantity: Decimal::from(4),
        })
    );
}

#[test]
fn malformed_allocation_cannot_mutate_existing_member_target() {
    let mut c = Coordinator::new(Decimal::ONE, Decimal::from(20)).unwrap();
    c.register("member").unwrap();
    c.report_holding(
        "member",
        &holding(Some(Decimal::from(3)), None),
        Decimal::ONE,
        false,
    ).unwrap();
    let before = serde_json::to_value(&c).unwrap();

    assert_eq!(
        c.report_holding(
            "member",
            &holding(None, Some(Decimal::new(5, 1))),
            Decimal::ZERO,
            false
        ),
        Err(CoordinatorError::Target(TargetHoldingError::Allocation))
    );
    assert_eq!(serde_json::to_value(&c).unwrap(), before);
}

#[test]
fn risk_exit_weight_cannot_increase_existing_exposure() {
    let mut c = Coordinator::new(Decimal::ONE, Decimal::from(20)).unwrap();
    c.register("member").unwrap();
    c.report_holding(
        "member",
        &holding(Some(Decimal::from(2)), None),
        Decimal::ONE,
        false,
    ).unwrap();
    let before = serde_json::to_value(&c).unwrap();

    assert_eq!(
        c.report_holding(
            "member",
            &holding(None, Some(Decimal::ONE)),
            Decimal::from(3),
            true
        ),
        Err(CoordinatorError::RiskExit)
    );
    assert_eq!(serde_json::to_value(&c).unwrap(), before);
}
