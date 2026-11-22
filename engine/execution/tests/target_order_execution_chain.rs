//! Offline execution lifecycle contract. Venue data below is a test fixture,
//! not a substitute for authenticated Nautilus fill/position reports.
use rust_decimal::Decimal;
use seealpha_execution::{
    TargetHolding,
    coordinator::{Command, Coordinator, Side, VirtualKind, VirtualOrder},
    coordinator_snapshot::{decode_coordinator, encode_coordinator},
};

#[test]
fn target_to_durable_intent_to_partial_fill_to_recovery() {
    let mut book = Coordinator::new(Decimal::ONE, Decimal::from(20)).unwrap();
    book.register("alpha").unwrap();
    book.register("hedge").unwrap();
    book.report_holding(
        "alpha", &TargetHolding::new("ETHUSDT", None, Some(Decimal::ONE)).unwrap(),
        Decimal::from(6), false,
    ).unwrap();
    book.report_holding(
        "hedge", &TargetHolding::new("ETHUSDT", Some(Decimal::from(-2)), None).unwrap(),
        Decimal::ONE, false,
    ).unwrap();
    let (_, command) = book.reconcile(None, Decimal::ZERO, "intent-1").unwrap();
    assert_eq!(command, Some(Command::Submit {
        id: "intent-1".into(), signed_quantity: Decimal::from(4)
    }));
    // Caller must durably register this intent before any Nautilus order submission.
    let stored = encode_coordinator("tenant:account:ETHUSDT", &book).unwrap();
    let (mut recovered, revision) = decode_coordinator(
        "tenant:account:ETHUSDT", &stored.json
    ).unwrap();
    assert_eq!(revision, stored.revision);
    assert_eq!(recovered.flight.as_ref().unwrap().id, "intent-1");
    assert!(recovered.reconcile(None, Decimal::ZERO, "intent-2").unwrap().1.is_none());
    // This report is a fixture: in production only Nautilus-authenticated
    // cumulative execution evidence may reach apply_venue_fill.
    assert!(recovered.apply_venue_fill(
        "intent-1", "trade-1", Decimal::ONE, Decimal::from(2000), None,
    ).unwrap().is_empty());
    assert!(recovered.apply_venue_fill(
        "intent-1", "trade-1", Decimal::ONE, Decimal::from(2000), None,
    ).unwrap().is_empty());
    assert_eq!(recovered.flight.as_ref().unwrap().filled, Decimal::ONE);
    let after_partial = encode_coordinator("tenant:account:ETHUSDT", &recovered).unwrap();
    let (mut restart, _) = decode_coordinator(
        "tenant:account:ETHUSDT", &after_partial.json
    ).unwrap();
    assert_eq!(restart.flight.as_ref().unwrap().filled, Decimal::ONE);
    assert!(restart.reconcile(None, Decimal::ONE, "intent-3").unwrap().1.is_none());
    restart.apply_venue_fill(
        "intent-1", "trade-2", Decimal::from(4), Decimal::from(2001), None,
    ).unwrap();
    assert!(restart.flight.is_none());
    assert!(restart.reconcile(None, Decimal::from(4), "intent-4").unwrap().1.is_none());
}

#[test]
fn virtual_order_is_part_of_durable_coordinator_snapshot() {
    let mut book = Coordinator::new(Decimal::ONE, Decimal::from(10)).unwrap();
    book.register("alpha").unwrap();
    book.accept_order(VirtualOrder {
        id: "virtual-1".into(), member_id: "alpha".into(),
        side: Side::Buy, quantity: Decimal::from(2), filled: Decimal::ZERO,
        kind: VirtualKind::Limit { price: Decimal::from(2000) },
        risk_exit: false, armed_at: None,
    }, Some(10)).unwrap();
    let persisted = encode_coordinator("tenant:account:ETHUSDT", &book).unwrap();
    let (restored, _) = decode_coordinator("tenant:account:ETHUSDT", &persisted.json).unwrap();
    assert_eq!(restored.virtual_order("virtual-1").unwrap().quantity, Decimal::from(2));
    assert_eq!(restored.virtual_order("virtual-1").unwrap().armed_at, Some(10));
}
