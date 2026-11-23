//! Target quantity and weight contract tests.
use rust_decimal::Decimal;
use seealpha_execution::{TargetHolding, TargetHoldingError};
use std::str::FromStr;

fn dec(value: &str) -> Decimal {
    Decimal::from_str(value).expect("fixture decimal")
}

#[test]
fn rejects_missing_ambiguous_and_out_of_range_targets() {
    for (quantity, weight) in [(None, None), (Some(dec("1")), Some(dec("0.5")))] {
        assert_eq!(
            TargetHolding::new("ETHUSDT", quantity, weight),
            Err(TargetHoldingError::Selection)
        );
    }
    for weight in ["-1.00000001", "1.00000001", "2", "-2"] {
        assert_eq!(
            TargetHolding::new("ETHUSDT", None, Some(dec(weight))),
            Err(TargetHoldingError::Weight)
        );
    }
    for weight in ["-1", "-0.25", "0", "0.25", "1"] {
        assert!(TargetHolding::new("ETHUSDT", None, Some(dec(weight))).is_ok());
    }
}

#[test]
fn weights_resolve_against_allocation_with_signed_decimal_precision() {
    let cases = [
        ("-1", "3.125", "-3.125"),
        ("-0.25", "0.0008", "-0.0002"),
        ("0", "5", "0"),
        ("0.5", "2.4", "1.2"),
        ("1", "3.125", "3.125"),
    ];
    for (weight, allocation, expected) in cases {
        let target = TargetHolding::new("ETHUSDT", None, Some(dec(weight))).unwrap();
        assert_eq!(
            target.signed_quantity(dec(allocation)).unwrap(),
            dec(expected),
            "weight={weight}, allocation={allocation}"
        );
    }
}

#[test]
fn absolute_targets_preserve_sign_and_validate_positive_allocation() {
    for raw in ["-100.00001", "-0.00000001", "0", "12.3456789"] {
        let target = TargetHolding::new("ETHUSDT", Some(dec(raw)), None).unwrap();
        assert_eq!(target.signed_quantity(dec("0.001")).unwrap(), dec(raw));
        for allocation in [dec("0"), dec("-0.1")] {
            assert_eq!(
                target.signed_quantity(allocation),
                Err(TargetHoldingError::Allocation)
            );
        }
    }
}
