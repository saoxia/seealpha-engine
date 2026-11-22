//! Bar-synchronous portfolio target replay: avoid artificial fills from
//! opposing strategies publishing on the same market bar.
use rust_decimal::Decimal;
use seealpha_execution::TargetHolding;
use seealpha_backtest::{
    HoldingPublication, HoldingReplayError, IndexedHoldingPublication, replay_holdings_by_bar,
};

fn item(bar: usize, member: &str, weight: &str, price: i64) -> IndexedHoldingPublication {
    IndexedHoldingPublication {
        bar_index: bar,
        publication: HoldingPublication {
            member_id: member.to_owned(),
            holding: TargetHolding::new(
                "ETHUSDT",
                None,
                Some(weight.parse::<Decimal>().unwrap()),
            ).unwrap(),
            allocation: Decimal::from(4),
            price: Decimal::from(price),
        },
    }
}

#[test]
fn opposing_targets_on_same_bar_net_before_first_fill() {
    let pubs = [item(0, "a", "0.75", 100), item(0, "b", "-0.25", 100)];
    let (fills, position) =
        replay_holdings_by_bar(&pubs, Decimal::ZERO, Decimal::from(10), Decimal::ONE).unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].side, "buy");
    assert_eq!(fills[0].quantity, Decimal::from(2));
    assert_eq!(position, Decimal::from(2));
}

#[test]
fn exact_same_bar_hedge_emits_no_external_paper_fill() {
    let pubs = [item(0, "a", "1", 100), item(0, "b", "-1", 100)];
    let (fills, position) =
        replay_holdings_by_bar(&pubs, Decimal::ZERO, Decimal::from(10), Decimal::ONE).unwrap();
    assert!(fills.is_empty());
    assert_eq!(position, Decimal::ZERO);
}

#[test]
fn latest_member_target_persists_across_bars() {
    let pubs = [
        item(0, "a", "0.75", 100),
        item(0, "b", "-0.25", 100),
        item(1, "a", "0", 110),
    ];
    let (fills, position) =
        replay_holdings_by_bar(&pubs, Decimal::ZERO, Decimal::from(10), Decimal::ONE).unwrap();
    assert_eq!(fills.len(), 2);
    assert_eq!(fills[0].quantity, Decimal::from(2));
    assert_eq!(fills[1].side, "sell");
    assert_eq!(fills[1].quantity, Decimal::from(3));
    assert_eq!(position, Decimal::from(-1));
}

#[test]
fn repeated_member_publications_on_same_bar_use_last_target() {
    let pubs = [item(0, "a", "1", 100), item(0, "a", "0.25", 100)];
    let (fills, position) =
        replay_holdings_by_bar(&pubs, Decimal::ZERO, Decimal::from(10), Decimal::ONE).unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].quantity, Decimal::ONE);
    assert_eq!(position, Decimal::ONE);
}

#[test]
fn rejects_reordered_bars_and_conflicting_same_bar_prices() {
    let unordered = [item(2, "a", "1", 100), item(1, "b", "-1", 100)];
    assert_eq!(
        replay_holdings_by_bar(&unordered, Decimal::ZERO, Decimal::from(10), Decimal::ONE),
        Err(HoldingReplayError::BarOrder)
    );
    let conflicting = [item(0, "a", "1", 100), item(0, "b", "-1", 101)];
    assert_eq!(
        replay_holdings_by_bar(&conflicting, Decimal::ZERO, Decimal::from(10), Decimal::ONE),
        Err(HoldingReplayError::BarPrice)
    );
}

#[test]
fn invalid_allocation_fails_before_any_paper_execution() {
    let mut pubs = vec![item(0, "a", "1", 100), item(0, "b", "-1", 100)];
    pubs[1].publication.allocation = Decimal::ZERO;
    assert!(replay_holdings_by_bar(
        &pubs, Decimal::ZERO, Decimal::from(10), Decimal::ONE
    ).is_err());
}
