use super::*;

fn d(value: i64) -> Decimal { Decimal::from(value) }
fn c(id: &str, product: Product) -> Contract {
    Contract {
        id: id.into(), quote_currency: "USDT".into(), product,
        multiplier: Decimal::ONE, initial_margin_ratio: Decimal::new(1, 1),
        maintenance_margin_ratio: Decimal::new(5, 2),
        taker_fee_rate: Decimal::ZERO, settlement_fee_rate: Decimal::ZERO,
    }
}
fn q(id: &str, t: i64, bid: i64, ask: i64, bid_size: i64, ask_size: i64) -> Event {
    Event::Quote(Quote { time_ms: t, instrument: id.into(),
        bid: d(bid), ask: d(ask), bid_size: d(bid_size), ask_size: d(ask_size) })
}
fn order(id: &str, instrument: &str, side: Side, quantity: i64, kind: OrderType) -> Order {
    Order { id: id.into(), instrument: instrument.into(),
        side, quantity: d(quantity), kind }
}
fn policy(cash: i64) -> Policy {
    Policy { starting_cash: d(cash), max_mark_age_ms: 1_000,
        taker_slippage_bps: Decimal::ZERO }
}
struct Start { commands: Vec<Command> }
impl Strategy for Start {
    fn on_start(&mut self, _: &Account) -> Vec<Command> {
        std::mem::take(&mut self.commands)
    }
}
fn starts(orders: Vec<Order>) -> Start {
    Start { commands: orders.into_iter().map(Command::Submit).collect() }
}

#[test]
fn uses_only_displayed_size_once_and_partially_fills_across_quotes() {
    let mut spot = c("S", Product::Spot);
    spot.taker_fee_rate = Decimal::new(1, 3);
    let mut s = starts(vec![order("one", "S", Side::Buy, 3, OrderType::Market)]);
    let report = replay(&[spot], &[
        q("S",1,99,100,10,2), q("S",2,100,101,10,1)
    ], policy(1_000), &mut s).unwrap();
    assert_eq!(report.fills.len(), 2);
    assert_eq!(report.fills[0].quantity, d(2));
    assert_eq!(report.fills[1].quantity, d(1));
    assert_eq!(report.fills[0].price, d(100));
    assert_eq!(report.fills[1].price, d(101));
    assert_eq!(report.commissions, Decimal::new(301,3));
    assert_eq!(report.cash, Decimal::new(698699,3));
    assert_eq!(report.equity, Decimal::new(998699,3));
    assert!(report.pending.is_empty());
    assert!(!report.historical_coverage_certified);
}

#[test]
fn competing_orders_cannot_reuse_a_single_quote_volume() {
    let mut s = starts(vec![
        order("a","S",Side::Buy,2,OrderType::Market),
        order("b","S",Side::Buy,2,OrderType::Market),
    ]);
    let report = replay(&[c("S",Product::Spot)],
        &[q("S",1,99,100,10,3)],policy(1_000),&mut s).unwrap();
    assert_eq!(report.fills.len(),2);
    assert_eq!(report.fills[0].quantity,d(2));
    assert_eq!(report.fills[1].quantity,d(1));
    assert_eq!(report.pending.len(),1);
    assert_eq!(report.pending[0].remaining,d(1));
}

struct SubmitOnQuote { emitted: bool }
impl Strategy for SubmitOnQuote {
    fn on_quote(&mut self, _: &Quote, _: &Account) -> Vec<Command> {
        if std::mem::replace(&mut self.emitted,true) {return Vec::new();}
        vec![Command::Submit(order("from-close","S",Side::Buy,1,OrderType::Market))]
    }
}
#[test]
fn callback_cannot_consume_the_quote_that_caused_the_decision() {
    let mut s=SubmitOnQuote{emitted:false};
    let report=replay(&[c("S",Product::Spot)],&[
        q("S",1,99,100,10,10),q("S",2,100,110,10,10)
    ],policy(1_000),&mut s).unwrap();
    assert_eq!(report.fills.len(),1);
    assert_eq!(report.fills[0].time_ms,2);
    assert_eq!(report.fills[0].price,d(110));
    assert_eq!(report.curve[0].equity,d(1_000));
}

#[test]
fn limit_does_not_fill_on_an_ohlc_touch_or_trade_print_alone() {
    let mut s=starts(vec![order("L","S",Side::Buy,1,OrderType::Limit(d(100)))]);
    let report=replay(&[c("S",Product::Spot)],&[
        q("S",1,104,105,10,10),
        Event::Trade(Trade {time_ms:2,instrument:"S".into(),price:d(90)}),
        q("S",3,98,99,10,1)
    ],policy(1_000),&mut s).unwrap();
    assert_eq!(report.fills.len(),1);
    assert_eq!(report.fills[0].time_ms,3);
    assert_eq!(report.fills[0].price,d(99));
}

#[test]
fn slippage_can_prevent_a_limit_from_crossing() {
    let mut s=starts(vec![order("L","S",Side::Buy,1,OrderType::Limit(d(100)))]);
    let mut config=policy(1_000);
    config.taker_slippage_bps=d(200);
    let report=replay(&[c("S",Product::Spot)],
        &[q("S",1,99,100,10,10)],config,&mut s).unwrap();
    assert!(report.fills.is_empty());
    assert_eq!(report.pending.len(),1);
}

#[test]
fn stop_requires_later_print_and_still_later_executable_quote() {
    let mut s=starts(vec![order("stop","S",Side::Buy,1,OrderType::StopMarket(d(105)))]);
    let report=replay(&[c("S",Product::Spot)],&[
        q("S",1,99,100,10,10),
        Event::Trade(Trade {time_ms:2,instrument:"S".into(),price:d(105)}),
        q("S",2,106,107,10,10),
        q("S",3,108,109,10,10),
    ],policy(1_000),&mut s).unwrap();
    assert_eq!(report.fills.len(),1);
    assert_eq!(report.fills[0].time_ms,3);
    assert_eq!(report.fills[0].price,d(109));
}

struct ReplaceAfterPartial { changed: bool }
impl Strategy for ReplaceAfterPartial {
    fn on_start(&mut self, _: &Account) -> Vec<Command> {
        vec![Command::Submit(order("old","S",Side::Buy,3,OrderType::Market))]
    }
    fn on_fill(&mut self, _: &Fill, _: &Account) -> Vec<Command> {
        if std::mem::replace(&mut self.changed,true) {return Vec::new();}
        vec![Command::Replace {
            old_id:"old".into(),
            new_order:order("new","S",Side::Buy,2,OrderType::Limit(d(101))),
        }]
    }
}
#[test]
fn partial_fill_then_replace_keeps_first_fill_and_restarts_priority() {
    let mut s=ReplaceAfterPartial{changed:false};
    let report=replay(&[c("S",Product::Spot)],&[
        q("S",1,99,100,10,1),q("S",2,99,100,10,2)
    ],policy(1_000),&mut s).unwrap();
    assert_eq!(report.fills.len(),2);
    assert_eq!(report.fills[0].order_id,"old");
    assert_eq!(report.fills[0].quantity,d(1));
    assert_eq!(report.fills[1].order_id,"new");
    assert_eq!(report.fills[1].quantity,d(2));
    assert!(report.pending.is_empty());
}

struct CancelAfterPartial;
impl Strategy for CancelAfterPartial {
    fn on_start(&mut self, _: &Account) -> Vec<Command> {
        vec![Command::Submit(order("cancel","S",Side::Buy,3,OrderType::Market))]
    }
    fn on_fill(&mut self, _: &Fill, _: &Account) -> Vec<Command> {
        vec![Command::Cancel("cancel".into())]
    }
}
#[test]
fn cancel_removes_only_remaining_quantity() {
    let mut s=CancelAfterPartial;
    let r=replay(&[c("S",Product::Spot)],&[
        q("S",1,99,100,10,1),q("S",2,99,100,10,10)
    ],policy(1_000),&mut s).unwrap();
    assert_eq!(r.fills.len(),1);
    assert_eq!(r.positions["S"],d(1));
    assert!(r.pending.is_empty());
}

#[test]
fn signed_perpetual_funding_only_from_supplied_settlement_evidence() {
    let mut p=c("P",Product::LinearPerpetual);
    p.initial_margin_ratio=Decimal::new(1,1);
    p.maintenance_margin_ratio=Decimal::new(5,2);
    let mut s=starts(vec![order("short","P",Side::Sell,1,OrderType::Market)]);
    let report=replay(&[p],&[
        q("P",1,100,101,1,1),
        Event::Funding(FundingSettlement {time_ms:17,instrument:"P".into(),
            evidence_id:"historical-settlement-17".into(),
            rate:Decimal::new(1,2),mark_price:d(100)})
    ],policy(1_000),&mut s).unwrap();
    assert_eq!(report.funding,d(1));
    assert_eq!(report.cash,d(1_001));
    assert_eq!(report.equity,d(1_001));
    assert_eq!(report.positions["P"],-Decimal::ONE);
    let mut s2=starts(vec![order("short","P",Side::Sell,1,OrderType::Market)]);
    let mut p2=c("P",Product::LinearPerpetual);
    p2.initial_margin_ratio=Decimal::new(1,1);
    p2.maintenance_margin_ratio=Decimal::new(5,2);
    let without=replay(&[p2],&[
        q("P",1,100,101,1,1),
        Event::Mark(Mark {time_ms:17,instrument:"P".into(),price:d(100)})
    ],policy(1_000),&mut s2).unwrap();
    assert_eq!(without.funding,Decimal::ZERO);
    assert_eq!(without.cash,d(1_000));
}

#[test]
fn spot_and_perp_share_equity_without_counting_perp_notional_as_cash() {
    let mut s=starts(vec![
        order("spot","S",Side::Buy,1,OrderType::Market),
        order("perp","P",Side::Sell,1,OrderType::Market),
    ]);
    let mut perp=c("P",Product::LinearPerpetual);
    perp.initial_margin_ratio=Decimal::new(1,1);
    perp.maintenance_margin_ratio=Decimal::new(5,2);
    let report=replay(&[c("S",Product::Spot),perp],&[
        q("S",1,100,100,1,1),q("P",2,100,100,1,1),
        Event::Funding(FundingSettlement {time_ms:3,instrument:"P".into(),
            evidence_id:"settled-3".into(),rate:Decimal::new(1,2),mark_price:d(100)})
    ],policy(1_000),&mut s).unwrap();
    assert_eq!(report.positions["S"],d(1));
    assert_eq!(report.positions["P"],d(-1));
    assert_eq!(report.cash,d(901));
    assert_eq!(report.equity,d(1_001));
}

fn call(expiry_ms: i64) -> Contract {
    c("CALL",Product::CashOption {
        underlying:"ETHUSDT".into(), right:OptionRight::Call,
        strike:d(100), expiry_ms,
    })
}
#[test]
fn option_requires_real_settlement_event_and_uses_multiplier_cash_payoff() {
    let mut option=call(10);
    option.multiplier=d(2);
    option.settlement_fee_rate=Decimal::new(1,2);
    let events=[
        q("CALL",1,4,5,1,1),
        Event::OptionSettlement(OptionSettlement{
            time_ms:10,instrument:"CALL".into(),evidence_id:"expiry-10".into(),
            underlying_settlement_price:d(110),
        }),
    ];
    let mut s=starts(vec![order("buy","CALL",Side::Buy,1,OrderType::Market)]);
    let r=replay(&[option.clone()],&events,policy(100),&mut s).unwrap();
    assert_eq!(r.fills.len(),1);
    assert_eq!(r.fills[0].price,d(5));
    assert_eq!(r.settlements.len(),1);
    assert_eq!(r.settlements[0].gross,d(20));
    assert_eq!(r.settlements[0].fee,Decimal::new(2,1));
    assert_eq!(r.cash,d(108));
    assert_eq!(r.equity,d(108));
    assert!(r.positions.is_empty());

    let mut s=starts(vec![order("buy","CALL",Side::Buy,1,OrderType::Market)]);
    let no_settlement=[q("CALL",1,4,5,1,1),
        Event::Trade(Trade {time_ms:11,instrument:"CALL".into(),price:d(6)})];
    assert!(replay(&[option],&no_settlement,policy(100),&mut s).unwrap_err()
        .contains("missing verified expiry"));
}

#[test]
fn rejects_margin_excess_instead_of_issuing_a_fictional_liquidation() {
    let mut p=c("P",Product::LinearPerpetual);
    p.initial_margin_ratio=Decimal::new(5,1);
    p.maintenance_margin_ratio=Decimal::new(2,1);
    let mut s=starts(vec![order("too-large","P",Side::Buy,1,OrderType::Market)]);
    let r=replay(&[p],&[q("P",1,999,1000,1,1)],policy(100),&mut s).unwrap();
    assert!(r.fills.is_empty());
    assert_eq!(r.rejected,vec!["too-large".to_string()]);
    assert_eq!(r.cash,d(100));
}

#[test]
fn duplicate_quotes_and_unverified_funding_fail_closed() {
    let mut s=starts(Vec::new());
    assert!(replay(&[c("S",Product::Spot)],
        &[q("S",1,99,100,1,1),q("S",1,99,100,1,1)],
        policy(100),&mut s).is_err());
    let mut s=starts(Vec::new());
    assert!(replay(&[c("P",Product::LinearPerpetual)],
        &[Event::Funding(FundingSettlement {time_ms:1,instrument:"P".into(),
            evidence_id:String::new(),rate:Decimal::ZERO,mark_price:d(100)})],
        policy(100),&mut s).is_err());
}

struct CrossAssetFollowOn;
impl Strategy for CrossAssetFollowOn {
    fn on_start(&mut self, _: &Account) -> Vec<Command> {
        vec![Command::Submit(order("a","A",Side::Buy,1,OrderType::Market))]
    }
    fn on_fill(&mut self, _: &Fill, _: &Account) -> Vec<Command> {
        vec![Command::Submit(order("b","B",Side::Buy,1,OrderType::Market))]
    }
}
#[test]
fn fill_callback_cannot_trade_another_instrument_at_the_same_timestamp() {
    let mut s=CrossAssetFollowOn;
    let r=replay(&[c("A",Product::Spot),c("B",Product::Spot)],&[
        q("A",1,99,100,1,1),q("B",1,99,100,1,1),
        q("B",2,99,100,1,1)
    ],policy(1_000),&mut s).unwrap();
    assert_eq!(r.fills.len(),2);
    assert_eq!(r.fills[0].time_ms,1);
    assert_eq!(r.fills[1].time_ms,2);
}
