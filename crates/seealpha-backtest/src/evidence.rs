//! Evidence-driven two-leg risk accounting (no synthetic bar prices, no
//! assumed eight-hour funding, no trading order submission).
use rust_decimal::Decimal;
use serde_json::{Value,json};

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub struct MarkBar {
    pub mark_time_ms:i64,
    pub spot:Decimal,
    pub perp:Decimal,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub struct SettledFunding {
    pub at_ms:i64,
    pub rate:Decimal,
    pub mark_price:Decimal,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub struct CarryPosition {
    pub spot_qty:Decimal,
    pub perp_qty:Decimal,
    pub starting_balance:Decimal,
    pub max_gross_notional:Decimal,
    pub fee_rate:Decimal,
}

fn checked_mul(a:Decimal,b:Decimal)->Result<Decimal,&'static str>{
    a.checked_mul(b).ok_or("decimal overflow")
}
fn checked_add(a:Decimal,b:Decimal)->Result<Decimal,&'static str>{
    a.checked_add(b).ok_or("decimal overflow")
}
pub fn run_evidence_carry(
    bars:&[MarkBar],funding:&[SettledFunding],position:CarryPosition,
)->Result<Value,&'static str>{
    if bars.is_empty() || position.starting_balance<=Decimal::ZERO
        || position.max_gross_notional<=Decimal::ZERO
        || !(Decimal::ZERO..=Decimal::ONE).contains(&position.fee_rate)
        || position.spot_qty<Decimal::ZERO {
        return Err("invalid carry position or unsupported spot borrow");
    }
    let first=bars[0];
    if first.spot<=Decimal::ZERO || first.perp<=Decimal::ZERO {
        return Err("invalid first mark");
    }
    let mut last_time=None;
    for bar in bars {
        if bar.spot<=Decimal::ZERO || bar.perp<=Decimal::ZERO ||
            last_time.is_some_and(|prior|bar.mark_time_ms<=prior) {
            return Err("invalid or unordered evidence bars");
        }
        last_time=Some(bar.mark_time_ms);
    }
    let mut last_funding=None;
    for event in funding {
        if event.at_ms<=first.mark_time_ms ||
            event.at_ms>bars.last().map(|v|v.mark_time_ms).unwrap_or(first.mark_time_ms) ||
            last_funding.is_some_and(|time|event.at_ms<=time) ||
            event.mark_price<=Decimal::ZERO {
            return Err("funding event outside the held position or nonadvancing");
        }
        last_funding=Some(event.at_ms);
    }
    let initial_spot=checked_mul(first.spot,position.spot_qty)?;
    let initial_perp=checked_mul(first.perp,position.perp_qty.abs())?;
    let first_gross=checked_add(initial_spot,initial_perp)?;
    if first_gross>position.max_gross_notional ||
        initial_spot>position.starting_balance {
        return Err("initial exposure exceeds cash or risk limit");
    }
    // Entry fees are explicit. No imaginary closing fill is charged.
    let fees=checked_mul(first_gross,position.fee_rate)?;
    let cash=position.starting_balance.checked_sub(initial_spot)
        .and_then(|v|v.checked_sub(fees)).ok_or("decimal overflow")?;
    if cash<Decimal::ZERO {return Err("entry costs exceed available cash");}
    let mut income=Decimal::ZERO;
    let mut next_funding=0_usize;
    let mut curve=Vec::with_capacity(bars.len());
    let mut payments=Vec::new();
    let mut peak=Decimal::ZERO;
    let mut max_drawdown=Decimal::ZERO;
    for bar in bars {
        while next_funding<funding.len() &&
            funding[next_funding].at_ms<=bar.mark_time_ms {
            let f=funding[next_funding];
            // A short perpetual receives positive settled funding.
            let cashflow=checked_mul(
                checked_mul(-position.perp_qty,f.mark_price)?,f.rate
            )?;
            income=checked_add(income,cashflow)?;
            payments.push(json!({"funding_time_ms":f.at_ms,
                "funding_rate":f.rate.to_string(),"mark_price":f.mark_price.to_string(),
                "cashflow":cashflow.to_string()}));
            next_funding+=1;
        }
        let spot_notional=checked_mul(position.spot_qty,bar.spot)?;
        let perp_notional=checked_mul(position.perp_qty.abs(),bar.perp)?;
        let gross=checked_add(spot_notional,perp_notional)?;
        if gross>position.max_gross_notional {
            return Err("risk exposure exceeded during historical window");
        }
        let perp_pnl=checked_mul(position.perp_qty,
            bar.perp.checked_sub(first.perp).ok_or("decimal overflow")?)?;
        let equity=checked_add(
            checked_add(checked_add(cash,spot_notional)?,perp_pnl)?,income
        )?;
        peak=peak.max(equity);
        if peak>Decimal::ZERO {
            let drawdown=peak.checked_sub(equity).ok_or("decimal overflow")?
                .checked_div(peak).ok_or("decimal overflow")?;
            max_drawdown=max_drawdown.max(drawdown);
        }
        curve.push(json!({"mark_time_ms":bar.mark_time_ms,
            "spot_close":bar.spot.to_string(),"perp_close":bar.perp.to_string(),
            "gross_notional":gross.to_string(),
            "cumulative_funding":income.to_string(),
            "equity":equity.to_string()}));
    }
    let end_equity=curve.last().and_then(|row|row["equity"].as_str())
        .ok_or("missing ending equity")?;
    Ok(json!({"kind":"carry_evidence","paper_only":true,"exchange_orders":0,
        "entry_time_ms":first.mark_time_ms,"bars":bars.len(),
        "spot_qty":position.spot_qty.to_string(),"perp_qty":position.perp_qty.to_string(),
        "starting_balance":position.starting_balance.to_string(),
        "entry_fees":fees.to_string(),"fee_rate":position.fee_rate.to_string(),
        "funding_cashflow":income.to_string(),"funding_events":payments,
        "ending_balance":end_equity,"max_drawdown_ratio":max_drawdown.to_string(),
        "equity_curve":curve,"pricing":"actual_local_spot_and_perp_bars",
        "funding_coverage":"exchange_events_present_not_completeness_certified",
        "limitations":["no_exit_fill","no_liquidation_engine","no_borrow_or_margin_financing"]}))
}

#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn short_perp_earns_positive_funding_without_fabricated_intervals(){
        let bars=[MarkBar{mark_time_ms:0,spot:Decimal::from(100),perp:Decimal::from(100)},
            MarkBar{mark_time_ms:60_000,spot:Decimal::from(110),perp:Decimal::from(111)}];
        let position=CarryPosition{spot_qty:Decimal::ONE,perp_qty:-Decimal::ONE,
            starting_balance:Decimal::from(200),max_gross_notional:Decimal::from(300),
            fee_rate:Decimal::ZERO};
        let funding=[SettledFunding{at_ms:30_000,rate:Decimal::new(1,2),mark_price:Decimal::from(100)}];
        let report=run_evidence_carry(&bars,&funding,position).unwrap();
        assert_eq!(report["funding_cashflow"],"1.00");
        assert_eq!(report["ending_balance"],"200.00");
        assert_eq!(report["funding_events"].as_array().unwrap().len(),1);
    }
    #[test]
    fn rejects_time_travel_and_exposure_overflow(){
        let bars=[MarkBar{mark_time_ms:0,spot:Decimal::from(100),perp:Decimal::from(100)}];
        let pos=CarryPosition{spot_qty:Decimal::ONE,perp_qty:-Decimal::ONE,
            starting_balance:Decimal::from(100),max_gross_notional:Decimal::from(10),
            fee_rate:Decimal::ZERO};
        assert!(run_evidence_carry(&bars,&[],pos).is_err());
    }
}
