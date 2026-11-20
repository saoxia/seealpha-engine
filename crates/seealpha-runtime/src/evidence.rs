//! Native cross-product portfolio replay from local verified bar evidence.
//! Unlike the old synthetic carry-window demonstration, neither historical
//! prices nor funding settlements are fabricated by this execution path.
use crate::{Runtime,RuntimeError,map_ledger};
use rust_decimal::Decimal;
use seealpha_backtest::evidence::{MarkBar,SettledFunding,CarryPosition,run_evidence_carry};
use seealpha_market::interval_ms;
use serde_json::{json,Value};

fn error(message:&str)->RuntimeError {RuntimeError::Conflict(message.into())}

impl Runtime {
    #[allow(clippy::too_many_arguments)]
    pub fn run_evidence_carry_backtest(
        &self,tenant:&str,account:&str,symbol:&str,interval:&str,
        start_ms:i64,end_ms:i64,
        spot_qty:Decimal,perp_qty:Decimal,starting_balance:Decimal,
        max_gross_notional:Decimal,fee_rate:Decimal,
    )->Result<Value,RuntimeError>{
        let t=self.require_active(tenant)?;
        if t.purpose!="development" {return Err(error("只有开发租户可以回放真实历史证据。"));}
        seealpha_market::check_symbol(symbol).map_err(|_|error("回测合约无效。"))?;
        let step=interval_ms(interval).map_err(|_|error("回测 K 线周期无效。"))?;
        if start_ms<0||end_ms<=start_ms || start_ms%step!=0 || end_ms%step!=0 ||
            (end_ms-start_ms)/step>5000 {
            return Err(error("跨市场历史回测必须使用 5000 根以内对齐的闭合 K 线。"));
        }
        let expected=((end_ms-start_ms)/step) as usize;
        let spot=self.ledger.catalog_bars(tenant,"spot",symbol,interval,start_ms,end_ms,5000)
            .map_err(map_ledger)?;
        let perp=self.ledger.catalog_bars(tenant,"futures_um",symbol,interval,start_ms,end_ms,5000)
            .map_err(map_ledger)?;
        if spot.len()!=expected||perp.len()!=expected {
            return Err(error("跨市场历史 K 线缺失，不允许静默填充。"));
        }
        let mut marks=Vec::with_capacity(expected);
        for (index,(s,p)) in spot.iter().zip(&perp).enumerate() {
            let at=start_ms.checked_add((index as i64)*step)
                .ok_or_else(||error("历史 K 线时间溢出。"))?;
            if s.open_time_ms!=at || p.open_time_ms!=at {
                return Err(error("现货与永续历史时间轴不一致。"));
            }
            let spot_close=s.close.parse::<Decimal>().map_err(|_|error("现货历史价格无效。"))?;
            let perp_close=p.close.parse::<Decimal>().map_err(|_|error("永续历史价格无效。"))?;
            marks.push(MarkBar{
                mark_time_ms:at.checked_add(step-1).ok_or_else(||error("历史时间溢出。"))?,
                spot:spot_close,perp:perp_close,
            });
        }
        let funding_start=marks.first().map(|b|b.mark_time_ms.saturating_add(1))
            .ok_or_else(||error("历史 K 线为空。"))?;
        let events=self.ledger.funding_events(tenant,symbol,funding_start,end_ms,5000)
            .map_err(map_ledger)?;
        let mut settlement=Vec::with_capacity(events.len());
        for row in &events {
            let time=row["funding_time_ms"].as_i64().ok_or_else(||error("结算时间无效。"))?;
            let rate=row["funding_rate"].as_str()
                .ok_or_else(||error("结算资金费率无效。"))?.parse::<Decimal>()
                .map_err(|_|error("结算资金费率无效。"))?;
            let mark=row["mark_price"].as_str()
                .ok_or_else(||error("资金费结算价格无效。"))?.parse::<Decimal>()
                .map_err(|_|error("资金费结算价格无效。"))?;
            settlement.push(SettledFunding{at_ms:time,rate,mark_price:mark});
        }
        let mut report=run_evidence_carry(&marks,&settlement,CarryPosition{
            spot_qty,perp_qty,starting_balance,max_gross_notional,fee_rate
        }).map_err(error)?;
        report["symbol"]=json!(symbol);
        report["interval"]=json!(interval);
        report["start_ms"]=json!(start_ms);
        report["end_ms"]=json!(end_ms);
        report["spot_data_source"]=json!("tenant_catalog_spot");
        report["perp_data_source"]=json!("tenant_catalog_futures_um");
        report["funding_data_source"]=json!("tenant_settled_funding");
        self.ledger.save_backtest(tenant,account,"carry",&report.to_string())
            .map_err(map_ledger)?;
        Ok(report)
    }
}
