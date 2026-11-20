//! Tenant/account/environment-isolated evidence events. These are not
//! balance postings: order ACKs and public funding observations never mint cash.
use crate::{Ledger,LedgerError,now_ms};
use rusqlite::{params,OptionalExtension,TransactionBehavior};
use serde_json::{Value,json};
use rust_decimal::Decimal;

fn sql(e:rusqlite::Error)->LedgerError{LedgerError::Sqlite(e.to_string())}
fn invalid()->LedgerError{LedgerError::Conflict("account event lacks immutable verified evidence".into())}

impl Ledger {
    /// Materialize a paginatable account event only from an existing signed
    /// exchange fill/settlement. Caller cannot choose amount or mark success.
    pub fn record_account_evidence_event(
        &self,tenant:&str,account:&str,environment:&str,
        kind:&str,evidence_id:&str,
    )->Result<i64,LedgerError>{
        if tenant.is_empty()||account.is_empty()||evidence_id.is_empty()
            || !matches!(environment,"production"|"sandbox")
            || !matches!(kind,"VERIFIED_FILL"|"FUNDING_FEE"|"OPTION_EXERCISE") {
            return Err(invalid());
        }
        let mut conn=self.lock()?;
        let tx=conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(sql)?;
        let raw:Option<String>=match kind {
            "VERIFIED_FILL"=>tx.query_row(
                "SELECT fill_json FROM fills WHERE tenant_id=?1 AND account_id=?2
                 AND environment=?3 AND fill_id=?4",
                params![tenant,account,environment,evidence_id],|r|r.get(0),
            ).optional().map_err(sql)?,
            "FUNDING_FEE"|"OPTION_EXERCISE"=>{
                let (product,event_kind)=if kind=="FUNDING_FEE" {
                    ("futures_um","FUNDING_FEE")
                } else {("options","EXERCISE")};
                tx.query_row(
                    "SELECT evidence_json FROM account_settlements
                     WHERE tenant_id=?1 AND account_id=?2 AND environment=?3
                     AND product=?4 AND event_kind=?5 AND event_id=?6",
                    params![tenant,account,environment,product,event_kind,evidence_id],
                    |r|r.get(0),
                ).optional().map_err(sql)?
            },
            _=>return Err(invalid()),
        };
        let raw=raw.ok_or_else(invalid)?;
        let parsed:Value=serde_json::from_str(&raw).map_err(|_|invalid())?;
        let verified=match kind {
            "VERIFIED_FILL"=>
                parsed["source"]=="signed_exchange_user_trades"
                && parsed["trade_id"].as_str()==Some(evidence_id)
                && parsed["quantity"].as_str().and_then(|s|s.parse::<Decimal>().ok())
                    .is_some_and(|n|n>Decimal::ZERO)
                && parsed["price"].as_str().and_then(|s|s.parse::<Decimal>().ok())
                    .is_some_and(|n|n>Decimal::ZERO),
            "FUNDING_FEE"=>
                parsed["source"]=="binance_signed_income"
                && parsed["income_type"]=="FUNDING_FEE"
                && parsed["transaction_id"].as_str()==Some(evidence_id)
                && parsed["income"].as_str().and_then(|s|s.parse::<Decimal>().ok()).is_some(),
            "OPTION_EXERCISE"=>
                parsed["source"]=="binance_signed_exercise_record"
                && parsed["account_exercise_confirmed"]==true
                && parsed["exercise_id"].as_str()==Some(evidence_id),
            _=>false,
        };
        if !verified{return Err(invalid());}
        let event=json!({"kind":kind,"environment":environment,"evidence_id":evidence_id,
            "evidence":parsed,"posting":"evidence_only"}).to_string();
        let prior:Option<(i64,String)>=tx.query_row(
            "SELECT id,evidence_json FROM account_evidence_events WHERE
             tenant_id=?1 AND account_id=?2 AND environment=?3 AND kind=?4 AND evidence_id=?5",
            params![tenant,account,environment,kind,evidence_id],
            |r|Ok((r.get(0)?,r.get(1)?)),
        ).optional().map_err(sql)?;
        if let Some((id,old))=prior {
            if old!=event { return Err(invalid()); }
            return Ok(id);
        }
        tx.execute(
            "INSERT INTO account_evidence_events
             (tenant_id,account_id,environment,kind,evidence_id,evidence_json,recorded_at_ms)
             VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![tenant,account,environment,kind,evidence_id,event,now_ms()],
        ).map_err(sql)?;
        let id=tx.last_insert_rowid();
        tx.commit().map_err(sql)?;
        Ok(id)
    }

    pub fn account_evidence_event_page(
        &self,tenant:&str,account:&str,environment:&str,after:i64,limit:i64,
    )->Result<Value,LedgerError>{
        if !matches!(environment,"production"|"sandbox")||after<0{
            return Err(invalid());
        }
        let conn=self.lock()?;
        let mut stmt=conn.prepare(
            "SELECT id,evidence_json,recorded_at_ms FROM account_evidence_events
             WHERE tenant_id=?1 AND account_id=?2 AND environment=?3 AND id>?4
             ORDER BY id LIMIT ?5",
        ).map_err(sql)?;
        let rows=stmt.query_map(params![tenant,account,environment,after,limit.clamp(1,1000)],|r|
            Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?))
        ).map_err(sql)?;
        let mut page=Vec::new();
        let mut next=after;
        for row in rows {
            let (id,body,recorded_at_ms)=row.map_err(sql)?;
            page.push(json!({"id":id,
                "body":serde_json::from_str::<Value>(&body).map_err(|_|invalid())?,
                "recorded_at_ms":recorded_at_ms}));
            next=id;
        }
        Ok(json!({"events":page,"next":next}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn signed_funding_event_is_idempotent_and_environment_scoped() {
        let book=Ledger::open(std::path::Path::new(":memory:")).unwrap();
        let signed=json!({"product":"futures_um","symbol":"ETHUSDT",
            "income_type":"FUNDING_FEE","income":"-0.00000001","asset":"USDT",
            "time_ms":10,"transaction_id":"abc","source":"binance_signed_income"});
        assert!(book.record_account_evidence_event(
            "t","a","production","FUNDING_FEE","abc").is_err());
        book.import_account_settlements("t","a","production","futures_um",&[signed]).unwrap();
        let id=book.record_account_evidence_event(
            "t","a","production","FUNDING_FEE","abc").unwrap();
        assert_eq!(id,book.record_account_evidence_event(
            "t","a","production","FUNDING_FEE","abc").unwrap());
        assert_eq!(book.account_evidence_event_page(
            "t","a","production",0,100).unwrap()["events"].as_array().unwrap().len(),1);
        assert!(book.account_evidence_event_page(
            "t","a","sandbox",0,100).unwrap()["events"].as_array().unwrap().is_empty());
        assert!(book.account_evidence_event_page(
            "other","a","production",0,100).unwrap()["events"].as_array().unwrap().is_empty());
        assert!(book.record_account_evidence_event(
            "t","a","production","VERIFIED_FILL","abc").is_err());
    }
}
