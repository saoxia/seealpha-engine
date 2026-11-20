//! Append-only public exercise reference evidence. Never interpreted as
//! customer exercise confirmations or automatically applied as cash fills.
use crate::{Ledger,LedgerError};
use rusqlite::{params,OptionalExtension,TransactionBehavior};
use serde_json::Value;

fn sql(e:rusqlite::Error)->LedgerError{LedgerError::Sqlite(e.to_string())}
fn incompatible()->LedgerError{
    LedgerError::Conflict("期权公开行权参考记录与历史证据冲突。".into())
}
impl Ledger{
    pub fn import_public_exercises(
        &self,tenant:&str,rows:&[Value],
    )->Result<usize,LedgerError>{
        if rows.len()>=100{return Err(incompatible());}
        let mut conn=self.lock()?;
        let tx=conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(sql)?;
        for row in rows{
            let symbol=row["symbol"].as_str().ok_or_else(incompatible)?;
            let time=row["expiry_time_ms"].as_i64().ok_or_else(incompatible)?;
            if time<=0||row["source"]!="binance_public_exercise_history"||
                row["account_exercise_confirmed"]!=false||
                !matches!(row["outcome"].as_str(),
                    Some("REALISTIC_VALUE_STRICKEN"|"EXTRINSIC_VALUE_EXPIRED")){
                return Err(incompatible());
            }
            let current=row.to_string();
            let prev:Option<String>=tx.query_row(
                "SELECT evidence_json FROM option_exercise_observations
                 WHERE tenant_id=?1 AND symbol=?2 AND expiry_time_ms=?3",
                params![tenant,symbol,time],|r|r.get(0),
            ).optional().map_err(sql)?;
            if let Some(existing)=prev {
                if existing!=current{return Err(incompatible());}
            }else{
                tx.execute(
                    "INSERT INTO option_exercise_observations
                     (tenant_id,symbol,expiry_time_ms,evidence_json)
                     VALUES(?1,?2,?3,?4)",
                    params![tenant,symbol,time,current],
                ).map_err(sql)?;
            }
        }
        tx.commit().map_err(sql)?;
        Ok(rows.len())
    }
    pub fn public_exercises(
        &self,tenant:&str,symbol:Option<&str>,from:i64,to:i64,
    )->Result<Vec<Value>,LedgerError>{
        if from<0||to<=from||to-from>365*86_400_000 {
            return Err(incompatible());
        }
        let conn=self.lock()?;
        let mut stmt=conn.prepare(
            "SELECT evidence_json FROM option_exercise_observations
             WHERE tenant_id=?1 AND (?2 IS NULL OR symbol=?2)
                AND expiry_time_ms>=?3 AND expiry_time_ms<?4
             ORDER BY expiry_time_ms,symbol LIMIT 1000",
        ).map_err(sql)?;
        let rows=stmt.query_map(params![tenant,symbol,from,to],|r|r.get::<_,String>(0))
            .map_err(sql)?;
        rows.map(|item|{
            serde_json::from_str(&item.map_err(sql)?).map_err(|_|incompatible())
        }).collect()
    }
}
#[cfg(test)]
mod tests{
    use super::*;
    use serde_json::json;
    #[test]
    fn public_exercise_evidence_never_claims_account_settlement(){
        let p=std::env::temp_dir().join(format!("sa-option-exercise-{}.db",std::process::id()));
        let _=std::fs::remove_file(&p);
        let ledger=Ledger::open(&p).unwrap();
        let row=json!({"symbol":"BTC-271231-90000-P","expiry_time_ms":1820000000000i64,
            "source":"binance_public_exercise_history",
            "account_exercise_confirmed":false,"outcome":"REALISTIC_VALUE_STRICKEN",
            "strike":"90000","settlement_price":"81000"});
        ledger.import_public_exercises("t",&[row.clone()]).unwrap();
        ledger.import_public_exercises("t",&[row]).unwrap();
        assert_eq!(ledger.public_exercises("t",None,1810000000000,1830000000000).unwrap().len(),1);
        assert!(ledger.public_exercises("other",None,1810000000000,1830000000000).unwrap().is_empty());
        let _=std::fs::remove_file(p);
    }
}
