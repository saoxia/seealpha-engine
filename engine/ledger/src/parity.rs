//! Trading account compatibility reads. Non-financial Pipeline state lives in ControlStore.
use crate::{Ledger, LedgerError};
use seealpha_database::params;
use serde_json::{Value, json};

fn sqlite(err: seealpha_database::Error) -> LedgerError {
    LedgerError::Database(err.to_string())
}

fn parse(text: &str) -> Result<Value, LedgerError> {
    serde_json::from_str(text).map_err(|_| LedgerError::Conflict("存储的 JSON 记录损坏。".into()))
}

impl Ledger {
    pub fn positions(
        &self,
        tenant_id: &str,
        account_id: &str,
        environment: &str,
    ) -> Result<Vec<Value>, LedgerError> {
        let conn = self.lock()?;
        let mut query = conn
            .prepare(
                "SELECT position_json FROM positions WHERE tenant_id=$1 AND account_id=$2
             AND environment=$3 ORDER BY position_id",
            )
            .map_err(sqlite)?;
        let rows = query
            .query_map(params![tenant_id, account_id, environment], |row| {
                row.get::<_, String>(0)
            })
            .map_err(sqlite)?;
        rows.map(|row| parse(&row.map_err(sqlite)?)).collect()
    }

    pub fn request(
        &self,
        tenant_id: &str,
        account_id: &str,
        request_id: &str,
    ) -> Result<Option<Value>, LedgerError> {
        self.get_order_request(tenant_id, account_id, request_id)?
            .map(|value| parse(&value))
            .transpose()
    }

    pub fn gaps_for_scope(
        &self,
        tenant_id: &str,
        account_id: &str,
        scope: Option<&str>,
    ) -> Result<Vec<Value>, LedgerError> {
        let conn = self.lock()?;
        let mut query = conn
            .prepare(
                "SELECT gap_json FROM gaps WHERE tenant_id=$1 AND account_id=$2
             AND ($3 IS NULL OR scope=$3) ORDER BY scope,gap_key",
            )
            .map_err(sqlite)?;
        let rows = query
            .query_map(params![tenant_id, account_id, scope], |r| {
                r.get::<_, String>(0)
            })
            .map_err(sqlite)?;
        rows.map(|r| parse(&r.map_err(sqlite)?)).collect()
    }

    pub fn event_page(
        &self,
        tenant_id: &str,
        account_id: &str,
        after: i64,
        limit: i64,
    ) -> Result<Value, LedgerError> {
        let conn = self.lock()?;
        let mut query = conn
            .prepare(
                "SELECT id,kind,body_json,created_at FROM events
             WHERE tenant_id=$1 AND account_id=$2 AND id>$3 ORDER BY id LIMIT $4",
            )
            .map_err(sqlite)?;
        let rows = query
            .query_map(
                params![tenant_id, account_id, after, limit.clamp(1, 1000)],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                },
            )
            .map_err(sqlite)?;
        let mut items = Vec::new();
        let mut next = after;
        for row in rows {
            let (id, kind, body, time) = row.map_err(sqlite)?;
            let body = parse(&body)?;
            items.push(json!({"id":id,"kind":kind,"body":body,"created_at":time}));
            next = id;
        }
        Ok(json!({"events":items,"next":next}))
    }

}
