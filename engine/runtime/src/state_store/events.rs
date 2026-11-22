//! Application-owned gap and operational event projections, not economic evidence.
use super::{StateStore, now_ms};
use seealpha_ledger::LedgerError;
use seealpha_database::params;
use serde_json::{Value, json};
fn db_error(err: seealpha_database::Error) -> LedgerError { LedgerError::Database(err.to_string()) }
fn parse(text: &str) -> Result<Value, LedgerError> { serde_json::from_str(text).map_err(|_| LedgerError::Conflict("存储的 JSON 记录损坏。".into())) }
impl StateStore {
    pub fn put_gap(
        &self,
        tenant_id: &str,
        account_id: &str,
        scope: &str,
        gap_key: &str,
        gap_json: &str,
    ) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO gaps (tenant_id, account_id, scope, gap_key, gap_json, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT(tenant_id, account_id, scope, gap_key) DO UPDATE SET
                   gap_json=excluded.gap_json, updated_at=excluded.updated_at",
                params![tenant_id, account_id, scope, gap_key, gap_json, now_ms()],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        Ok(())
    }

    pub fn add_event(
        &self,
        tenant_id: &str,
        account_id: &str,
        kind: &str,
        body: &str,
    ) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO events (tenant_id, account_id, kind, body_json, created_at) VALUES ($1, $2, $3, $4, $5)",
                params![tenant_id, account_id, kind, body, now_ms()],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        Ok(())
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
            .map_err(db_error)?;
        let rows = query
            .query_map(params![tenant_id, account_id, scope], |r| {
                r.get::<_, String>(0)
            })
            .map_err(db_error)?;
        rows.map(|r| parse(&r.map_err(db_error)?)).collect()
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
            .map_err(db_error)?;
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
            .map_err(db_error)?;
        let mut items = Vec::new();
        let mut next = after;
        for row in rows {
            let (id, kind, body, time) = row.map_err(db_error)?;
            let body = parse(&body)?;
            items.push(json!({"id":id,"kind":kind,"body":body,"created_at":time}));
            next = id;
        }
        Ok(json!({"events":items,"next":next}))
    }
}
