//! Durable GET-only reconciliation work scheduling.
//! This queue is NOT an exchange sender, an ACK verifier or a fill source.
//! A crashed worker lease may expire; only signed read queries can be retried.
use super::StateStore;
use seealpha_ledger::LedgerError;
use seealpha_database::{OptionalExtension, TransactionBehavior, params};

fn sql(err: seealpha_database::Error) -> LedgerError {
    LedgerError::Database(err.to_string())
}
fn conflict(msg: &str) -> LedgerError {
    LedgerError::Conflict(msg.into())
}
fn identifier(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationTask {
    pub tenant_id: String,
    pub account_id: String,
    pub environment: String,
    pub request_id: String,
    pub client_order_id: String,
    pub state: String,
    pub attempts: i64,
    pub next_due_ms: i64,
    pub lease_owner: Option<String>,
    pub lease_until_ms: Option<i64>,
    pub last_error_code: Option<String>,
}

impl StateStore {
    /// Synchronize the queue from immutable order intents. Repeated scans
    /// preserve current leases, retries and manual-review decisions.
    pub fn enqueue_order_reconciliations(
        &self,
        tenant: &str,
        now_ms: i64,
    ) -> Result<usize, LedgerError> {
        if !identifier(tenant) || now_ms <= 0 {
            return Err(conflict("invalid reconciliation tenant or clock"));
        }
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let inserted = tx
            .execute(
                "INSERT INTO order_reconciliation_tasks(
                tenant_id,account_id,environment,request_id,client_order_id,
                state,attempts,next_due_ms,updated_at_ms)
             SELECT tenant_id,account_id,environment,request_id,client_order_id,
                    'pending',0,$2,$2
             FROM order_intents WHERE tenant_id=$1 ON CONFLICT DO NOTHING",
                params![tenant, now_ms],
            )
            .map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(inserted)
    }

    /// Claim exactly one task within an IMMEDIATE transaction. Expired leases
    /// are recoverable ONLY for read-side reconciliation. Lease ownership is
    /// verified before any completion/next-attempt update.
    pub fn claim_order_reconciliation(
        &self,
        tenant: &str,
        worker: &str,
        now_ms: i64,
        lease_ms: i64,
    ) -> Result<Option<ReconciliationTask>, LedgerError> {
        if !identifier(tenant)
            || !identifier(worker)
            || now_ms <= 0
            || !(1_000..=300_000).contains(&lease_ms)
        {
            return Err(conflict("invalid reconciliation claim"));
        }
        let deadline = now_ms
            .checked_add(lease_ms)
            .ok_or_else(|| conflict("lease clock overflow"))?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let found: Option<(String, String)> = tx
            .query_row(
                "SELECT account_id,request_id FROM order_reconciliation_tasks
             WHERE tenant_id=$1 AND (
                (state IN ('pending','retry') AND next_due_ms<=$2)
                 OR (state='leased' AND lease_until_ms<=$2)
             )
             ORDER BY next_due_ms,account_id,request_id LIMIT 1",
                params![tenant, now_ms],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql)?;
        let Some((account, request)) = found else {
            return Ok(None);
        };
        tx.execute(
            "UPDATE order_reconciliation_tasks SET state='leased',attempts=attempts+1,
                lease_owner=$4,lease_until_ms=$5,updated_at_ms=$3
             WHERE tenant_id=$1 AND account_id=$2 AND request_id=$6",
            params![tenant, account, now_ms, worker, deadline, request],
        )
        .map_err(sql)?;
        let task = tx
            .query_row(
                "SELECT tenant_id,account_id,environment,request_id,client_order_id,
                    state,attempts,next_due_ms,lease_owner,lease_until_ms,last_error_code
             FROM order_reconciliation_tasks
             WHERE tenant_id=$1 AND account_id=$2 AND request_id=$3",
                params![tenant, account, request],
                |row| {
                    Ok(ReconciliationTask {
                        tenant_id: row.get(0)?,
                        account_id: row.get(1)?,
                        environment: row.get(2)?,
                        request_id: row.get(3)?,
                        client_order_id: row.get(4)?,
                        state: row.get(5)?,
                        attempts: row.get(6)?,
                        next_due_ms: row.get(7)?,
                        lease_owner: row.get(8)?,
                        lease_until_ms: row.get(9)?,
                        last_error_code: row.get(10)?,
                    })
                },
            )
            .map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(Some(task))
    }

    /// Failure is never interpreted as an unexecuted order. Keep all retries
    /// for signed GETs only; exhausted attempts require human reconciliation.
    /// Success of the GET alone remains pending for account/fill verification.
    #[allow(clippy::too_many_arguments)]
    pub fn finish_order_reconciliation(
        &self,
        tenant: &str,
        account: &str,
        request: &str,
        worker: &str,
        expected_attempts: i64,
        now_ms: i64,
        outcome: &str,
        error_code: Option<&str>,
    ) -> Result<(), LedgerError> {
        if [tenant, account, request, worker]
            .iter()
            .any(|id| !identifier(id))
            || expected_attempts <= 0
            || now_ms <= 0
            || !matches!(outcome, "retry" | "manual" | "evidence_recorded")
            || error_code.is_some_and(|code| !identifier(code) || code.len() > 64)
        {
            return Err(conflict("invalid reconciliation completion"));
        }
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let previous: Option<(String, Option<String>, Option<i64>, i64)> = tx
            .query_row(
                "SELECT state,lease_owner,lease_until_ms,attempts
             FROM order_reconciliation_tasks
             WHERE tenant_id=$1 AND account_id=$2 AND request_id=$3",
                params![tenant, account, request],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(sql)?;
        let Some((state, owner, until, attempts)) = previous else {
            return Err(conflict("reconciliation task is missing"));
        };
        if state != "leased"
            || owner.as_deref() != Some(worker)
            || attempts != expected_attempts
            || until.is_none_or(|deadline| deadline < now_ms)
        {
            return Err(conflict(
                "reconciliation lease is stale or owned by another worker",
            ));
        }
        let next_state = if outcome == "manual" || attempts >= 8 {
            "manual"
        } else {
            "retry" // A GET observation is not settlement evidence.
        };
        let exponent = attempts.clamp(1, 8) as u32;
        let delay = 10_000_i64.saturating_mul(1_i64 << exponent).min(900_000);
        let due = now_ms
            .checked_add(delay)
            .ok_or_else(|| conflict("retry clock overflow"))?;
        tx.execute(
            "UPDATE order_reconciliation_tasks SET state=$4,
             next_due_ms=$5,lease_owner=NULL,lease_until_ms=NULL,
             last_error_code=$6,updated_at_ms=$7
             WHERE tenant_id=$1 AND account_id=$2 AND request_id=$3",
            params![
                tenant, account, request, next_state, due, error_code, now_ms
            ],
        )
        .map_err(sql)?;
        tx.commit().map_err(sql)
    }

    #[cfg(test)]
    pub fn order_reconciliation_task(
        &self,
        tenant: &str,
        account: &str,
        request: &str,
    ) -> Result<Option<ReconciliationTask>, LedgerError> {
        if [tenant, account, request].iter().any(|id| !identifier(id)) {
            return Err(conflict("invalid reconciliation task scope"));
        }
        let conn = self.lock()?;
        conn.query_row(
            "SELECT tenant_id,account_id,environment,request_id,client_order_id,
                    state,attempts,next_due_ms,lease_owner,lease_until_ms,last_error_code
             FROM order_reconciliation_tasks
             WHERE tenant_id=$1 AND account_id=$2 AND request_id=$3",
            params![tenant, account, request],
            |row| {
                Ok(ReconciliationTask {
                    tenant_id: row.get(0)?,
                    account_id: row.get(1)?,
                    environment: row.get(2)?,
                    request_id: row.get(3)?,
                    client_order_id: row.get(4)?,
                    state: row.get(5)?,
                    attempts: row.get(6)?,
                    next_due_ms: row.get(7)?,
                    lease_owner: row.get(8)?,
                    lease_until_ms: row.get(9)?,
                    last_error_code: row.get(10)?,
                })
            },
        )
        .optional()
        .map_err(sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{Arc, Barrier};
    use std::thread;
    fn seed(db: &StateStore) {
        db.ensure_tenant("t", "production", "active").unwrap();
        let intent = json!({"action":"NEW","environment":"production",
            "client_order_id":"client","symbol":"ETHUSDT",
            "side":"BUY","quantity":"0.1"});
        db.preregister_order_intent(
            "t",
            "a",
            "production",
            "request",
            "client",
            &"a".repeat(64),
            &intent,
        )
        .unwrap();
    }
    #[test]
    fn concurrent_workers_claim_once_and_retries_do_not_create_orders() {
        let db = Arc::new(StateStore::open(std::path::Path::new(":memory:")).unwrap());
        seed(&db);
        assert_eq!(db.enqueue_order_reconciliations("t", 10).unwrap(), 1);
        assert_eq!(db.enqueue_order_reconciliations("t", 11).unwrap(), 0);
        let barrier = Arc::new(Barrier::new(8));
        let workers: Vec<_> = (0..8)
            .map(|i| {
                let db = db.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    db.claim_order_reconciliation("t", &format!("worker{i}"), 20, 1000)
                        .unwrap()
                })
            })
            .collect();
        let claimed: Vec<_> = workers
            .into_iter()
            .filter_map(|w| w.join().unwrap())
            .collect();
        assert_eq!(claimed.len(), 1);
        let first = &claimed[0];
        assert!(
            db.finish_order_reconciliation(
                "t",
                "a",
                "request",
                "other",
                first.attempts,
                25,
                "retry",
                Some("TIMEOUT")
            )
            .is_err()
        );
        db.finish_order_reconciliation(
            "t",
            "a",
            "request",
            first.lease_owner.as_deref().unwrap(),
            first.attempts,
            25,
            "retry",
            Some("TIMEOUT"),
        )
        .unwrap();
        assert!(
            db.claim_order_reconciliation("t", "worker2", 30, 1000)
                .unwrap()
                .is_none()
        );
        let next = db
            .claim_order_reconciliation("t", "worker2", 30_000, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(next.attempts, 2);
        assert_eq!(db.order_recovery_page("t", 0, 10).unwrap().len(), 1);
        assert!(db.order_recovery_page("t", 0, 10).unwrap()[0].reconciliation_only);
    }
    #[test]
    fn retry_schedule_and_manual_state_survive_sqlite_restart() {
        let path = std::env::temp_dir().join(format!(
            "seealpha-reconcile-task-{}-{:?}.sqlite",
            std::process::id(),
            std::thread::current().id(),
        ));
        let _ = std::fs::remove_file(&path);
        {
            let db = StateStore::open(&path).unwrap();
            seed(&db);
            db.enqueue_order_reconciliations("t", 100).unwrap();
            let claimed = db
                .claim_order_reconciliation("t", "worker1", 100, 1000)
                .unwrap()
                .unwrap();
            assert_eq!(claimed.attempts, 1);
            db.finish_order_reconciliation(
                "t",
                "a",
                "request",
                "worker1",
                claimed.attempts,
                200,
                "retry",
                Some("GET_TIMEOUT"),
            )
            .unwrap();
        }
        {
            let db = StateStore::open(&path).unwrap();
            let persisted = db
                .order_reconciliation_task("t", "a", "request")
                .unwrap()
                .unwrap();
            assert_eq!(persisted.state, "retry");
            assert_eq!(persisted.last_error_code.as_deref(), Some("GET_TIMEOUT"));
            assert!(
                db.claim_order_reconciliation("t", "worker2", 201, 1000)
                    .unwrap()
                    .is_none()
            );
            let next = db
                .claim_order_reconciliation("t", "worker2", 20_201, 1000)
                .unwrap()
                .unwrap();
            assert_eq!(next.attempts, 2);
            db.finish_order_reconciliation(
                "t",
                "a",
                "request",
                "worker2",
                next.attempts,
                20_202,
                "manual",
                Some("MISMATCH"),
            )
            .unwrap();
        }
        {
            let db = StateStore::open(&path).unwrap();
            assert_eq!(db.enqueue_order_reconciliations("t", 30_000).unwrap(), 0);
            assert!(
                db.claim_order_reconciliation("t", "worker3", 100_000, 1000)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                db.order_reconciliation_task("t", "a", "request")
                    .unwrap()
                    .unwrap()
                    .state,
                "manual"
            );
            assert_eq!(db.order_recovery_page("t", 0, 10).unwrap().len(), 1);
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn lease_expiry_is_read_recovery_only_and_manual_stops_claims() {
        let db = StateStore::open(std::path::Path::new(":memory:")).unwrap();
        seed(&db);
        db.enqueue_order_reconciliations("t", 100).unwrap();
        let first = db
            .claim_order_reconciliation("t", "worker1", 100, 1000)
            .unwrap()
            .unwrap();
        assert!(
            db.claim_order_reconciliation("t", "worker2", 500, 1000)
                .unwrap()
                .is_none()
        );
        let second = db
            .claim_order_reconciliation("t", "worker2", 1101, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(second.attempts, first.attempts + 1);
        // Even when the worker identity is reused, a stale attempt cannot
        // modify a newer lease owned by that same logical worker.
        assert!(
            db.finish_order_reconciliation(
                "t",
                "a",
                "request",
                "worker2",
                first.attempts,
                1102,
                "retry",
                None
            )
            .is_err()
        );
        assert!(
            db.finish_order_reconciliation(
                "t",
                "a",
                "request",
                "worker1",
                first.attempts,
                1102,
                "retry",
                None
            )
            .is_err()
        );
        db.finish_order_reconciliation(
            "t",
            "a",
            "request",
            "worker2",
            second.attempts,
            1102,
            "manual",
            Some("EVIDENCE_CONFLICT"),
        )
        .unwrap();
        assert!(
            db.claim_order_reconciliation("t", "worker3", 999999, 1000)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            db.order_reconciliation_task("t", "a", "request")
                .unwrap()
                .unwrap()
                .state,
            "manual"
        );
    }
}
