//! Transactional compare-and-swap for portfolio coordinator recovery records.
use crate::{Ledger, LedgerError};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCoordinatorState {
    pub revision: String,
    pub json: String,
}

impl Ledger {
    pub fn load_coordinator_state(
        &self,
        tenant: &str,
        account: &str,
        instrument: &str,
    ) -> Result<Option<StoredCoordinatorState>, LedgerError> {
        let conn = self
            .connection
            .lock()
            .map_err(|_| LedgerError::Sqlite("poisoned ledger".into()))?;
        conn.query_row(
            "SELECT revision,body_json FROM portfolio_coordinator_states
                        WHERE tenant_id=?1 AND account_id=?2 AND instrument=?3",
            params![tenant, account, instrument],
            |row| {
                Ok(StoredCoordinatorState {
                    revision: row.get(0)?,
                    json: row.get(1)?,
                })
            },
        )
        .optional()
        .map_err(|e| LedgerError::Sqlite(e.to_string()))
    }
    pub fn save_coordinator_state(
        &self,
        tenant: &str,
        account: &str,
        instrument: &str,
        json: &str,
        revision: &str,
        expected: Option<&str>,
    ) -> Result<StoredCoordinatorState, LedgerError> {
        if tenant.is_empty()
            || account.is_empty()
            || instrument.is_empty()
            || revision.len() != 64
            || !revision.bytes().all(|b| b.is_ascii_hexdigit())
            || json.len() > 1024 * 1024
        {
            return Err(LedgerError::Conflict("invalid coordinator identity".into()));
        }
        let mut conn = self
            .connection
            .lock()
            .map_err(|_| LedgerError::Sqlite("poisoned ledger".into()))?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        let current: Option<StoredCoordinatorState> = tx
            .query_row(
                "SELECT revision,body_json FROM portfolio_coordinator_states
             WHERE tenant_id=?1 AND account_id=?2 AND instrument=?3",
                params![tenant, account, instrument],
                |row| {
                    Ok(StoredCoordinatorState {
                        revision: row.get(0)?,
                        json: row.get(1)?,
                    })
                },
            )
            .optional()
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        let next = StoredCoordinatorState {
            revision: revision.into(),
            json: json.into(),
        };
        if current.as_ref() == Some(&next) {
            return Ok(next);
        }
        if current.as_ref().map(|c| c.revision.as_str()) != expected {
            return Err(LedgerError::Conflict(
                "portfolio state changed; reload before action".into(),
            ));
        }
        if current.is_none() {
            tx.execute(
                "INSERT INTO portfolio_coordinator_states
                (tenant_id,account_id,instrument,revision,body_json) VALUES (?1,?2,?3,?4,?5)",
                params![tenant, account, instrument, revision, json],
            )
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        } else {
            tx.execute(
                "UPDATE portfolio_coordinator_states SET revision=?4,body_json=?5
                WHERE tenant_id=?1 AND account_id=?2 AND instrument=?3",
                params![tenant, account, instrument, revision, json],
            )
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        }
        tx.commit()
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        Ok(next)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cas_rejects_stale_update() {
        let ledger = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        ledger
            .save_coordinator_state("t", "a", "ETHUSDT", "1", &a, None)
            .unwrap();
        assert!(
            ledger
                .save_coordinator_state("t", "a", "ETHUSDT", "2", &b, None)
                .is_err()
        );
        ledger
            .save_coordinator_state("t", "a", "ETHUSDT", "2", &b, Some(&a))
            .unwrap();
        assert_eq!(
            ledger
                .load_coordinator_state("t", "a", "ETHUSDT")
                .unwrap()
                .unwrap()
                .revision,
            b
        );
    }
}
