//! SQLite compare-and-swap persistence for research basket journals.
//! The caller must validate journal state with seealpha-portfolio before writing.
use crate::{Ledger, LedgerError};
use seealpha_database::{OptionalExtension, TransactionBehavior, params};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBasketJournal {
    pub revision: String,
    pub body_json: String,
}
impl Ledger {
    pub fn load_basket_journal(
        &self,
        tenant: &str,
        account: &str,
        basket_id: &str,
    ) -> Result<Option<StoredBasketJournal>, LedgerError> {
        let conn = self
            .connection
            .lock()
            .map_err(|_| LedgerError::Database("ledger mutex poisoned".into()))?;
        conn.query_row(
            "SELECT revision,body_json FROM basket_journals
             WHERE tenant_id=$1 AND account_id=$2 AND basket_id=$3",
            params![tenant, account, basket_id],
            |row| {
                Ok(StoredBasketJournal {
                    revision: row.get(0)?,
                    body_json: row.get(1)?,
                })
            },
        )
        .optional()
        .map_err(|e| LedgerError::Database(e.to_string()))
    }
    /// Save only when the expected prior revision matches. No live order is sent.
    /// Retrying identical content is idempotent even if the expected revision is stale.
    pub fn save_basket_journal(
        &self,
        tenant: &str,
        account: &str,
        basket_id: &str,
        body_json: &str,
        revision: &str,
        expected_revision: Option<&str>,
    ) -> Result<StoredBasketJournal, LedgerError> {
        if tenant.is_empty()
            || account.is_empty()
            || basket_id.is_empty()
            || revision.len() != 64
            || !revision.bytes().all(|b| b.is_ascii_hexdigit())
            || body_json.len() > 1024 * 1024
        {
            return Err(LedgerError::Conflict(
                "invalid basket journal scope or revision".into(),
            ));
        }
        let mut conn = self
            .connection
            .lock()
            .map_err(|_| LedgerError::Database("ledger mutex poisoned".into()))?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        let old: Option<StoredBasketJournal> = tx
            .query_row(
                "SELECT revision,body_json FROM basket_journals WHERE
             tenant_id=$1 AND account_id=$2 AND basket_id=$3",
                params![tenant, account, basket_id],
                |r| {
                    Ok(StoredBasketJournal {
                        revision: r.get(0)?,
                        body_json: r.get(1)?,
                    })
                },
            )
            .optional()
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        let proposed = StoredBasketJournal {
            revision: revision.to_owned(),
            body_json: body_json.to_owned(),
        };
        if old.as_ref() == Some(&proposed) {
            return Ok(proposed);
        }
        if old.as_ref().map(|r| r.revision.as_str()) != expected_revision {
            return Err(LedgerError::Conflict(
                "basket journal revision changed; reload before advancing".into(),
            ));
        }
        if old.is_none() {
            tx.execute(
                "INSERT INTO basket_journals (tenant_id,account_id,basket_id,revision,body_json)
                 VALUES ($1,$2,$3,$4,$5)",
                params![tenant, account, basket_id, revision, body_json],
            )
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        } else {
            tx.execute(
                "UPDATE basket_journals SET revision=$4,body_json=$5
                 WHERE tenant_id=$1 AND account_id=$2 AND basket_id=$3",
                params![tenant, account, basket_id, revision, body_json],
            )
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        }
        tx.commit()
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        Ok(proposed)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stale_revision_is_rejected() {
        let db = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        db.save_basket_journal("t", "a", "basket", "{}", &a, None)
            .unwrap();
        assert!(
            db.save_basket_journal("t", "a", "basket", "{\"x\":1}", &b, None)
                .is_err()
        );
        db.save_basket_journal("t", "a", "basket", "{\"x\":1}", &b, Some(&a))
            .unwrap();
        assert_eq!(
            db.load_basket_journal("t", "a", "basket")
                .unwrap()
                .unwrap()
                .revision,
            b
        );
    }
}
