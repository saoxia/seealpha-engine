//! Exchange-account state is a versioned observation, not an order ACK.
//! Decimal strings are mandatory for every monetary and position quantity.
use crate::{Ledger, LedgerError};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use rust_decimal::Decimal;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSnapshot {
    pub revision: i64,
    pub evidence_id: String,
    pub observed_at_ms: i64,
    pub source: String,
    /// {"balances":{"USDT":"123.0001"},"positions":{"futures_um:ETHUSDT":"-0.2"}}
    pub state: Value,
}

fn sql(e: rusqlite::Error) -> LedgerError {
    LedgerError::Sqlite(e.to_string())
}
fn invalid() -> LedgerError {
    LedgerError::Conflict("account snapshot is stale, untrusted or not decimal-exact".into())
}

fn validate_decimal_map(value: &Value) -> bool {
    value.as_object().is_some_and(|map| {
        map.len() <= 2048
            && map.iter().all(|(key, amount)| {
                !key.is_empty()
                    && key.len() <= 160
                    && amount
                        .as_str()
                        .is_some_and(|text| text.len() <= 128 && text.parse::<Decimal>().is_ok())
            })
    })
}
fn valid_state(value: &Value) -> bool {
    value.as_object().is_some_and(|map| {
        map.len() == 2
            && validate_decimal_map(&value["balances"])
            && validate_decimal_map(&value["positions"])
    })
}

impl Ledger {
    /// CAS an authenticated complete account snapshot. A missing/mismatched
    /// expected revision never overwrites an existing observation. No fills,
    /// settlements, or transfers are synthesized from the balances.
    #[allow(clippy::too_many_arguments)]
    pub fn save_account_snapshot(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        expected_revision: Option<i64>,
        evidence_id: &str,
        observed_at_ms: i64,
        source: &str,
        state: &Value,
    ) -> Result<AccountSnapshot, LedgerError> {
        if tenant.is_empty()
            || account.is_empty()
            || tenant.len() > 128
            || account.len() > 128
            || !matches!(environment, "production" | "sandbox" | "paper")
            || evidence_id.is_empty()
            || evidence_id.len() > 128
            || observed_at_ms <= 0
            || !valid_state(state)
            || !match environment {
                "paper" => source == "paper_simulator",
                _ => source == "signed_exchange_account_query",
            }
        {
            return Err(invalid());
        }
        let body = state.to_string();
        if body.len() > 1024 * 1024 {
            return Err(invalid());
        }
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let active: bool = tx
            .query_row(
                "SELECT status FROM tenants WHERE tenant_id=?1",
                params![tenant],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(sql)?
            .is_some_and(|s| s == "active");
        if !active {
            return Err(invalid());
        }
        let previous: Option<(i64,String,i64,String,String)> = tx.query_row(
            "SELECT revision,evidence_id,observed_at_ms,source,balances_json FROM account_snapshots
             WHERE tenant_id=?1 AND account_id=?2 AND environment=?3",
            params![tenant,account,environment],
            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)),
        ).optional().map_err(sql)?;
        // An evidence identity is immutable within the current observation.
        // The previous behavior allowed reusing the same evidence_id with a
        // later timestamp and different balances after a CAS update.
        if let Some((revision, old_id, old_time, old_source, old_state)) = &previous
            && old_id == evidence_id
        {
            if *old_time != observed_at_ms || old_source != source || old_state != &body {
                return Err(LedgerError::Conflict(
                    "account snapshot evidence ID reused with different content".into(),
                ));
            }
            return Ok(AccountSnapshot {
                revision: *revision,
                evidence_id: evidence_id.into(),
                observed_at_ms,
                source: source.into(),
                state: state.clone(),
            });
        }
        if previous.as_ref().map(|row| row.0) != expected_revision {
            return Err(LedgerError::Conflict(
                "account state CAS conflict; reload and reconcile".into(),
            ));
        }
        if previous.as_ref().is_some_and(|row| observed_at_ms <= row.2) {
            return Err(LedgerError::Conflict(
                "account snapshot must move forward in exchange time".into(),
            ));
        }
        let revision = previous
            .as_ref()
            .map_or(Some(1), |row| row.0.checked_add(1))
            .ok_or_else(invalid)?;
        if previous.is_none() {
            tx.execute(
                "INSERT INTO account_snapshots
                 (tenant_id,account_id,environment,revision,evidence_id,observed_at_ms,source,balances_json)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![tenant,account,environment,revision,evidence_id,observed_at_ms,source,body],
            ).map_err(sql)?;
        } else {
            tx.execute(
                "UPDATE account_snapshots SET revision=?4,evidence_id=?5,observed_at_ms=?6,
                 source=?7,balances_json=?8 WHERE tenant_id=?1 AND account_id=?2 AND environment=?3
                 AND revision=?9",
                params![
                    tenant,
                    account,
                    environment,
                    revision,
                    evidence_id,
                    observed_at_ms,
                    source,
                    body,
                    expected_revision
                ],
            )
            .map_err(sql)?;
        }
        tx.commit().map_err(sql)?;
        Ok(AccountSnapshot {
            revision,
            evidence_id: evidence_id.into(),
            observed_at_ms,
            source: source.into(),
            state: state.clone(),
        })
    }

    pub fn account_snapshot(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
    ) -> Result<Option<AccountSnapshot>, LedgerError> {
        let conn = self.lock()?;
        let row: Option<(i64,String,i64,String,String)> = conn.query_row(
            "SELECT revision,evidence_id,observed_at_ms,source,balances_json FROM account_snapshots
             WHERE tenant_id=?1 AND account_id=?2 AND environment=?3",
            params![tenant,account,environment],
            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)),
        ).optional().map_err(sql)?;
        row.map(|(revision, evidence_id, observed_at_ms, source, body)| {
            let state = serde_json::from_str(&body).map_err(|_| invalid())?;
            if !valid_state(&state) {
                return Err(invalid());
            }
            Ok(AccountSnapshot {
                revision,
                evidence_id,
                observed_at_ms,
                source,
                state,
            })
        })
        .transpose()
    }

    /// Obtain the persisted account observation for risk or basket recovery
    /// under one explicitly supplied revision and freshness window.
    ///
    /// This verifies only persisted provenance metadata, CAS revision and age.
    /// It does NOT authenticate a new Nautilus AccountState, query the venue,
    /// or authorize sending an order. The upstream account adapter must still
    /// independently verify the exchange source before storing the snapshot.
    pub fn account_snapshot_for_risk(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        expected_revision: i64,
        now_ns: u64,
        max_age_ns: u64,
    ) -> Result<AccountSnapshot, LedgerError> {
        if tenant.is_empty()
            || account.is_empty()
            || expected_revision <= 0
            || now_ns == 0
            || max_age_ns == 0
            || !matches!(environment, "production" | "sandbox" | "paper")
        {
            return Err(invalid());
        }
        let snapshot = self
            .account_snapshot(tenant, account, environment)?
            .ok_or_else(|| LedgerError::Conflict("account snapshot is missing".into()))?;
        let expected_source = if environment == "paper" {
            "paper_simulator"
        } else {
            "signed_exchange_account_query"
        };
        if snapshot.revision != expected_revision || snapshot.source != expected_source {
            return Err(LedgerError::Conflict(
                "account snapshot revision or source mismatch".into(),
            ));
        }
        let observed_ns = u64::try_from(snapshot.observed_at_ms)
            .ok()
            .and_then(|ms| ms.checked_mul(1_000_000))
            .ok_or_else(invalid)?;
        if observed_ns == 0 || observed_ns > now_ns || now_ns - observed_ns > max_age_ns {
            return Err(LedgerError::Conflict(
                "account snapshot is stale or timestamp is in the future".into(),
            ));
        }
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn risk_snapshot_requires_exact_revision_source_and_fresh_time() {
        let book = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        book.ensure_tenant("t", "production", "active").unwrap();
        let state = json!({"balances":{"USDT":"100.00"},"positions":{"futures_um:ETHUSDT":"-0.5"}});
        let original = book.save_account_snapshot(
            "t", "a", "production", None, "signed1", 1_000,
            "signed_exchange_account_query", &state,
        ).unwrap();
        assert_eq!(original.revision, 1);
        // Exact boundary is allowed, but neither a future observation nor
        // an observation older than the explicitly supplied window is.
        assert_eq!(book.account_snapshot_for_risk(
            "t", "a", "production", 1, 1_300_000_000, 300_000_000,
        ).unwrap(), original);
        for (revision, now_ns, max_age_ns) in [
            (2, 1_300_000_000, 300_000_000),
            (1, 1_300_000_001, 300_000_000),
            (1, 999_999_999, 300_000_000),
            (1, 0, 300_000_000),
            (1, 1_300_000_000, 0),
        ] {
            assert!(book.account_snapshot_for_risk(
                "t", "a", "production", revision, now_ns, max_age_ns,
            ).is_err());
        }
        assert!(book.account_snapshot_for_risk(
            "t", "a", "sandbox", 1, 1_300_000_000, 300_000_000,
        ).is_err());
        assert!(book.account_snapshot_for_risk(
            "other", "a", "production", 1, 1_300_000_000, 300_000_000,
        ).is_err());
        assert!(book.account_snapshot_for_risk(
            "t", "other", "production", 1, 1_300_000_000, 300_000_000,
        ).is_err());
        let same = book.save_account_snapshot(
            "t", "a", "production", None, "signed1", 1_000,
            "signed_exchange_account_query", &state,
        ).unwrap();
        assert_eq!(same.revision, 1);
        // Current evidence ID is immutable even when the CAS revision matches
        // and the proposed observation timestamp increases.
        assert!(book.save_account_snapshot(
            "t", "a", "production", Some(1), "signed1", 1_001,
            "signed_exchange_account_query", &state,
        ).is_err());
        let changed = json!({"balances":{"USDT":"99.99"},"positions":{}});
        assert!(book.save_account_snapshot(
            "t", "a", "production", Some(1), "signed1", 1_001,
            "signed_exchange_account_query", &changed,
        ).is_err());
        assert_eq!(book.account_snapshot("t", "a", "production").unwrap(), Some(original));
    }

    #[test]
    fn paper_snapshot_gate_does_not_promote_simulation_to_exchange_evidence() {
        let book = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        book.ensure_tenant("research", "development", "active").unwrap();
        let simulated = book.save_account_snapshot(
            "research", "paper1", "paper", None, "sim1", 10,
            "paper_simulator", &json!({"balances":{"USDT":"0"},"positions":{}}),
        ).unwrap();
        assert_eq!(book.account_snapshot_for_risk(
            "research", "paper1", "paper", 1, 10_000_000, 1,
        ).unwrap(), simulated);
        assert!(book.account_snapshot_for_risk(
            "research", "paper1", "production", 1, 10_000_000, 1,
        ).is_err());
    }

    #[test]
    fn risk_snapshot_gate_reloads_the_same_state_after_sqlite_restart() {
        use std::time::{SystemTime, UNIX_EPOCH};
        let path = std::env::temp_dir().join(format!(
            "seealpha-account-gate-{}-{}.sqlite",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        {
            let book = Ledger::open(&path).unwrap();
            book.ensure_tenant("tenant1", "production", "active").unwrap();
            book.save_account_snapshot(
                "tenant1", "account1", "sandbox", None, "venue-10", 20,
                "signed_exchange_account_query",
                &json!({"balances":{"USDT":"23.0001"},"positions":{}}),
            ).unwrap();
        }
        {
            let recovered = Ledger::open(&path).unwrap();
            let checked = recovered.account_snapshot_for_risk(
                "tenant1", "account1", "sandbox", 1, 21_000_000, 1_000_000,
            ).unwrap();
            assert_eq!(checked.evidence_id, "venue-10");
            assert_eq!(checked.state["balances"]["USDT"].as_str(), Some("23.0001"));
            assert!(recovered.account_snapshot_for_risk(
                "tenant1", "account1", "sandbox", 1, 21_000_001, 1_000_000,
            ).is_err());
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn decimal_cas_and_stale_account_evidence_fail_closed() {
        let book = Arc::new(Ledger::open(std::path::Path::new(":memory:")).unwrap());
        book.ensure_tenant("t", "production", "active").unwrap();
        let state = json!({"balances":{"USDT":"100.00000000000000000001"},
            "positions":{"futures_um:ETHUSDT":"-0.00000001"}});
        let first = book
            .save_account_snapshot(
                "t",
                "a",
                "production",
                None,
                "e1",
                1,
                "signed_exchange_account_query",
                &state,
            )
            .unwrap();
        assert_eq!(first.revision, 1);
        let barrier = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|i| {
                let l = book.clone();
                let b = barrier.clone();
                let next = json!({"balances":{"USDT":format!("100.{i}")},"positions":{}});
                thread::spawn(move || {
                    b.wait();
                    l.save_account_snapshot(
                        "t",
                        "a",
                        "production",
                        Some(1),
                        &format!("e{i}"),
                        2,
                        "signed_exchange_account_query",
                        &next,
                    )
                })
            })
            .collect();
        let success = handles
            .into_iter()
            .map(|t| t.join().unwrap())
            .filter(|result| result.is_ok())
            .count();
        assert_eq!(success, 1);
        let last = book
            .account_snapshot("t", "a", "production")
            .unwrap()
            .unwrap();
        assert_eq!(last.revision, 2);
        assert!(
            book.save_account_snapshot(
                "t",
                "a",
                "production",
                Some(2),
                "e3",
                1,
                "signed_exchange_account_query",
                &state
            )
            .is_err()
        );
        assert!(
            book.save_account_snapshot(
                "t",
                "a",
                "production",
                Some(2),
                "e3",
                3,
                "signed_exchange_account_query",
                &json!({"balances":{"USDT":100.1},"positions":{}})
            )
            .is_err()
        );
        assert!(
            book.account_snapshot("other", "a", "production")
                .unwrap()
                .is_none()
        );
        assert!(
            book.account_snapshot("t", "a", "sandbox")
                .unwrap()
                .is_none()
        );
    }
}
