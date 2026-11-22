//! Native AccountState read-only subscription, scoped balance view and restart gate.
//!
//! Use within a single isolated Nautilus LiveNode runtime. It is NOT a signed
//! exchange-query adapter and must not write to Ledger's authoritative snapshots.
use std::{
    cell::RefCell,
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    rc::Rc,
};

use anyhow::{Context, Result, ensure};
use nautilus_common::msgbus::{self, TypedHandler};
use nautilus_model::{enums::AccountType, events::AccountState};
use serde::{Deserialize, Serialize};

use crate::native_account::inspect_native_account_report;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BalanceView {
    pub total: String,
    pub locked: String,
    pub free: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarginView {
    pub instrument_id: Option<String>,
    pub currency: String,
    pub initial: String,
    pub maintenance: String,
}

/// A Nautilus runtime view, not a trusted Ledger account snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReportedAccountView {
    pub account_id: String,
    pub event_id: String,
    pub observed_at_ns: u64,
    pub balances: BTreeMap<String, BalanceView>,
    pub margins: Vec<MarginView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RecoveryCursor {
    account_id: String,
    event_id: String,
    observed_at_ns: u64,
}

/// One writer per account. On restart the previous view is deliberately
/// unavailable until a later reported account event passes all checks.
pub struct NativeAccountSession {
    account_id: String,
    account_type: AccountType,
    checkpoint_path: PathBuf,
    last: Option<RecoveryCursor>,
    current: Option<ReportedAccountView>,
}

impl NativeAccountSession {
    pub fn open(account_id: &str, account_type: AccountType, checkpoint_path: &Path) -> Result<Self> {
        ensure!(!account_id.is_empty() && account_id.len() <= 128, "invalid account ID");
        ensure!(
            account_id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "invalid account ID"
        );
        // Neither the directory nor the account binding may be fabricated on
        // restart: the file always contains the previous native account ID.
        if let Ok(meta) = fs::symlink_metadata(checkpoint_path) {
            ensure!(meta.is_file() && !meta.file_type().is_symlink(), "account cursor must be regular file");
        }
        let last: Option<RecoveryCursor> = match fs::read(checkpoint_path) {
            Ok(bytes) => {
                let cursor: RecoveryCursor =
                    serde_json::from_slice(&bytes).context("corrupt native account cursor")?;
                ensure!(cursor.account_id == account_id, "account cursor identity mismatch");
                ensure!(cursor.observed_at_ns > 0 && !cursor.event_id.is_empty(), "invalid cursor");
                Some(cursor)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error).context("cannot load native account cursor"),
        };
        Ok(Self {
            account_id: account_id.into(),
            account_type,
            checkpoint_path: checkpoint_path.into(),
            last,
            current: None,
        })
    }

    /// Native socket disconnect / connection replacement invalidates the
    /// current in-memory view. A later full reported state must bootstrap it.
    pub fn mark_disconnected(&mut self) {
        self.current = None;
    }

    /// Only expose an already observed view while it is within the caller's
    /// absolute wall-clock freshness limit.
    pub fn fresh(&self, now_ns: u64, max_age_ns: u64) -> Option<&ReportedAccountView> {
        let view = self.current.as_ref()?;
        if now_ns == 0
            || max_age_ns == 0
            || view.observed_at_ns > now_ns
            || now_ns - view.observed_at_ns > max_age_ns
        {
            return None;
        }
        Some(view)
    }

    /// Observe an event routed through the isolated Nautilus native node.
    /// The event has not been independently authenticated to Ledger.
    pub fn accept(&mut self, event: &AccountState, now_ns: u64, max_age_ns: u64) -> Result<bool> {
        let candidate = inspect_native_account_report(
            event, &self.account_id, self.account_type, now_ns, max_age_ns,
        )?;
        if let Some(ref previous) = self.last {
            if candidate.observed_at_ns < previous.observed_at_ns {
                anyhow::bail!("out-of-order native account report");
            }
            if candidate.observed_at_ns == previous.observed_at_ns {
                ensure!(
                    candidate.event_id == previous.event_id,
                    "conflicting native account report at same timestamp"
                );
                // Replayed event is not a post-restart account reconciliation.
                return Ok(false);
            }
            ensure!(
                candidate.event_id != previous.event_id,
                "native account event ID reused at a different time"
            );
        }

        // Reuse Nautilus' own balance and margin amounts; do not infer assets
        // from order acks or changes between two account-state observations.
        let mut balances = BTreeMap::new();
        for b in &event.balances {
            ensure!(
                b.total.currency == b.currency
                    && b.locked.currency == b.currency
                    && b.free.currency == b.currency
                    && b.locked.checked_add(b.free) == Some(b.total),
                "account balance values inconsistent"
            );
            let currency = b.currency.to_string();
            ensure!(
                balances.insert(currency, BalanceView {
                    total: b.total.as_decimal().to_string(),
                    locked: b.locked.as_decimal().to_string(),
                    free: b.free.as_decimal().to_string(),
                }).is_none(),
                "duplicated currency in account report"
            );
        }
        let mut margins = Vec::with_capacity(event.margins.len());
        let mut margin_keys = std::collections::HashSet::new();
        for m in &event.margins {
            ensure!(
                m.initial.currency == m.currency && m.maintenance.currency == m.currency,
                "native account margin currency mismatch"
            );
            let instrument_id = m.instrument_id.map(|id| id.to_string());
            let currency = m.currency.to_string();
            ensure!(
                margin_keys.insert((instrument_id.clone(), currency.clone())),
                "duplicate native account margin"
            );
            margins.push(MarginView {
                instrument_id,
                currency,
                initial: m.initial.as_decimal().to_string(),
                maintenance: m.maintenance.as_decimal().to_string(),
            });
        }

        let next_cursor = RecoveryCursor {
            account_id: candidate.account_id.clone(),
            event_id: candidate.event_id.clone(),
            observed_at_ns: candidate.observed_at_ns,
        };
        persist_cursor(&self.checkpoint_path, &next_cursor)?;
        // Publishing state after durable cursor update prevents returning
        // an unrecorded view to callers. Error remains fail-closed.
        self.current = Some(ReportedAccountView {
            account_id: candidate.account_id,
            event_id: candidate.event_id,
            observed_at_ns: candidate.observed_at_ns,
            balances,
            margins,
        });
        self.last = Some(next_cursor);
        Ok(true)
    }
}

fn persist_cursor(path: &Path, cursor: &RecoveryCursor) -> Result<()> {
    let parent = path.parent().filter(|p| !p.as_os_str().is_empty())
        .context("account cursor requires parent directory")?;
    ensure!(parent.is_dir(), "account cursor parent directory is missing");
    let temp = path.with_extension("tmp");
    let bytes = serde_json::to_vec(cursor)?;
    let mut file = OpenOptions::new()
        .write(true).create_new(true).open(&temp)
        .context("cannot create account cursor temporary file")?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(temp, path).context("cannot atomically commit account cursor")?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

/// Subscribe to Nautilus' *typed* account-state message bus.
pub struct NativeAccountSubscription {
    handler: TypedHandler<AccountState>,
    topic: String,
    session: Rc<RefCell<NativeAccountSession>>,
}

impl NativeAccountSubscription {
    pub fn attach(
        session: Rc<RefCell<NativeAccountSession>>,
        now_ns: impl Fn() -> u64 + 'static,
        max_age_ns: u64,
    ) -> Self {
        let topic = format!("events.account.{}", session.borrow().account_id);
        let received = Rc::clone(&session);
        let handler = TypedHandler::from(move |event: &AccountState| {
            let now = now_ns();
            // Release the mutable borrow before accessing the accepted view.
            let result = received.borrow_mut().accept(event, now, max_age_ns);
            match result {
                Ok(true) => {
                    let current = received.borrow();
                    if let Some(view) = current.fresh(now, max_age_ns) {
                        // Read-only output of Nautilus-reported numbers, never
                        // "signed" or "verified" Ledger financial evidence.
                        if let Ok(row) = serde_json::to_string(view) {
                            println!("{row}");
                        }
                    }
                }
                Ok(false) => log::debug!("duplicate native account report ignored"),
                Err(error) => log::warn!("native account observation rejected: {error}"),
            }
        });
        // Subscribe to this account only, rather than receiving all account
        // events from a process-wide wildcard. Identity/type are still checked
        // inside accept(): topic routing alone never authenticates a venue.
        msgbus::subscribe_account_state(topic.clone().into(), handler.clone(), None);
        Self { handler, topic, session }
    }

    pub fn session(&self) -> &Rc<RefCell<NativeAccountSession>> {
        &self.session
    }
}

impl Drop for NativeAccountSubscription {
    fn drop(&mut self) {
        msgbus::unsubscribe_account_state(self.topic.clone().into(), &self.handler);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        identifiers::AccountId,
        types::{AccountBalance, Currency, Money},
    };

    fn test_path(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "seealpha-account-{}-{}-{}", name, std::process::id(), UUID4::new()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn event(account: &str, ts: u64, reported: bool) -> AccountState {
        AccountState::new(
            AccountId::from(account),
            AccountType::Cash,
            vec![AccountBalance::new(
                Money::new(100.0, Currency::USD()),
                Money::new(20.0, Currency::USD()),
                Money::new(80.0, Currency::USD()),
            )],
            vec![],
            reported,
            UUID4::new(),
            UnixNanos::new(ts),
            UnixNanos::new(ts),
            Some(Currency::USD()),
        )
    }

    #[test]
    fn native_account_view_syncs_and_requires_newer_report_after_restart() {
        let root = test_path("restart");
        let checkpoint = root.join("account.json");
        let first = event("BINANCE-SPOT-001", 1_000_000_000, true);
        {
            let mut session = NativeAccountSession::open(
                "BINANCE-SPOT-001", AccountType::Cash, &checkpoint,
            ).unwrap();
            assert!(session.fresh(1_000_000_000, 1).is_none());
            assert!(session.accept(&first, 1_001_000_000, 5_000_000).unwrap());
            let view = session.fresh(1_001_000_000, 5_000_000).unwrap();
            assert_eq!(view.balances["USD"].total.parse::<rust_decimal::Decimal>().unwrap(),
                rust_decimal::Decimal::from(100));
            session.mark_disconnected();
            assert!(session.fresh(1_001_000_000, 5_000_000).is_none());
        }
        {
            let mut recovered = NativeAccountSession::open(
                "BINANCE-SPOT-001", AccountType::Cash, &checkpoint,
            ).unwrap();
            assert!(recovered.fresh(1_001_000_000, 5_000_000).is_none());
            assert!(!recovered.accept(&first, 1_001_000_000, 5_000_000).unwrap());
            assert!(recovered.fresh(1_001_000_000, 5_000_000).is_none());
            let next = event("BINANCE-SPOT-001", 1_002_000_000, true);
            assert!(recovered.accept(&next, 1_002_000_000, 5_000_000).unwrap());
            assert!(recovered.fresh(1_002_000_000, 5_000_000).is_some());
            assert!(recovered.fresh(1_008_000_000, 5_000_000).is_none());
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_wrong_account_local_calculation_and_corrupted_recovery() {
        let root = test_path("auth");
        let checkpoint = root.join("account.json");
        let mut session = NativeAccountSession::open(
            "BINANCE-SPOT-001", AccountType::Cash, &checkpoint,
        ).unwrap();
        assert!(session.accept(
            &event("BINANCE-SPOT-002", 1_000, true), 2_000, 2_000
        ).is_err());
        assert!(session.accept(
            &event("BINANCE-SPOT-001", 1_000, false), 2_000, 2_000
        ).is_err());
        assert!(session.fresh(2_000, 2_000).is_none());
        fs::write(&checkpoint, b"{broken").unwrap();
        assert!(NativeAccountSession::open(
            "BINANCE-SPOT-001", AccountType::Cash, &checkpoint
        ).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn typed_message_bus_account_subscription_delivers_only_matching_account() {
        let root = test_path("msgbus");
        let checkpoint = root.join("account.json");
        let session = Rc::new(RefCell::new(NativeAccountSession::open(
            "BINANCE-SPOT-001", AccountType::Cash, &checkpoint,
        ).unwrap()));
        {
            let _subscription = NativeAccountSubscription::attach(
                Rc::clone(&session), || 2_000, 2_000,
            );
            msgbus::publish_account_state(
                "events.account.BINANCE-SPOT-002".into(),
                &event("BINANCE-SPOT-002", 1_000, true),
            );
            assert!(session.borrow().fresh(2_000, 2_000).is_none());
            msgbus::publish_account_state(
                "events.account.BINANCE-SPOT-001".into(),
                &event("BINANCE-SPOT-001", 1_000, true),
            );
            assert!(session.borrow().fresh(2_000, 2_000).is_some());
        }
        fs::remove_dir_all(root).unwrap();
    }
}
