//! Durable adapter between Venue's one-shot admission contract and Ledger.
//!
//! This module intentionally does not expose an order sender. A venue dispatch
//! needs independent portfolio/risk authorization and a reviewed cutover gate.
use seealpha_ledger::{IntentStart, Ledger};
use seealpha_venue::{Product, VenueError};
use seealpha_venue::order_guard::{
    Admission, IntentJournal, IntentRecord, RecordedOutcome, WriteAction,
};
use serde_json::{json, Value};

pub(crate) struct LedgerIntentJournal<'a> {
    ledger: &'a Ledger,
    tenant: &'a str,
    account: &'a str,
    environment: &'a str,
    immutable_intent: Value,
}

impl<'a> LedgerIntentJournal<'a> {
    pub(crate) fn new(
        ledger: &'a Ledger,
        tenant: &'a str,
        account: &'a str,
        environment: &'a str,
        immutable_intent: Value,
    ) -> Self {
        Self { ledger, tenant, account, environment, immutable_intent }
    }

    fn valid_identity(&self, record: &IntentRecord) -> bool {
        let scope = format!("{}:{}:{}", self.tenant, self.account, self.environment);
        record.account_scope == scope
            && matches!(self.environment, "production" | "sandbox" | "paper")
            && record.action == WriteAction::NewOrder
            && self.immutable_intent["action"] == "NEW"
            && self.immutable_intent["environment"] == self.environment
            && self.immutable_intent["client_order_id"] == record.client_order_id
            && self.immutable_intent["symbol"] == record.symbol
            && self.immutable_intent["product"] == match record.product {
                Product::Spot => "spot",
                Product::FuturesUm => "futures_um",
                Product::Option => "options",
            }
    }
}

impl IntentJournal for LedgerIntentJournal<'_> {
    fn admit(&mut self, record: &IntentRecord) -> Result<Admission, VenueError> {
        if !self.valid_identity(record) {
            return Err(VenueError::Unconfirmed);
        }
        let registered = self.ledger.preregister_order_intent(
            self.tenant,
            self.account,
            self.environment,
            &record.request_id,
            &record.client_order_id,
            &record.payload_sha256,
            &self.immutable_intent,
        ).map_err(|_| VenueError::Unconfirmed)?;
        Ok(match registered {
            IntentStart::Fresh => Admission::Fresh,
            IntentStart::ReconcileOnly => Admission::Seen,
        })
    }

    fn observe(&mut self, record: &IntentRecord, outcome: RecordedOutcome)
        -> Result<(), VenueError>
    {
        if !self.valid_identity(record) {
            return Err(VenueError::Unconfirmed);
        }
        // ACK is not signed evidence of a fill. Record a local attempt only.
        // Restart recovery reads the authoritative immutable UNKNOWN intent.
        let label = match outcome {
            RecordedOutcome::Accepted => "acknowledged_unverified",
            RecordedOutcome::Rejected => "rejected_unverified",
            RecordedOutcome::Unknown => "unknown",
        };
        let event = json!({
            "request_id": record.request_id,
            "client_order_id": record.client_order_id,
            "environment": self.environment,
            "status": label,
            "payload_sha256": record.payload_sha256,
            "requires_reconciliation": true,
        });
        self.ledger.add_event(
            self.tenant, self.account, "venue_send_attempt", &event.to_string(),
        ).map_err(|_| VenueError::Unconfirmed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use seealpha_venue::order_guard::{PreparedWrite, SignedTransport};

    struct Wire { attempts: usize }
    impl SignedTransport for Wire {
        fn send(&mut self, _method: &str, _path: &str, _params: &[(String,String)])
            -> Result<Value, VenueError>
        {
            self.attempts += 1;
            Err(VenueError::Unconfirmed)
        }
    }

    #[test]
    fn restart_is_reconciliation_only_and_never_sends_twice() {
        let path = std::env::temp_dir().join(format!(
            "seealpha-intent-adapter-{}-{:?}.sqlite",
            std::process::id(), std::thread::current().id(),
        ));
        let _ = std::fs::remove_file(&path);
        let ledger = Ledger::open(&path).unwrap();
        ledger.ensure_tenant("tenant1", "production", "active").unwrap();
        let intent = json!({
            "environment":"production", "action":"NEW",
            "client_order_id":"client1", "symbol":"ETHUSDT", "product":"spot",
            "side":"BUY", "quantity":"0.01", "price":"2000",
        });
        let make = || PreparedWrite::new(
            "tenant1:account1:production", "request1", Product::Spot,
            WriteAction::NewOrder, "ETHUSDT", "client1",
            vec![
                ("symbol".into(),"ETHUSDT".into()),
                ("newClientOrderId".into(),"client1".into()),
            ],
        ).unwrap();
        let mut wire = Wire { attempts: 0 };
        {
            let mut journal = LedgerIntentJournal::new(
                &ledger, "tenant1", "account1", "production", intent.clone(),
            );
            assert!(make().dispatch(&mut journal, &mut wire).is_err());
        }
        assert_eq!(wire.attempts, 1);
        drop(ledger);
        let recovered = Ledger::open(&path).unwrap();
        let mut journal = LedgerIntentJournal::new(
            &recovered, "tenant1", "account1", "production", intent,
        );
        assert!(make().dispatch(&mut journal, &mut wire).is_err());
        assert_eq!(wire.attempts, 1);
        drop(journal);
        drop(recovered);
        let _ = std::fs::remove_file(&path);
    }
}
