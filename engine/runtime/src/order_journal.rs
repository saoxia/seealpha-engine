//! Durable adapter between Venue's one-shot admission contract and Ledger.
//!
//! This module intentionally does not expose an order sender. A venue dispatch
//! needs independent portfolio/risk authorization and a reviewed cutover gate.
use seealpha_ledger::{BasketAdmission, IntentStart, Ledger};
use seealpha_venue::order_guard::{
    Admission, IntentJournal, IntentRecord, RecordedOutcome, WriteAction,
};
use seealpha_venue::{Product, VenueError};
use serde_json::{Value, json};

pub(crate) struct LedgerIntentJournal<'a> {
    ledger: &'a Ledger,
    tenant: &'a str,
    account: &'a str,
    environment: &'a str,
    immutable_intent: Value,
    basket: Option<BasketReservation>,
}

#[derive(Clone)]
pub(crate) struct BasketReservation {
    pub(crate) basket_id: String,
    pub(crate) basket_revision: String,
    pub(crate) account_revision: i64,
}

impl<'a> LedgerIntentJournal<'a> {
    pub(crate) fn new(
        ledger: &'a Ledger,
        tenant: &'a str,
        account: &'a str,
        environment: &'a str,
        immutable_intent: Value,
    ) -> Self {
        Self {
            ledger,
            tenant,
            account,
            environment,
            immutable_intent,
            basket: None,
        }
    }

    /// Basket reservation is checked by Ledger in the same transaction that
    /// inserts the order intent. Construct only after Portfolio semantic review.
    pub(crate) fn new_basket(
        ledger: &'a Ledger,
        tenant: &'a str,
        account: &'a str,
        environment: &'a str,
        immutable_intent: Value,
        basket: BasketReservation,
    ) -> Self {
        Self {
            ledger,
            tenant,
            account,
            environment,
            immutable_intent,
            basket: Some(basket),
        }
    }

    fn wire_matches_intent(&self, record: &IntentRecord) -> bool {
        let field = |name: &str| {
            record
                .parameters
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.as_str())
        };
        // Values in the immutable authorization envelope must describe the
        // actual outgoing command, not merely copy an arbitrary parameter list.
        // Conservative string equality also refuses silent precision changes.
        field("side") == self.immutable_intent["side"].as_str()
            && field("side").is_some_and(|v| matches!(v, "BUY" | "SELL"))
            && field("quantity") == self.immutable_intent["quantity"].as_str()
            && field("quantity").is_some()
            && field("price") == self.immutable_intent.get("price").and_then(Value::as_str)
            && match self.immutable_intent.get("order_type") {
                Some(value) => field("type") == value.as_str(),
                None => field("type").is_none(),
            }
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
            && self.wire_matches_intent(record)
            // Bind the COMPLETE prepared wire payload, not merely the client ID.
            // Otherwise a different side, quantity, or price could be signed
            // under an otherwise valid ledger intent. JSON arrays preserve
            // pair order; PreparedWrite rejects duplicate parameter names.
            && self.immutable_intent.get("wire_parameters")
                == serde_json::to_value(&record.parameters).ok().as_ref()
            && self.immutable_intent["product"] == match record.product {
                Product::Spot => "spot",
                Product::FuturesUm => "futures_um",
                Product::Option => "options",
            }
    }
}

impl IntentJournal<IntentRecord, VenueError> for LedgerIntentJournal<'_> {
    fn admit(&mut self, record: &IntentRecord) -> Result<Admission, VenueError> {
        if !self.valid_identity(record) {
            return Err(VenueError::Unconfirmed);
        }
        let registered = if let Some(basket) = &self.basket {
            self.ledger.preregister_basket_order_intent(
                self.tenant,
                self.account,
                self.environment,
                &record.request_id,
                &record.client_order_id,
                &record.payload_sha256,
                &self.immutable_intent,
                BasketAdmission {
                    basket_id: &basket.basket_id,
                    basket_revision: &basket.basket_revision,
                    account_revision: basket.account_revision,
                },
            )
        } else {
            self.ledger.preregister_order_intent(
                self.tenant,
                self.account,
                self.environment,
                &record.request_id,
                &record.client_order_id,
                &record.payload_sha256,
                &self.immutable_intent,
            )
        }
        .map_err(|_| VenueError::Unconfirmed)?;
        Ok(match registered {
            IntentStart::Fresh => Admission::Fresh,
            IntentStart::ReconcileOnly => Admission::Seen,
        })
    }

    fn observe(
        &mut self,
        record: &IntentRecord,
        outcome: RecordedOutcome,
    ) -> Result<(), VenueError> {
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
        self.ledger
            .add_event(
                self.tenant,
                self.account,
                "venue_send_attempt",
                &event.to_string(),
            )
            .map_err(|_| VenueError::Unconfirmed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use seealpha_venue::order_guard::{PreparedWrite, SignedTransport};

    struct Wire {
        attempts: usize,
    }
    impl SignedTransport for Wire {
        fn send(
            &mut self,
            _method: &str,
            _path: &str,
            _params: &[(String, String)],
        ) -> Result<Value, VenueError> {
            self.attempts += 1;
            Err(VenueError::Unconfirmed)
        }
    }

    #[test]
    fn altered_wire_payload_cannot_create_a_ledger_intent_or_send() {
        let ledger = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        ledger
            .ensure_tenant("tenant1", "production", "active")
            .unwrap();
        let immutable = json!({
            "environment":"production", "action":"NEW",
            "client_order_id":"client1", "symbol":"ETHUSDT", "product":"spot",
            "side":"BUY", "quantity":"0.01",
            "wire_parameters":[
                ["symbol", "ETHUSDT"],
                ["newClientOrderId", "client1"],
                ["side", "BUY"],
                ["quantity", "0.01"]
            ]
        });
        let mut changed = PreparedWrite::new(
            "tenant1:account1:production",
            "request1",
            Product::Spot,
            WriteAction::NewOrder,
            "ETHUSDT",
            "client1",
            vec![
                ("symbol".into(), "ETHUSDT".into()),
                ("newClientOrderId".into(), "client1".into()),
                ("side".into(), "SELL".into()),
                ("quantity".into(), "0.01".into()),
            ],
        )
        .unwrap();
        let mut journal =
            LedgerIntentJournal::new(&ledger, "tenant1", "account1", "production", immutable);
        let mut wire = Wire { attempts: 0 };
        assert!(changed.dispatch(&mut journal, &mut wire).is_err());
        assert_eq!(wire.attempts, 0);
        assert!(
            ledger
                .request("tenant1", "account1", "request1")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn internally_inconsistent_authorization_envelope_is_rejected() {
        let ledger = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        ledger
            .ensure_tenant("tenant1", "production", "active")
            .unwrap();
        let intent = json!({
            "environment":"production", "action":"NEW",
            "client_order_id":"client1", "symbol":"ETHUSDT", "product":"spot",
            "side":"BUY", "quantity":"0.01", "price":"2000",
            "wire_parameters":[
                ["symbol", "ETHUSDT"], ["newClientOrderId", "client1"],
                ["side", "SELL"], ["quantity", "0.01"], ["price", "2000"]
            ]
        });
        // All wire fields equal the envelope's copied parameter list,
        // but the authoritative direction disagrees. Never register/send.
        let mut write = PreparedWrite::new(
            "tenant1:account1:production",
            "request2",
            Product::Spot,
            WriteAction::NewOrder,
            "ETHUSDT",
            "client1",
            vec![
                ("symbol".into(), "ETHUSDT".into()),
                ("newClientOrderId".into(), "client1".into()),
                ("side".into(), "SELL".into()),
                ("quantity".into(), "0.01".into()),
                ("price".into(), "2000".into()),
            ],
        )
        .unwrap();
        let mut journal =
            LedgerIntentJournal::new(&ledger, "tenant1", "account1", "production", intent);
        let mut wire = Wire { attempts: 0 };
        assert!(write.dispatch(&mut journal, &mut wire).is_err());
        assert_eq!(wire.attempts, 0);
        assert!(
            ledger
                .request("tenant1", "account1", "request2")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn restart_is_reconciliation_only_and_never_sends_twice() {
        let path = std::env::temp_dir().join(format!(
            "seealpha-intent-adapter-{}-{:?}.sqlite",
            std::process::id(),
            std::thread::current().id(),
        ));
        let _ = std::fs::remove_file(&path);
        let ledger = Ledger::open(&path).unwrap();
        ledger
            .ensure_tenant("tenant1", "production", "active")
            .unwrap();
        let intent = json!({
            "environment":"production", "action":"NEW",
            "client_order_id":"client1", "symbol":"ETHUSDT", "product":"spot",
            "side":"BUY", "quantity":"0.01", "price":"2000",
            "wire_parameters": [
                ["symbol", "ETHUSDT"], ["newClientOrderId", "client1"],
                ["side", "BUY"], ["quantity", "0.01"], ["price", "2000"]
            ],
        });
        let make = || {
            PreparedWrite::new(
                "tenant1:account1:production",
                "request1",
                Product::Spot,
                WriteAction::NewOrder,
                "ETHUSDT",
                "client1",
                vec![
                    ("symbol".into(), "ETHUSDT".into()),
                    ("newClientOrderId".into(), "client1".into()),
                    ("side".into(), "BUY".into()),
                    ("quantity".into(), "0.01".into()),
                    ("price".into(), "2000".into()),
                ],
            )
            .unwrap()
        };
        let mut wire = Wire { attempts: 0 };
        {
            let mut journal = LedgerIntentJournal::new(
                &ledger,
                "tenant1",
                "account1",
                "production",
                intent.clone(),
            );
            assert!(make().dispatch(&mut journal, &mut wire).is_err());
        }
        assert_eq!(wire.attempts, 1);
        drop(ledger);
        let recovered = Ledger::open(&path).unwrap();
        let mut journal =
            LedgerIntentJournal::new(&recovered, "tenant1", "account1", "production", intent);
        assert!(make().dispatch(&mut journal, &mut wire).is_err());
        assert_eq!(wire.attempts, 1);
        drop(journal);
        drop(recovered);
        let _ = std::fs::remove_file(&path);
    }
}
