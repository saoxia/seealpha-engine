//! Durable, single-attempt admission for Binance REST side effects.
//! Implementors MUST fsync/commit admit() before returning Fresh. No retry of
//! a previously admitted request is allowed, even after process restart.
use crate::{Credentials, Product, VenueClient, VenueError, venue_symbol};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteAction {
    NewOrder,
    CancelOrder,
    ModifyOrder,
    CancelReplace,
    NewOco,
    CancelList,
    NewUmAlgo,
    CancelUmAlgo,
}
impl WriteAction {
    fn route(self, product: Product) -> Option<(&'static str, &'static str, &'static str)> {
        match (self, product) {
            (Self::NewOrder, Product::Spot) => Some(("POST", "/api/v3/order", "newClientOrderId")),
            (Self::NewOrder, Product::FuturesUm) => {
                Some(("POST", "/fapi/v1/order", "newClientOrderId"))
            }
            (Self::NewOrder, Product::Option) => Some(("POST", "/eapi/v1/order", "clientOrderId")),
            (Self::CancelOrder, Product::Spot) => {
                Some(("DELETE", "/api/v3/order", "origClientOrderId"))
            }
            (Self::CancelOrder, Product::FuturesUm) => {
                Some(("DELETE", "/fapi/v1/order", "origClientOrderId"))
            }
            (Self::CancelOrder, Product::Option) => {
                Some(("DELETE", "/eapi/v1/order", "clientOrderId"))
            }
            (Self::ModifyOrder, Product::FuturesUm) => {
                Some(("PUT", "/fapi/v1/order", "origClientOrderId"))
            }
            (Self::CancelReplace, Product::Spot) => {
                Some(("POST", "/api/v3/order/cancelReplace", "newClientOrderId"))
            }
            (Self::NewOco, Product::Spot) => {
                Some(("POST", "/api/v3/orderList/oco", "listClientOrderId"))
            }
            (Self::CancelList, Product::Spot) => {
                Some(("DELETE", "/api/v3/orderList", "listClientOrderId"))
            }
            (Self::NewUmAlgo, Product::FuturesUm) => {
                Some(("POST", "/fapi/v1/algoOrder", "clientAlgoId"))
            }
            (Self::CancelUmAlgo, Product::FuturesUm) => {
                Some(("DELETE", "/fapi/v1/algoOrder", "clientAlgoId"))
            }
            _ => None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntentRecord {
    pub account_scope: String,
    pub request_id: String,
    pub product: Product,
    pub action: WriteAction,
    pub symbol: String,
    pub client_order_id: String,
    pub payload_sha256: String,
    /// Exact untrusted wire fields used to construct payload_sha256.
    /// A durable intent journal must bind these to the authorized intent.
    pub parameters: Vec<(String, String)>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    Fresh,
    Seen,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordedOutcome {
    Accepted,
    Rejected,
    Unknown,
}
pub trait IntentJournal {
    /// Atomic unique insert by (account_scope, request_id); Seen never sends.
    fn admit(&mut self, record: &IntentRecord) -> Result<Admission, VenueError>;
    fn observe(
        &mut self,
        record: &IntentRecord,
        outcome: RecordedOutcome,
    ) -> Result<(), VenueError>;
}
/// Mockable wire interface. The only crate-provided real implementation is
/// private and is constructed by dispatch_live after durable admission.
pub trait SignedTransport {
    fn send(
        &mut self,
        method: &str,
        path: &str,
        params: &[(String, String)],
    ) -> Result<Value, VenueError>;
}
struct RegisteredTransport<'a> {
    client: &'a VenueClient,
    credentials: &'a Credentials,
}
impl SignedTransport for RegisteredTransport<'_> {
    fn send(&mut self, m: &str, p: &str, args: &[(String, String)]) -> Result<Value, VenueError> {
        self.client.signed_wire(self.credentials, m, p, args)
    }
}
fn valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 36
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':' | b'/'))
}
pub struct PreparedWrite {
    record: IntentRecord,
    method: &'static str,
    path: &'static str,
    parameters: Vec<(String, String)>,
    attempted: bool,
}
impl PreparedWrite {
    pub fn new(
        account: &str,
        request: &str,
        product: Product,
        action: WriteAction,
        symbol: &str,
        client_id: &str,
        parameters: Vec<(String, String)>,
    ) -> Result<Self, VenueError> {
        let (method, path, id_key) = action.route(product).ok_or(VenueError::Unconfirmed)?;
        if account.trim().is_empty()
            || account.len() > 128
            || !valid_id(request)
            || !valid_id(client_id)
        {
            return Err(VenueError::Unconfirmed);
        }
        let canonical = venue_symbol(symbol)?;
        let mut names = BTreeSet::new();
        for (key, _) in &parameters {
            if !names.insert(key.as_str())
                || matches!(key.as_str(), "timestamp" | "signature" | "recvWindow")
            {
                return Err(VenueError::Unconfirmed);
            }
        }
        if !parameters
            .iter()
            .any(|(k, v)| k == id_key && v == client_id)
            || (action != WriteAction::CancelUmAlgo
                && !parameters
                    .iter()
                    .any(|(k, v)| k == "symbol" && v == &canonical))
        {
            return Err(VenueError::Unconfirmed);
        }
        if action == WriteAction::CancelReplace
            && !parameters
                .iter()
                .any(|(k, v)| k == "cancelReplaceMode" && v == "STOP_ON_FAILURE")
        {
            return Err(VenueError::Unconfirmed);
        }
        let mut sorted = parameters.clone();
        sorted.sort();
        let mut digest = Sha256::new();
        for field in [account, request, method, path, &canonical, client_id] {
            digest.update((field.len() as u64).to_be_bytes());
            digest.update(field.as_bytes());
        }
        for (k, v) in &sorted {
            for field in [k, v] {
                digest.update((field.len() as u64).to_be_bytes());
                digest.update(field.as_bytes());
            }
        }
        Ok(Self {
            record: IntentRecord {
                account_scope: account.into(),
                request_id: request.into(),
                product,
                action,
                symbol: canonical,
                client_order_id: client_id.into(),
                payload_sha256: format!("{:x}", digest.finalize()),
                parameters: parameters.clone(),
            },
            method,
            path,
            parameters,
            attempted: false,
        })
    }
    pub fn record(&self) -> &IntentRecord {
        &self.record
    }
    /// The journal is the sole grant of permission to issue one write request.
    pub fn dispatch<J: IntentJournal, T: SignedTransport>(
        &mut self,
        journal: &mut J,
        transport: &mut T,
    ) -> Result<Value, VenueError> {
        if self.attempted {
            return Err(VenueError::Unconfirmed);
        }
        self.attempted = true;
        if journal.admit(&self.record)? != Admission::Fresh {
            return Err(VenueError::Unconfirmed);
        }
        // An exchange 4xx may still mean ambiguous execution (e.g. timeout).
        // No response failure is proof that a write did not take effect.
        let result = transport
            .send(self.method, self.path, &self.parameters)
            .map_err(|_| VenueError::Unconfirmed)
            .and_then(|v| {
                if self.ack_matches(&v) {
                    Ok(v)
                } else {
                    Err(VenueError::Unconfirmed)
                }
            });
        let outcome = if result.is_ok() {
            RecordedOutcome::Accepted
        } else {
            RecordedOutcome::Unknown
        };
        // Persistence failure after a wire call must stay UNKNOWN.
        journal
            .observe(&self.record, outcome)
            .map_err(|_| VenueError::Unconfirmed)?;
        result
    }
    pub fn dispatch_live<J: IntentJournal>(
        &mut self,
        journal: &mut J,
        client: &VenueClient,
        credentials: &Credentials,
    ) -> Result<Value, VenueError> {
        crate::check_credentials(credentials)?;
        let mut wire = RegisteredTransport {
            client,
            credentials,
        };
        self.dispatch(journal, &mut wire)
    }
    fn ack_matches(&self, v: &Value) -> bool {
        let id = self.record.client_order_id.as_str();
        match self.record.action {
            WriteAction::NewOco => {
                v["listClientOrderId"].as_str() == Some(id)
                    && v["contingencyType"].as_str() == Some("OCO")
                    && v["orderListId"].as_u64().is_some()
                    && v["orders"].as_array().is_some_and(|a| a.len() == 2)
            }
            WriteAction::NewUmAlgo => {
                v["clientAlgoId"].as_str() == Some(id) && v["algoId"].as_u64().is_some()
            }
            WriteAction::CancelUmAlgo => v["clientAlgoId"].as_str() == Some(id),
            WriteAction::CancelReplace => false, // mixed leg outcomes need read-side reconciliation
            WriteAction::CancelList => v["listClientOrderId"].as_str() == Some(id),
            WriteAction::ModifyOrder => {
                v["clientOrderId"].as_str() == Some(id) && !v["orderId"].is_null()
            }
            WriteAction::NewOrder => {
                v["clientOrderId"].as_str() == Some(id)
                    && v["symbol"].as_str() == Some(self.record.symbol.as_str())
                    && !v["orderId"].is_null()
                    && v["status"].as_str().is_some()
            }
            WriteAction::CancelOrder =>
            // Spot cancellation may assign a NEW clientOrderId for the
            // cancellation request; origClientOrderId identifies the
            // original order that must be reconciled.
            {
                (v["origClientOrderId"].as_str() == Some(id)
                    || v["clientOrderId"].as_str() == Some(id))
                    && v["symbol"].as_str() == Some(self.record.symbol.as_str())
                    && !v["orderId"].is_null()
                    && matches!(v["status"].as_str(), Some("CANCELED" | "CANCELLED"))
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;
    #[derive(Default)]
    struct Journal {
        seen: BTreeMap<String, String>,
        outcomes: Vec<RecordedOutcome>,
        broken: bool,
    }
    impl IntentJournal for Journal {
        fn admit(&mut self, r: &IntentRecord) -> Result<Admission, VenueError> {
            if self.broken {
                return Err(VenueError::Unconfirmed);
            }
            if self
                .seen
                .insert(r.request_id.clone(), r.payload_sha256.clone())
                .is_some()
            {
                return Ok(Admission::Seen);
            }
            Ok(Admission::Fresh)
        }
        fn observe(&mut self, _: &IntentRecord, o: RecordedOutcome) -> Result<(), VenueError> {
            self.outcomes.push(o);
            if self.broken {
                Err(VenueError::Unconfirmed)
            } else {
                Ok(())
            }
        }
    }
    struct Mock {
        calls: usize,
        timeout: bool,
    }
    impl SignedTransport for Mock {
        fn send(&mut self, _: &str, _: &str, _: &[(String, String)]) -> Result<Value, VenueError> {
            self.calls += 1;
            if self.timeout {
                Err(VenueError::Unconfirmed)
            } else {
                Ok(json!({"clientOrderId":"client1","symbol":"ETHUSDT","orderId":1,"status":"NEW"}))
            }
        }
    }
    fn prepared(request: &str) -> PreparedWrite {
        PreparedWrite::new(
            "tenant:account",
            request,
            Product::Spot,
            WriteAction::NewOrder,
            "ETHUSDT",
            "client1",
            vec![
                ("symbol".into(), "ETHUSDT".into()),
                ("newClientOrderId".into(), "client1".into()),
            ],
        )
        .unwrap()
    }
    #[test]
    fn durable_first_then_only_one_post_even_after_restart() {
        let mut journal = Journal::default();
        let mut transport = Mock {
            calls: 0,
            timeout: true,
        };
        assert_eq!(
            prepared("req1").dispatch(&mut journal, &mut transport),
            Err(VenueError::Unconfirmed)
        );
        assert_eq!(journal.outcomes, vec![RecordedOutcome::Unknown]);
        assert_eq!(
            prepared("req1").dispatch(&mut journal, &mut transport),
            Err(VenueError::Unconfirmed)
        );
        assert_eq!(transport.calls, 1);
        journal.broken = true;
        assert!(
            prepared("req2")
                .dispatch(&mut journal, &mut transport)
                .is_err()
        );
        assert_eq!(transport.calls, 1);
    }
    #[test]
    fn new_ack_is_not_a_fill_and_replays_cannot_repost() {
        let mut journal = Journal::default();
        let mut transport = Mock {
            calls: 0,
            timeout: false,
        };
        let mut packet = prepared("req3");
        assert_eq!(
            packet.dispatch(&mut journal, &mut transport).unwrap()["status"],
            "NEW"
        );
        assert!(packet.dispatch(&mut journal, &mut transport).is_err());
        assert_eq!(transport.calls, 1);
    }
    #[test]
    fn spot_cancel_ack_matches_original_client_id_not_replacement_id() {
        struct CancelMock;
        impl SignedTransport for CancelMock {
            fn send(
                &mut self,
                m: &str,
                p: &str,
                _: &[(String, String)],
            ) -> Result<Value, VenueError> {
                assert_eq!(m, "DELETE");
                assert_eq!(p, "/api/v3/order");
                Ok(
                    json!({"origClientOrderId":"client1","clientOrderId":"cancel1",
                    "orderId":17,"symbol":"ETHUSDT","status":"CANCELED",
                    "executedQty":"0.25","origQty":"1"}),
                )
            }
        }
        let mut journal = Journal::default();
        let mut packet = PreparedWrite::new(
            "tenant:account",
            "cancel_req1",
            Product::Spot,
            WriteAction::CancelOrder,
            "ETHUSDT",
            "client1",
            vec![
                ("symbol".into(), "ETHUSDT".into()),
                ("origClientOrderId".into(), "client1".into()),
            ],
        )
        .unwrap();
        let ack = packet.dispatch(&mut journal, &mut CancelMock).unwrap();
        assert_eq!(ack["executedQty"], "0.25");
        assert_eq!(journal.outcomes, vec![RecordedOutcome::Accepted]);
        // The ACK is not a ledger fill; real trade ID is absent.
        assert!(ack["id"].is_null());
    }
    #[test]
    fn rejects_conflicting_identity_and_cancel_replace_guessing() {
        assert!(
            PreparedWrite::new(
                "a",
                "req4",
                Product::Spot,
                WriteAction::NewOrder,
                "ETHUSDT",
                "client1",
                vec![("newClientOrderId".into(), "wrong".into())]
            )
            .is_err()
        );
        assert!(
            PreparedWrite::new(
                "a",
                "req4",
                Product::Option,
                WriteAction::NewOco,
                "ETHUSDT",
                "client1",
                vec![]
            )
            .is_err()
        );
        assert!(
            PreparedWrite::new(
                "a",
                "req4",
                Product::Spot,
                WriteAction::CancelReplace,
                "ETHUSDT",
                "client1",
                vec![
                    ("symbol".into(), "ETHUSDT".into()),
                    ("newClientOrderId".into(), "client1".into()),
                    ("cancelReplaceMode".into(), "ALLOW_FAILURE".into())
                ]
            )
            .is_err()
        );
    }
}
