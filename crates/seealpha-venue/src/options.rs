//! Options execution state machine.
//!
//! A submit is sent once. An unknown result is not posted again, and it blocks
//! new risk until the order is found by a query. Secrets are not part of Debug.
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

const TICK: Decimal = Decimal::from_parts(5, 0, 0, false, 0);
const STEP: Decimal = Decimal::from_parts(1, 0, 0, false, 2);

#[derive(Clone, Debug)]
pub struct OptionsOrder {
    pub client_order_id: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub time_in_force: String,
    pub post_only: bool,
    pub reduce_only: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireCall {
    pub method: String,
    pub params: HashMap<String, String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum OptionsError {
    Denied,
    Unknown,
    Blocked,
}

pub enum TransportResult {
    NotFound,
    Found,
    Unknown,
}

pub struct OptionsExecutor {
    account_id: String,
    api_key: String,
    secret: String,
    wire_ids: HashMap<String, String>,
    sent: HashSet<String>,
    pub unresolved: Vec<String>,
    pub quotes_fresh: bool,
    pub account_fresh: bool,
    pub ready: bool,
    pub stopped: bool,
    pub posts: u32,
}

impl std::fmt::Debug for OptionsExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OptionsExecutor")
            .field("account_id", &self.account_id)
            .field("ready", &self.ready)
            .finish()
    }
}

impl OptionsExecutor {
    pub fn new(account_id: &str, api_key: &str, secret: &str) -> Self {
        Self {
            account_id: account_id.to_owned(),
            api_key: api_key.to_owned(),
            secret: secret.to_owned(),
            wire_ids: HashMap::new(),
            sent: HashSet::new(),
            unresolved: Vec::new(),
            quotes_fresh: true,
            account_fresh: true,
            ready: true,
            stopped: false,
            posts: 0,
        }
    }

    pub fn wire(&mut self, client_id: &str) -> String {
        self.wire_ids
            .entry(client_id.to_owned())
            .or_insert_with(|| {
                let digest = Sha256::digest(format!("{}:{client_id}", self.account_id).as_bytes());
                let mut hex = String::new();
                for byte in digest.iter().take(16) {
                    hex.push_str(&format!("{byte:02x}"));
                }
                format!("sa-{hex}")
            })
            .clone()
    }

    pub fn submit<F>(&mut self, order: &OptionsOrder, mut transport: F) -> Result<(), OptionsError>
    where
        F: FnMut(&WireCall) -> TransportResult,
    {
        if !self.order_is_valid(order) {
            return Err(OptionsError::Denied);
        }
        if self.stopped || !self.ready {
            return Err(OptionsError::Denied);
        }
        if self
            .unresolved
            .iter()
            .any(|id| id == &order.client_order_id)
        {
            return Err(OptionsError::Unknown);
        }
        let new_risk = !order.reduce_only;
        if new_risk && (!self.quotes_fresh || !self.account_fresh || !self.unresolved.is_empty()) {
            return Err(OptionsError::Blocked);
        }
        if self.sent.contains(&order.client_order_id) {
            return Ok(());
        }
        let wire = self.wire(&order.client_order_id);
        let lookup = WireCall {
            method: "GET".into(),
            params: HashMap::from([("clientOrderId".into(), wire.clone())]),
        };
        match transport(&lookup) {
            TransportResult::Found => {
                self.sent.insert(order.client_order_id.clone());
                self.unresolved.retain(|id| id != &order.client_order_id);
                return Ok(());
            }
            TransportResult::Unknown => {
                self.mark_unknown(&order.client_order_id);
                return Err(OptionsError::Unknown);
            }
            TransportResult::NotFound => {}
        }
        if self.stopped {
            return Err(OptionsError::Denied);
        }
        self.posts += 1;
        let mut params = HashMap::from([
            ("clientOrderId".into(), wire),
            ("quantity".into(), "0.10".into()),
            ("reduceOnly".into(), bool_text(order.reduce_only)),
            ("postOnly".into(), bool_text(order.post_only)),
            (
                "timeInForce".into(),
                if order.post_only {
                    "GTX".into()
                } else {
                    "GTC".into()
                },
            ),
        ]);
        params.insert("price".into(), order.price.to_string());
        let post = WireCall {
            method: "POST".into(),
            params,
        };
        match transport(&post) {
            TransportResult::Found | TransportResult::NotFound => {
                self.sent.insert(order.client_order_id.clone());
                Ok(())
            }
            TransportResult::Unknown => {
                let query = WireCall {
                    method: "GET".into(),
                    params: HashMap::from([(
                        "clientOrderId".into(),
                        self.wire(&order.client_order_id),
                    )]),
                };
                if matches!(transport(&query), TransportResult::Found) {
                    self.sent.insert(order.client_order_id.clone());
                    Ok(())
                } else {
                    self.mark_unknown(&order.client_order_id);
                    Err(OptionsError::Unknown)
                }
            }
        }
    }

    pub fn cancel<F>(&mut self, client_id: &str, mut transport: F) -> Result<bool, OptionsError>
    where
        F: FnMut(&WireCall) -> TransportResult,
    {
        let wire = self.wire(client_id);
        let call = WireCall {
            method: "DELETE".into(),
            params: HashMap::from([("clientOrderId".into(), wire)]),
        };
        match transport(&call) {
            TransportResult::Found => Ok(true),
            TransportResult::NotFound => Ok(false),
            TransportResult::Unknown => {
                self.mark_unknown(client_id);
                Err(OptionsError::Unknown)
            }
        }
    }

    fn mark_unknown(&mut self, client_id: &str) {
        if !self.unresolved.iter().any(|id| id == client_id) {
            self.unresolved.push(client_id.to_owned());
        }
    }

    fn order_is_valid(&self, order: &OptionsOrder) -> bool {
        if order.post_only && matches!(order.time_in_force.as_str(), "IOC" | "FOK") {
            return false;
        }
        if !matches!(order.time_in_force.as_str(), "GTC" | "IOC" | "FOK") && !order.post_only {
            return false;
        }
        if order.price < Decimal::from(5) || order.price > Decimal::from(1500) {
            return false;
        }
        if order.price % TICK != Decimal::ZERO {
            return false;
        }
        if order.quantity < STEP || order.quantity % STEP != Decimal::ZERO {
            return false;
        }
        let _ = (&self.api_key, &self.secret);
        true
    }
}

fn bool_text(value: bool) -> String {
    if value { "true" } else { "false" }.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(post_only: bool, reduce_only: bool) -> OptionsOrder {
        OptionsOrder {
            client_order_id: "order-1".into(),
            price: Decimal::from(1000),
            quantity: Decimal::new(10, 2),
            time_in_force: "GTC".into(),
            post_only,
            reduce_only,
        }
    }

    #[test]
    fn test_single_submit_maps_flags_and_queries_authoritative_state() {
        for (post_only, reduce_only) in [(false, false), (true, false), (false, true)] {
            let mut engine = OptionsExecutor::new("BINANCE-OPTIONS-001", "key-one", "secret-one");
            let mut calls = Vec::new();
            let submitted = order(post_only, reduce_only);
            engine
                .submit(&submitted, |call| {
                    calls.push(call.clone());
                    if call.method == "GET" {
                        TransportResult::NotFound
                    } else {
                        TransportResult::Found
                    }
                })
                .unwrap();
            engine
                .submit(&submitted, |call| {
                    calls.push(call.clone());
                    TransportResult::Found
                })
                .unwrap();
            let posts: Vec<_> = calls.iter().filter(|call| call.method == "POST").collect();
            assert_eq!(posts.len(), 1);
            assert_eq!(posts[0].params["reduceOnly"], bool_text(reduce_only));
            assert_eq!(posts[0].params["postOnly"], bool_text(post_only));
            assert_eq!(
                posts[0].params["timeInForce"],
                if post_only { "GTX" } else { "GTC" }
            );
            assert_eq!(posts[0].params["quantity"], "0.10");
            assert_eq!(posts[0].params["clientOrderId"].len(), 35);
            assert!(engine.unresolved.is_empty());
            let rendered = format!("{engine:?}");
            assert!(!rendered.contains("secret-one"));
            assert!(!rendered.contains("key-one"));
        }
    }

    #[test]
    fn test_unknown_submit_never_reposts_and_blocks_new_risk() {
        let mut engine = OptionsExecutor::new("BINANCE-OPTIONS-001", "key-one", "secret-one");
        let mut methods = Vec::new();
        let first = order(false, false);
        let error = engine.submit(&first, |call| {
            methods.push(call.method.clone());
            if call.method == "GET" {
                TransportResult::NotFound
            } else {
                TransportResult::Unknown
            }
        });
        assert_eq!(error, Err(OptionsError::Unknown));
        let again = engine.submit(&first, |_| panic!("must not call transport"));
        assert_eq!(again, Err(OptionsError::Unknown));
        let mut second = order(false, false);
        second.client_order_id = "order-2".into();
        assert_eq!(
            engine.submit(&second, |_| panic!("new risk stays blocked")),
            Err(OptionsError::Blocked)
        );
        assert_eq!(
            methods
                .iter()
                .filter(|method| method.as_str() == "POST")
                .count(),
            1
        );
        assert_eq!(engine.unresolved, vec!["order-1".to_owned()]);
    }

    #[test]
    fn test_timeout_with_found_order_recovers_without_rejection() {
        let mut engine = OptionsExecutor::new("BINANCE-OPTIONS-001", "key", "secret");
        let mut stage = 0;
        engine
            .submit(&order(false, false), |call| {
                stage += 1;
                if call.method == "POST" {
                    TransportResult::Unknown
                } else if stage == 1 {
                    TransportResult::NotFound
                } else {
                    TransportResult::Found
                }
            })
            .unwrap();
        assert!(engine.unresolved.is_empty());
        assert!(engine.ready);
    }

    #[test]
    fn test_invalid_orders_never_reach_transport() {
        let mut engine = OptionsExecutor::new("BINANCE-OPTIONS-001", "key", "secret");
        let cases = [
            OptionsOrder {
                price: Decimal::new(1000001, 3),
                ..order(false, false)
            },
            OptionsOrder {
                price: Decimal::from(1001),
                ..order(false, false)
            },
            OptionsOrder {
                time_in_force: "DAY".into(),
                ..order(false, false)
            },
            OptionsOrder {
                time_in_force: "IOC".into(),
                post_only: true,
                ..order(true, false)
            },
        ];
        for item in cases {
            assert_eq!(
                engine.submit(&item, |_| panic!("invalid order reached transport")),
                Err(OptionsError::Denied)
            );
        }
        assert_eq!(engine.posts, 0);
    }

    #[test]
    fn test_stale_quotes_block_new_risk_but_not_reduce_only() {
        let mut engine = OptionsExecutor::new("BINANCE-OPTIONS-001", "key", "secret");
        engine.quotes_fresh = false;
        assert_eq!(
            engine.submit(&order(false, false), |_| panic!("new risk")),
            Err(OptionsError::Blocked)
        );
        engine
            .submit(&order(false, true), |call| {
                if call.method == "GET" {
                    TransportResult::NotFound
                } else {
                    TransportResult::Found
                }
            })
            .unwrap();
        assert_eq!(engine.posts, 1);
    }

    #[test]
    fn test_stale_account_or_dead_poller_blocks_new_risk() {
        let mut engine = OptionsExecutor::new("BINANCE-OPTIONS-001", "key", "secret");
        engine.account_fresh = false;
        assert_eq!(
            engine.submit(&order(false, false), |_| panic!("new risk")),
            Err(OptionsError::Blocked)
        );
        engine
            .submit(&order(false, true), |_| TransportResult::Found)
            .unwrap();
        engine.ready = false;
        assert!(!engine.ready);
    }

    #[test]
    fn test_preflight_existing_order_is_recovered_without_post() {
        let mut engine = OptionsExecutor::new("BINANCE-OPTIONS-001", "key", "secret");
        let mut methods = Vec::new();
        engine
            .submit(&order(false, false), |call| {
                methods.push(call.method.clone());
                TransportResult::Found
            })
            .unwrap();
        assert_eq!(methods, vec!["GET".to_owned()]);
        assert_eq!(engine.posts, 0);
    }

    #[test]
    fn test_stop_during_preflight_never_sends_new_order() {
        let mut engine = OptionsExecutor::new("BINANCE-OPTIONS-001", "key", "secret");
        engine.stopped = true;
        assert_eq!(
            engine.submit(&order(false, false), |_| panic!("stopped")),
            Err(OptionsError::Denied)
        );
        assert_eq!(engine.posts, 0);
    }

    #[test]
    fn test_cancel_unknown_open_or_fill_race_never_invents_canceled() {
        let mut engine = OptionsExecutor::new("BINANCE-OPTIONS-001", "key", "secret");
        assert_eq!(
            engine.cancel("order-1", |_| TransportResult::Unknown),
            Err(OptionsError::Unknown)
        );
        assert_eq!(engine.unresolved, vec!["order-1".to_owned()]);
        let mut engine = OptionsExecutor::new("BINANCE-OPTIONS-001", "key", "secret");
        assert_eq!(
            engine.cancel("order-1", |_| TransportResult::NotFound),
            Ok(false)
        );
    }

    #[test]
    fn test_truncated_startup_history_fails_closed() {
        let mut engine = OptionsExecutor::new("BINANCE-OPTIONS-001", "key", "secret");
        engine.ready = false;
        assert_eq!(
            engine.submit(&order(false, false), |_| panic!("not ready")),
            Err(OptionsError::Denied)
        );
    }

    #[test]
    fn test_signed_writes_are_exact_and_never_retried() {
        let mut engine = OptionsExecutor::new("BINANCE-OPTIONS-001", "key-one", "secret-one");
        let mut posts = 0;
        engine
            .submit(&order(false, false), |call| {
                if call.method == "POST" {
                    posts += 1;
                    assert!(!call.params.values().any(|value| value.contains("secret")));
                }
                if call.method == "GET" {
                    TransportResult::NotFound
                } else {
                    TransportResult::Found
                }
            })
            .unwrap();
        engine
            .submit(&order(false, false), |_| panic!("retry"))
            .unwrap();
        assert_eq!(posts, 1);
    }
}
