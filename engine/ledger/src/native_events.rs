//! Immutable quarantine of Nautilus-native execution event metadata.
//!
//! These rows are NOT exchange-authoritative evidence. In particular an
//! OrderFilled or FillReport marker does not create a Ledger fill. A future
//! authenticated Nautilus-to-Ledger consumer must separately reconcile real
//! exchange-authored trade identities and amounts before settlement.
use crate::{Ledger, LedgerError};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy)]
pub struct UnverifiedNativeOrderEvent<'a> {
    pub tenant: &'a str,
    pub account: &'a str,
    pub environment: &'a str,
    pub request_id: &'a str,
    pub client_order_id: &'a str,
    pub product: &'a str,
    pub symbol: &'a str,
    pub event_id: &'a str,
    pub event_type: &'a str,
    pub observed_at_ns: i64,
}

fn conflict(reason: &str) -> LedgerError {
    LedgerError::Conflict(reason.to_owned())
}
fn sql(error: rusqlite::Error) -> LedgerError {
    LedgerError::Sqlite(error.to_string())
}
fn identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128
        && value.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b':' | b'.'))
}
fn event_kind(value: &str) -> bool {
    matches!(
        value,
        "OrderSubmitted" | "OrderAccepted" | "OrderRejected" | "OrderCanceled"
        | "OrderExpired" | "OrderFilled" | "OrderFillVoided"
        | "OrderStatusReport" | "FillReport"
    )
}

impl Ledger {
    /// Atomically attach a non-authoritative native event descriptor to an
    /// already registered native order. Replays with identical event content
    /// are no-ops, and conflicting event IDs are rejected.
    ///
    /// There is deliberately NO promotion to order_observations or fills.
    pub fn quarantine_native_order_event(
        &self,
        event: UnverifiedNativeOrderEvent<'_>,
    ) -> Result<bool, LedgerError> {
        if !identifier(event.tenant) || !identifier(event.account)
            || !identifier(event.request_id) || !identifier(event.client_order_id)
            || !identifier(event.event_id)
            || !matches!(event.environment, "production" | "sandbox")
            || !matches!(event.product, "spot" | "futures_um")
            || !event_kind(event.event_type) || event.observed_at_ns <= 0
            || event.symbol.is_empty() || event.symbol.len() > 50
        {
            return Err(conflict("invalid native order event identity"));
        }
        let body = json!({
            "request_id": event.request_id,
            "client_order_id": event.client_order_id,
            "environment": event.environment,
            "product": event.product,
            "symbol": event.symbol,
            "event_id": event.event_id,
            "event_type": event.event_type,
            "observed_at_ns": event.observed_at_ns,
            "evidence_status": "unverified_native_event",
        });
        let encoded = body.to_string();
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(sql)?;
        let stored: Option<(String, String, String)> = tx.query_row(
            "SELECT environment,client_order_id,intent_json FROM order_intents
             WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3",
            params![event.tenant,event.account,event.request_id],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
        ).optional().map_err(sql)?;
        let Some((environment,client_id,original)) = stored else {
            return Err(conflict("native event has no registered order"));
        };
        let intent: Value = serde_json::from_str(&original)
            .map_err(|_| conflict("registered native order is corrupt"))?;
        if environment != event.environment || client_id != event.client_order_id
            || intent["execution_backend"] != "nautilus_binance_native"
            || intent["action"] != "NEW"
            || intent["environment"] != event.environment
            || intent["client_order_id"] != event.client_order_id
            || intent["product"] != event.product
            || intent["symbol"] != event.symbol
        {
            return Err(conflict("native report does not match immutable order intent"));
        }
        let previous: Option<String> = tx.query_row(
            "SELECT event_json FROM native_order_event_quarantine
             WHERE tenant_id=?1 AND account_id=?2 AND environment=?3
             AND client_order_id=?4 AND event_id=?5",
            params![event.tenant,event.account,event.environment,event.client_order_id,event.event_id],
            |row| row.get(0),
        ).optional().map_err(sql)?;
        if let Some(previous) = previous {
            if previous == encoded {
                return Ok(false);
            }
            return Err(conflict("conflicting immutable native order event"));
        }
        tx.execute(
            "INSERT INTO native_order_event_quarantine
             (tenant_id,account_id,environment,client_order_id,event_id,event_json)
             VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                event.tenant,event.account,event.environment,
                event.client_order_id,event.event_id,encoded
            ],
        ).map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(true)
    }

    /// Read-only audit retrieval, not a replay or send authorization.
    pub fn quarantined_native_order_events(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        client_order_id: &str,
    ) -> Result<Vec<Value>, LedgerError> {
        if !identifier(tenant) || !identifier(account) || !identifier(client_order_id)
            || !matches!(environment, "production" | "sandbox")
        {
            return Err(conflict("invalid native event audit scope"));
        }
        let conn = self.lock()?;
        let mut query = conn.prepare(
            "SELECT event_json FROM native_order_event_quarantine
             WHERE tenant_id=?1 AND account_id=?2 AND environment=?3 AND client_order_id=?4
             ORDER BY event_id"
        ).map_err(sql)?;
        let entries = query.query_map(
            params![tenant,account,environment,client_order_id],
            |row| row.get::<_, String>(0),
        ).map_err(sql)?;
        entries.map(|entry| {
            serde_json::from_str::<Value>(&entry.map_err(sql)?)
                .map_err(|_| conflict("native event audit record is corrupt"))
        }).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IntentStart;

    fn order_intent() -> Value {
        json!({
            "action":"NEW","environment":"sandbox","product":"spot",
            "symbol":"ETHUSDT","client_order_id":"client1",
            "side":"BUY","quantity":"0.1","price":"2000",
            "execution_backend":"nautilus_binance_native",
        })
    }

    fn event<'a>() -> UnverifiedNativeOrderEvent<'a> {
        UnverifiedNativeOrderEvent {
            tenant:"tenant1", account:"account1", environment:"sandbox",
            request_id:"request1", client_order_id:"client1",
            product:"spot", symbol:"ETHUSDT",
            event_id:"event1", event_type:"OrderFilled",
            observed_at_ns:1000,
        }
    }

    fn setup() -> Ledger {
        let ledger = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        ledger.ensure_tenant("tenant1","production","active").unwrap();
        assert_eq!(
            ledger.preregister_order_intent(
                "tenant1","account1","sandbox","request1","client1",
                &"a".repeat(64),&order_intent(),
            ).unwrap(),
            IntentStart::Fresh
        );
        ledger
    }

    #[test]
    fn native_fill_report_is_quarantined_without_ledger_fill_or_order_observation() {
        let ledger = setup();
        assert_eq!(ledger.quarantine_native_order_event(event()).unwrap(),true);
        assert_eq!(ledger.quarantine_native_order_event(event()).unwrap(),false);
        let events = ledger.quarantined_native_order_events(
            "tenant1","account1","sandbox","client1",
        ).unwrap();
        assert_eq!(events.len(),1);
        assert_eq!(events[0]["evidence_status"],"unverified_native_event");
        assert!(ledger.fills("tenant1","account1","sandbox").unwrap().is_empty());
        assert!(ledger.latest_order_observation(
            "tenant1","account1","sandbox","client1"
        ).unwrap().is_none());
    }

    #[test]
    fn changing_event_identity_or_order_scope_rejects_before_mutation() {
        let ledger = setup();
        ledger.quarantine_native_order_event(event()).unwrap();
        for bad in [
            UnverifiedNativeOrderEvent { event_type:"FillReport",..event() },
            UnverifiedNativeOrderEvent { symbol:"BTCUSDT",event_id:"event2",..event() },
            UnverifiedNativeOrderEvent { environment:"production",event_id:"event2",..event() },
            UnverifiedNativeOrderEvent { client_order_id:"other",event_id:"event2",..event() },
            UnverifiedNativeOrderEvent { request_id:"other",event_id:"event2",..event() },
            UnverifiedNativeOrderEvent { observed_at_ns:0,event_id:"event2",..event() },
        ] {
            assert!(ledger.quarantine_native_order_event(bad).is_err());
        }
        assert_eq!(
            ledger.quarantined_native_order_events(
                "tenant1","account1","sandbox","client1"
            ).unwrap().len(),1
        );
    }

    #[test]
    fn events_from_legacy_intents_cannot_enter_native_quarantine() {
        let ledger = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        ledger.ensure_tenant("tenant1","production","active").unwrap();
        let mut legacy = order_intent();
        legacy.as_object_mut().unwrap().remove("execution_backend");
        ledger.preregister_order_intent(
            "tenant1","account1","sandbox","request1","client1",
            &"a".repeat(64),&legacy,
        ).unwrap();
        assert!(ledger.quarantine_native_order_event(event()).is_err());
    }
}
