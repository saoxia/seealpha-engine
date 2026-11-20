//! Durable pre-send intent gate and append-only authoritative order observations.
//! This module cannot send an order. A repeated/unknown intent is NEVER a send permit.
use crate::{Ledger, LedgerError, now_ms};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use rust_decimal::Decimal;
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntentStart {
    /// The intent was durably registered for the first time. Caller must still
    /// pass venue/risk checks before its sole initial transport attempt.
    Fresh,
    /// A matching intent already exists. Query/reconcile; never blindly send.
    ReconcileOnly,
}

fn sql(error: rusqlite::Error) -> LedgerError {
    LedgerError::Sqlite(error.to_string())
}
fn conflict(message: &str) -> LedgerError {
    LedgerError::Conflict(message.into())
}
fn scoped(tenant: &str, account: &str, env: &str) -> bool {
    !tenant.is_empty() && !account.is_empty() && tenant.len() <= 128
        && account.len() <= 128
        && matches!(env, "production" | "sandbox" | "paper")
}
fn identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128
        && value.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b':' | b'.'))
}
fn exact_json(value: &Value) -> bool {
    match value {
        Value::Number(number) => number.is_i64() || number.is_u64(),
        Value::Array(items) => items.iter().all(exact_json),
        Value::Object(items) => items.values().all(exact_json),
        _ => true,
    }
}
fn valid_intent(intent: &Value, environment: &str, client_id: &str) -> bool {
    if !exact_json(intent) || !intent.is_object()
        || intent["environment"].as_str() != Some(environment)
        || intent["client_order_id"].as_str() != Some(client_id)
    {
        return false;
    }
    for key in ["quantity","price","stop_price","trigger_price","quote_quantity"] {
        if let Some(field) = intent.get(key) {
            if field.as_str().and_then(|s|s.parse::<Decimal>().ok()).is_none() {
                return false;
            }
        }
    }
    match intent["action"].as_str() {
        Some("NEW") => {
            intent["symbol"].as_str().is_some_and(identifier)
                && matches!(intent["side"].as_str(), Some("BUY" | "SELL"))
                && intent["quantity"].as_str()
                    .and_then(|v| v.parse::<Decimal>().ok())
                    .is_some_and(|v| v > Decimal::ZERO)
                && intent.get("price").is_none_or(|v| v.as_str()
                    .and_then(|s| s.parse::<Decimal>().ok())
                    .is_some_and(|p| p > Decimal::ZERO))
        }
        Some("CANCEL" | "AMEND" | "REPLACE") => intent["symbol"].as_str().is_some_and(identifier),
        _ => false,
    }
}

impl Ledger {
    /// Persist an immutable intent AND a Python-readable UNKNOWN request row
    /// in one IMMEDIATE transaction. Only Fresh is eligible for *one* attempt.
    #[allow(clippy::too_many_arguments)]
    pub fn preregister_order_intent(
        &self, tenant: &str, account: &str, environment: &str,
        request_id: &str, client_order_id: &str, fingerprint: &str,
        intent: &Value,
    ) -> Result<IntentStart, LedgerError> {
        if !scoped(tenant, account, environment)
            || !identifier(request_id) || !identifier(client_order_id)
            || fingerprint.len() != 64 || !fingerprint.bytes().all(|b| b.is_ascii_hexdigit())
            || !valid_intent(intent, environment, client_order_id)
        {
            return Err(conflict("invalid order intent scope, identity or decimal quantity"));
        }
        let body = intent.to_string();
        if body.len() > 1024 * 1024 {
            return Err(conflict("order intent exceeds size limit"));
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(sql)?;
        let tenant_active: bool = tx.query_row(
            "SELECT status FROM tenants WHERE tenant_id=?1",
            params![tenant], |row| row.get::<_, String>(0),
        ).optional().map_err(sql)?.is_some_and(|status| status == "active");
        if !tenant_active {
            return Err(conflict("tenant must be active before recording an order intent"));
        }
        let previous: Option<(String, String, String, String)> = tx.query_row(
            "SELECT environment,client_order_id,fingerprint,intent_json FROM order_intents
             WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3",
            params![tenant, account, request_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        ).optional().map_err(sql)?;
        if let Some((env, client, prior_fingerprint, prior_body)) = previous {
            if env != environment || client != client_order_id
                || prior_fingerprint != fingerprint || prior_body != body {
                return Err(conflict("request identity already belongs to a different order intent"));
            }
            return Ok(IntentStart::ReconcileOnly);
        }
        // A different request ID cannot mint a second first-send permit for
        // the same NEW client order ID, even with a different fingerprint.
        if intent["action"] == "NEW" {
            let mut stmt = tx.prepare(
                "SELECT intent_json FROM order_intents
                 WHERE tenant_id=?1 AND account_id=?2 AND environment=?3 AND client_order_id=?4",
            ).map_err(sql)?;
            let rows = stmt.query_map(params![tenant,account,environment,client_order_id],
                |r|r.get::<_,String>(0)).map_err(sql)?;
            for row in rows {
                let stored: Value = serde_json::from_str(&row.map_err(sql)?)
                    .map_err(|_|conflict("stored order intent is corrupt"))?;
                if stored["action"] == "NEW" {
                    return Err(conflict("client order ID already belongs to another NEW intent"));
                }
            }
        }
        // A historical Python request with the same identity cannot be
        // replaced or retroactively certified as a pre-send registration.
        let occupied: bool = tx.query_row(
            "SELECT 1 FROM order_requests WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3",
            params![tenant, account, request_id], |_| Ok(()),
        ).optional().map_err(sql)?.is_some();
        if occupied {
            return Err(conflict("legacy request exists; reconcile manually before any send"));
        }
        tx.execute(
            "INSERT INTO order_intents
             (tenant_id,account_id,request_id,environment,client_order_id,fingerprint,intent_json,created_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![tenant,account,request_id,environment,client_order_id,fingerprint,body,now_ms()],
        ).map_err(sql)?;
        let compatible = json!({
            "status": "UNKNOWN", "reconcile_required": true, "environment": environment,
            "client_order_id": client_order_id, "fingerprint": fingerprint, "intent": intent,
        });
        tx.execute(
            "INSERT INTO order_requests(tenant_id,account_id,request_id,body_json,updated_at)
             VALUES(?1,?2,?3,?4,?5)",
            params![tenant,account,request_id,compatible.to_string(),now_ms()],
        ).map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(IntentStart::Fresh)
    }

    /// Append an authenticated observation, without crediting cash/position.
    /// A FILLED/PARTIALLY_FILLED label requires separate, already persisted
    /// trade IDs, not merely the order ACK or executedQty field.
    pub fn append_order_observation(
        &self, tenant: &str, account: &str, environment: &str,
        client_order_id: &str, observation_id: &str, evidence: &Value,
    ) -> Result<(), LedgerError> {
        if !scoped(tenant,account,environment) || !identifier(client_order_id)
            || !identifier(observation_id)
            || evidence["client_order_id"].as_str() != Some(client_order_id)
            || evidence["environment"].as_str() != Some(environment)
        {
            return Err(conflict("order evidence scope mismatch"));
        }
        let status = evidence["status"].as_str()
            .ok_or_else(|| conflict("missing order status"))?;
        if !matches!(status, "UNKNOWN" | "NEW" | "ACCEPTED" | "TRIGGERED"
            | "PARTIALLY_FILLED" | "FILLED" | "CANCELED" | "REJECTED" | "EXPIRED")
            || !matches!(evidence["source"].as_str(),
                Some("signed_exchange_order_query" | "authenticated_user_stream"))
            || evidence["observed_at_ns"].as_i64().is_none_or(|t| t <= 0)
        {
            return Err(conflict("untrusted or invalid exchange order observation"));
        }
        let observed_at = evidence["observed_at_ns"].as_i64()
            .ok_or_else(|| conflict("invalid observation timestamp"))?;
        let body = evidence.to_string();
        if body.len() > 1024 * 1024 {
            return Err(conflict("order observation exceeds size limit"));
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(sql)?;
        let intended_quantity: Option<Decimal> = {
            let mut stmt = tx.prepare(
                "SELECT intent_json FROM order_intents WHERE tenant_id=?1 AND account_id=?2
                 AND environment=?3 AND client_order_id=?4",
            ).map_err(sql)?;
            let rows = stmt.query_map(params![tenant,account,environment,client_order_id],
                |row| row.get::<_, String>(0)).map_err(sql)?;
            let mut quantity = None;
            for row in rows {
                let raw = row.map_err(sql)?;
                let value: Value = serde_json::from_str(&raw)
                    .map_err(|_| conflict("stored order intent is corrupt"))?;
                if value["action"] == "NEW" {
                    let next = value["quantity"].as_str()
                        .and_then(|v|v.parse::<Decimal>().ok())
                        .ok_or_else(||conflict("stored NEW intent has invalid quantity"))?;
                    if quantity.replace(next).is_some() {
                        return Err(conflict("multiple NEW intents bound to one client order ID"));
                    }
                }
            }
            quantity
        };
        let intended_quantity = intended_quantity
            .ok_or_else(||conflict("authoritative order observation has no prior NEW intent"))?;
        let prior: Option<String> = tx.query_row(
            "SELECT evidence_json FROM order_observations WHERE tenant_id=?1 AND account_id=?2
             AND environment=?3 AND client_order_id=?4 AND observation_id=?5",
            params![tenant,account,environment,client_order_id,observation_id],
            |row| row.get(0),
        ).optional().map_err(sql)?;
        if let Some(old) = prior {
            if old == body { return Ok(()); }
            return Err(conflict("conflicting immutable order observation"));
        }
        let latest: Option<(String,i64)> = tx.query_row(
            "SELECT status,observed_at_ns FROM order_observations
             WHERE tenant_id=?1 AND account_id=?2 AND environment=?3 AND client_order_id=?4
             ORDER BY observed_at_ns DESC,observation_id DESC LIMIT 1",
            params![tenant,account,environment,client_order_id],
            |row| Ok((row.get(0)?,row.get(1)?)),
        ).optional().map_err(sql)?;
        if let Some((old_status, last_time)) = latest {
            if observed_at < last_time
                || (matches!(old_status.as_str(), "FILLED" | "CANCELED" | "REJECTED" | "EXPIRED")
                    && status != old_status)
            {
                return Err(conflict("stale or conflicting terminal order state"));
            }
        }
        if matches!(status, "FILLED" | "PARTIALLY_FILLED") {
            let order_id = evidence["venue_order_id"].as_str()
                .filter(|v| identifier(v))
                .ok_or_else(|| conflict("fill status lacks venue order identity"))?;
            let trade_ids = evidence["trade_ids"].as_array()
                .filter(|ids| !ids.is_empty() && ids.len() < 1000)
                .ok_or_else(|| conflict("fill status requires verified trade identities"))?;
            let mut seen = std::collections::HashSet::new();
            let mut cumulative = Decimal::ZERO;
            for id in trade_ids {
                let id = id.as_str().filter(|v| identifier(v))
                    .ok_or_else(|| conflict("invalid trade identity"))?;
                if !seen.insert(id) {
                    return Err(conflict("duplicate trade identity in order observation"));
                }
                let raw: Option<String> = tx.query_row(
                    "SELECT fill_json FROM fills WHERE tenant_id=?1 AND account_id=?2
                     AND environment=?3 AND fill_id=?4",
                    params![tenant,account,environment,id],
                    |row| row.get(0),
                ).optional().map_err(sql)?;
                let raw = raw.ok_or_else(|| conflict("fill status references unverified trade"))?;
                let fill: Value = serde_json::from_str(&raw)
                    .map_err(|_| conflict("stored fill JSON corrupt"))?;
                if fill["trade_id"].as_str() != Some(id)
                    || fill["venue_order_id"].as_str() != Some(order_id)
                    || fill["source"] != "signed_exchange_user_trades"
                    || fill["price"].as_str()
                        .and_then(|v| v.parse::<Decimal>().ok())
                        .is_none_or(|n| n <= Decimal::ZERO)
                {
                    return Err(conflict("trade evidence not verified for this order"));
                }
                let qty = fill["quantity"].as_str()
                    .and_then(|v|v.parse::<Decimal>().ok())
                    .filter(|q|*q > Decimal::ZERO)
                    .ok_or_else(||conflict("trade has no positive Decimal quantity"))?;
                cumulative = cumulative.checked_add(qty)
                    .ok_or_else(||conflict("cumulative fill quantity overflow"))?;
            }
            if (status == "FILLED" && cumulative != intended_quantity)
                || (status == "PARTIALLY_FILLED"
                    && (cumulative <= Decimal::ZERO || cumulative >= intended_quantity)) {
                return Err(conflict("order status conflicts with total verified fill quantity"));
            }
        }
        tx.execute(
            "INSERT INTO order_observations
             (tenant_id,account_id,environment,client_order_id,observation_id,observed_at_ns,status,evidence_json)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![tenant,account,environment,client_order_id,observation_id,observed_at,status,body],
        ).map_err(sql)?;
        tx.commit().map_err(sql)
    }

    pub fn latest_order_observation(
        &self, tenant: &str, account: &str, environment: &str, client_id: &str,
    ) -> Result<Option<Value>, LedgerError> {
        let conn = self.lock()?;
        let body: Option<String> = conn.query_row(
            "SELECT evidence_json FROM order_observations
             WHERE tenant_id=?1 AND account_id=?2 AND environment=?3 AND client_order_id=?4
             ORDER BY observed_at_ns DESC,observation_id DESC LIMIT 1",
            params![tenant,account,environment,client_id], |r| r.get(0),
        ).optional().map_err(sql)?;
        body.map(|value| serde_json::from_str(&value)
            .map_err(|_| conflict("stored order evidence corrupt"))).transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn new_intent() -> Value {
        json!({"action":"NEW","environment":"production","client_order_id":"cl1",
            "symbol":"ETHUSDT","side":"BUY","quantity":"0.00000001","price":"1024.00000001"})
    }
    #[test]
    fn parallel_registration_has_exactly_one_initial_send_permit() {
        let ledger = Arc::new(Ledger::open(std::path::Path::new(":memory:")).unwrap());
        ledger.ensure_tenant("t","production","active").unwrap();
        let handles: Vec<_> = (0..12).map(|_| {
            let l = ledger.clone();
            thread::spawn(move || l.preregister_order_intent(
                "t","a","production","req1","cl1",&"a".repeat(64),&new_intent()).unwrap())
        }).collect();
        let results: Vec<_> = handles.into_iter().map(|x| x.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|x| **x == IntentStart::Fresh).count(), 1);
        assert_eq!(results.iter().filter(|x| **x == IntentStart::ReconcileOnly).count(), 11);
        assert!(ledger.request("t","a","req1").unwrap().unwrap()["reconcile_required"] == true);
        assert!(ledger.request("other","a","req1").unwrap().is_none());
        assert!(ledger.preregister_order_intent(
            "t","a","sandbox","req1","cl1",&"a".repeat(64),&new_intent()).is_err());
        assert!(ledger.preregister_order_intent(
            "t","a","production","different-request","cl1",&"c".repeat(64),&new_intent()).is_err());
        let mut imprecise=new_intent();
        imprecise["price"]=json!(1024.00000001);
        assert!(ledger.preregister_order_intent("t","a","production","r2","cl1",
            &"b".repeat(64),&imprecise).is_err());
    }

    #[test]
    fn partial_and_complete_orders_require_exact_verified_decimal_sum() {
        let book=Ledger::open(std::path::Path::new(":memory:")).unwrap();
        book.ensure_tenant("t","production","active").unwrap();
        let intent=json!({"action":"NEW","environment":"production","client_order_id":"c3",
            "symbol":"ETHUSDT","side":"BUY","quantity":"0.3"});
        book.preregister_order_intent("t","a","production","req3","c3",
            &"d".repeat(64),&intent).unwrap();
        let t1=json!({"trade_id":"binance:futures_um:ETHUSDT:301","venue_order_id":"100",
            "quantity":"0.1","price":"100","source":"signed_exchange_user_trades"});
        let t2=json!({"trade_id":"binance:futures_um:ETHUSDT:302","venue_order_id":"100",
            "quantity":"0.2","price":"100","source":"signed_exchange_user_trades"});
        book.import_verified_fills("t","a","production","100",&[t1,t2]).unwrap();
        let mut observed=json!({"environment":"production","client_order_id":"c3",
            "status":"PARTIALLY_FILLED","source":"signed_exchange_order_query",
            "observed_at_ns":1,"venue_order_id":"100",
            "trade_ids":["binance:futures_um:ETHUSDT:301"]});
        book.append_order_observation("t","a","production","c3","partial",&observed).unwrap();
        observed["status"]=json!("FILLED");
        observed["observed_at_ns"]=json!(2);
        assert!(book.append_order_observation("t","a","production","c3","early",&observed).is_err());
        observed["trade_ids"]=json!(["binance:futures_um:ETHUSDT:301",
            "binance:futures_um:ETHUSDT:302"]);
        book.append_order_observation("t","a","production","c3","complete",&observed).unwrap();
        assert_eq!(book.latest_order_observation("t","a","production","c3")
            .unwrap().unwrap()["status"],"FILLED");
    }

    #[test]
    fn ack_unknown_cannot_be_rewritten_into_a_fill() {
        let ledger = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        ledger.ensure_tenant("t","production","active").unwrap();
        ledger.preregister_order_intent("t","a","production","r","cl1",&"a".repeat(64),&new_intent()).unwrap();
        let observed = json!({"environment":"production","client_order_id":"cl1",
            "status":"NEW","source":"authenticated_user_stream","observed_at_ns":1});
        ledger.append_order_observation("t","a","production","cl1","o1",&observed).unwrap();
        assert!(ledger.fills("t","a","production").unwrap().is_empty());
        let filled = json!({"environment":"production","client_order_id":"cl1",
            "status":"FILLED","source":"signed_exchange_order_query","observed_at_ns":2,
            "venue_order_id":"42","trade_ids":["binance:futures_um:ETHUSDT:99"]});
        assert!(ledger.append_order_observation("t","a","production","cl1","o2",&filled).is_err());
        let trade = json!({"trade_id":"binance:futures_um:ETHUSDT:99","venue_order_id":"42",
            "quantity":"0.00000001","price":"1024.00000001","source":"signed_exchange_user_trades"});
        ledger.import_verified_fills("t","a","production","42",&[trade]).unwrap();
        ledger.append_order_observation("t","a","production","cl1","o2",&filled).unwrap();
        ledger.append_order_observation("t","a","production","cl1","o2",&filled).unwrap();
        assert_eq!(ledger.fills("t","a","production").unwrap().len(),1);
        let mut contradictory = observed;
        contradictory["observed_at_ns"]=json!(3);
        assert!(ledger.append_order_observation("t","a","production","cl1","o3",&contradictory).is_err());
        assert_eq!(ledger.latest_order_observation("t","a","production","cl1").unwrap().unwrap()["status"],"FILLED");
    }
}
