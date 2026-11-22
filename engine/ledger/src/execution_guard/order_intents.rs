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
    !tenant.is_empty()
        && !account.is_empty()
        && tenant.len() <= 128
        && account.len() <= 128
        && matches!(env, "production" | "sandbox" | "paper")
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b':' | b'.'))
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
    if !exact_json(intent)
        || !intent.is_object()
        || intent["environment"].as_str() != Some(environment)
        || intent["client_order_id"].as_str() != Some(client_id)
    {
        return false;
    }
    for key in [
        "quantity",
        "price",
        "stop_price",
        "trigger_price",
        "quote_quantity",
    ] {
        if let Some(field) = intent.get(key)
            && field
                .as_str()
                .and_then(|s| s.parse::<Decimal>().ok())
                .is_none()
        {
            return false;
        }
    }
    match intent["action"].as_str() {
        Some("NEW") => {
            intent["symbol"].as_str().is_some_and(identifier)
                && matches!(intent["side"].as_str(), Some("BUY" | "SELL"))
                && intent["quantity"]
                    .as_str()
                    .and_then(|v| v.parse::<Decimal>().ok())
                    .is_some_and(|v| v > Decimal::ZERO)
                && intent.get("price").is_none_or(|v| {
                    v.as_str()
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .is_some_and(|p| p > Decimal::ZERO)
                })
        }
        Some("CANCEL" | "AMEND" | "REPLACE") => intent["symbol"].as_str().is_some_and(identifier),
        _ => false,
    }
}

/// A caller-verified reservation reference. The Ledger rechecks both CAS
/// revisions inside the same transaction that inserts the immutable intent.
/// This is NOT a trading permission: upstream must replay-verify the basket,
/// authenticate account evidence, and independently authorize the exact leg.
#[derive(Debug, Clone, Copy)]
pub struct BasketAdmission<'a> {
    pub basket_id: &'a str,
    pub basket_revision: &'a str,
    pub account_revision: i64,
}

/// Durable, read-only recovery work item. A terminal observation still
/// requires independent fill/account reconciliation; it is NOT a send permit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRecoveryItem {
    pub tenant_id: String,
    pub account_id: String,
    pub environment: String,
    pub request_id: String,
    pub client_order_id: String,
    pub product: String,
    pub symbol: String,
    pub last_verified_status: Option<String>,
    pub observed_at_ns: Option<i64>,
    pub reconciliation_only: bool,
}

impl Ledger {
    /// Retrieve the exact immutable registration envelope for signed GET
    /// reconciliation. Never infer a new send authorization from this read.
    pub fn registered_order_intent(
        &self,
        tenant: &str,
        account: &str,
        request: &str,
    ) -> Result<Option<Value>, LedgerError> {
        if !identifier(tenant) || !identifier(account) || !identifier(request) {
            return Err(conflict("invalid registered intent scope"));
        }
        let conn = self.lock()?;
        let raw: Option<(String, String, String)> = conn
            .query_row(
                "SELECT environment,client_order_id,intent_json FROM order_intents
             WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3",
                params![tenant, account, request],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(sql)?;
        raw.map(|(env, client, text)| {
            let intent: Value = serde_json::from_str(&text)
                .map_err(|_| conflict("registered order intent is corrupt"))?;
            if !valid_intent(&intent, &env, &client) {
                return Err(conflict("registered order intent is invalid"));
            }
            Ok(intent)
        })
        .transpose()
    }

    /// Discover immutable registered intents on restart or on explicit read.
    /// Every item is reconciliation-only, including unknown and terminal
    /// observations: order ACKs cannot establish financial settlement.
    /// Bounded pagination avoids skipping unknown orders in large books.
    pub fn order_recovery_page(
        &self,
        tenant: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<OrderRecoveryItem>, LedgerError> {
        if tenant.is_empty()
            || tenant.len() > 128
            || limit == 0
            || limit > 500
            || offset > i64::MAX as usize
        {
            return Err(conflict("invalid recovery query scope or page size"));
        }
        let conn = self.lock()?;
        let mut statement = conn
            .prepare(
                "SELECT oi.tenant_id,oi.account_id,oi.environment,oi.request_id,
                    oi.client_order_id,oi.intent_json,
                    (SELECT oo.evidence_json FROM order_observations oo
                     WHERE oo.tenant_id=oi.tenant_id AND oo.account_id=oi.account_id
                       AND oo.environment=oi.environment
                       AND oo.client_order_id=oi.client_order_id
                     ORDER BY oo.observed_at_ns DESC,oo.observation_id DESC LIMIT 1)
             FROM order_intents oi
             WHERE oi.tenant_id=?1
             ORDER BY oi.created_at,oi.account_id,oi.request_id
             LIMIT ?2 OFFSET ?3",
            )
            .map_err(sql)?;
        let rows = statement
            .query_map(params![tenant, limit as i64, offset as i64], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })
            .map_err(sql)?;
        let mut items = Vec::new();
        for row in rows {
            let (
                tenant_id,
                account_id,
                environment,
                request_id,
                client_order_id,
                intent_text,
                latest_text,
            ) = row.map_err(sql)?;
            let intent: Value = serde_json::from_str(&intent_text)
                .map_err(|_| conflict("persisted order intent is corrupt"))?;
            if !valid_intent(&intent, &environment, &client_order_id) {
                return Err(conflict(
                    "persisted order intent has invalid scope or amounts",
                ));
            }
            let (last_verified_status, observed_at_ns) = if let Some(raw) = latest_text {
                let observed: Value = serde_json::from_str(&raw)
                    .map_err(|_| conflict("persisted order observation is corrupt"))?;
                let source = observed["source"].as_str();
                let status = observed["status"].as_str();
                let at = observed["observed_at_ns"].as_i64();
                if !matches!(
                    source,
                    Some("signed_exchange_order_query" | "authenticated_user_stream")
                ) || status.is_none()
                    || at.is_none_or(|v| v <= 0)
                    || observed["client_order_id"].as_str() != Some(client_order_id.as_str())
                    || observed["environment"].as_str() != Some(environment.as_str())
                {
                    return Err(conflict(
                        "stored recovery evidence is untrusted or mismatched",
                    ));
                }
                (status.map(str::to_owned), at)
            } else {
                (None, None)
            };
            let symbol = intent["symbol"].as_str().unwrap_or("").to_owned();
            let product = intent["product"].as_str().unwrap_or("unknown").to_owned();
            items.push(OrderRecoveryItem {
                tenant_id,
                account_id,
                environment,
                request_id,
                client_order_id,
                product,
                symbol,
                last_verified_status,
                observed_at_ns,
                reconciliation_only: true,
            });
        }
        Ok(items)
    }

    /// Atomic, fail-closed reservation binding. No Venue call is made.
    #[allow(clippy::too_many_arguments)]
    pub fn preregister_basket_order_intent(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        request_id: &str,
        client_order_id: &str,
        fingerprint: &str,
        intent: &Value,
        reservation: BasketAdmission<'_>,
    ) -> Result<IntentStart, LedgerError> {
        if !identifier(reservation.basket_id)
            || reservation.basket_revision.len() != 64
            || !reservation
                .basket_revision
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
            || reservation.account_revision <= 0
            || intent["basket_id"].as_str() != Some(reservation.basket_id)
            || intent["basket_revision"].as_str() != Some(reservation.basket_revision)
            || intent["account_revision"].as_i64() != Some(reservation.account_revision)
            || intent["basket_request_id"].as_str() != Some(request_id)
        {
            return Err(conflict("basket intent reservation identity mismatch"));
        }
        self.preregister_order_intent_inner(
            tenant,
            account,
            environment,
            request_id,
            client_order_id,
            fingerprint,
            intent,
            Some(reservation),
        )
    }

    /// Persist an immutable intent AND a Python-readable UNKNOWN request row
    /// in one IMMEDIATE transaction. Only Fresh is eligible for *one* attempt.
    #[allow(clippy::too_many_arguments)]
    pub fn preregister_order_intent(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        request_id: &str,
        client_order_id: &str,
        fingerprint: &str,
        intent: &Value,
    ) -> Result<IntentStart, LedgerError> {
        self.preregister_order_intent_inner(
            tenant,
            account,
            environment,
            request_id,
            client_order_id,
            fingerprint,
            intent,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn preregister_order_intent_inner(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        request_id: &str,
        client_order_id: &str,
        fingerprint: &str,
        intent: &Value,
        reservation: Option<BasketAdmission<'_>>,
    ) -> Result<IntentStart, LedgerError> {
        if !scoped(tenant, account, environment)
            || !identifier(request_id)
            || !identifier(client_order_id)
            || fingerprint.len() != 64
            || !fingerprint.bytes().all(|b| b.is_ascii_hexdigit())
            || !valid_intent(intent, environment, client_order_id)
        {
            return Err(conflict(
                "invalid order intent scope, identity or decimal quantity",
            ));
        }
        let body = intent.to_string();
        if body.len() > 1024 * 1024 {
            return Err(conflict("order intent exceeds size limit"));
        }
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let tenant_active: bool = tx
            .query_row(
                "SELECT status FROM tenants WHERE tenant_id=?1",
                params![tenant],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql)?
            .is_some_and(|status| status == "active");
        if !tenant_active {
            return Err(conflict(
                "tenant must be active before recording an order intent",
            ));
        }
        if let Some(reserved) = reservation {
            // All checks occur with the order INSERT under one IMMEDIATE
            // transaction, so a concurrent basket/account CAS update cannot
            // make a stale reservation appear current.
            let journal: Option<String> = tx
                .query_row(
                    "SELECT body_json FROM basket_journals WHERE tenant_id=?1
                 AND account_id=?2 AND basket_id=?3 AND revision=?4",
                    params![
                        tenant,
                        account,
                        reserved.basket_id,
                        reserved.basket_revision
                    ],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql)?;
            let journal =
                journal.ok_or_else(|| conflict("basket reservation revision is stale"))?;
            let encoded: Value = serde_json::from_str(&journal)
                .map_err(|_| conflict("basket checkpoint is corrupt"))?;
            let pending = encoded["payload"]["events"]
                .as_array()
                .and_then(|events| events.last())
                .is_some_and(|event| {
                    event["kind"] == "begin" && event["request_id"].as_str() == Some(request_id)
                });
            if encoded["revision"].as_str() != Some(reserved.basket_revision) || !pending {
                return Err(conflict("basket is not awaiting this exact request"));
            }
            let snapshot: Option<(i64, String)> = tx
                .query_row(
                    "SELECT revision,source FROM account_snapshots WHERE tenant_id=?1
                 AND account_id=?2 AND environment=?3",
                    params![tenant, account, environment],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(sql)?;
            let expected_source = if environment == "paper" {
                "paper_simulator"
            } else {
                "signed_exchange_account_query"
            };
            if snapshot.as_ref().is_none_or(|(revision, source)| {
                *revision != reserved.account_revision || source != expected_source
            }) {
                return Err(conflict(
                    "authoritative account snapshot changed after risk reservation",
                ));
            }
        }
        let previous: Option<(String, String, String, String)> = tx
            .query_row(
                "SELECT environment,client_order_id,fingerprint,intent_json FROM order_intents
             WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3",
                params![tenant, account, request_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(sql)?;
        if let Some((env, client, prior_fingerprint, prior_body)) = previous {
            if env != environment
                || client != client_order_id
                || prior_fingerprint != fingerprint
                || prior_body != body
            {
                return Err(conflict(
                    "request identity already belongs to a different order intent",
                ));
            }
            return Ok(IntentStart::ReconcileOnly);
        }
        // A different request ID cannot mint a second first-send permit
        // for the same action on the same client order ID, even after a timeout.
        {
            let mut stmt = tx
                .prepare(
                    "SELECT intent_json FROM order_intents
                 WHERE tenant_id=?1 AND account_id=?2 AND environment=?3 AND client_order_id=?4",
                )
                .map_err(sql)?;
            let rows = stmt
                .query_map(
                    params![tenant, account, environment, client_order_id],
                    |r| r.get::<_, String>(0),
                )
                .map_err(sql)?;
            for row in rows {
                let stored: Value = serde_json::from_str(&row.map_err(sql)?)
                    .map_err(|_| conflict("stored order intent is corrupt"))?;
                if stored["action"] == intent["action"] {
                    return Err(conflict(
                        "client order action already has a registered intent; reconcile",
                    ));
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
            return Err(conflict(
                "legacy request exists; reconcile manually before any send",
            ));
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
            params![
                tenant,
                account,
                request_id,
                compatible.to_string(),
                now_ms()
            ],
        )
        .map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(IntentStart::Fresh)
    }

    /// Append an authenticated observation, without crediting cash/position.
    /// A FILLED/PARTIALLY_FILLED label requires separate, already persisted
    /// trade IDs, not merely the order ACK or executedQty field.
    pub fn append_order_observation(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        client_order_id: &str,
        observation_id: &str,
        evidence: &Value,
    ) -> Result<(), LedgerError> {
        if !scoped(tenant, account, environment)
            || !identifier(client_order_id)
            || !identifier(observation_id)
            || evidence["client_order_id"].as_str() != Some(client_order_id)
            || evidence["environment"].as_str() != Some(environment)
        {
            return Err(conflict("order evidence scope mismatch"));
        }
        let status = evidence["status"]
            .as_str()
            .ok_or_else(|| conflict("missing order status"))?;
        if !matches!(
            status,
            "UNKNOWN"
                | "NEW"
                | "ACCEPTED"
                | "TRIGGERED"
                | "PARTIALLY_FILLED"
                | "FILLED"
                | "CANCELED"
                | "REJECTED"
                | "EXPIRED"
        ) || !matches!(
            evidence["source"].as_str(),
            Some("signed_exchange_order_query" | "authenticated_user_stream")
        ) || evidence["observed_at_ns"].as_i64().is_none_or(|t| t <= 0)
        {
            return Err(conflict("untrusted or invalid exchange order observation"));
        }
        let observed_at = evidence["observed_at_ns"]
            .as_i64()
            .ok_or_else(|| conflict("invalid observation timestamp"))?;
        let body = evidence.to_string();
        if body.len() > 1024 * 1024 {
            return Err(conflict("order observation exceeds size limit"));
        }
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let intended_quantity: Option<Decimal> = {
            let mut stmt = tx
                .prepare(
                    "SELECT intent_json FROM order_intents WHERE tenant_id=?1 AND account_id=?2
                 AND environment=?3 AND client_order_id=?4",
                )
                .map_err(sql)?;
            let rows = stmt
                .query_map(
                    params![tenant, account, environment, client_order_id],
                    |row| row.get::<_, String>(0),
                )
                .map_err(sql)?;
            let mut quantity = None;
            for row in rows {
                let raw = row.map_err(sql)?;
                let value: Value = serde_json::from_str(&raw)
                    .map_err(|_| conflict("stored order intent is corrupt"))?;
                if value["action"] == "NEW" {
                    let next = value["quantity"]
                        .as_str()
                        .and_then(|v| v.parse::<Decimal>().ok())
                        .ok_or_else(|| conflict("stored NEW intent has invalid quantity"))?;
                    if quantity.replace(next).is_some() {
                        return Err(conflict(
                            "multiple NEW intents bound to one client order ID",
                        ));
                    }
                }
            }
            quantity
        };
        let intended_quantity = intended_quantity
            .ok_or_else(|| conflict("authoritative order observation has no prior NEW intent"))?;
        let prior: Option<String> = tx
            .query_row(
                "SELECT evidence_json FROM order_observations WHERE tenant_id=?1 AND account_id=?2
             AND environment=?3 AND client_order_id=?4 AND observation_id=?5",
                params![
                    tenant,
                    account,
                    environment,
                    client_order_id,
                    observation_id
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)?;
        if let Some(old) = prior {
            if old == body {
                return Ok(());
            }
            return Err(conflict("conflicting immutable order observation"));
        }
        let latest: Option<(String, i64, String)> = tx
            .query_row(
                "SELECT status,observed_at_ns,evidence_json FROM order_observations
             WHERE tenant_id=?1 AND account_id=?2 AND environment=?3 AND client_order_id=?4
             ORDER BY observed_at_ns DESC,observation_id DESC LIMIT 1",
                params![tenant, account, environment, client_order_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(sql)?;
        let mut previous_fills = std::collections::HashSet::new();
        if let Some((old_status, last_time, prior_json)) = &latest {
            if observed_at < *last_time
                || (matches!(
                    old_status.as_str(),
                    "FILLED" | "CANCELED" | "REJECTED" | "EXPIRED"
                ) && status != old_status)
            {
                return Err(conflict("stale or conflicting terminal order state"));
            }
            let previous: Value = serde_json::from_str(prior_json)
                .map_err(|_| conflict("stored order observation is corrupt"))?;
            if let Some(ids) = previous["trade_ids"].as_array() {
                for id in ids {
                    let id = id
                        .as_str()
                        .filter(|v| identifier(v))
                        .ok_or_else(|| conflict("stored trade identity is corrupt"))?;
                    if !previous_fills.insert(id.to_owned()) {
                        return Err(conflict(
                            "stored order observation repeats a trade identity",
                        ));
                    }
                }
            }
            if old_status == "PARTIALLY_FILLED" && status == "NEW" {
                return Err(conflict(
                    "order status cannot return to NEW after verified fills",
                ));
            }
        }
        // Every subsequent observation must retain the previously verified
        // trade identities. An empty/non-fill status cannot erase executions.
        if !previous_fills.is_empty() {
            let next_ids = evidence["trade_ids"]
                .as_array()
                .ok_or_else(|| conflict("observed order omitted previously verified trades"))?;
            let next = next_ids
                .iter()
                .filter_map(Value::as_str)
                .collect::<std::collections::HashSet<_>>();
            if !previous_fills.iter().all(|id| next.contains(id.as_str())) {
                return Err(conflict("verified cumulative trade identities regressed"));
            }
        }
        if matches!(status, "FILLED" | "PARTIALLY_FILLED") {
            let order_id = evidence["venue_order_id"]
                .as_str()
                .filter(|v| identifier(v))
                .ok_or_else(|| conflict("fill status lacks venue order identity"))?;
            let trade_ids = evidence["trade_ids"]
                .as_array()
                .filter(|ids| !ids.is_empty() && ids.len() < 1000)
                .ok_or_else(|| conflict("fill status requires verified trade identities"))?;
            let mut seen = std::collections::HashSet::new();
            let mut cumulative = Decimal::ZERO;
            for id in trade_ids {
                let id = id
                    .as_str()
                    .filter(|v| identifier(v))
                    .ok_or_else(|| conflict("invalid trade identity"))?;
                if !seen.insert(id) {
                    return Err(conflict("duplicate trade identity in order observation"));
                }
                let raw: Option<String> = tx
                    .query_row(
                        "SELECT fill_json FROM fills WHERE tenant_id=?1 AND account_id=?2
                     AND environment=?3 AND fill_id=?4",
                        params![tenant, account, environment, id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(sql)?;
                let raw = raw.ok_or_else(|| conflict("fill status references unverified trade"))?;
                let fill: Value =
                    serde_json::from_str(&raw).map_err(|_| conflict("stored fill JSON corrupt"))?;
                if fill["trade_id"].as_str() != Some(id)
                    || fill["venue_order_id"].as_str() != Some(order_id)
                    || fill["source"] != "signed_exchange_user_trades"
                    || fill["price"]
                        .as_str()
                        .and_then(|v| v.parse::<Decimal>().ok())
                        .is_none_or(|n| n <= Decimal::ZERO)
                {
                    return Err(conflict("trade evidence not verified for this order"));
                }
                let qty = fill["quantity"]
                    .as_str()
                    .and_then(|v| v.parse::<Decimal>().ok())
                    .filter(|q| *q > Decimal::ZERO)
                    .ok_or_else(|| conflict("trade has no positive Decimal quantity"))?;
                cumulative = cumulative
                    .checked_add(qty)
                    .ok_or_else(|| conflict("cumulative fill quantity overflow"))?;
            }
            if (status == "FILLED" && cumulative != intended_quantity)
                || (status == "PARTIALLY_FILLED"
                    && (cumulative <= Decimal::ZERO || cumulative >= intended_quantity))
            {
                return Err(conflict(
                    "order status conflicts with total verified fill quantity",
                ));
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
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        client_id: &str,
    ) -> Result<Option<Value>, LedgerError> {
        let conn = self.lock()?;
        let body: Option<String> = conn
            .query_row(
                "SELECT evidence_json FROM order_observations
             WHERE tenant_id=?1 AND account_id=?2 AND environment=?3 AND client_order_id=?4
             ORDER BY observed_at_ns DESC,observation_id DESC LIMIT 1",
                params![tenant, account, environment, client_id],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql)?;
        body.map(|value| {
            serde_json::from_str(&value).map_err(|_| conflict("stored order evidence corrupt"))
        })
        .transpose()
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
    fn basket_admission_requires_atomic_revision_and_request_binding() {
        let book = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        book.ensure_tenant("t", "production", "active").unwrap();
        let revision = "a".repeat(64);
        let encoded = json!({"revision":revision,"payload":{"events":[
            {"kind":"begin","request_id":"req-basket"}
        ]}})
        .to_string();
        book.save_basket_journal("t", "a", "basket1", &encoded, &revision, None)
            .unwrap();
        let account_state = json!({"balances":{"USDT":"100"},"positions":{}});
        book.save_account_snapshot(
            "t",
            "a",
            "production",
            None,
            "signed1",
            1,
            "signed_exchange_account_query",
            &account_state,
        )
        .unwrap();
        let mut intent = json!({"action":"NEW","environment":"production",
            "client_order_id":"order1","symbol":"ETHUSDT","side":"BUY",
            "quantity":"0.1","basket_id":"basket1","basket_revision":revision,
            "basket_request_id":"req-basket","account_revision":1});
        let guard = BasketAdmission {
            basket_id: "basket1",
            basket_revision: &revision,
            account_revision: 1,
        };
        let fingerprint = "b".repeat(64);
        assert!(
            book.preregister_basket_order_intent(
                "t",
                "a",
                "production",
                "wrong-request",
                "order1",
                &fingerprint,
                &intent,
                guard
            )
            .is_err()
        );
        assert!(book.request("t", "a", "wrong-request").unwrap().is_none());
        // The basket begin event must bind to the order request ID.
        intent["basket_request_id"] = json!("req-basket");
        assert_eq!(
            book.preregister_basket_order_intent(
                "t",
                "a",
                "production",
                "req-basket",
                "order1",
                &fingerprint,
                &intent,
                guard
            )
            .unwrap(),
            IntentStart::Fresh
        );
        assert_eq!(
            book.preregister_basket_order_intent(
                "t",
                "a",
                "production",
                "req-basket",
                "order1",
                &fingerprint,
                &intent,
                guard
            )
            .unwrap(),
            IntentStart::ReconcileOnly
        );
        let newer = json!({"balances":{"USDT":"101"},"positions":{}});
        book.save_account_snapshot(
            "t",
            "a",
            "production",
            Some(1),
            "signed2",
            2,
            "signed_exchange_account_query",
            &newer,
        )
        .unwrap();
        let mut second = intent.clone();
        second["client_order_id"] = json!("order2");
        assert!(
            book.preregister_basket_order_intent(
                "t",
                "a",
                "production",
                "req-basket",
                "order2",
                &fingerprint,
                &second,
                guard
            )
            .is_err()
        );
    }

    #[test]
    fn recovered_unknown_order_survives_file_restart_without_new_send_grant() {
        let path = std::env::temp_dir().join(format!(
            "seealpha-recovery-{}-{:?}.sqlite",
            std::process::id(),
            std::thread::current().id(),
        ));
        let _ = std::fs::remove_file(&path);
        let intent = new_intent();
        {
            let ledger = Ledger::open(&path).unwrap();
            ledger.ensure_tenant("t", "production", "active").unwrap();
            assert_eq!(
                ledger
                    .preregister_order_intent(
                        "t",
                        "a",
                        "production",
                        "req1",
                        "cl1",
                        &"a".repeat(64),
                        &intent
                    )
                    .unwrap(),
                IntentStart::Fresh
            );
        }
        let reopened = Ledger::open(&path).unwrap();
        let page = reopened.order_recovery_page("t", 0, 100).unwrap();
        assert_eq!(page.len(), 1);
        assert!(page[0].reconciliation_only);
        assert_eq!(page[0].last_verified_status, None);
        assert_eq!(
            reopened
                .preregister_order_intent(
                    "t",
                    "a",
                    "production",
                    "req1",
                    "cl1",
                    &"a".repeat(64),
                    &intent
                )
                .unwrap(),
            IntentStart::ReconcileOnly
        );
        drop(reopened);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn recovery_listing_is_read_only_scoped_and_restart_safe() {
        let book = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        book.ensure_tenant("t", "production", "active").unwrap();
        book.ensure_tenant("other", "production", "active").unwrap();
        let intent = new_intent();
        assert_eq!(
            book.preregister_order_intent(
                "t",
                "a",
                "production",
                "req1",
                "cl1",
                &"a".repeat(64),
                &intent
            )
            .unwrap(),
            IntentStart::Fresh
        );
        let first = book.order_recovery_page("t", 0, 10).unwrap();
        assert_eq!(first.len(), 1);
        assert!(first[0].reconciliation_only);
        assert_eq!(first[0].last_verified_status, None);
        assert!(book.order_recovery_page("other", 0, 10).unwrap().is_empty());
        assert!(book.order_recovery_page("t", 1, 10).unwrap().is_empty());
        let observed = json!({"environment":"production","client_order_id":"cl1",
            "status":"NEW","source":"signed_exchange_order_query",
            "observed_at_ns":10});
        book.append_order_observation("t", "a", "production", "cl1", "o1", &observed)
            .unwrap();
        let after = book.order_recovery_page("t", 0, 1).unwrap();
        assert_eq!(after[0].last_verified_status.as_deref(), Some("NEW"));
        assert_eq!(after[0].observed_at_ns, Some(10));
        assert!(after[0].reconciliation_only);
        assert!(book.order_recovery_page("t", 0, 501).is_err());
        assert_eq!(
            book.preregister_order_intent(
                "t",
                "a",
                "production",
                "req1",
                "cl1",
                &"a".repeat(64),
                &intent
            )
            .unwrap(),
            IntentStart::ReconcileOnly
        );
    }

    #[test]
    fn parallel_registration_has_exactly_one_initial_send_permit() {
        let ledger = Arc::new(Ledger::open(std::path::Path::new(":memory:")).unwrap());
        ledger.ensure_tenant("t", "production", "active").unwrap();
        let handles: Vec<_> = (0..12)
            .map(|_| {
                let l = ledger.clone();
                thread::spawn(move || {
                    l.preregister_order_intent(
                        "t",
                        "a",
                        "production",
                        "req1",
                        "cl1",
                        &"a".repeat(64),
                        &new_intent(),
                    )
                    .unwrap()
                })
            })
            .collect();
        let results: Vec<_> = handles.into_iter().map(|x| x.join().unwrap()).collect();
        assert_eq!(
            results.iter().filter(|x| **x == IntentStart::Fresh).count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|x| **x == IntentStart::ReconcileOnly)
                .count(),
            11
        );
        assert!(ledger.request("t", "a", "req1").unwrap().unwrap()["reconcile_required"] == true);
        assert!(ledger.request("other", "a", "req1").unwrap().is_none());
        assert!(
            ledger
                .preregister_order_intent(
                    "t",
                    "a",
                    "sandbox",
                    "req1",
                    "cl1",
                    &"a".repeat(64),
                    &new_intent()
                )
                .is_err()
        );
        assert!(
            ledger
                .preregister_order_intent(
                    "t",
                    "a",
                    "production",
                    "different-request",
                    "cl1",
                    &"c".repeat(64),
                    &new_intent()
                )
                .is_err()
        );
        let mut imprecise = new_intent();
        imprecise["price"] = json!(1024.00000001);
        assert!(
            ledger
                .preregister_order_intent(
                    "t",
                    "a",
                    "production",
                    "r2",
                    "cl1",
                    &"b".repeat(64),
                    &imprecise
                )
                .is_err()
        );
    }

    #[test]
    fn partial_and_complete_orders_require_exact_verified_decimal_sum() {
        let book = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        book.ensure_tenant("t", "production", "active").unwrap();
        let intent = json!({"action":"NEW","environment":"production","client_order_id":"c3",
            "symbol":"ETHUSDT","side":"BUY","quantity":"0.3"});
        book.preregister_order_intent(
            "t",
            "a",
            "production",
            "req3",
            "c3",
            &"d".repeat(64),
            &intent,
        )
        .unwrap();
        let t1 = json!({"trade_id":"binance:futures_um:ETHUSDT:301","venue_order_id":"100",
            "quantity":"0.1","price":"100","source":"signed_exchange_user_trades"});
        let t2 = json!({"trade_id":"binance:futures_um:ETHUSDT:302","venue_order_id":"100",
            "quantity":"0.2","price":"100","source":"signed_exchange_user_trades"});
        book.import_verified_fills("t", "a", "production", "100", &[t1, t2])
            .unwrap();
        let mut observed = json!({"environment":"production","client_order_id":"c3",
            "status":"PARTIALLY_FILLED","source":"signed_exchange_order_query",
            "observed_at_ns":1,"venue_order_id":"100",
            "trade_ids":["binance:futures_um:ETHUSDT:301"]});
        book.append_order_observation("t", "a", "production", "c3", "partial", &observed)
            .unwrap();
        observed["status"] = json!("FILLED");
        observed["observed_at_ns"] = json!(2);
        assert!(
            book.append_order_observation("t", "a", "production", "c3", "early", &observed)
                .is_err()
        );
        observed["trade_ids"] = json!([
            "binance:futures_um:ETHUSDT:301",
            "binance:futures_um:ETHUSDT:302"
        ]);
        book.append_order_observation("t", "a", "production", "c3", "complete", &observed)
            .unwrap();
        assert_eq!(
            book.latest_order_observation("t", "a", "production", "c3")
                .unwrap()
                .unwrap()["status"],
            "FILLED"
        );
    }

    #[test]
    fn observed_partial_fills_cannot_disappear_on_later_snapshots() {
        let book = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        book.ensure_tenant("t", "production", "active").unwrap();
        let intent = json!({"action":"NEW","environment":"production","client_order_id":"client",
            "symbol":"ETHUSDT","side":"BUY","quantity":"0.3"});
        book.preregister_order_intent(
            "t",
            "a",
            "production",
            "request",
            "client",
            &"a".repeat(64),
            &intent,
        )
        .unwrap();
        let t1 = json!({"trade_id":"binance:futures_um:ETHUSDT:501","venue_order_id":"42",
            "quantity":"0.1","price":"100","source":"signed_exchange_user_trades"});
        let t2 = json!({"trade_id":"binance:futures_um:ETHUSDT:502","venue_order_id":"42",
            "quantity":"0.2","price":"100","source":"signed_exchange_user_trades"});
        book.import_verified_fills("t", "a", "production", "42", &[t1, t2])
            .unwrap();
        let base = json!({"environment":"production","client_order_id":"client",
            "status":"PARTIALLY_FILLED","source":"signed_exchange_order_query",
            "observed_at_ns":100,"venue_order_id":"42",
            "trade_ids":["binance:futures_um:ETHUSDT:501"]});
        book.append_order_observation("t", "a", "production", "client", "obs1", &base)
            .unwrap();
        let mut rollback = base.clone();
        rollback["observed_at_ns"] = json!(101);
        rollback["status"] = json!("NEW");
        assert!(
            book.append_order_observation("t", "a", "production", "client", "obs2", &rollback)
                .is_err()
        );
        rollback["status"] = json!("FILLED");
        rollback["trade_ids"] = json!(["binance:futures_um:ETHUSDT:502"]);
        assert!(
            book.append_order_observation("t", "a", "production", "client", "obs3", &rollback)
                .is_err()
        );
        rollback["trade_ids"] = json!([
            "binance:futures_um:ETHUSDT:501",
            "binance:futures_um:ETHUSDT:502"
        ]);
        book.append_order_observation("t", "a", "production", "client", "obs4", &rollback)
            .unwrap();
        assert_eq!(
            book.latest_order_observation("t", "a", "production", "client")
                .unwrap()
                .unwrap()["status"],
            "FILLED"
        );
    }

    #[test]
    fn ack_unknown_cannot_be_rewritten_into_a_fill() {
        let ledger = Ledger::open(std::path::Path::new(":memory:")).unwrap();
        ledger.ensure_tenant("t", "production", "active").unwrap();
        ledger
            .preregister_order_intent(
                "t",
                "a",
                "production",
                "r",
                "cl1",
                &"a".repeat(64),
                &new_intent(),
            )
            .unwrap();
        let observed = json!({"environment":"production","client_order_id":"cl1",
            "status":"NEW","source":"authenticated_user_stream","observed_at_ns":1});
        ledger
            .append_order_observation("t", "a", "production", "cl1", "o1", &observed)
            .unwrap();
        assert!(ledger.fills("t", "a", "production").unwrap().is_empty());
        let filled = json!({"environment":"production","client_order_id":"cl1",
            "status":"FILLED","source":"signed_exchange_order_query","observed_at_ns":2,
            "venue_order_id":"42","trade_ids":["binance:futures_um:ETHUSDT:99"]});
        assert!(
            ledger
                .append_order_observation("t", "a", "production", "cl1", "o2", &filled)
                .is_err()
        );
        let trade = json!({"trade_id":"binance:futures_um:ETHUSDT:99","venue_order_id":"42",
            "quantity":"0.00000001","price":"1024.00000001","source":"signed_exchange_user_trades"});
        ledger
            .import_verified_fills("t", "a", "production", "42", &[trade])
            .unwrap();
        ledger
            .append_order_observation("t", "a", "production", "cl1", "o2", &filled)
            .unwrap();
        ledger
            .append_order_observation("t", "a", "production", "cl1", "o2", &filled)
            .unwrap();
        assert_eq!(ledger.fills("t", "a", "production").unwrap().len(), 1);
        let mut contradictory = observed;
        contradictory["observed_at_ns"] = json!(3);
        assert!(
            ledger
                .append_order_observation("t", "a", "production", "cl1", "o3", &contradictory)
                .is_err()
        );
        assert_eq!(
            ledger
                .latest_order_observation("t", "a", "production", "cl1")
                .unwrap()
                .unwrap()["status"],
            "FILLED"
        );
    }
}
