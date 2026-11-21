//! Signed order commands. Credentials stay in the request and never enter SQLite.
use crate::{Runtime, RuntimeError, map_ledger};
use seealpha_venue::conditional::{UmConditionalIntent, validated_algo_view};
use seealpha_venue::spot_oco::SpotOcoIntent;
use seealpha_venue::{Credentials, OrderIntent, Product, VenueClient, VenueError};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

const UNKNOWN: &str = "请求已登记，交易结果待核实。请刷新委托，不要重复提交。";
const UNCONFIRMED: &str = "交易结果待核实，请刷新委托并按客户订单号核对，不要重复提交。";

fn account_locks() -> &'static Mutex<HashMap<String, std::sync::Arc<Mutex<()>>>> {
    static LOCKS: OnceLock<Mutex<HashMap<String, std::sync::Arc<Mutex<()>>>>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub struct OrderCommand {
    pub tenant_id: String,
    pub account_id: String,
    pub environment: String,
    pub api_key: String,
    pub secret: String,
    pub action: String,
    pub values: Value,
    pub venue_base: String,
}

impl Runtime {
    pub fn execute_order(&self, command: OrderCommand) -> Result<Value, RuntimeError> {
        // Migration gate: allow synthetic loopback fixtures only. In particular,
        // production tenant status must never authorize an external REST write.
        // This check intentionally precedes all intent/order state mutations.
        let loopback_only = loopback_fixture_url(&command.venue_base);
        if !loopback_only {
            return Err(RuntimeError::Conflict(
                "Rust 实盘订单发送尚未通过验收，禁止访问外部交易所。".into(),
            ));
        }
        let tenant = self.require_active(&command.tenant_id)?;
        if tenant.purpose != "production" {
            return Err(RuntimeError::Conflict("开发租户不能向交易所下单。".into()));
        }
        // Explicit native backend is staged durably, never dispatched through
        // the legacy Venue. Production/live host protection above still applies.
        if let Some(backend) = command.values.get("execution_backend") {
            if backend.as_str() != Some("nautilus_binance_native") {
                return Err(RuntimeError::Unprocessable("未知的订单执行后端。".into()));
            }
            return crate::native_handoff::stage_native_order_command(self, &command);
        }
        if !matches!(command.action.as_str(), "submit" | "cancel") {
            return Err(RuntimeError::Unprocessable(
                "交易操作或账户环境不匹配。".into(),
            ));
        }
        let values_env = command
            .values
            .get("environment")
            .and_then(Value::as_str)
            .unwrap_or("");
        if values_env != command.environment {
            return Err(RuntimeError::Unprocessable(
                "交易操作或账户环境不匹配。".into(),
            ));
        }
        let request_id = command
            .values
            .get("request_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if request_id.len() != 36 {
            return Err(RuntimeError::Unprocessable("请求编号无效。".into()));
        }
        let symbol = command
            .values
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if symbol.is_empty() {
            return Err(RuntimeError::Unprocessable("合约无效。".into()));
        }
        let fingerprint = fingerprint(&command.action, &command.values);
        let client_order_id = if command.action == "submit" {
            format!("SA{}", request_id.replace('-', ""))
        } else {
            command
                .values
                .get("client_order_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };
        let lock_key = format!(
            "{}:{}:{}",
            command.tenant_id, command.account_id, command.environment
        );
        let guard = account_guard(&lock_key)?;
        let _held = guard.try_lock().map_err(|_| {
            RuntimeError::Conflict("该账户有交易操作正在处理，请稍后核对委托。".into())
        })?;
        if let Some(existing) = self
            .ledger
            .get_order_request(&command.tenant_id, &command.account_id, &request_id)
            .map_err(map_ledger)?
        {
            let mut stored: Value = serde_json::from_str(&existing)
                .map_err(|_| RuntimeError::BadRequest("委托记录损坏。".into()))?;
            if stored.get("fingerprint").and_then(Value::as_str) != Some(fingerprint.as_str()) {
                return Err(RuntimeError::Conflict("请求编号已用于另一笔操作。".into()));
            }
            if let Some(object) = stored.as_object_mut() {
                object.insert("replay".into(), Value::Bool(true));
            }
            return Ok(stored);
        }
        let mut body = json!({
            "fingerprint": fingerprint,
            "response": {
                "request_id": request_id,
                "client_order_id": client_order_id,
                "status": "unknown",
                "order": null,
                "message": UNKNOWN,
            },
            "symbol": symbol,
            "action": command.action,
            "environment": command.environment,
            "product": command.values.get("product").cloned().unwrap_or(Value::Null),
            "records": [],
            "ledger_synced": false,
        });
        // Validate the instrument and record routing before the only pre-send
        // immutable UNKNOWN insert. Invalid inputs must not leave a send record.
        let product = match command.values.get("product").and_then(Value::as_str) {
            Some("futures_um") => Product::FuturesUm,
            Some("spot") => Product::Spot,
            Some("options") => Product::Option,
            Some(_) => return Err(RuntimeError::Unprocessable("交易产品无效。".into())),
            None if symbol.ends_with("-PERP") => Product::FuturesUm,
            None if seealpha_market::options::parse_option_symbol(&symbol).is_ok() => {
                Product::Option
            }
            None => Product::Spot,
        };
        if (product == Product::Option
            && seealpha_market::options::parse_option_symbol(&symbol).is_err())
            || (product == Product::Spot && symbol.contains('-'))
            || (product == Product::FuturesUm && symbol.contains('-') && !symbol.ends_with("-PERP"))
        {
            return Err(RuntimeError::Unprocessable(
                "交易合约与产品类型不匹配。".into(),
            ));
        }
        let order_type = command
            .values
            .get("order_type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_uppercase();
        let conditional = product == Product::FuturesUm
            && (command.values.get("algo_order").and_then(Value::as_bool) == Some(true)
                || matches!(
                    order_type.as_str(),
                    "STOP"
                        | "STOP_MARKET"
                        | "TAKE_PROFIT"
                        | "TAKE_PROFIT_MARKET"
                        | "TRAILING_STOP_MARKET"
                ));
        let spot_oco = product == Product::Spot
            && command.values.get("order_list").and_then(Value::as_str) == Some("oco");
        body["order_list"] = if spot_oco { json!("oco") } else { Value::Null };
        body["algo_order"] = json!(conditional);
        // Persist the recovery routing BEFORE the first network write.
        self.ledger
            .save_order_request(
                &command.tenant_id,
                &command.account_id,
                &request_id,
                &body.to_string(),
            )
            .map_err(map_ledger)?;
        let venue = VenueClient::new(&command.venue_base)
            .map_err(|error| RuntimeError::Conflict(error.to_string()))?;
        let credentials = Credentials {
            api_key: command.api_key,
            secret: command.secret,
        };
        let outcome = if spot_oco && command.action == "cancel" {
            venue.cancel_spot_oco(&credentials, &symbol, &client_order_id)
        } else if spot_oco {
            venue.place_spot_oco(
                &credentials,
                &SpotOcoIntent {
                    symbol: symbol.clone(),
                    side: command.values["side"]
                        .as_str()
                        .unwrap_or("")
                        .to_ascii_uppercase(),
                    quantity: command.values["quantity"].as_str().unwrap_or("").into(),
                    list_client_order_id: client_order_id.clone(),
                    above_type: command.values["above_type"]
                        .as_str()
                        .unwrap_or("")
                        .to_ascii_uppercase(),
                    below_type: command.values["below_type"]
                        .as_str()
                        .unwrap_or("")
                        .to_ascii_uppercase(),
                    above_price: command.values["above_price"].as_str().map(str::to_owned),
                    above_stop_price: command.values["above_stop_price"]
                        .as_str()
                        .map(str::to_owned),
                    below_price: command.values["below_price"].as_str().map(str::to_owned),
                    below_stop_price: command.values["below_stop_price"]
                        .as_str()
                        .map(str::to_owned),
                    above_time_in_force: command.values["above_time_in_force"]
                        .as_str()
                        .map(str::to_owned),
                    below_time_in_force: command.values["below_time_in_force"]
                        .as_str()
                        .map(str::to_owned),
                },
            )
        } else if conditional && command.action == "cancel" {
            venue.cancel_um_conditional(&credentials, &symbol, &client_order_id)
        } else if conditional {
            venue.place_um_conditional(
                &credentials,
                &UmConditionalIntent {
                    symbol: symbol.clone(),
                    client_algo_id: client_order_id.clone(),
                    side: command.values["side"]
                        .as_str()
                        .unwrap_or("")
                        .to_ascii_uppercase(),
                    order_type: order_type.clone(),
                    quantity: command.values["quantity"].as_str().map(str::to_owned),
                    trigger_price: command.values["trigger_price"].as_str().map(str::to_owned),
                    limit_price: command.values["price"].as_str().map(str::to_owned),
                    activate_price: command.values["activate_price"].as_str().map(str::to_owned),
                    callback_rate: command.values["callback_rate"].as_str().map(str::to_owned),
                    position_side: command.values["position_side"]
                        .as_str()
                        .unwrap_or("BOTH")
                        .to_ascii_uppercase(),
                    working_type: command.values["working_type"]
                        .as_str()
                        .unwrap_or("CONTRACT_PRICE")
                        .to_ascii_uppercase(),
                    time_in_force: command.values["time_in_force"]
                        .as_str()
                        .unwrap_or("GTC")
                        .to_ascii_uppercase(),
                    reduce_only: command.values["reduce_only"].as_bool().unwrap_or(false),
                    close_position: command.values["close_position"].as_bool().unwrap_or(false),
                },
            )
        } else if command.action == "cancel" {
            venue.cancel_order(&credentials, product, &symbol, &client_order_id)
        } else {
            venue.place_order(
                &credentials,
                &OrderIntent {
                    product,
                    symbol: symbol.clone(),
                    side: command
                        .values
                        .get("side")
                        .and_then(Value::as_str)
                        .unwrap_or("buy")
                        .into(),
                    order_type: command
                        .values
                        .get("order_type")
                        .and_then(Value::as_str)
                        .unwrap_or("limit")
                        .into(),
                    quantity: command
                        .values
                        .get("quantity")
                        .and_then(Value::as_str)
                        .unwrap_or("0")
                        .into(),
                    price: command
                        .values
                        .get("price")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    client_order_id: client_order_id.clone(),
                    reduce_only: command
                        .values
                        .get("reduce_only")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    time_in_force: command
                        .values
                        .get("time_in_force")
                        .and_then(Value::as_str)
                        .unwrap_or("GTC")
                        .into(),
                },
            )
        };
        match outcome {
            Ok(order) => {
                body["response"]["status"] = json!("completed");
                body["response"]["order"] = order.clone();
                body["response"]["message"] = json!(format!(
                    "交易所订单状态：{}",
                    order["status"].as_str().unwrap_or("NEW")
                ));
                self.ledger
                    .upsert_order_row(
                        &command.tenant_id,
                        &command.account_id,
                        &command.environment,
                        &client_order_id,
                        &order.to_string(),
                    )
                    .map_err(map_ledger)?;
                // Acknowledged or newly opened venue orders are NOT fills.
                // We must only journal exchange-authored trade IDs from the
                // authenticated trade-history or user-stream reconciliation.
                // The previous placeholder fill-{client_order_id} fabricated
                // executions for NEW and PARTIALLY_FILLED orders.
                body["records"] = json!([]);
                self.ledger
                    .add_event(
                        &command.tenant_id,
                        &command.account_id,
                        "order_acknowledged",
                        &json!({"client_order_id": client_order_id,
                            "status": order["status"],
                            "executed_qty_reported": order["filled"],
                            "fills_verified": false})
                        .to_string(),
                    )
                    .map_err(map_ledger)?;
            }
            Err(VenueError::Rejected(message)) => {
                body["response"]["status"] = json!("completed");
                body["response"]["message"] = json!(message);
            }
            Err(VenueError::SpotReduceOnly) => {
                body["response"]["status"] = json!("completed");
                body["response"]["message"] = json!("现货不支持 Reduce Only。");
            }
            Err(VenueError::Unconfirmed)
            | Err(VenueError::Credentials)
            | Err(VenueError::Symbol) => {
                body["response"]["message"] = json!(UNCONFIRMED);
            }
        }
        drop(credentials);
        // The pre-send request is immutable. Store the confirmed send result as
        // a separate append-only projection for idempotent GET/replay.
        self.ledger
            .append_order_request_view(
                &command.tenant_id,
                &command.account_id,
                &request_id,
                &body.to_string(),
            )
            .map_err(map_ledger)?;
        Ok(body)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reconcile_order(
        &self,
        tenant_id: &str,
        account_id: &str,
        request_id: &str,
        environment: &str,
        api_key: &str,
        secret: &str,
        venue_base: &str,
        account_revision: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        let Some(existing) = self
            .ledger
            .get_order_request(tenant_id, account_id, request_id)
            .map_err(map_ledger)?
        else {
            return Err(RuntimeError::NotFound("没有该请求".into()));
        };
        let mut stored: Value = serde_json::from_str(&existing)
            .map_err(|_| RuntimeError::BadRequest("委托记录损坏。".into()))?;
        // Native-staged intents are not legacy REST requests. Never attempt
        // recovery through VenueClient, including loopback, until native
        // authenticated execution evidence reconciliation is implemented.
        if stored["intent"]["execution_backend"] == "nautilus_binance_native" {
            return Err(RuntimeError::Conflict(
                "原生执行订单仅允许原生取证对账；该恢复链路尚未接入。".into(),
            ));
        }
        if stored["environment"] != environment {
            return Err(RuntimeError::Conflict("账户环境与原始委托不一致。".into()));
        }
        let prior_view = stored.clone();
        if stored["response"]["status"] == "unknown" {
            let symbol = stored["symbol"].as_str().unwrap_or("").to_string();
            let client_order_id = stored["response"]["client_order_id"]
                .as_str()
                .unwrap_or("")
                .to_string();
            let product = match stored.get("product").and_then(Value::as_str) {
                Some("futures_um") => Product::FuturesUm,
                Some("options") => Product::Option,
                Some("spot") => Product::Spot,
                _ if symbol.ends_with("-PERP") => Product::FuturesUm,
                _ if seealpha_market::options::parse_option_symbol(&symbol).is_ok() => {
                    Product::Option
                }
                _ => Product::Spot,
            };
            if let Ok(client) = VenueClient::new(venue_base)
                && let Ok(Some(order)) = (if stored["order_list"] == "oco" {
                    client
                        .query_spot_oco(
                            &Credentials {
                                api_key: api_key.into(),
                                secret: secret.into(),
                            },
                            &symbol,
                            &client_order_id,
                        )
                        .map(Some)
                } else if stored["algo_order"] == true {
                    client
                        .query_um_conditional(
                            &Credentials {
                                api_key: api_key.into(),
                                secret: secret.into(),
                            },
                            &symbol,
                            &client_order_id,
                        )
                        .and_then(|v| {
                            seealpha_venue::venue_symbol(&symbol).and_then(|canonical| {
                                validated_algo_view(&v, &client_order_id, &canonical)
                            })
                        })
                        .map(Some)
                } else {
                    client.query_order(
                        &Credentials {
                            api_key: api_key.into(),
                            secret: secret.into(),
                        },
                        product,
                        &symbol,
                        &client_order_id,
                    )
                })
            {
                stored["response"]["status"] = json!("completed");
                stored["response"]["order"] = order.clone();
                stored["response"]["message"] = json!(format!(
                    "已在交易所核对到委托：{}",
                    order["status"].as_str().unwrap_or("")
                ));
                // A signed GET may show a later status than the original
                // immutable ACK. Append evidence; never overwrite that ACK.
                self.ledger
                    .add_event(
                        tenant_id,
                        account_id,
                        "signed_order_query_observed",
                        &json!({"request_id":request_id,"client_order_id":client_order_id,
                            "environment":environment,"source":"signed_exchange_order_query",
                            "order":order,"fills_verified":false})
                        .to_string(),
                    )
                    .map_err(map_ledger)?;
            }
            if stored != prior_view {
                self.ledger
                    .append_order_request_view(
                        tenant_id,
                        account_id,
                        request_id,
                        &stored.to_string(),
                    )
                    .map_err(map_ledger)?;
            }
        }
        let _ = account_revision;
        Ok(stored)
    }

    pub fn mark_ledger_synced(
        &self,
        tenant_id: &str,
        account_id: &str,
        request_id: &str,
    ) -> Result<Value, RuntimeError> {
        let Some(existing) = self
            .ledger
            .get_order_request(tenant_id, account_id, request_id)
            .map_err(map_ledger)?
        else {
            return Err(RuntimeError::NotFound("没有该请求".into()));
        };
        let mut stored: Value = serde_json::from_str(&existing)
            .map_err(|_| RuntimeError::BadRequest("委托记录损坏。".into()))?;
        if stored["order_list"] == "oco" {
            if stored["fills_complete"] != true
                || stored["ledger_synced"] != true
                || stored["order_children"]
                    .as_array()
                    .is_none_or(|c| c.len() != 2)
                || stored["response"]["order"]["list_status"] != "ALL_DONE"
            {
                return Err(RuntimeError::Conflict(
                    "OCO 列表及两条子订单成交尚未全部核实，不能标记完成。".into(),
                ));
            }
            return Ok(stored);
        }
        let order = if stored["algo_order"] == true {
            &stored["triggered_order"]
        } else {
            &stored["response"]["order"]
        };
        if stored["response"]["status"] != "completed" || order.is_null() {
            return Err(RuntimeError::Conflict(
                "订单尚未在交易所核实，不能标记已对账。".into(),
            ));
        }
        let terminal = matches!(
            order["status"].as_str(),
            Some("FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED")
        );
        if !terminal {
            return Err(RuntimeError::Conflict(
                "订单仍可能继续成交，不能将开放委托标记为全部对账。".into(),
            ));
        }
        let filled = order["filled"]
            .as_str()
            .ok_or_else(|| RuntimeError::Conflict("订单累计成交量未核实。".into()))?
            .parse::<rust_decimal::Decimal>()
            .map_err(|_| RuntimeError::Conflict("订单累计成交量无效。".into()))?;
        if filled < rust_decimal::Decimal::ZERO
            || (filled > rust_decimal::Decimal::ZERO && stored["fills_complete"] != true)
        {
            return Err(RuntimeError::Conflict(
                "订单已有成交但交易所 Trade ID 尚未完整核对。".into(),
            ));
        }
        stored["ledger_synced"] = json!(true);
        self.ledger
            .append_order_request_view(tenant_id, account_id, request_id, &stored.to_string())
            .map_err(map_ledger)?;
        Ok(stored)
    }
}

// Numeric loopback URLs only; reject remote hosts and deceptive authorities.
fn loopback_fixture_url(value: &str) -> bool {
    value
        .strip_prefix("http://")
        .and_then(|rest| rest.split(['/', '?', '#']).next())
        .and_then(|authority| authority.parse::<std::net::SocketAddr>().ok())
        .is_some_and(|address| address.ip().is_loopback())
}

fn fingerprint(action: &str, values: &Value) -> String {
    let payload = json!([action, values]);
    let text = serde_json::to_string(&payload).unwrap_or_default();
    hex_encode(Sha256::digest(text.as_bytes()))
}

fn account_guard(key: &str) -> Result<std::sync::Arc<Mutex<()>>, RuntimeError> {
    let mut locks = account_locks()
        .lock()
        .map_err(|_| RuntimeError::Conflict("该账户有交易操作正在处理，请稍后核对委托。".into()))?;
    Ok(locks
        .entry(key.to_string())
        .or_insert_with(|| std::sync::Arc::new(Mutex::new(())))
        .clone())
}

pub(super) fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.as_ref().len() * 2);
    for byte in bytes.as_ref() {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, Runtime};
    use axum::extract::Request;
    use serde_json::json;

    #[test]
    fn trading_fixture_url_must_be_numeric_loopback() {
        assert!(loopback_fixture_url("http://127.0.0.1:18080"));
        assert!(loopback_fixture_url("http://[::1]:18080"));
        for invalid in [
            "https://127.0.0.1:18080",
            "http://localhost:18080",
            "http://127.0.0.1:18080@binance.com",
            "http://127.0.0.1.evil.test:18080",
            "http://8.8.8.8:18080",
            "https://fapi.binance.com",
        ] {
            assert!(
                !loopback_fixture_url(invalid),
                "unexpected fixture URL: {invalid}"
            );
        }
    }

    #[test]
    fn legacy_write_gate_never_sends_and_replay_keeps_secrets_out_of_sqlite() {
        let (sender, receiver) = std::sync::mpsc::channel();
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = hits.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let app = axum::Router::new().route(
                    "/fapi/v1/order",
                    axum::routing::post(move |_request: Request| {
                        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        async move {
                            axum::Json(json!({
                                "orderId": 7,
                                "clientOrderId": "SA00000000000000000000000000000001",
                                "symbol": "ETHUSDT",
                                "status": "NEW",
                                "side": "BUY",
                                "type": "LIMIT",
                                "origQty": "0.01",
                                "executedQty": "0",
                                "price": "2000"
                            }))
                        }
                    }),
                );
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                sender.send(listener.local_addr().unwrap().port()).unwrap();
                axum::serve(listener, app).await.unwrap();
            });
        });
        let port = receiver.recv().unwrap();
        let root = std::env::temp_dir().join(format!("seealpha-trade-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let config = Config {
            host: "127.0.0.1".into(),
            port: 0,
            auth_mode: crate::AuthMode::None,
            token: String::new(),
            state_dir: root.join("state"),
            project: root.clone(),
            tenant_id: None,
            tenant_purpose: None,
            research_signing_key: None,
            research_mcp_enabled: false,
            venue_base: Some(format!("http://127.0.0.1:{port}")),
            python: std::path::PathBuf::from("python3"),
        };
        let runtime = Runtime::open(&config).unwrap();
        runtime.ensure_tenant("7", "production", "active").unwrap();
        let values = json!({
            "request_id": "00000000-0000-0000-0000-000000000001",
            "symbol": "ETHUSDT-PERP",
            "environment": "production",
            "side": "buy",
            "order_type": "limit",
            "quantity": "0.01",
            "price": "2000"
        });
        let command = || OrderCommand {
            tenant_id: "7".into(),
            account_id: "11".into(),
            environment: "production".into(),
            api_key: "key-one".into(),
            secret: "secret-one".into(),
            action: "submit".into(),
            values: values.clone(),
            venue_base: format!("http://127.0.0.1:{port}"),
        };
        let first = runtime.execute_order(command()).unwrap();
        assert_eq!(first["response"]["status"], "completed");
        assert!(first["response"]["order"].is_null());
        assert!(
            first["response"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("持久化订单意图登记门禁")
        );
        // The legacy transport intentionally forbids all POST/DELETE writes.
        // An immutable order request is recorded, but no network send occurs.
        // NEW/ACK is not a fill. Only signed userTrades may enter fills.
        assert!(
            runtime
                .ledger
                .fills("7", "11", "production")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            first["response"]["client_order_id"],
            "SA00000000000000000000000000000001"
        );
        let second = runtime.execute_order(command()).unwrap();
        assert_eq!(second["replay"], true);
        assert_eq!(
            second["response"]["client_order_id"],
            first["response"]["client_order_id"]
        );
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
        let mut changed = values.clone();
        changed["quantity"] = json!("0.02");
        let rejected = runtime.execute_order(OrderCommand {
            values: changed,
            ..command()
        });
        assert!(rejected.is_err());
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
        let mut blob = Vec::new();
        for entry in std::fs::read_dir(&config.state_dir).unwrap() {
            let entry = entry.unwrap();
            if let Ok(bytes) = std::fs::read(entry.path()) {
                blob.extend(bytes);
            }
        }
        assert!(!blob.windows(10).any(|window| window == b"secret-one"));
        assert!(!blob.windows(7).any(|window| window == b"key-one"));
        let _ = std::fs::remove_dir_all(root);
    }
}
