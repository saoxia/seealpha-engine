//! Signed order commands. Credentials stay in the request and never enter SQLite.
use crate::{Runtime, RuntimeError, map_ledger};
use seealpha_venue::conditional::validated_algo_view;
use seealpha_venue::{Credentials, Product, VenueClient};
use serde_json::{Value, json};

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
    /// Only the explicit Nautilus-native backend may stage a new order.
    /// There is deliberately no fallback to the legacy REST sender.
    /// Native dispatch remains disabled until durable pre-send authorization
    /// and native execution evidence are accepted end to end.
    pub fn execute_order(&self, command: OrderCommand) -> Result<Value, RuntimeError> {
        // Preserve the existing fail-closed external-host gate. No real
        // exchange address is reachable before production cutover.
        if !loopback_fixture_url(&command.venue_base) {
            return Err(RuntimeError::Conflict(
                "Rust 实盘订单发送尚未通过验收，禁止访问外部交易所。".into(),
            ));
        }
        let tenant = self.require_active(&command.tenant_id)?;
        if tenant.purpose != "production" {
            return Err(RuntimeError::Conflict("开发租户不能向交易所下单。".into()));
        }
        if command.values.get("execution_backend").and_then(Value::as_str)
            != Some("nautilus_binance_native")
        {
            return Err(RuntimeError::Conflict(
                "旧 Binance REST 订单写入已停用；请使用明确的 Nautilus 原生执行后端。".into(),
            ));
        }
        crate::native_handoff::stage_native_order_command(self, &command)
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
    fn legacy_order_write_is_rejected_before_any_ledger_or_network_effect() {
        let root = std::env::temp_dir().join(format!(
            "seealpha-retired-venue-{}-{:?}",
            std::process::id(),
            std::thread::current().id(),
        ));
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
            venue_base: Some("http://127.0.0.1:18080".into()),
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
        for action in ["submit", "cancel"] {
            let result = runtime.execute_order(OrderCommand {
                tenant_id: "7".into(),
                account_id: "11".into(),
                environment: "production".into(),
                api_key: "key-one".into(),
                secret: "secret-one".into(),
                action: action.into(),
                values: values.clone(),
                venue_base: "http://127.0.0.1:18080".into(),
            });
            assert!(result.is_err(), "legacy {action} must be disabled");
        }
        assert!(runtime.ledger.get_order_request(
            "7", "11", "00000000-0000-0000-0000-000000000001",
        ).unwrap().is_none());
        assert!(runtime.ledger.fills("7", "11", "production").unwrap().is_empty());
        assert!(runtime.ledger.order_recovery_page("7", 0, 10).unwrap().is_empty());

        let mut blob = Vec::new();
        for entry in std::fs::read_dir(&config.state_dir).unwrap() {
            let entry = entry.unwrap();
            if let Ok(bytes) = std::fs::read(entry.path()) {
                blob.extend(bytes);
            }
        }
        assert!(!blob.windows(10).any(|window| window == b"secret-one"));
        assert!(!blob.windows(7).any(|window| window == b"key-one"));
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }
}
