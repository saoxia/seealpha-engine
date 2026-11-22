//! Native-only order entry and conservative ledger completion checks.
use crate::{Runtime, RuntimeError, map_ledger};
use serde_json::Value;

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

    /// Read-only request history for legacy API consumers. This does not
    /// authorize a send, settlement, or native account reconciliation.
    pub fn order_request_view(
        &self,
        tenant_id: &str,
        account_id: &str,
        request_id: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        let raw = self.state_store.get_order_request(tenant_id, account_id, request_id)
            .map_err(map_ledger)?
            .ok_or_else(|| RuntimeError::NotFound("没有该订单请求记录。".into()))?;
        serde_json::from_str(&raw)
            .map_err(|_| RuntimeError::Conflict("订单请求记录损坏。".into()))
    }

    /// Only the native authenticated event stream can reconcile native orders.
    /// No implicit retry, no custom REST recovery, no invented fills.
    #[allow(clippy::too_many_arguments)]
    pub fn reconcile_order(
        &self,
        tenant_id: &str,
        account_id: &str,
        request_id: &str,
        environment: &str,
        _api_key: &str,
        _secret: &str,
        _venue_base: &str,
        _account_revision: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        let Some(intent) = self.state_store
            .registered_order_intent(tenant_id, account_id, request_id)
            .map_err(map_ledger)?
        else {
            return Err(RuntimeError::NotFound("没有已登记的原生订单意图。".into()));
        };
        if intent["execution_backend"] != "nautilus_binance_native"
            || intent["environment"].as_str() != Some(environment)
        {
            return Err(RuntimeError::Conflict("订单不属于原生执行后端或环境不匹配。".into()));
        }
        Err(RuntimeError::Conflict(
            "Nautilus 原生订单认证状态和真实成交对账尚未接通，不得使用旧 REST 查询恢复。".into(),
        ))
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
        assert!(runtime.state_store.get_order_request(
            "7", "11", "00000000-0000-0000-0000-000000000001",
        ).unwrap().is_none());
        assert!(runtime.ledger.fills("7", "11", "production").unwrap().is_empty());
        assert!(runtime.state_store.order_recovery_page("7", 0, 10).unwrap().is_empty());

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
