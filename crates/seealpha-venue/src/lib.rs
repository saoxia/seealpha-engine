//! Signed Binance REST connector.
//!
//! Callers pass the base URL. Nothing in this crate selects a production host.
//! Credentials are used only to build the request and are not returned or logged.
pub mod options;
pub mod options_rules;

use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum VenueError {
    #[error("交易账户凭据无效。")]
    Credentials,
    #[error("不支持的交易账户环境或合约。")]
    Symbol,
    #[error("现货不支持 Reduce Only。")]
    SpotReduceOnly,
    #[error("{0}")]
    Rejected(String),
    #[error("交易结果待核实，请刷新委托并按客户订单号核对，不要重复提交。")]
    Unconfirmed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Product {
    FuturesUm,
    Spot,
    Option,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderIntent {
    pub product: Product,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub quantity: String,
    pub price: Option<String>,
    pub client_order_id: String,
    pub reduce_only: bool,
    pub time_in_force: String,
}

#[derive(Debug, Clone)]
pub struct Credentials {
    pub api_key: String,
    pub secret: String,
}

pub struct VenueClient {
    http: reqwest::blocking::Client,
    base: String,
}

impl VenueClient {
    pub fn new(base: impl Into<String>) -> Result<Self, VenueError> {
        let base = base.into().trim_end_matches('/').to_string();
        if !(base.starts_with("http://127.0.0.1:") || base.starts_with("http://localhost:"))
            && !base.starts_with("https://")
        {
            return Err(VenueError::Rejected("交易网关地址无效。".into()));
        }
        let http = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(8))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("seealpha-engine/0.1")
            .build()
            .map_err(|_| VenueError::Unconfirmed)?;
        Ok(Self { http, base })
    }

    pub fn place_order(
        &self,
        credentials: &Credentials,
        intent: &OrderIntent,
    ) -> Result<Value, VenueError> {
        check_credentials(credentials)?;
        if intent.product == Product::Spot && intent.reduce_only {
            return Err(VenueError::SpotReduceOnly);
        }
        let path = match intent.product {
            Product::FuturesUm => "/fapi/v1/order",
            Product::Spot => "/api/v3/order",
            Product::Option => "/eapi/v1/order",
        };
        let mut pairs = vec![
            ("symbol".to_string(), venue_symbol(&intent.symbol)?),
            ("side".to_string(), intent.side.to_ascii_uppercase()),
            ("type".to_string(), intent.order_type.to_ascii_uppercase()),
            ("quantity".to_string(), intent.quantity.clone()),
            (
                "newClientOrderId".to_string(),
                intent.client_order_id.clone(),
            ),
        ];
        if intent.order_type.eq_ignore_ascii_case("limit") {
            let price = intent
                .price
                .clone()
                .ok_or_else(|| VenueError::Rejected("限价单需要有效价格。".into()))?;
            pairs.push(("price".to_string(), price));
            pairs.push(("timeInForce".to_string(), intent.time_in_force.clone()));
        }
        if intent.product == Product::FuturesUm {
            pairs.push((
                "reduceOnly".to_string(),
                if intent.reduce_only { "true" } else { "false" }.into(),
            ));
        }
        let value = self.signed(credentials, "POST", path, &pairs)?;
        Ok(order_view(&value, &intent.client_order_id))
    }

    pub fn cancel_order(
        &self,
        credentials: &Credentials,
        product: Product,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<Value, VenueError> {
        check_credentials(credentials)?;
        let path = match product {
            Product::FuturesUm => "/fapi/v1/order",
            Product::Spot => "/api/v3/order",
            Product::Option => "/eapi/v1/order",
        };
        let pairs = vec![
            ("symbol".to_string(), venue_symbol(symbol)?),
            ("origClientOrderId".to_string(), client_order_id.to_string()),
        ];
        let value = self.signed(credentials, "DELETE", path, &pairs)?;
        Ok(order_view(&value, client_order_id))
    }

    pub fn query_order(
        &self,
        credentials: &Credentials,
        product: Product,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<Option<Value>, VenueError> {
        check_credentials(credentials)?;
        let path = match product {
            Product::FuturesUm => "/fapi/v1/order",
            Product::Spot => "/api/v3/order",
            Product::Option => "/eapi/v1/order",
        };
        let pairs = vec![
            ("symbol".to_string(), venue_symbol(symbol)?),
            ("origClientOrderId".to_string(), client_order_id.to_string()),
        ];
        match self.signed(credentials, "GET", path, &pairs) {
            Ok(value) => Ok(Some(order_view(&value, client_order_id))),
            Err(VenueError::Rejected(message))
                if message.contains("-2013") || message.contains("not found") =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Signed account read. The returned JSON keeps balances and positions only.
    pub fn account_snapshot(&self, credentials: &Credentials) -> Result<Value, VenueError> {
        check_credentials(credentials)?;
        let value = self.signed(credentials, "GET", "/fapi/v2/account", &[])?;
        Ok(snapshot_view(&value))
    }

    fn signed(
        &self,
        credentials: &Credentials,
        method: &str,
        path: &str,
        pairs: &[(String, String)],
    ) -> Result<Value, VenueError> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or(0);
        let mut query = encode_pairs(pairs);
        if !query.is_empty() {
            query.push('&');
        }
        query.push_str(&format!("timestamp={timestamp}&recvWindow=5000"));
        let signature = sign_payload(&credentials.secret, &query);
        query.push_str("&signature=");
        query.push_str(&signature);
        let url = format!("{}{path}?{query}", self.base);
        let response = self
            .http
            .request(method.parse().map_err(|_| VenueError::Unconfirmed)?, url)
            .header("X-MBX-APIKEY", &credentials.api_key)
            .send()
            .map_err(|_| VenueError::Unconfirmed)?;
        let status = response.status();
        let body = response.text().map_err(|_| VenueError::Unconfirmed)?;
        if body.len() > 128 * 1024 {
            return Err(VenueError::Unconfirmed);
        }
        let value: Value = serde_json::from_str(&body).unwrap_or(json!({ "raw": "unparsed" }));
        if !status.is_success() {
            let code = value
                .get("code")
                .map(|item| item.to_string())
                .unwrap_or_default();
            let msg = value
                .get("msg")
                .and_then(Value::as_str)
                .unwrap_or("交易所拒绝了该委托。");
            if status.is_server_error() {
                return Err(VenueError::Unconfirmed);
            }
            return Err(VenueError::Rejected(format!("{code} {msg}")));
        }
        Ok(value)
    }
}

pub fn sign_payload(secret: &str, payload: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac key");
    mac.update(payload.as_bytes());
    hex_encode(mac.finalize().into_bytes())
}

pub fn venue_symbol(symbol: &str) -> Result<String, VenueError> {
    let bare = symbol.strip_suffix("-PERP").unwrap_or(symbol);
    if bare.len() < 2
        || !bare
            .chars()
            .all(|char| char.is_ascii_uppercase() || char.is_ascii_digit() || char == '-')
    {
        return Err(VenueError::Symbol);
    }
    Ok(bare.to_string())
}

fn check_credentials(credentials: &Credentials) -> Result<(), VenueError> {
    if credentials.api_key.is_empty()
        || credentials.secret.is_empty()
        || credentials.api_key == credentials.secret
        || credentials
            .api_key
            .chars()
            .any(|char| char == '\n' || char == '\r')
        || credentials
            .secret
            .chars()
            .any(|char| char == '\n' || char == '\r')
    {
        return Err(VenueError::Credentials);
    }
    Ok(())
}

fn encode_pairs(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .map(|(key, value)| format!("{}={}", encode(key), encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn encode(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn snapshot_view(value: &Value) -> Value {
    json!({
        "balances": value.get("assets").cloned().unwrap_or_else(|| json!([])),
        "positions": value.get("positions").cloned().unwrap_or_else(|| json!([])),
        "info": { "canTrade": value.get("canTrade").cloned().unwrap_or(Value::Bool(false)) },
        "observed_at": value.get("updateTime").cloned().unwrap_or(Value::Null),
    })
}

fn order_view(value: &Value, client_order_id: &str) -> Value {
    json!({
        "client_order_id": value.get("clientOrderId").and_then(Value::as_str).unwrap_or(client_order_id),
        "order_id": value.get("orderId").map(|item| item.to_string()),
        "symbol": value.get("symbol").and_then(Value::as_str).unwrap_or(""),
        "side": value.get("side").and_then(Value::as_str).unwrap_or(""),
        "order_type": value.get("type").and_then(Value::as_str).unwrap_or(""),
        "quantity": value.get("origQty").and_then(Value::as_str).unwrap_or(""),
        "filled": value.get("executedQty").and_then(Value::as_str).unwrap_or("0"),
        "price": value.get("price").and_then(Value::as_str),
        "status": value.get("status").and_then(Value::as_str).unwrap_or("NEW"),
        "reduce_only": value.get("reduceOnly").and_then(Value::as_bool).unwrap_or(false),
        "open": value.get("status").and_then(Value::as_str).is_none_or(|status| matches!(status, "NEW" | "PARTIALLY_FILLED")),
    })
}

fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
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

    #[test]
    fn signature_matches_python_hmac_sha256() {
        // hmac.new(b"secret", b"symbol=ETHUSDT&timestamp=1", hashlib.sha256).hexdigest()
        assert_eq!(
            sign_payload("secret", "symbol=ETHUSDT&timestamp=1"),
            "4925e3391a9fa6295a68ca8c7b9b0b835c0f98ea582e717ca4c540d7db547d59"
        );
    }

    #[test]
    fn signature_is_stable() {
        assert_eq!(
            sign_payload("secret", "symbol=ETHUSDT&timestamp=1"),
            sign_payload("secret", "symbol=ETHUSDT&timestamp=1")
        );
        assert_ne!(
            sign_payload("secret", "symbol=ETHUSDT&timestamp=1"),
            sign_payload("other", "symbol=ETHUSDT&timestamp=1")
        );
    }

    #[test]
    fn rejects_identical_key_and_secret() {
        let error = check_credentials(&Credentials {
            api_key: "same".into(),
            secret: "same".into(),
        });
        assert_eq!(error, Err(VenueError::Credentials));
    }

    #[test]
    fn strips_perp_suffix_without_changing_spot_symbols() {
        assert_eq!(venue_symbol("ETHUSDT-PERP").unwrap(), "ETHUSDT");
        assert_eq!(venue_symbol("ETHUSDT").unwrap(), "ETHUSDT");
        assert_eq!(
            venue_symbol("BTC-241227-100000-C").unwrap(),
            "BTC-241227-100000-C"
        );
    }

    #[test]
    fn signed_post_hits_the_mock_and_not_a_default_host() {
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let app = axum::Router::new().route(
                    "/fapi/v1/order",
                    axum::routing::post(mock_order).get(mock_order),
                );
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                sender.send(listener.local_addr().unwrap().port()).unwrap();
                axum::serve(listener, app).await.unwrap();
            });
        });
        let port = receiver.recv().unwrap();
        let client = VenueClient::new(format!("http://127.0.0.1:{port}")).unwrap();
        let order = client
            .place_order(
                &Credentials {
                    api_key: "key-one".into(),
                    secret: "secret-one".into(),
                },
                &OrderIntent {
                    product: Product::FuturesUm,
                    symbol: "ETHUSDT-PERP".into(),
                    side: "buy".into(),
                    order_type: "limit".into(),
                    quantity: "0.01".into(),
                    price: Some("2000".into()),
                    client_order_id: "SA00000000000000000000000000000001".into(),
                    reduce_only: false,
                    time_in_force: "GTC".into(),
                },
            )
            .unwrap();
        assert_eq!(
            order["client_order_id"],
            "SA00000000000000000000000000000001"
        );
        assert_eq!(order["status"], "NEW");
        let again = client
            .query_order(
                &Credentials {
                    api_key: "key-one".into(),
                    secret: "secret-one".into(),
                },
                Product::FuturesUm,
                "ETHUSDT-PERP",
                "SA00000000000000000000000000000001",
            )
            .unwrap();
        assert!(again.is_some());
    }

    async fn mock_order(request: axum::extract::Request) -> axum::Json<serde_json::Value> {
        let query = request.uri().query().unwrap_or("");
        assert!(query.contains("signature="));
        assert!(query.contains("symbol=ETHUSDT"));
        assert!(!query.contains("secret"));
        let key = request
            .headers()
            .get("x-mbx-apikey")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(key, "key-one");
        let signed = query.split("&signature=").next().unwrap();
        assert_eq!(
            sign_payload("secret-one", signed),
            query.split("signature=").nth(1).unwrap()
        );
        axum::Json(json!({
            "orderId": 1,
            "clientOrderId": "SA00000000000000000000000000000001",
            "symbol": "ETHUSDT",
            "side": "BUY",
            "type": "LIMIT",
            "origQty": "0.01",
            "executedQty": "0",
            "price": "2000",
            "status": "NEW",
            "reduceOnly": false
        }))
    }
}
