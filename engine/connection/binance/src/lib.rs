//! Signed Binance REST connector.
//!
//! Callers pass the base URL. Nothing in this crate selects a production host.
//! Credentials are used only to build the request and are not returned or logged.
#[path = "usd_m/conditional.rs"]
pub mod conditional;
#[path = "account/evidence.rs"]
pub mod evidence;
pub mod lifecycle {
    // Historical API compatibility while Runtime is moved off seealpha-venue.
    pub use seealpha_execution::lifecycle::{OrderPhase, VerifiedOrderState};
}
#[path = "options/trades.rs"]
pub mod option_trades;
#[path = "orders/guard.rs"]
pub mod order_guard;
#[path = "account/reconciliation.rs"]
pub mod reconciliation;
#[path = "account/settlement.rs"]
pub mod settlement;
#[path = "spot/oco.rs"]
pub mod spot_oco;
#[path = "spot/oto.rs"]
pub mod spot_oto;
#[path = "orders/trades.rs"]
pub mod trades;
#[path = "orders/user_stream.rs"]
pub mod user_stream;

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

impl From<seealpha_execution::OrderStateError> for VenueError {
    fn from(_: seealpha_execution::OrderStateError) -> Self {
        Self::Unconfirmed
    }
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

/// Signed Binance requests must never leak API keys to a configurable
/// arbitrary HTTPS origin. Loopback is reserved for local test/proxy adapters.
fn checked_gateway(base: &str) -> Result<String, VenueError> {
    let url = reqwest::Url::parse(base)
        .map_err(|_| VenueError::Rejected("交易网关 URL 无效。".into()))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(VenueError::Rejected(
            "交易网关包含不允许的路径或用户信息。".into(),
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| VenueError::Rejected("交易网关域名无效。".into()))?;
    let official = matches!(
        host,
        "api.binance.com" | "fapi.binance.com" | "eapi.binance.com"
    );
    let sandbox = matches!(host, "testnet.binancefuture.com" | "testnet.binance.vision");
    let local = matches!(host, "127.0.0.1" | "localhost" | "::1");
    if (url.scheme() == "https" && (official || sandbox) && url.port().is_none())
        || (url.scheme() == "http" && local && url.port().is_some())
    {
        return Ok(base.trim_end_matches('/').to_owned());
    }
    Err(VenueError::Rejected(
        "交易所签名请求仅允许 Binance 官方域名或带端口的本地网关。".into(),
    ))
}
fn route_gateway<'a>(base: &'a str, path: &str) -> Result<&'a str, VenueError> {
    let host = reqwest::Url::parse(base)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .ok_or_else(|| VenueError::Rejected("交易网关 URL 无效。".into()))?;
    if matches!(
        host.as_str(),
        "api.binance.com" | "fapi.binance.com" | "eapi.binance.com"
    ) {
        // Static official hosts have equal lifetime to 'base', but avoid
        // returning a temporary URL string by selecting a static literal.
        return Ok(match path.split('/').nth(1).unwrap_or("") {
            "api" => "https://api.binance.com",
            "fapi" => "https://fapi.binance.com",
            "eapi" => "https://eapi.binance.com",
            _ => return Err(VenueError::Symbol),
        });
    }
    if (host == "testnet.binancefuture.com" && path.starts_with("/fapi/"))
        || (host == "testnet.binance.vision" && path.starts_with("/api/"))
        || matches!(host.as_str(), "127.0.0.1" | "localhost" | "::1")
    {
        return Ok(base);
    }
    Err(VenueError::Rejected(
        "沙盒交易网关与订单产品不匹配，拒绝跨市场发送密钥。".into(),
    ))
}

pub struct VenueClient {
    http: reqwest::blocking::Client,
    base: String,
}

impl VenueClient {
    pub fn new(base: impl Into<String>) -> Result<Self, VenueError> {
        let base = checked_gateway(&base.into())?;
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
        if intent.product == Product::FuturesUm
            && matches!(
                intent.order_type.to_ascii_uppercase().as_str(),
                "STOP"
                    | "STOP_MARKET"
                    | "TAKE_PROFIT"
                    | "TAKE_PROFIT_MARKET"
                    | "TRAILING_STOP_MARKET"
            )
        {
            return Err(VenueError::Rejected(
                "USD-M 条件单必须通过 Algo Service 及预登记的条件单链路提交。".into(),
            ));
        }
        let path = match intent.product {
            Product::FuturesUm => "/fapi/v1/order",
            Product::Spot => "/api/v3/order",
            Product::Option => seealpha_binance_options::order::EAPI_ORDER_PATH,
        };
        let canonical_symbol = venue_symbol(&intent.symbol)?;
        let pairs = if intent.product == Product::Option {
            seealpha_binance_options::build_options_order_pairs(
                &seealpha_binance_options::OptionsOrderFields {
                    symbol: &canonical_symbol,
                    side: &intent.side,
                    order_type: &intent.order_type,
                    quantity: &intent.quantity,
                    price: intent.price.as_deref(),
                    client_order_id: &intent.client_order_id,
                    reduce_only: intent.reduce_only,
                    time_in_force: &intent.time_in_force,
                },
            )
            .map_err(|e| VenueError::Rejected(e.message().into()))?
        } else {
            let mut pairs = vec![
                ("symbol".to_string(), canonical_symbol),
                ("side".to_string(), intent.side.to_ascii_uppercase()),
                ("type".to_string(), intent.order_type.to_ascii_uppercase()),
                ("quantity".to_string(), intent.quantity.clone()),
                ("newClientOrderId".to_string(), intent.client_order_id.clone()),
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
            pairs
        };
        let value = self.signed(credentials, "POST", path, &pairs)?;
        // A successful HTTP status without an exchange order identity is an
        // unknown result, not an accepted fill or authoritative NEW order.
        if value.get("clientOrderId").and_then(Value::as_str)
            != Some(intent.client_order_id.as_str())
            || value.get("status").and_then(Value::as_str).is_none()
            || value.get("symbol").and_then(Value::as_str)
                != Some(venue_symbol(&intent.symbol)?.as_str())
        {
            return Err(VenueError::Unconfirmed);
        }
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
            Product::Option => seealpha_binance_options::order::EAPI_ORDER_PATH,
        };
        let symbol = venue_symbol(symbol)?;
        let pairs = if product == Product::Option {
            seealpha_binance_options::order::options_lookup_pairs(&symbol, client_order_id)
        } else {
            vec![
                ("symbol".to_string(), symbol),
                ("origClientOrderId".to_string(), client_order_id.to_string()),
            ]
        };
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
            Product::Option => seealpha_binance_options::order::EAPI_ORDER_PATH,
        };
        let symbol = venue_symbol(symbol)?;
        let pairs = if product == Product::Option {
            seealpha_binance_options::order::options_lookup_pairs(&symbol, client_order_id)
        } else {
            vec![
                ("symbol".to_string(), symbol.clone()),
                ("origClientOrderId".to_string(), client_order_id.to_string()),
            ]
        };
        match self.signed(credentials, "GET", path, &pairs) {
            Ok(value) => {
                // Do not rely on order_view's presentation fallback for
                // authority: the signed response must itself bind identity.
                let expected_symbol = venue_symbol(&symbol)?;
                if value["clientOrderId"].as_str() != Some(client_order_id)
                    || value["symbol"].as_str() != Some(expected_symbol.as_str())
                    || value["orderId"].is_null()
                {
                    return Err(VenueError::Unconfirmed);
                }
                Ok(Some(order_view(&value, client_order_id)))
            }
            Err(VenueError::Rejected(message))
                if message.contains("-2013") || message.contains("not found") =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Resolve a triggered USD-M child order by the exchange order ID,
    /// never by the parent conditional clientAlgoId.
    pub fn query_um_order_by_id(
        &self,
        credentials: &Credentials,
        symbol: &str,
        order_id: &str,
    ) -> Result<Value, VenueError> {
        check_credentials(credentials)?;
        if order_id.is_empty() || !order_id.bytes().all(|b| b.is_ascii_digit()) {
            return Err(VenueError::Unconfirmed);
        }
        let symbol = venue_symbol(symbol)?;
        let value = self.signed(
            credentials,
            "GET",
            "/fapi/v1/order",
            &[
                ("symbol".into(), symbol.clone()),
                ("orderId".into(), order_id.into()),
            ],
        )?;
        let matched = value.get("orderId").and_then(|v| {
            v.as_u64()
                .map(|n| n.to_string())
                .or_else(|| v.as_str().map(str::to_owned))
        });
        if matched.as_deref() != Some(order_id)
            || value.get("symbol").and_then(Value::as_str) != Some(symbol.as_str())
            || value
                .get("clientOrderId")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
        {
            return Err(VenueError::Unconfirmed);
        }
        Ok(order_view(
            &value,
            value["clientOrderId"]
                .as_str()
                .ok_or(VenueError::Unconfirmed)?,
        ))
    }

    /// Verified exchange trade IDs with bounded fromId pagination. The caller
    /// must ALSO compare cumulative trade quantity with a signed GET order.
    /// A saturated final page, repeated ID, malformed cursor or expired
    /// exchange history fails closed; only signed GET requests are sent.
    pub fn query_order_trades(
        &self,
        credentials: &Credentials,
        product: Product,
        symbol: &str,
        order_id: &str,
    ) -> Result<Vec<Value>, VenueError> {
        check_credentials(credentials)?;
        if product == Product::Option {
            return self.option_order_trades(credentials, symbol, order_id);
        }
        if order_id.is_empty() || !order_id.bytes().all(|b| b.is_ascii_digit()) {
            return Err(VenueError::Unconfirmed);
        }
        let path = match product {
            Product::Spot => "/api/v3/myTrades",
            Product::FuturesUm => "/fapi/v1/userTrades",
            Product::Option => return Err(VenueError::Unconfirmed),
        };
        let symbol = venue_symbol(symbol)?;
        let mut next_id = None::<u64>;
        let mut latest = None::<u64>;
        let mut out = Vec::new();
        for _ in 0..32 {
            let mut params = vec![
                ("symbol".into(), symbol.clone()),
                ("orderId".into(), order_id.to_owned()),
                ("limit".into(), "1000".into()),
            ];
            if let Some(cursor) = next_id {
                params.push(("fromId".into(), cursor.to_string()));
            }
            let raw = self.signed(credentials, "GET", path, &params)?;
            let size = raw.as_array().ok_or(VenueError::Unconfirmed)?.len();
            let page = trades::normalize_order_trades_page(product, &symbol, order_id, &raw, true)?;
            for row in page {
                let id = row["venue_trade_id"]
                    .as_str()
                    .ok_or(VenueError::Unconfirmed)?
                    .parse::<u64>()
                    .map_err(|_| VenueError::Unconfirmed)?;
                if latest.is_some_and(|previous| id <= previous)
                    || next_id.is_some_and(|cursor| id < cursor)
                {
                    return Err(VenueError::Unconfirmed);
                }
                latest = Some(id);
                out.push(row);
            }
            if size < 1000 {
                return Ok(out);
            }
            next_id = Some(
                latest
                    .ok_or(VenueError::Unconfirmed)?
                    .checked_add(1)
                    .ok_or(VenueError::Unconfirmed)?,
            );
        }
        Err(VenueError::Unconfirmed)
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
        // Legacy entrypoints have no auditable admission proof: fail closed.
        // Only PreparedWrite::dispatch_live may reach signed_wire after a
        // successfully committed IntentJournal::admit.
        if method != "GET" {
            return Err(VenueError::Rejected(
                "交易命令未通过持久化订单意图登记门禁。".into(),
            ));
        }
        self.signed_wire(credentials, method, path, pairs)
    }

    fn signed_wire(
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
        let target = route_gateway(&self.base, path)?;
        let url = format!("{target}{path}?{query}");
        let response = self
            .http
            .request(method.parse().map_err(|_| VenueError::Unconfirmed)?, url)
            .header("X-MBX-APIKEY", &credentials.api_key)
            .send()
            .map_err(|_| VenueError::Unconfirmed)?;
        let status = response.status();
        // 1000-trade pages exceed 128 KiB; other endpoints stay on a
        // smaller limit. Bound the actual socket read, not just the string.
        let maximum = if method == "GET"
            && matches!(
                path,
                "/api/v3/myTrades" | "/fapi/v1/userTrades" | "/eapi/v1/userTrades"
            ) {
            2 * 1024 * 1024
        } else {
            128 * 1024
        };
        use std::io::Read;
        let mut bytes = Vec::new();
        response
            .take((maximum + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| VenueError::Unconfirmed)?;
        if bytes.len() > maximum {
            return Err(VenueError::Unconfirmed);
        }
        let body = std::str::from_utf8(&bytes).map_err(|_| VenueError::Unconfirmed)?;
        let value: Value = serde_json::from_str(body).unwrap_or(json!({ "raw": "unparsed" }));
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
        "order_id": value.get("orderId").and_then(|item|
            item.as_str().map(str::to_owned)
                .or_else(||item.as_u64().map(|n|n.to_string()))),
        "symbol": value.get("symbol").and_then(Value::as_str).unwrap_or(""),
        "side": value.get("side").and_then(Value::as_str).unwrap_or(""),
        "order_type": value.get("type").and_then(Value::as_str).unwrap_or(""),
        "quantity": value.get("origQty").or_else(||value.get("quantity")).and_then(Value::as_str).unwrap_or(""),
        "filled": value.get("executedQty").and_then(Value::as_str).unwrap_or("0"),
        "price": value.get("price").and_then(Value::as_str),
        "status": value.get("status").and_then(Value::as_str).unwrap_or("UNKNOWN"),
        "reduce_only": value.get("reduceOnly").and_then(Value::as_bool).unwrap_or(false),
        "open": value.get("status").and_then(Value::as_str).is_some_and(|status| matches!(status, "NEW" | "ACCEPTED" | "PARTIALLY_FILLED")),
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
    fn signed_gateway_never_sends_exchange_credentials_to_arbitrary_https() {
        for invalid in [
            "https://api.binance.com.evil.example",
            "https://evil.example",
            "http://fapi.binance.com",
            "https://api.binance.com/path",
            "https://user:password@api.binance.com",
            "http://localhost",
        ] {
            assert!(VenueClient::new(invalid).is_err(), "{invalid}");
        }
        let official = VenueClient::new("https://fapi.binance.com").unwrap();
        assert_eq!(
            route_gateway(&official.base, "/eapi/v1/order").unwrap(),
            "https://eapi.binance.com"
        );
        assert_eq!(
            route_gateway(&official.base, "/api/v3/order").unwrap(),
            "https://api.binance.com"
        );
        assert_eq!(
            route_gateway(&official.base, "/fapi/v1/order").unwrap(),
            "https://fapi.binance.com"
        );
        let testnet = VenueClient::new("https://testnet.binancefuture.com").unwrap();
        assert!(route_gateway(&testnet.base, "/eapi/v1/order").is_err());
        assert!(route_gateway("http://127.0.0.1:18767", "/eapi/v1/order").is_ok());
    }

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
    fn normalized_exchange_order_id_never_contains_json_string_quotes() {
        let raw = json!({"orderId":"12345678901234567890",
            "clientOrderId":"sa-123","symbol":"BTC-271231-90000-C",
            "status":"NEW"});
        let view = order_view(&raw, "sa-123");
        assert_eq!(view["order_id"], "12345678901234567890");
        assert_eq!(view["open"], true);
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
        let denied = client
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
            .unwrap_err();
        assert!(matches!(denied, VenueError::Rejected(_)));
        // A separately committed journal ticket is the only way to POST.
        use crate::order_guard::{
            Admission, IntentJournal, IntentRecord, PreparedWrite, RecordedOutcome, WriteAction,
        };
        struct MockDurable {
            registered: bool,
            outcome: Option<RecordedOutcome>,
        }
        impl IntentJournal for MockDurable {
            fn admit(&mut self, _: &IntentRecord) -> Result<Admission, VenueError> {
                if self.registered {
                    return Ok(Admission::Seen);
                }
                self.registered = true;
                Ok(Admission::Fresh)
            }
            fn observe(
                &mut self,
                _: &IntentRecord,
                outcome: RecordedOutcome,
            ) -> Result<(), VenueError> {
                self.outcome = Some(outcome);
                Ok(())
            }
        }
        let order_id = "SA00000000000000000000000000000001";
        let mut operation = PreparedWrite::new(
            "tenant:account",
            "mock_req_1",
            Product::FuturesUm,
            WriteAction::NewOrder,
            "ETHUSDT-PERP",
            order_id,
            vec![
                ("symbol".into(), "ETHUSDT".into()),
                ("newClientOrderId".into(), order_id.into()),
                ("type".into(), "LIMIT".into()),
                ("side".into(), "BUY".into()),
                ("quantity".into(), "0.01".into()),
                ("price".into(), "2000".into()),
                ("timeInForce".into(), "GTC".into()),
            ],
        )
        .unwrap();
        let creds = Credentials {
            api_key: "key-one".into(),
            secret: "secret-one".into(),
        };
        let mut journal = MockDurable {
            registered: false,
            outcome: None,
        };
        let accepted = operation
            .dispatch_live(&mut journal, &client, &creds)
            .unwrap();
        assert_eq!(accepted["status"], "NEW");
        assert_eq!(journal.outcome, Some(RecordedOutcome::Accepted));
        assert!(
            operation
                .dispatch_live(&mut journal, &client, &creds)
                .is_err()
        );
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

    #[test]
    fn paginated_spot_trades_require_monotonic_exchange_ids() {
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let app = axum::Router::new()
                    .route("/api/v3/myTrades", axum::routing::get(mock_my_trades));
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                sender.send(listener.local_addr().unwrap().port()).unwrap();
                axum::serve(listener, app).await.unwrap();
            });
        });
        let client =
            VenueClient::new(format!("http://127.0.0.1:{}", receiver.recv().unwrap())).unwrap();
        let creds = Credentials {
            api_key: "key-one".into(),
            secret: "secret-one".into(),
        };
        let filled = client
            .query_order_trades(&creds, Product::Spot, "ETHUSDT", "9")
            .unwrap();
        assert_eq!(filled.len(), 1001);
        assert_eq!(filled[0]["venue_trade_id"], "1");
        assert_eq!(filled[1000]["venue_trade_id"], "1001");
        // A server that ignores fromId must never claim a complete history.
        assert_eq!(
            client.query_order_trades(&creds, Product::Spot, "ETHUSDT", "10"),
            Err(VenueError::Unconfirmed)
        );
    }
    async fn mock_my_trades(request: axum::extract::Request) -> axum::Json<serde_json::Value> {
        let query = request.uri().query().unwrap_or("");
        assert!(query.contains("signature="));
        assert!(query.contains("symbol=ETHUSDT"));
        let replay = query.contains("orderId=10");
        let start = if replay || !query.contains("fromId=") {
            1
        } else {
            1001
        };
        let end = if start == 1 { 1000 } else { 1001 };
        let order = if replay { 10 } else { 9 };
        let rows = (start..=end)
            .map(|tid| {
                json!({
                    "id":tid,"orderId":order,"symbol":"ETHUSDT","buyer":true,
                    "qty":"0.001","price":"2000","time":100,
                    "commission":"0","commissionAsset":"USDT"
                })
            })
            .collect::<Vec<_>>();
        axum::Json(json!(rows))
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
