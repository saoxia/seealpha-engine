//! Shared Binance signed-wire policy, with no credentials store or HTTP sender.
//! All products use the same host allowlist, EAPI/FAPI/API route selection,
//! canonical parameter encoding and HMAC-SHA256 signature.
use hmac::{Hmac, Mac};
use sha2::Sha256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayError {
    InvalidUrl,
    InvalidComponents,
    InvalidHost,
    DisallowedHost,
    InvalidRoute,
    IncompatibleProduct,
}

pub fn checked_gateway(base: &str) -> Result<String, GatewayError> {
    let url = reqwest::Url::parse(base).map_err(|_| GatewayError::InvalidUrl)?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(GatewayError::InvalidComponents);
    }
    let host = url.host_str().ok_or(GatewayError::InvalidHost)?;
    let official = matches!(host, "api.binance.com" | "fapi.binance.com" | "eapi.binance.com");
    let sandbox = matches!(host, "testnet.binancefuture.com" | "testnet.binance.vision");
    let local = matches!(host, "127.0.0.1" | "localhost" | "::1");
    if (url.scheme() == "https" && (official || sandbox) && url.port().is_none())
        || (url.scheme() == "http" && local && url.port().is_some())
    {
        return Ok(base.trim_end_matches('/').to_owned());
    }
    Err(GatewayError::DisallowedHost)
}

pub fn route_gateway<'a>(base: &'a str, path: &str) -> Result<&'a str, GatewayError> {
    let host = reqwest::Url::parse(base)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .ok_or(GatewayError::InvalidUrl)?;
    if matches!(host.as_str(), "api.binance.com" | "fapi.binance.com" | "eapi.binance.com") {
        return match path.split('/').nth(1).unwrap_or("") {
            "api" => Ok("https://api.binance.com"),
            "fapi" => Ok("https://fapi.binance.com"),
            "eapi" => Ok("https://eapi.binance.com"),
            _ => Err(GatewayError::InvalidRoute),
        };
    }
    if (host == "testnet.binancefuture.com" && path.starts_with("/fapi/"))
        || (host == "testnet.binance.vision" && path.starts_with("/api/"))
        || matches!(host.as_str(), "127.0.0.1" | "localhost" | "::1")
    {
        return Ok(base);
    }
    Err(GatewayError::IncompatibleProduct)
}

pub fn sign_payload(secret: &str, payload: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac key");
    mac.update(payload.as_bytes());
    hex_encode(mac.finalize().into_bytes())
}

/// Pure signed query construction. Does not create an HTTP request or
/// authorize a write. Timestamp is provided by the legacy transport.
pub fn signed_query(secret: &str, pairs: &[(String, String)], timestamp: i64) -> String {
    let mut query = encode_pairs(pairs);
    if !query.is_empty() {
        query.push('&');
    }
    query.push_str(&format!("timestamp={timestamp}&recvWindow=5000"));
    let signature = sign_payload(secret, &query);
    query.push_str("&signature=");
    query.push_str(&signature);
    query
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireResponseError {
    Unconfirmed,
    Rejected(String),
}

/// Decode one HTTP response with the existing strict byte ceiling.
/// Neither an ACK nor a successful HTTP status confirms an order fill.
pub fn decode_response<R: std::io::Read>(
    status: reqwest::StatusCode,
    reader: R,
    method: &str,
    path: &str,
) -> Result<serde_json::Value, WireResponseError> {
    use std::io::Read;
    let maximum = if method == "GET"
        && matches!(
            path,
            "/api/v3/myTrades" | "/fapi/v1/userTrades" | "/eapi/v1/userTrades"
        )
    {
        2 * 1024 * 1024
    } else {
        128 * 1024
    };
    let mut bytes = Vec::new();
    reader
        .take((maximum + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| WireResponseError::Unconfirmed)?;
    if bytes.len() > maximum {
        return Err(WireResponseError::Unconfirmed);
    }
    let body = std::str::from_utf8(&bytes).map_err(|_| WireResponseError::Unconfirmed)?;
    let value: serde_json::Value =
        serde_json::from_str(body).unwrap_or(serde_json::json!({"raw": "unparsed"}));
    if !status.is_success() {
        let code = value
            .get("code")
            .map(|item| item.to_string())
            .unwrap_or_default();
        let msg = value
            .get("msg")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("交易所拒绝了该委托。");
        if status.is_server_error() {
            return Err(WireResponseError::Unconfirmed);
        }
        return Err(WireResponseError::Rejected(format!("{code} {msg}")));
    }
    Ok(value)
}

pub fn encode_pairs(pairs: &[(String, String)]) -> String {
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
    fn host_allowlist_and_market_routing() {
        for host in ["https://api.binance.com.evil.example", "https://evil.example",
                     "http://fapi.binance.com", "https://api.binance.com/path",
                     "https://user:password@api.binance.com", "http://localhost"] {
            assert!(checked_gateway(host).is_err(), "{host}");
        }
        let base = checked_gateway("https://fapi.binance.com").unwrap();
        assert_eq!(route_gateway(&base, "/eapi/v1/order").unwrap(), "https://eapi.binance.com");
        assert_eq!(route_gateway(&base, "/api/v3/order").unwrap(), "https://api.binance.com");
        assert_eq!(route_gateway(&base, "/fapi/v1/order").unwrap(), "https://fapi.binance.com");
        assert_eq!(route_gateway(&base, "/unknown").unwrap_err(), GatewayError::InvalidRoute);
        assert!(route_gateway("https://testnet.binancefuture.com", "/eapi/v1/order").is_err());
        assert!(route_gateway("http://127.0.0.1:18767", "/eapi/v1/order").is_ok());
    }

    #[test]
    fn signed_query_keeps_canonical_order_and_signature() {
        let pairs = vec![
            ("symbol".to_string(), "ETHUSDT".to_string()),
            ("note".to_string(), "a b".to_string()),
        ];
        let canonical = "symbol=ETHUSDT&note=a%20b&timestamp=123&recvWindow=5000";
        assert_eq!(
            signed_query("secret", &pairs, 123),
            format!("{canonical}&signature={}", sign_payload("secret", canonical))
        );
        let empty = "timestamp=123&recvWindow=5000";
        assert_eq!(
            signed_query("secret", &[], 123),
            format!("{empty}&signature={}", sign_payload("secret", empty))
        );
    }

    #[test]
    fn decoder_preserves_success_and_exchange_errors() {
        use reqwest::StatusCode;
        use std::io::Cursor;
        let ok = decode_response(
            StatusCode::OK, Cursor::new(br#"{"status":"NEW"}"#), "GET", "/api/v3/order",
        ).unwrap();
        assert_eq!(ok["status"], "NEW");
        assert_eq!(
            decode_response(
                StatusCode::BAD_REQUEST,
                Cursor::new(br#"{"code":-2013,"msg":"Order not found"}"#),
                "GET",
                "/api/v3/order",
            ),
            Err(WireResponseError::Rejected("-2013 Order not found".into()))
        );
        assert_eq!(
            decode_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                Cursor::new(br#"{"code":500,"msg":"failed"}"#),
                "GET",
                "/api/v3/order",
            ),
            Err(WireResponseError::Unconfirmed)
        );
        assert_eq!(
            decode_response(StatusCode::OK, Cursor::new(b"not-json"), "GET", "/api/v3/order")
                .unwrap(),
            serde_json::json!({"raw":"unparsed"})
        );
    }

    #[test]
    fn decoder_bounds_actual_reader_and_preserves_trade_page_exception() {
        use reqwest::StatusCode;
        use std::io::Cursor;
        let payload = vec![b' '; 128 * 1024 + 1];
        assert_eq!(
            decode_response(
                StatusCode::OK, Cursor::new(payload.clone()), "GET", "/api/v3/order",
            ),
            Err(WireResponseError::Unconfirmed)
        );
        assert_eq!(
            decode_response(
                StatusCode::OK, Cursor::new(payload), "GET", "/eapi/v1/userTrades",
            ).unwrap(),
            serde_json::json!({"raw":"unparsed"})
        );
        let enormous = vec![b' '; 2 * 1024 * 1024 + 1];
        assert_eq!(
            decode_response(
                StatusCode::OK, Cursor::new(enormous), "GET", "/fapi/v1/userTrades",
            ),
            Err(WireResponseError::Unconfirmed)
        );
    }

    #[test]
    fn signed_payload_matches_existing_contract() {
        assert_eq!(
            sign_payload("secret", "symbol=ETHUSDT&timestamp=1"),
            "4925e3391a9fa6295a68ca8c7b9b0b835c0f98ea582e717ca4c540d7db547d59"
        );
        assert_eq!(encode_pairs(&[("symbol".into(), "BTC-271231-90000-C".into()),
            ("memo".into(), "a b&c".into())]),
            "symbol=BTC-271231-90000-C&memo=a%20b%26c");
    }
}
