//! HTTP v2 boundary. Every response carries `X-SeeAlpha-Engine-Contract-Version: 2`.
//! Live exchange routes refuse before any credential is stored.
mod derivatives;
mod parity;
pub mod research;
mod research_v1;
#[path = "routes/system.rs"]
mod system_routes;
#[path = "routes/strategies.rs"]
mod strategy_routes;
#[path = "routes/orders.rs"]
mod order_routes;
#[path = "routes/market_data.rs"]
mod market_data_routes;
#[path = "routes/backtests.rs"]
mod backtest_routes;
#[path = "routes/accounts.rs"]
mod account_routes;
use system_routes::*;
use strategy_routes::*;
use order_routes::*;
use market_data_routes::*;
use backtest_routes::*;
use account_routes::*;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use base64::Engine;
use seealpha_market::{LoadQuery, MarketSnapshot, Product, PublicSource, check_symbol};
use seealpha_runtime::{
    AuthMode, Config, OrderCommand, PackageBody, PaperRequest, Runtime, RuntimeError,
    StrategyInput, StrategySubmission,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tower_http::limit::RequestBodyLimitLayer;

pub const CONTRACT_HEADER: &str = "X-SeeAlpha-Engine-Contract-Version";
pub const CONTRACT_VERSION: &str = "2";

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub runtime: Arc<Runtime>,
    pub market: Arc<dyn PublicSource>,
}

pub fn router(state: AppState) -> Router {
    let mut router = Router::new()
        .route("/health", get(health))
        .route("/tenants/{tenant_id}", put(ensure_tenant))
        .route("/tenants/{tenant_id}/runnable", post(runnable))
        .route("/tenants/{tenant_id}/jobs", get(jobs))
        .route(
            "/tenants/{tenant_id}/jobs/worker-settings",
            get(worker_settings).put(update_worker_settings),
        )
        .route(
            "/tenants/{tenant_id}/strategy-packages/inspect",
            post(inspect_strategy),
        )
        .route(
            "/tenants/{tenant_id}/strategy-loads",
            post(strategy_load).get(list_strategies),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/strategy-preview",
            post(strategy_preview),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/paper/sessions",
            post(paper_session),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/orders",
            get(list_orders).post(order_command),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/spot/orders",
            post(order_command),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/trading/controls",
            post(order_command),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/requests/{request_id}/ledger-sync",
            post(ledger_sync),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/requests/{request_id}/reconcile",
            post(reconcile_order),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/fills",
            get(list_fills),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/gaps",
            get(list_gaps).post(put_gap),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/events",
            get(list_events),
        )
        .route("/tenants/{tenant_id}/data/catalog/bars", post(import_bars))
        .route(
            "/tenants/{tenant_id}/data/clickhouse/config",
            get(clickhouse_settings).put(update_clickhouse_settings),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/backtests",
            post(run_backtest),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/connections",
            post(register_connection).delete(forget_connection),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/health",
            get(account_health),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/snapshots",
            post(account_snapshot),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/backtests/latest",
            get(latest_backtest),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/carry-backtests",
            post(run_carry),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/carry-backtests/latest",
            get(latest_carry),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/protected-option-backtests",
            post(run_protected),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/protected-option-backtests/latest",
            get(latest_protected),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/candidate-backtests",
            post(run_candidate).delete(cancel_candidate),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/candidate-backtests/latest",
            get(latest_candidate),
        )
        .merge(parity::routes())
        .merge(derivatives::routes())
        .fallback(not_found);
    if state.config.research_signing_key.is_some() {
        router = router.merge(research_v1::routes(state.config.research_mcp_enabled));
    }
    router
        .layer(RequestBodyLimitLayer::new(2 * 1024 * 1024))
        .layer(axum::middleware::from_fn_with_state(state.clone(), gate))
        .with_state(state)
}

pub async fn serve(state: AppState) -> Result<(), std::io::Error> {
    let host = state.config.host.clone();
    let port = state.config.port;
    let listener = tokio::net::TcpListener::bind((host.as_str(), port)).await?;
    // Restart only tasks that were durably QUEUED and never started.
    // A RUNNING task recovered as INTERRUPTED must be explicitly reviewed/retried.
    resume_queued_research_jobs(&state)?;
    axum::serve(listener, router(state)).await
}

fn resume_queued_research_jobs(state: &AppState) -> Result<(), std::io::Error> {
    let tenants = state
        .runtime
        .ledger
        .active_tenant_ids()
        .map_err(std::io::Error::other)?;
    for tenant in tenants {
        if state
            .config
            .tenant_id
            .as_deref()
            .is_some_and(|id| id != tenant)
        {
            continue;
        }
        let jobs = state
            .runtime
            .control
            .backtest_jobs(&tenant)
            .map_err(std::io::Error::other)?;
        for job in jobs {
            if job["action"] == "backtest" && job["status"] == "queued" {
                let id = job["id"].as_str().ok_or_else(|| {
                    std::io::Error::other("queued research task has no durable identity")
                })?;
                parity::spawn_backtest(state.runtime.clone(), tenant.clone(), id.to_owned());
            }
        }
    }
    Ok(())
}

async fn gate(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let path = request.uri().path().to_string();
    let research_http = path.starts_with("/research/v1/");
    let research_mcp = path == "/research/mcp";
    let research = research_http || research_mcp;
    let research_header = || {
        let mut response = error(StatusCode::UPGRADE_REQUIRED, "Engine HTTP 契约版本不兼容。");
        if research_http {
            response.headers_mut().insert(
                "X-SeeAlpha-Research-Contract-Version",
                HeaderValue::from_static("1"),
            );
        }
        response
    };
    if !research_mcp
        && let Some(version) = request
            .headers()
            .get(CONTRACT_HEADER)
            .and_then(|value| value.to_str().ok())
        && version != CONTRACT_VERSION
    {
        return research_header();
    }
    if let Some(pinned) = &state.config.tenant_id
        && let Some(target) = tenant_from(&path)
        && target != pinned
    {
        return error(StatusCode::FORBIDDEN, "Engine 租户不匹配。");
    }
    if !research && state.config.auth_mode == AuthMode::Token {
        let presented = request
            .headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        if !bearer_matches(presented, &state.config.token) {
            return error(StatusCode::UNAUTHORIZED, "引擎令牌无效。");
        }
    }
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(CONTRACT_HEADER, HeaderValue::from_static(CONTRACT_VERSION));
    if research_http && state.config.research_signing_key.is_some() {
        response.headers_mut().insert(
            "X-SeeAlpha-Research-Contract-Version",
            HeaderValue::from_static("1"),
        );
    }
    response
}

fn text(body: &Value, field: &str) -> String {
    body.get(field)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned()
}

fn decimal_field(
    body: &Value,
    field: &str,
    default: &str,
) -> Result<rust_decimal::Decimal, AppError> {
    body.get(field)
        .and_then(Value::as_str)
        .unwrap_or(default)
        .parse()
        .map_err(|_| AppError::bad_request("数值无效。"))
}

fn window_count(body: &Value) -> Result<usize, AppError> {
    let start = body.get("start").and_then(Value::as_str).unwrap_or("");
    let end = body.get("end").and_then(Value::as_str).unwrap_or("");
    let interval = body.get("interval").and_then(Value::as_str).unwrap_or("1m");
    let start_ms = parse_time_ms(start).ok_or_else(|| AppError::bad_request("开始时间无效。"))?;
    let end_ms = parse_time_ms(end).ok_or_else(|| AppError::bad_request("结束时间无效。"))?;
    seealpha_backtest::matching::window_bars(start_ms, end_ms, interval)
        .map_err(|error| AppError::conflict(error.to_owned()))
}

fn parse_time_ms(value: &str) -> Option<i64> {
    if let Ok(millis) = value.parse::<i64>() {
        return Some(millis);
    }
    let text = value
        .trim()
        .trim_end_matches('Z')
        .trim_end_matches("+00:00");
    let (date, time) = text.split_once('T')?;
    let mut dates = date.split('-');
    let year: i64 = dates.next()?.parse().ok()?;
    let month: i64 = dates.next()?.parse().ok()?;
    let day: i64 = dates.next()?.parse().ok()?;
    let mut clock = time.split(':');
    let hour: i64 = clock.next()?.parse().ok()?;
    let minute: i64 = clock.next()?.parse().ok()?;
    let second_text = clock.next()?;
    if second_text.contains('.') && !second_text.split('.').nth(1)?.chars().all(|c| c == '0') {
        return None;
    }
    let second: i64 = second_text.split('.').next()?.parse().ok()?;
    if !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=59).contains(&second)
        || clock.next().is_some()
    {
        return None;
    }
    let days = days_from_civil(year, month, day)?;
    Some(((days * 24 + hour) * 60 + minute) * 60 + second).map(|secs| secs * 1000)
}

fn days_from_civil(year: i64, month: i64, day: i64) -> Option<i64> {
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if day < 1 || day > max_day {
        return None;
    }
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let mp = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146097 + doe - 719468)
}

async fn not_found() -> Result<Json<Value>, AppError> {
    Err(AppError::not_found("路径不存在。"))
}

fn bars_snapshot(body: &PreviewBody) -> Result<MarketSnapshot, AppError> {
    let interval_ms = seealpha_market::interval_ms(&body.interval)
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    let mut klines = Vec::with_capacity(body.bars.len());
    for (index, bar) in body.bars.iter().enumerate() {
        let open_time_ms = bar.open_time_ms.unwrap_or(index as i64 * interval_ms);
        klines.push(seealpha_market::Bar {
            open_time_ms,
            close_time_ms: open_time_ms + interval_ms - 1,
            open: parse_decimal(&bar.open)?,
            high: parse_decimal(&bar.high)?,
            low: parse_decimal(&bar.low)?,
            close: parse_decimal(&bar.close)?,
            volume: parse_decimal(&bar.volume)?,
        });
    }
    Ok(MarketSnapshot {
        product: Product::Spot,
        symbol: body.symbol.clone(),
        interval: body.interval.clone(),
        klines,
        trades: Vec::new(),
        depth: seealpha_market::DepthBook {
            bids: Vec::new(),
            asks: Vec::new(),
        },
    })
}

fn parse_decimal(text: &str) -> Result<rust_decimal::Decimal, AppError> {
    text.parse()
        .map_err(|_| AppError::bad_request("K 线数值无效。"))
}

fn tenant_from(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("/tenants/")?;
    let id = rest.split('/').next().unwrap_or("");
    if id.is_empty() { None } else { Some(id) }
}

fn bearer_matches(presented: &str, token: &str) -> bool {
    let expected = format!("Bearer {token}");
    presented.len() == expected.len() && bool::from(presented.as_bytes().ct_eq(expected.as_bytes()))
}

fn error(status: StatusCode, detail: &str) -> Response {
    (
        status,
        [(CONTRACT_HEADER, HeaderValue::from_static(CONTRACT_VERSION))],
        Json(json!({ "detail": detail })),
    )
        .into_response()
}

#[derive(Debug)]
pub enum AppError {
    Conflict(String),
    BadRequest(String),
    NotFound(String),
    Forbidden(String),
    Unauthorized(String),
    Upgrade(String),
    Unprocessable(String),
    NotImplemented(String),
}

impl AppError {
    fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict(message.into())
    }
    fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }
    fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }
    fn forbidden(message: impl Into<String>) -> Self {
        Self::Forbidden(message.into())
    }
    fn unauthorized(message: impl Into<String>) -> Self {
        Self::Unauthorized(message.into())
    }
    fn upgrade(message: impl Into<String>) -> Self {
        Self::Upgrade(message.into())
    }
    fn unprocessable(message: impl Into<String>) -> Self {
        Self::Unprocessable(message.into())
    }
    fn not_implemented(message: impl Into<String>) -> Self {
        Self::NotImplemented(message.into())
    }
}

impl From<RuntimeError> for AppError {
    fn from(error: RuntimeError) -> Self {
        match error {
            RuntimeError::Conflict(message) => Self::Conflict(message),
            RuntimeError::BadRequest(message) => Self::BadRequest(message),
            RuntimeError::NotFound(message) => Self::NotFound(message),
            RuntimeError::Unprocessable(message) => Self::Unprocessable(message),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, detail) = match self {
            Self::Conflict(detail) => (StatusCode::CONFLICT, detail),
            Self::BadRequest(detail) => (StatusCode::BAD_REQUEST, detail),
            Self::NotFound(detail) => (StatusCode::NOT_FOUND, detail),
            Self::Forbidden(detail) => (StatusCode::FORBIDDEN, detail),
            Self::Unauthorized(detail) => (StatusCode::UNAUTHORIZED, detail),
            Self::Upgrade(detail) => (StatusCode::UPGRADE_REQUIRED, detail),
            Self::Unprocessable(detail) => (StatusCode::UNPROCESSABLE_ENTITY, detail),
            Self::NotImplemented(detail) => (StatusCode::NOT_IMPLEMENTED, detail),
        };
        error(status, &detail)
    }
}

#[derive(Deserialize)]
struct TenantBody {
    purpose: String,
    #[serde(default = "default_status")]
    status: String,
}

#[derive(Deserialize)]
struct RunnableBody {
    action: String,
}

#[derive(Deserialize)]
struct StrategyLoadBody {
    account_id: String,
    issuance_id: String,
    version: String,
    code_key: String,
    entry_point: String,
    package_hash: String,
    grant_purpose: String,
    #[serde(default)]
    parameters: Value,
    archive: String,
}

#[derive(Deserialize)]
struct AccountQuery {
    account_id: Option<String>,
}

#[derive(Deserialize)]
struct EnvironmentQuery {
    environment: Option<String>,
}

#[derive(Deserialize)]
struct PaperBody {
    symbol: String,
    #[serde(default = "default_interval")]
    interval: String,
    #[serde(default = "default_limit")]
    limit: u32,
    #[serde(default = "default_product")]
    product: String,
    #[serde(default)]
    use_sample: bool,
    /// Opt-in Rust-only EMA sample; requires use_sample=true.
    #[serde(default)]
    native_sample: bool,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    parameters: Value,
}

#[derive(Deserialize)]
struct PreviewBody {
    symbol: String,
    #[serde(default = "default_interval")]
    interval: String,
    bars: Vec<PreviewBar>,
    #[serde(default)]
    request_id: Option<String>,
}

#[derive(Deserialize)]
struct PreviewBar {
    open_time_ms: Option<i64>,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
}

fn default_status() -> String {
    "active".into()
}
fn default_interval() -> String {
    "1m".into()
}
fn default_limit() -> u32 {
    120
}
fn default_product() -> String {
    "spot".into()
}

#[cfg(test)]
mod tests;
