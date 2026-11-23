//! HTTP v2 boundary. Every response carries `X-SeeAlpha-Engine-Contract-Version: 2`.
//! Live exchange routes refuse before any credential is stored.
mod config;
mod strategy_preview;
mod derivatives;
mod parity;
mod pipeline_subscriptions;
mod research_v1;
#[path = "routes/system.rs"]
mod system_routes;
#[path = "routes/strategies.rs"]
mod strategy_routes;
#[path = "routes/orders.rs"]
mod order_routes;
use system_routes::*;
use strategy_routes::*;
use order_routes::*;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use seealpha_market::{MarketSnapshot, Product, PublicSource};
pub use config::Config;
use seealpha_catalog::ControlStore;
use seealpha_ledger::Ledger;
use seealpha_strategy::store::StrategyStore;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use tower_http::limit::RequestBodyLimitLayer;

pub const CONTRACT_HEADER: &str = "X-SeeAlpha-Engine-Contract-Version";
pub const CONTRACT_VERSION: &str = "2";

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub ledger: Arc<Ledger>,
    pub strategy_store: Arc<StrategyStore>,
    pub control: Arc<ControlStore>,
    pub market: Arc<dyn PublicSource>,
}

// Legacy PostgreSQL and immutable strategy storage still use a namespace column.
// External routes never accept a tenant identifier; historical rows are preserved.
pub(crate) const LEGACY_STORAGE_SCOPE: &str = "local";

impl AppState {
    /// Use the same mounted Nautilus v2 Catalog path as the former Runtime facade.
    fn catalog(&self) -> Result<Option<seealpha_catalog::Catalog>, AppError> {
        let path = match std::env::var("ENGINE_CATALOG_DIR") {
            Ok(value) if !value.trim().is_empty() => {
                let selected = std::path::PathBuf::from(value);
                if selected.is_absolute() {
                    selected
                } else {
                    self.config.project.join(selected)
                }
            }
            _ => self.config.project.join("data").join("catalog"),
        };
        match path.symlink_metadata() {
            Ok(_) => seealpha_catalog::Catalog::open_existing(&path)
                .map(Some)
                .map_err(|error| AppError::conflict(error.to_string())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(AppError::conflict(format!("Catalog 数据目录不可用：{error}"))),
        }
    }

    fn require_catalog(&self) -> Result<seealpha_catalog::Catalog, AppError> {
        self.catalog()?
            .ok_or_else(|| AppError::not_found("未挂载 Python Nautilus v2 Catalog。"))
    }
}

pub fn router(state: AppState) -> Router {
    let mut router = Router::new()
        .route("/health", get(health))
        .route(
            "/strategy-packages/inspect",
            post(inspect_strategy),
        )
        .route(
            "/strategy-loads",
            post(strategy_load).get(list_strategies),
        )
        .route(
            "/accounts/{account_id}/strategy-preview",
            post(strategy_preview),
        )
        .route(
            "/accounts/{account_id}/orders",
            get(list_orders),
        )
        .route(
            "/accounts/{account_id}/fills",
            get(list_fills),
        )
        .merge(parity::routes())
        .merge(pipeline_subscriptions::routes())
        .merge(derivatives::routes())
        .fallback(not_found);
    router = router.merge(research_v1::routes(state.config.research_mcp_enabled));
    router
        .layer(RequestBodyLimitLayer::new(2 * 1024 * 1024))
        .layer(axum::middleware::from_fn(gate))
        .with_state(state)
}

pub async fn serve(state: AppState) -> Result<(), std::io::Error> {
    let host = state.config.host.clone();
    let port = state.config.port;
    let listener = tokio::net::TcpListener::bind((host.as_str(), port)).await?;
    // Restart only tasks that were durably QUEUED and never started.
    // A RUNNING task recovered as INTERRUPTED must be explicitly reviewed/retried.
    axum::serve(listener, router(state)).await
}

async fn gate(request: Request, next: Next) -> Response {
    let path=request.uri().path();
    let research_http=path.starts_with("/research/v1/");
    let research_mcp=path=="/research/mcp";
    if !research_mcp
        && let Some(version)=request.headers().get(CONTRACT_HEADER).and_then(|v|v.to_str().ok())
        && version!=CONTRACT_VERSION
    {
        return error(StatusCode::UPGRADE_REQUIRED,"Engine HTTP 契约版本不兼容。");
    }
    let mut response=next.run(request).await;
    response.headers_mut().insert(CONTRACT_HEADER,HeaderValue::from_static(CONTRACT_VERSION));
    if research_http {
        response.headers_mut().insert("X-SeeAlpha-Research-Contract-Version",HeaderValue::from_static("1"));
    }
    response
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
    fn unprocessable(message: impl Into<String>) -> Self {
        Self::Unprocessable(message.into())
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
struct StrategyLoadBody {
    account_id: String,
    issuance_id: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    code_key: String,
    #[serde(default)]
    entry_point: String,
    #[serde(default)]
    package_hash: String,
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

fn default_interval() -> String {
    "1m".into()
}

#[cfg(test)]
mod tests;
