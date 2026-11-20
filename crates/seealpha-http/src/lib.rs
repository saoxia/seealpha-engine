//! HTTP v2 boundary. Every response carries `X-SeeAlpha-Engine-Contract-Version: 2`.
//! Live exchange routes refuse before any credential is stored.
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use base64::Engine;
use seealpha_market::{LoadQuery, MarketSnapshot, Product, PublicSource, check_symbol};
use seealpha_research::{require_op, verify_research_grant};
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
const RESEARCH_HEADER: &str = "X-SeeAlpha-Research-Contract-Version";
const RESEARCH_VERSION: &str = "1";

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
            get(worker_settings),
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
            "/tenants/{tenant_id}/accounts/{account_id}/backtests",
            post(run_backtest),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/connections",
            post(live_disabled),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/snapshots",
            post(live_disabled),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/backtests/latest",
            get(latest_backtest),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/carry-backtests/latest",
            get(no_carry),
        )
        .route(
            "/tenants/{tenant_id}/accounts/{account_id}/protected-option-backtests/latest",
            get(no_option),
        )
        .fallback(not_found);
    if state.config.research_signing_key.is_some() {
        router = router.route("/research/v1/datasets", get(research_datasets));
        router = router.route("/research/v1/snapshots", post(create_snapshot));
        router = router.route("/research/v1/snapshots/{snapshot_id}", get(get_snapshot));
        if state.config.research_mcp_enabled {
            router = router.route("/research/mcp", post(research_mcp));
        }
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
    axum::serve(listener, router(state)).await
}

async fn gate(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let path = request.uri().path().to_string();
    if let Some(version) = request
        .headers()
        .get(CONTRACT_HEADER)
        .and_then(|value| value.to_str().ok())
        && version != CONTRACT_VERSION
    {
        return error(StatusCode::UPGRADE_REQUIRED, "Engine HTTP 契约版本不兼容。");
    }
    if let Some(pinned) = &state.config.tenant_id
        && let Some(target) = tenant_from(&path)
        && target != pinned
    {
        return error(StatusCode::FORBIDDEN, "Engine 租户不匹配。");
    }
    let research = path == "/research" || path.starts_with("/research/");
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
    response
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "workers": state.runtime.inflight(),
        "contract_version": CONTRACT_VERSION,
    }))
}

async fn ensure_tenant(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    Json(body): Json<TenantBody>,
) -> Result<Json<Value>, AppError> {
    if let Some(purpose) = &state.config.tenant_purpose
        && body.purpose != *purpose
    {
        return Err(AppError::forbidden("Engine 租户类型不匹配。"));
    }
    let tenant = state
        .runtime
        .ensure_tenant(&tenant_id, &body.purpose, &body.status)
        .map_err(AppError::from)?;
    Ok(Json(json!({
        "tenant_id": tenant.tenant_id,
        "purpose": tenant.purpose,
        "status": tenant.status,
    })))
}

async fn runnable(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    Json(body): Json<RunnableBody>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .runnable(&tenant_id, &body.action)
            .map_err(AppError::from)?,
    ))
}

async fn jobs() -> Json<Value> {
    Json(json!({ "items": [] }))
}

async fn worker_settings() -> Json<Value> {
    Json(json!({ "max_workers": 1, "timeout_seconds": 3600 }))
}

async fn strategy_load(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    Json(body): Json<StrategyLoadBody>,
) -> Result<Json<Value>, AppError> {
    let archive = base64::engine::general_purpose::STANDARD
        .decode(body.archive.trim())
        .map_err(|_| AppError::bad_request("策略包无效。"))?;
    Ok(Json(
        state
            .runtime
            .accept_strategy(&StrategySubmission {
                tenant_id: &tenant_id,
                account_id: &body.account_id,
                issuance_id: &body.issuance_id,
                version: &body.version,
                code_key: &body.code_key,
                entry_point: &body.entry_point,
                package_hash: &body.package_hash,
                grant_purpose: &body.grant_purpose,
                parameters: &body.parameters,
                archive: &archive,
            })
            .map_err(AppError::from)?,
    ))
}

async fn list_strategies(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    Query(query): Query<AccountQuery>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .strategy_loads(&tenant_id, query.account_id.as_deref())
            .map_err(AppError::from)?,
    ))
}

async fn strategy_preview(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<PreviewBody>,
) -> Result<Json<Value>, AppError> {
    let strategies = state
        .runtime
        .stored_strategies(&tenant_id, &account_id)
        .map_err(AppError::from)?;
    let snapshot = bars_snapshot(&body)?;
    Ok(Json(
        state
            .runtime
            .run_paper(PaperRequest {
                tenant_id,
                account_id,
                snapshot,
                strategies,
                request_id: body.request_id,
            })
            .map_err(AppError::from)?,
    ))
}

async fn paper_session(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<PaperBody>,
) -> Result<Json<Value>, AppError> {
    check_symbol(&body.symbol).map_err(|error| AppError::bad_request(error.to_string()))?;
    let tenant = state
        .runtime
        .require_active(&tenant_id)
        .map_err(AppError::from)?;
    let strategies = if body.use_sample {
        if tenant.purpose == "production" {
            return Err(AppError::forbidden("正式租户不能使用示例策略。"));
        }
        vec![StrategyInput {
            member_id: "ema_crossover".into(),
            package: PackageBody::Directory(state.config.sample_package()),
            parameters: if body.parameters.is_null() {
                json!({})
            } else {
                body.parameters
            },
        }]
    } else {
        state
            .runtime
            .stored_strategies(&tenant_id, &account_id)
            .map_err(AppError::from)?
    };
    let product =
        Product::parse(&body.product).map_err(|error| AppError::bad_request(error.to_string()))?;
    let snapshot = state
        .market
        .load(&LoadQuery {
            product,
            symbol: body.symbol,
            interval: body.interval,
            limit: body.limit,
        })
        .await
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    Ok(Json(
        state
            .runtime
            .run_paper(PaperRequest {
                tenant_id,
                account_id,
                snapshot,
                strategies,
                request_id: body.request_id,
            })
            .map_err(AppError::from)?,
    ))
}

async fn list_orders(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Query(query): Query<EnvironmentQuery>,
) -> Result<Json<Value>, AppError> {
    let environment = query.environment.unwrap_or_else(|| "paper".into());
    let orders = state
        .runtime
        .ledger
        .orders(&tenant_id, &account_id, &environment)
        .map_err(|error| match error {
            seealpha_ledger::LedgerError::Conflict(message) => AppError::conflict(message),
            seealpha_ledger::LedgerError::Sqlite(detail) => {
                AppError::bad_request(format!("状态库不可用。{detail}"))
            }
        })?;
    let items = orders
        .into_iter()
        .map(|order| {
            serde_json::from_str::<Value>(&order.order_json).unwrap_or(json!({ "venue": "paper" }))
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({ "orders": items })))
}

async fn order_command(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let venue_base = state
        .config
        .venue_base
        .clone()
        .ok_or_else(|| AppError::conflict("交易网关未配置，拒绝向默认交易所发单。"))?;
    let environment = body
        .get("environment")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let api_key = body
        .get("api_key")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let secret = body
        .get("api_secret")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let action = body
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let values = body.get("values").cloned().unwrap_or(Value::Null);
    let result = state.runtime.execute_order(OrderCommand {
        tenant_id,
        account_id,
        environment,
        api_key,
        secret,
        action,
        values,
        venue_base,
    });
    Ok(Json(result.map_err(AppError::from)?))
}

async fn ledger_sync(
    State(state): State<AppState>,
    Path((tenant_id, account_id, request_id)): Path<(String, String, String)>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .mark_ledger_synced(&tenant_id, &account_id, &request_id)
            .map_err(AppError::from)?,
    ))
}

async fn reconcile_order(
    State(state): State<AppState>,
    Path((tenant_id, account_id, request_id)): Path<(String, String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let venue_base = state.config.venue_base.clone().unwrap_or_default();
    Ok(Json(
        state
            .runtime
            .reconcile_order(
                &tenant_id,
                &account_id,
                &request_id,
                body.get("environment")
                    .and_then(Value::as_str)
                    .unwrap_or("production"),
                body.get("api_key").and_then(Value::as_str).unwrap_or(""),
                body.get("api_secret").and_then(Value::as_str).unwrap_or(""),
                &venue_base,
                body.get("account_revision")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
            )
            .map_err(AppError::from)?,
    ))
}

async fn list_fills(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Query(query): Query<EnvironmentQuery>,
) -> Result<Json<Value>, AppError> {
    let environment = query.environment.unwrap_or_else(|| "production".into());
    let fills = state
        .runtime
        .ledger
        .fills(&tenant_id, &account_id, &environment)
        .map_err(|error| match error {
            seealpha_ledger::LedgerError::Conflict(message) => AppError::conflict(message),
            seealpha_ledger::LedgerError::Sqlite(detail) => AppError::bad_request(detail),
        })?;
    let items = fills
        .into_iter()
        .filter_map(|row| serde_json::from_str::<Value>(&row).ok())
        .collect::<Vec<_>>();
    Ok(Json(json!({ "fills": items })))
}

async fn list_gaps(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let gaps = state
        .runtime
        .ledger
        .gaps(&tenant_id, &account_id)
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    let items = gaps
        .into_iter()
        .filter_map(|row| serde_json::from_str::<Value>(&row).ok())
        .collect::<Vec<_>>();
    Ok(Json(json!({ "gaps": items })))
}

async fn put_gap(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let scope = body.get("scope").and_then(Value::as_str).unwrap_or("");
    let gap_key = body.get("gap_key").and_then(Value::as_str).unwrap_or("");
    let gap = body.get("gap").cloned().unwrap_or(json!({}));
    state
        .runtime
        .ledger
        .put_gap(&tenant_id, &account_id, scope, gap_key, &gap.to_string())
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    state
        .runtime
        .ledger
        .add_event(&tenant_id, &account_id, "gap", &gap.to_string())
        .ok();
    Ok(Json(gap))
}

async fn list_events(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let events = state
        .runtime
        .ledger
        .events(&tenant_id, &account_id)
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    let items = events
        .into_iter()
        .map(|(kind, body)| json!({"kind": kind, "body": body}))
        .collect::<Vec<_>>();
    Ok(Json(json!({ "events": items })))
}

async fn import_bars(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let product = body
        .get("product")
        .and_then(Value::as_str)
        .unwrap_or("spot");
    let symbol = body.get("symbol").and_then(Value::as_str).unwrap_or("");
    let interval = body.get("interval").and_then(Value::as_str).unwrap_or("1m");
    let rows = body
        .get("bars")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let bars = rows
        .iter()
        .filter_map(|row| {
            Some(seealpha_ledger::CatalogBar {
                open_time_ms: row.get("open_time_ms")?.as_i64()?,
                open: row.get("open")?.as_str()?.to_string(),
                high: row.get("high")?.as_str()?.to_string(),
                low: row.get("low")?.as_str()?.to_string(),
                close: row.get("close")?.as_str()?.to_string(),
                volume: row.get("volume")?.as_str()?.to_string(),
            })
        })
        .collect::<Vec<_>>();
    let count = state
        .runtime
        .ledger
        .import_bars(&tenant_id, product, symbol, interval, &bars)
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    Ok(Json(json!({ "imported": count })))
}

async fn run_backtest(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let symbol = body
        .get("symbol")
        .and_then(Value::as_str)
        .unwrap_or("ETHUSDT");
    let quote = body
        .get("quote_currency")
        .and_then(Value::as_str)
        .unwrap_or("USDT");
    let starting: rust_decimal::Decimal = body
        .get("starting_balance")
        .and_then(Value::as_str)
        .unwrap_or("10000")
        .parse()
        .map_err(|_| AppError::bad_request("虚拟账户资金无效。"))?;
    let risk: rust_decimal::Decimal = body
        .get("risk_limit")
        .and_then(Value::as_str)
        .unwrap_or("10")
        .parse()
        .map_err(|_| AppError::bad_request("风险额度无效。"))?;
    let raw_bars = body
        .get("bars")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let bars = raw_bars
        .iter()
        .enumerate()
        .filter_map(|(index, row)| {
            let close = row.get("close")?.as_str()?.parse().ok()?;
            let open_time_ms = row
                .get("open_time_ms")
                .and_then(Value::as_i64)
                .unwrap_or(index as i64 * 60_000);
            Some(seealpha_backtest::HistoryBar {
                open_time_ms,
                close,
            })
        })
        .collect::<Vec<_>>();
    let targets = body
        .get("targets")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|row| {
            Some(seealpha_backtest::PublishedTarget {
                member_id: row
                    .get("member_id")
                    .and_then(Value::as_str)
                    .unwrap_or("strategy")
                    .to_string(),
                bar_index: row.get("bar_index")?.as_u64()? as usize,
                quantity: row.get("quantity")?.as_str()?.parse().ok()?,
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(
        state
            .runtime
            .run_account_backtest(
                &tenant_id,
                &account_id,
                symbol,
                quote,
                starting,
                risk,
                &bars,
                &targets,
            )
            .map_err(AppError::from)?,
    ))
}

async fn latest_backtest(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .latest_history_backtest(&tenant_id, &account_id)
            .map_err(AppError::from)?,
    ))
}

async fn live_disabled() -> Result<Json<Value>, AppError> {
    Err(AppError::conflict(
        "实盘下单已关闭，引擎不会向交易所发送订单或保存密钥。",
    ))
}

async fn no_carry() -> Result<Json<Value>, AppError> {
    Err(AppError::not_found("还没有资金费组合回测结果"))
}

async fn no_option() -> Result<Json<Value>, AppError> {
    Err(AppError::not_found("还没有期权保护组合回测结果"))
}

async fn not_found() -> Result<Json<Value>, AppError> {
    Err(AppError::not_found("路径不存在。"))
}

async fn research_datasets(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let key = state
        .config
        .research_signing_key
        .clone()
        .ok_or_else(|| AppError::not_found("路径不存在。"))?;
    if headers
        .get(CONTRACT_HEADER)
        .and_then(|value| value.to_str().ok())
        != Some(CONTRACT_VERSION)
        || headers
            .get(RESEARCH_HEADER)
            .and_then(|value| value.to_str().ok())
            != Some(RESEARCH_VERSION)
    {
        return Err(AppError::upgrade("Research contract version mismatch"));
    }
    let authorization = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let Some(token) = authorization.strip_prefix("Bearer ") else {
        return Err(AppError::unauthorized("Research authorization required"));
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0);
    let grant = verify_research_grant(&key, token.trim(), now)
        .map_err(|_| AppError::unauthorized("Research authorization rejected"))?;
    if state.config.tenant_purpose.as_deref() != Some("development")
        || state.config.tenant_id.as_deref() != Some(grant.tenant_id.as_str())
    {
        return Err(AppError::forbidden("Research tenant scope denied"));
    }
    let tenant = state
        .runtime
        .require_active(&grant.tenant_id)
        .map_err(|_| AppError::forbidden("Research requires an active development tenant"))?;
    if tenant.purpose != "development" {
        return Err(AppError::forbidden(
            "Research requires an active development tenant",
        ));
    }
    require_op(&grant, "datasets:read", None, None)
        .map_err(|_| AppError::forbidden("Research operation outside granted scope"))?;
    let mut response = Json(json!({ "items": grant.datasets })).into_response();
    response
        .headers_mut()
        .insert(RESEARCH_HEADER, HeaderValue::from_static(RESEARCH_VERSION));
    response
        .headers_mut()
        .insert(CONTRACT_HEADER, HeaderValue::from_static(CONTRACT_VERSION));
    Ok(response)
}

fn bearer_grant(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<seealpha_research::Grant, AppError> {
    let key = state
        .config
        .research_signing_key
        .clone()
        .ok_or_else(|| AppError::not_found("路径不存在。"))?;
    if headers
        .get(CONTRACT_HEADER)
        .and_then(|value| value.to_str().ok())
        != Some(CONTRACT_VERSION)
        || headers
            .get(RESEARCH_HEADER)
            .and_then(|value| value.to_str().ok())
            != Some(RESEARCH_VERSION)
    {
        return Err(AppError::upgrade("Research contract version mismatch"));
    }
    let authorization = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let Some(token) = authorization.strip_prefix("Bearer ") else {
        return Err(AppError::unauthorized("Research authorization required"));
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0);
    let grant = verify_research_grant(&key, token.trim(), now)
        .map_err(|_| AppError::unauthorized("Research authorization rejected"))?;
    if state.config.tenant_purpose.as_deref() != Some("development")
        || state.config.tenant_id.as_deref() != Some(grant.tenant_id.as_str())
    {
        return Err(AppError::forbidden("Research tenant scope denied"));
    }
    Ok(grant)
}

async fn create_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let grant = bearer_grant(&state, &headers)?;
    require_op(&grant, "snapshots:create", None, None)
        .map_err(|_| AppError::forbidden("Research operation outside granted scope"))?;
    Ok(Json(
        state
            .runtime
            .create_research_snapshot(&grant.tenant_id, &body.to_string())
            .map_err(AppError::from)?,
    ))
}

async fn get_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(snapshot_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let grant = bearer_grant(&state, &headers)?;
    require_op(&grant, "snapshots:read", None, None)
        .map_err(|_| AppError::forbidden("Research operation outside granted scope"))?;
    Ok(Json(
        state
            .runtime
            .research_snapshot(&grant.tenant_id, &snapshot_id)
            .map_err(AppError::from)?,
    ))
}

async fn research_mcp(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    if headers.get("origin").is_some() {
        return Err(AppError::forbidden(
            "Browser origins not allowed on Research MCP",
        ));
    }
    let grant = bearer_grant(&state, &headers)?;
    let id = body.get("id").cloned().unwrap_or(Value::Null);
    let method = body.get("method").and_then(Value::as_str).unwrap_or("");
    let result = match method {
        "initialize" => json!({
            "protocolVersion": "2025-11-25",
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "seealpha-engine", "version": "0.1.0" }
        }),
        "tools/list" => json!({
            "tools": [
                {"name": "list_datasets", "description": "List granted datasets"},
                {"name": "create_snapshot", "description": "Store a research snapshot"},
                {"name": "get_snapshot", "description": "Read a research snapshot"},
                {"name": "submit_backtest", "description": "Run a development backtest"},
                {"name": "get_backtest", "description": "Read the latest development backtest"}
            ]
        }),
        "tools/call" => {
            let name = body
                .pointer("/params/name")
                .and_then(Value::as_str)
                .unwrap_or("");
            let payload = match name {
                "list_datasets" => {
                    require_op(&grant, "datasets:read", None, None).map_err(|_| {
                        AppError::forbidden("Research operation outside granted scope")
                    })?;
                    json!({ "items": grant.datasets })
                }
                "get_backtest" => {
                    require_op(&grant, "backtests:read", None, None).map_err(|_| {
                        AppError::forbidden("Research operation outside granted scope")
                    })?;
                    state
                        .runtime
                        .latest_history_backtest(&grant.tenant_id, "research")
                        .unwrap_or(json!({ "detail": "还没有回测" }))
                }
                _ => json!({ "detail": "该研究工具请改用 HTTP 研究路由。" }),
            };
            json!({ "content": [{ "type": "text", "text": payload.to_string() }] })
        }
        _ => json!({ "ok": false }),
    };
    Ok(Json(
        json!({ "jsonrpc": "2.0", "id": id, "result": result }),
    ))
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
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use seealpha_market::BinanceSource;
    use seealpha_runtime::Config;
    use tower::ServiceExt;

    fn state() -> AppState {
        let root = std::env::temp_dir().join(format!(
            "seealpha-http-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let config = Config {
            host: "127.0.0.1".into(),
            port: 0,
            auth_mode: AuthMode::Token,
            token: "ci-engine-token-0123456789abcdef0123456789abcdef".into(),
            state_dir: root.join("state"),
            project: root.clone(),
            tenant_id: Some("42".into()),
            tenant_purpose: Some("development".into()),
            research_signing_key: None,
            research_mcp_enabled: false,
            venue_base: None,
            python: std::path::PathBuf::from("python3"),
        };
        let runtime = Arc::new(Runtime::open(&config).unwrap());
        let market = Arc::new(BinanceSource::new().unwrap_or_else(|_| panic!("client")));
        AppState {
            config: Arc::new(config),
            runtime,
            market,
        }
    }

    async fn call(app: &Router, request: Request) -> (StatusCode, String, Value) {
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let header = response
            .headers()
            .get(CONTRACT_HEADER)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value = serde_json::from_slice(&bytes).unwrap_or(json!({}));
        (status, header, value)
    }

    #[tokio::test]
    async fn handshake_statuses_match_platform_v2() {
        let app = router(state());
        let token = "ci-engine-token-0123456789abcdef0123456789abcdef";
        let (status, header, _) = call(
            &app,
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(header, "2");
        let (status, header, body) = call(
            &app,
            Request::builder()
                .uri("/health")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(header, "2");
        assert_eq!(body["contract_version"], "2");
        let (status, header, _) = call(
            &app,
            Request::builder()
                .uri("/health")
                .header("authorization", format!("Bearer {token}"))
                .header(CONTRACT_HEADER, "incompatible")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UPGRADE_REQUIRED);
        assert_eq!(header, "2");
        let (status, _, _) = call(
            &app,
            Request::builder()
                .uri("/tenants/99/jobs")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _, _) = call(
            &app,
            Request::builder()
                .uri("/tenants/42/accounts/a/orders")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .method("POST")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
    }
}
