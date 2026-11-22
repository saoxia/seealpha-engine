//! HTTP v2 compatibility endpoints migrated from the Python Engine.
//! No handler in this module can start a live collector or issue an order.
use crate::{AppError, AppState};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/tenants/{tenant_id}/accounts/{account_id}/positions",get(positions))
        .route("/tenants/{tenant_id}/accounts/{account_id}/requests/{request_id}",get(request))
        .route("/tenants/{tenant_id}/data/datasets",post(data_inventory))
        .route("/tenants/{tenant_id}/data/catalog/events",post(native_events))
        .route("/tenants/{tenant_id}/data/catalog/inventory",post(native_inventory))
}

#[derive(Deserialize)]
struct Environment {
    environment: Option<String>,
}
async fn positions(
    State(state): State<AppState>,
    Path((tenant, account)): Path<(String, String)>,
    Query(query): Query<Environment>,
) -> Result<Json<Value>, AppError> {
    state
        .runtime
        .require_active(&tenant)
        .map_err(AppError::from)?;
    let env = query
        .environment
        .ok_or_else(|| AppError::bad_request("账户环境无效。"))?;
    // Old Ledger positions are not an authoritative live or paper Position
    // service. Do not return an empty array (which could be interpreted as a
    // verified flat account) while the native position reader is unavailable.
    let _ = (account, env);
    Err(AppError::conflict(
        "Nautilus 原生持仓查询尚未接入，旧 Ledger 仓位不可用。",
    ))
}

async fn request(
    State(state): State<AppState>,
    Path((tenant, account, request_id)): Path<(String, String, String)>,
) -> Result<Json<Value>, AppError> {
    state
        .runtime
        .require_active(&tenant)
        .map_err(AppError::from)?;
    let value = state
        .runtime
        .order_request_view(&tenant, &account, &request_id)
        .map_err(AppError::from)?;
    Ok(Json(value))
}

async fn data_inventory(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state
        .runtime
        .require_active(&tenant)
        .map_err(AppError::from)?;
    let symbols = body["symbols"].as_array().cloned().unwrap_or_default();
    if symbols.len() > 200 {
        return Err(AppError::bad_request("查询品种数量超出范围。"));
    }
    if let Some(mut native) = state
        .runtime
        .native_dataset_inventory(&tenant)
        .map_err(AppError::from)?
    {
        native.retain(|item| symbols.is_empty() || symbols.contains(&item["symbol"]));
        return Ok(Json(json!({"items":native,"backend":"nautilus_v2_parquet",
            "quality":"unverified","data_ready":false})));
    }
    // Inventory remains read-only. Desired subscriptions are not evidence
    // of downloaded market data or validated Catalog readiness.
    let items = state.runtime.control.subscriptions(&tenant, None)
        .map_err(|error| AppError::bad_request(error.to_string()))?
        .into_iter()
        .filter(|item| symbols.is_empty() || symbols.iter().any(|s|s.as_str()==Some(&item.symbol)))
        .map(|item| json!({"product":item.product,"symbol":item.symbol,
            "history_start":item.history_start,"realtime_enabled":item.realtime_enabled,
            "enabled":item.enabled,"data_ready":false,"status":"not_verified",
            "catalog_backend":"subscription_declaration_only"}))
        .collect::<Vec<_>>();
    Ok(Json(json!({"items":items,"backend":"subscription_declarations"})))
}
/// Exact nanosecond read side for the Python Nautilus v2 Catalog.
/// Supports TradeTick, QuoteTick, Instrument, funding, Greeks, L2 and custom
/// types only when the original Arrow schema can be decoded losslessly.
fn ns_arg(body: &Value, key: &str) -> Result<i64, AppError> {
    body[key]
        .as_i64()
        .or_else(|| body[key].as_str().and_then(|s| s.parse::<i64>().ok()))
        .ok_or_else(|| AppError::bad_request("历史事件纳秒时间参数无效。"))
}
fn native_kind(body: &Value) -> Result<seealpha_catalog::RecordKind, AppError> {
    let raw = body["data_type"]
        .as_str()
        .ok_or_else(|| AppError::bad_request("历史数据类型缺失。"))?;
    if matches!(raw, "custom" | "supplement") {
        let source = body["feature_catalog"]
            .as_str()
            .ok_or_else(|| AppError::bad_request("自定义行情目录无效。"))?;
        let name = body["class_name"]
            .as_str()
            .ok_or_else(|| AppError::bad_request("自定义行情类型无效。"))?;
        return Ok(if raw == "custom" {
            seealpha_catalog::RecordKind::Feature {
                identifier: source.to_string(),
                name: name.to_string(),
            }
        } else {
            seealpha_catalog::RecordKind::Supplement {
                identifier: source.to_string(),
                name: name.to_string(),
            }
        });
    }
    seealpha_catalog::RecordKind::from_name(raw).map_err(|e| AppError::bad_request(e.to_string()))
}
async fn native_events(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let identity = body["identifier"]
        .as_str()
        .ok_or_else(|| AppError::bad_request("行情 InstrumentId/BarType 缺失。"))?;
    let from = ns_arg(&body, "start_ns")?;
    let until = ns_arg(&body, "end_ns")?;
    let limit = body["limit"].as_u64().unwrap_or(1000);
    if limit == 0 || limit > 50_000 {
        return Err(AppError::bad_request("最大事件批次不能超过 50,000。"));
    }
    let rows = state
        .runtime
        .native_market_events(
            &tenant,
            &native_kind(&body)?,
            identity,
            from,
            until,
            limit as usize,
        )
        .map_err(AppError::from)?;
    let count = rows.len();
    Ok(Json(json!({"records":rows,"count":count,
        "start_ns":from.to_string(),"end_ns":until.to_string(),
        "backend":"nautilus_v2_parquet","quality":"unverified",
        "side_effects":false})))
}
async fn native_inventory(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let identity = body["identifier"]
        .as_str()
        .ok_or_else(|| AppError::bad_request("行情目录标识缺失。"))?;
    let entries = state
        .runtime
        .native_market_inventory(&tenant, &native_kind(&body)?, identity)
        .map_err(AppError::from)?;
    Ok(Json(
        json!({"items":entries,"backend":"nautilus_v2_parquet",
        "quality":"unverified","does_not_start_collector":true}),
    ))
}
