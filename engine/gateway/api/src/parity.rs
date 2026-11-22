//! HTTP v2 compatibility endpoints migrated from the Python Engine.
//! No handler in this module can start a live collector or issue an order.
use crate::{AppError, AppState};
use axum::http::StatusCode;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::{Arc, OnceLock};

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/tenants/{tenant_id}/jobs",post(submit_job))
        .route("/tenants/{tenant_id}/jobs/{identity}",get(task))
        .route("/tenants/{tenant_id}/jobs/{identity}/result",get(task_result))
        .route("/tenants/{tenant_id}/jobs/{identity}/cancel",post(task_cancel))
        .route("/tenants/{tenant_id}/jobs/{identity}/retry",post(task_retry))
        .route("/tenants/{tenant_id}/accounts/{account_id}/positions",get(positions))
        .route("/tenants/{tenant_id}/accounts/{account_id}/requests/{request_id}",get(request))
        .route("/tenants/{tenant_id}/data/control/storage",post(storage))
        .route("/tenants/{tenant_id}/data/control/sync",post(data_sync))
        .route("/tenants/{tenant_id}/data/control/subscriptions",post(data_policies))
        .route("/tenants/{tenant_id}/data/control/services",post(data_services))
        .route("/tenants/{tenant_id}/data/control/snapshot",post(data_snapshot))
        .route("/tenants/{tenant_id}/data/control/jobs",post(data_jobs))
        .route("/tenants/{tenant_id}/data/control/job",post(data_job))
        .route("/tenants/{tenant_id}/data/control/job/start",post(data_start))
        .route("/tenants/{tenant_id}/data/control/job/cancel",post(data_cancel))
        .route("/tenants/{tenant_id}/data/control/job/retry",post(data_retry))
        .route("/tenants/{tenant_id}/data/control/configuration-plan",post(configuration_plan))
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

async fn submit_job(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    if body["kind"] != "data" {
        return Err(AppError::bad_request("仅支持数据任务。"));
    }
    let params = body.get("parameters")
        .ok_or_else(|| AppError::bad_request("任务参数无效。"))?;
    let queued = state.runtime.queue_data_job(&tenant, params)
        .map_err(AppError::from)?;
    if queued["status"] == "queued" {
        spawn_data_job(state.runtime.clone(), tenant, queued["id"].as_str().unwrap_or("").to_owned());
    }
    Ok((StatusCode::ACCEPTED, Json(queued)))
}
fn task_record(state: &AppState, tenant: &str, id: &str) -> Result<Value, AppError> {
    state.runtime.require_active(tenant).map_err(AppError::from)?;
    state.runtime.control.data_job(tenant, id)
        .map_err(|e| AppError::bad_request(e.to_string()))?
        .ok_or_else(|| AppError::not_found("数据任务不存在。"))
}
async fn task(
    State(state): State<AppState>,
    Path((tenant, id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(task_record(&state, &tenant, &id)?))
}
async fn task_result(
    State(state): State<AppState>,
    Path((tenant, id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let task = task_record(&state, &tenant, &id)?;
    if task["status"] == "completed" {
        return Ok(Json(task["result"].clone()));
    }
    if task["status"] == "failed" {
        return Err(AppError::conflict("任务失败，请查看任务记录。"));
    }
    Err(AppError::conflict("任务尚未完成，不能读取结果。"))
}
async fn task_cancel(
    State(state): State<AppState>,
    Path((tenant, id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(state.runtime.cancel_data_job(&tenant, &id).map_err(AppError::from)?))
}
async fn task_retry(
    State(state): State<AppState>,
    Path((tenant, id)): Path<(String, String)>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let before = task_record(&state, &tenant, &id)?;
    let status = before["status"].as_str().unwrap_or("");
    if !matches!(status, "failed" | "interrupted") {
        return Err(AppError::conflict("任务未处于失败或中断状态。"));
    }
    let transitioned = state.runtime.control
        .transition_data_job(&tenant, &id, status, "queued", None)
        .map_err(|e| AppError::bad_request(e.to_string()))?;
    if !transitioned {
        return Err(AppError::conflict("任务状态发生并发变化。"));
    }
    spawn_data_job(state.runtime.clone(), tenant.clone(), id.clone());
    Ok((StatusCode::ACCEPTED, Json(task_record(&state, &tenant, &id)?)))
}
async fn storage(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
) -> Result<Json<Value>, AppError> {
    state
        .runtime
        .require_active(&tenant)
        .map_err(AppError::from)?;
    Ok(Json(
        json!({"available":true,"backend":"sqlite","scope":"engine_native",
        "journal":"postgresql","raw_catalog_bridge":"not_implemented"}),
    ))
}
async fn data_sync(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let policies = body["policies"]
        .as_array()
        .ok_or_else(|| AppError::bad_request("行情配置无效。"))?;
    Ok(Json(
        state
            .runtime
            .sync_data_policies(&tenant, policies)
            .map_err(AppError::from)?,
    ))
}
async fn data_policies(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .data_policies(&tenant)
            .map_err(AppError::from)?,
    ))
}
async fn data_services(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .data_services(&tenant)
            .map_err(AppError::from)?,
    ))
}
async fn data_jobs(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .data_job_list(&tenant)
            .map_err(AppError::from)?,
    ))
}
async fn data_snapshot(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let policies = state
        .runtime
        .data_policies(&tenant)
        .map_err(AppError::from)?;
    let jobs = state
        .runtime
        .data_job_list(&tenant)
        .map_err(AppError::from)?;
    let selected = body["selected"].as_str().unwrap_or("");
    let detail = if selected.is_empty() {
        Value::Null
    } else {
        state
            .runtime
            .data_job(&tenant, selected)
            .map_err(AppError::from)?
    };
    Ok(Json(
        json!({"policies":policies["items"],"jobs":jobs["items"],"selected":detail}),
    ))
}
async fn data_job(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let id = body["id"]
        .as_str()
        .ok_or_else(|| AppError::bad_request("数据任务标识无效。"))?;
    Ok(Json(
        state
            .runtime
            .data_job(&tenant, id)
            .map_err(AppError::from)?,
    ))
}
async fn data_start(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let payload = body
        .get("payload")
        .ok_or_else(|| AppError::bad_request("行情任务参数无效。"))?;
    let normalized = payload.clone();
    let job = state
        .runtime
        .queue_data_job(&tenant, &normalized)
        .map_err(AppError::from)?;
    if job["status"] == "queued" {
        spawn_data_job(
            state.runtime.clone(),
            tenant,
            job["id"].as_str().unwrap_or("").to_string(),
        );
    }
    Ok(Json(job))
}
fn task_gate() -> Arc<tokio::sync::Semaphore> {
    static GATE: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    GATE.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
        .clone()
}
fn spawn_data_job(runtime: Arc<seealpha_runtime::Runtime>, tenant: String, id: String) {
    tokio::spawn(async move {
        let Ok(_permit) = task_gate().acquire_owned().await else {
            return;
        };
        let _ = runtime.execute_data_job(&tenant, &id).await;
    });
}
async fn data_cancel(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let id = body["id"]
        .as_str()
        .ok_or_else(|| AppError::bad_request("数据任务标识无效。"))?;
    Ok(Json(
        state
            .runtime
            .cancel_data_job(&tenant, id)
            .map_err(AppError::from)?,
    ))
}
async fn data_retry(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state
        .runtime
        .require_active(&tenant)
        .map_err(AppError::from)?;
    let id = body["id"]
        .as_str()
        .ok_or_else(|| AppError::bad_request("数据任务标识无效。"))?;
    let current = state
        .runtime
        .data_job(&tenant, id)
        .map_err(AppError::from)?;
    let previous = current["status"].as_str().unwrap_or("");
    if !matches!(previous, "failed" | "interrupted") {
        return Err(AppError::conflict("只有失败或中断的任务允许明确重试。"));
    }
    if !state
        .runtime
        .control
        .transition_data_job(&tenant, id, previous, "queued", None)
        .map_err(|e| AppError::bad_request(e.to_string()))?
    {
        return Err(AppError::conflict("任务状态已发生变化。"));
    }
    spawn_data_job(state.runtime.clone(), tenant.clone(), id.to_string());
    Ok(Json(
        state
            .runtime
            .data_job(&tenant, id)
            .map_err(AppError::from)?,
    ))
}

async fn configuration_plan(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let old = state
        .runtime
        .data_policies(&tenant)
        .map_err(AppError::from)?;
    Ok(Json(
        json!({"current":old["items"],"proposed":body["policy"],
        "effect":"review_only","will_delete":false}),
    ))
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
    let policies = state
        .runtime
        .control
        .policies(&tenant)
        .map_err(|e| AppError::bad_request(e.to_string()))?;
    let items = policies
        .into_iter()
        .filter(|policy| symbols.is_empty() || symbols.contains(&policy["symbol"]))
        .map(|policy| {
            json!({"product":policy["product"],"symbol":policy["symbol"],
        "base_level":policy["base_level"],"data_ready":false,
        "status":"not_verified","catalog_backend":"control_declaration_only"})
        })
        .collect::<Vec<_>>();
    Ok(Json(
        json!({"items":items,"backend":"control_declarations"}),
    ))
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
