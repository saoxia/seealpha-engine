//! HTTP v2 compatibility endpoints migrated from the Python Engine.
//! No handler in this module can start a live collector or issue an order.
use crate::{AppError, AppState, parse_time_ms};
use axum::{
    extract::{Path, Query, State},
    routing::{delete, get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::collections::HashMap;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/tenants/{tenant_id}/accounts/{account_id}/positions",get(positions))
        .route("/tenants/{tenant_id}/accounts/{account_id}/requests/{request_id}",get(request))
        .route("/tenants/{tenant_id}/accounts/{account_id}/market/subscriptions",
            get(subscriptions).post(subscription_put))
        .route("/tenants/{tenant_id}/accounts/{account_id}/strategy-market/subscriptions",
            post(strategy_subscribe))
        .route("/tenants/{tenant_id}/accounts/{account_id}/strategy-market/subscriptions/{issuance_id}",
            delete(strategy_unsubscribe))
        .route("/market/strategy-subscriptions",get(strategy_status))
        .route("/tenants/{tenant_id}/data/control/source/{product}/start",post(source_start))
        .route("/tenants/{tenant_id}/data/control/source/{product}/stop",post(source_stop))
        .route("/tenants/{tenant_id}/data/control/source/{product}",get(source_status))
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
        .route("/tenants/{tenant_id}/data/control/delete-plan",post(unsupported_delete))
        .route("/tenants/{tenant_id}/data/catalog/bars",get(catalog_bars))
}

#[derive(Deserialize)]
struct Environment { environment: Option<String> }
async fn positions(
    State(state):State<AppState>,
    Path((tenant,account)):Path<(String,String)>,
    Query(query):Query<Environment>,
)->Result<Json<Value>,AppError>{
    state.runtime.require_active(&tenant).map_err(AppError::from)?;
    let env=query.environment.ok_or_else(||AppError::bad_request("账户环境无效。"))?;
    let positions=state.runtime.ledger.positions(&tenant,&account,&env)
        .map_err(|e|AppError::bad_request(e.to_string()))?;
    Ok(Json(json!({"positions":positions})))
}

async fn request(
    State(state):State<AppState>,
    Path((tenant,account,request_id)):Path<(String,String,String)>,
)->Result<Json<Value>,AppError>{
    state.runtime.require_active(&tenant).map_err(AppError::from)?;
    let value=state.runtime.ledger.request(&tenant,&account,&request_id)
        .map_err(|e|AppError::bad_request(e.to_string()))?
        .ok_or_else(||AppError::not_found("服务器没有该请求记录，请在交易所核对后处理。"))?;
    Ok(Json(value))
}

async fn subscriptions(
    State(state):State<AppState>,
    Path((tenant,account)):Path<(String,String)>,
)->Result<Json<Value>,AppError>{
    Ok(Json(state.runtime.market_subscriptions(&tenant,&account).map_err(AppError::from)?))
}

async fn subscription_put(
    State(state):State<AppState>,
    Path((tenant,account)):Path<(String,String)>,
    Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    let product=body["product"].as_str().unwrap_or("");
    let symbol=body["symbol"].as_str().unwrap_or("");
    let role=body["role"].as_str().unwrap_or("");
    let status=body["state"].as_str().unwrap_or("");
    Ok(Json(state.runtime.note_market_subscription(
        &tenant,&account,product,symbol,role,status,&body["progress"]
    ).map_err(AppError::from)?))
}

async fn strategy_subscribe(
    State(state):State<AppState>,
    Path((tenant,account)):Path<(String,String)>,
    Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    let issuance=body["issuance_id"].as_str().unwrap_or("");
    // Only loaded, tenant-authorized strategies may ask for market views.
    let loads=state.runtime.strategy_loads(&tenant,Some(&account))
        .map_err(AppError::from)?;
    let authorized=loads["items"].as_array()
        .is_some_and(|items|items.iter().any(|item|item["issuance_id"]==issuance));
    if !authorized {return Err(AppError::conflict("策略尚未下发，不能订阅行情。"));}
    let lookback=body["lookback_bars"].as_i64().unwrap_or(1);
    Ok(Json(state.runtime.subscribe_strategy_market(
        &tenant,&account,issuance,&body["streams"],lookback,
    ).map_err(AppError::from)?))
}

async fn strategy_unsubscribe(
    State(state):State<AppState>,
    Path((tenant,account,issuance)):Path<(String,String,String)>,
)->Result<Json<Value>,AppError>{
    Ok(Json(state.runtime.unsubscribe_strategy_market(&tenant,&account,&issuance).map_err(AppError::from)?))
}

async fn strategy_status(
    State(state):State<AppState>,
)->Result<Json<Value>,AppError>{
    let tenant=state.config.tenant_id.as_deref()
        .ok_or_else(||AppError::forbidden("未固定租户，不能获取全局订阅。"))?;
    Ok(Json(state.runtime.strategy_market_status(tenant).map_err(AppError::from)?))
}

async fn source_start(
    State(state):State<AppState>,Path((tenant,product)):Path<(String,String)>,
)->Result<Json<Value>,AppError>{
    Ok(Json(state.runtime.start_native_source(&tenant,&product).map_err(AppError::from)?))
}
async fn source_stop(
    State(state):State<AppState>,Path((tenant,product)):Path<(String,String)>,
)->Result<Json<Value>,AppError>{
    Ok(Json(state.runtime.stop_native_source(&tenant,&product).map_err(AppError::from)?))
}
async fn source_status(
    State(state):State<AppState>,Path((tenant,product)):Path<(String,String)>,
)->Result<Json<Value>,AppError>{
    Ok(Json(state.runtime.native_source_status(&tenant,&product).map_err(AppError::from)?))
}
async fn storage(
    State(state):State<AppState>,Path(tenant):Path<String>,
)->Result<Json<Value>,AppError>{
    state.runtime.require_active(&tenant).map_err(AppError::from)?;
    Ok(Json(json!({"available":true,"backend":"sqlite","scope":"engine_native",
        "journal":"book.sqlite","raw_catalog_bridge":"not_implemented"})))
}
async fn data_sync(
    State(state):State<AppState>,Path(tenant):Path<String>,Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    let policies=body["policies"].as_array()
        .ok_or_else(||AppError::bad_request("行情配置无效。"))?;
    Ok(Json(state.runtime.sync_data_policies(&tenant,policies).map_err(AppError::from)?))
}
async fn data_policies(
    State(state):State<AppState>,Path(tenant):Path<String>,
)->Result<Json<Value>,AppError>{
    Ok(Json(state.runtime.data_policies(&tenant).map_err(AppError::from)?))
}
async fn data_services(
    State(state):State<AppState>,Path(tenant):Path<String>,
)->Result<Json<Value>,AppError>{
    Ok(Json(state.runtime.data_services(&tenant).map_err(AppError::from)?))
}
async fn data_jobs(
    State(state):State<AppState>,Path(tenant):Path<String>,
)->Result<Json<Value>,AppError>{
    Ok(Json(state.runtime.data_job_list(&tenant).map_err(AppError::from)?))
}
async fn data_snapshot(
    State(state):State<AppState>,Path(tenant):Path<String>,Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    let policies=state.runtime.data_policies(&tenant).map_err(AppError::from)?;
    let jobs=state.runtime.data_job_list(&tenant).map_err(AppError::from)?;
    let selected=body["selected"].as_str().unwrap_or("");
    let detail=if selected.is_empty(){Value::Null}else{
        state.runtime.data_job(&tenant,selected).map_err(AppError::from)?
    };
    Ok(Json(json!({"policies":policies["items"],"jobs":jobs["items"],"selected":detail})))
}
async fn data_job(
    State(state):State<AppState>,Path(tenant):Path<String>,Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    let id=body["id"].as_str().ok_or_else(||AppError::bad_request("数据任务标识无效。"))?;
    Ok(Json(state.runtime.data_job(&tenant,id).map_err(AppError::from)?))
}
async fn data_start(
    State(state):State<AppState>,Path(tenant):Path<String>,Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    let payload=body.get("payload").ok_or_else(||AppError::bad_request("行情任务参数无效。"))?;
    let mut normalized=payload.clone();
    if let Some(object)=normalized.as_object_mut() {
        // Accept the old API's UTC ISO window while retaining exact
        // millisecond bounds in the native job journal.
        for (legacy,native) in [("start","start_ms"),("end","end_ms")] {
            if !object.contains_key(native) {
                if let Some(value)=payload.get(legacy).and_then(Value::as_str) {
                    let ms=parse_time_ms(value)
                        .ok_or_else(||AppError::bad_request("行情任务时间无效。"))?;
                    object.insert(native.to_string(),json!(ms));
                }
            }
        }
    }
    let job=state.runtime.queue_data_job(&tenant,&normalized).map_err(AppError::from)?;
    if job["status"]=="queued" {
        spawn_data_job(state.runtime.clone(),tenant,job["id"].as_str().unwrap_or("").to_string());
    }
    Ok(Json(job))
}
fn spawn_data_job(runtime:Arc<seealpha_runtime::Runtime>,tenant:String,id:String) {
    tokio::spawn(async move {
        let _=runtime.execute_data_job(&tenant,&id).await;
    });
}
async fn data_cancel(
    State(state):State<AppState>,Path(tenant):Path<String>,Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    let id=body["id"].as_str().ok_or_else(||AppError::bad_request("数据任务标识无效。"))?;
    Ok(Json(state.runtime.cancel_data_job(&tenant,id).map_err(AppError::from)?))
}
async fn data_retry(
    State(state):State<AppState>,Path(tenant):Path<String>,Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    state.runtime.require_active(&tenant).map_err(AppError::from)?;
    let id=body["id"].as_str().ok_or_else(||AppError::bad_request("数据任务标识无效。"))?;
    let current=state.runtime.data_job(&tenant,id).map_err(AppError::from)?;
    let previous=current["status"].as_str().unwrap_or("");
    if !matches!(previous,"failed"|"interrupted") {
        return Err(AppError::conflict("只有失败或中断的任务允许明确重试。"));
    }
    if !state.runtime.ledger.transition_data_job(&tenant,id,previous,"queued",None)
        .map_err(|e|AppError::bad_request(e.to_string()))? {
        return Err(AppError::conflict("任务状态已发生变化。"));
    }
    spawn_data_job(state.runtime.clone(),tenant.clone(),id.to_string());
    Ok(Json(state.runtime.data_job(&tenant,id).map_err(AppError::from)?))
}

async fn configuration_plan(
    State(state):State<AppState>,Path(tenant):Path<String>,Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    let old=state.runtime.data_policies(&tenant).map_err(AppError::from)?;
    Ok(Json(json!({"current":old["items"],"proposed":body["policy"],
        "effect":"review_only","will_delete":false})))
}
async fn unsupported_delete()->Result<Json<Value>,AppError>{
    Err(AppError::conflict("历史数据删除尚未通过 Rust 快照与互斥锁验收，已拒绝。"))
}

async fn catalog_bars(
    State(state):State<AppState>,Path(tenant):Path<String>,
    Query(query):Query<HashMap<String,String>>,
)->Result<Json<Value>,AppError>{
    state.runtime.require_active(&tenant).map_err(AppError::from)?;
    let product=query.get("product").map(String::as_str).unwrap_or("spot");
    let symbol=query.get("symbol").map(String::as_str).unwrap_or("");
    let interval=query.get("interval").map(String::as_str).unwrap_or("1m");
    let start=query.get("start_ms").and_then(|v|v.parse::<i64>().ok())
        .ok_or_else(||AppError::bad_request("开始时间无效。"))?;
    let end=query.get("end_ms").and_then(|v|v.parse::<i64>().ok())
        .ok_or_else(||AppError::bad_request("结束时间无效。"))?;
    if start>=end || end-start>90*24*3600*1000 {
        return Err(AppError::bad_request("历史读取区间无效。"));
    }
    let bars=state.runtime.ledger.catalog_bars(&tenant,product,symbol,interval,start,end,5000)
        .map_err(|e|AppError::bad_request(e.to_string()))?;
    Ok(Json(json!({"items":bars.iter().map(|bar|json!({
        "open_time_ms":bar.open_time_ms,"open":bar.open,"high":bar.high,
        "low":bar.low,"close":bar.close,"volume":bar.volume,
    })).collect::<Vec<_>>(),"count":bars.len()})))
}
