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
use std::sync::{Arc,OnceLock};
use axum::http::StatusCode;
use std::collections::HashMap;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/tenants/{tenant_id}/jobs",post(submit_job))
        .route("/tenants/{tenant_id}/jobs/{identity}",get(task))
        .route("/tenants/{tenant_id}/jobs/{identity}/result",get(task_result))
        .route("/tenants/{tenant_id}/jobs/{identity}/cancel",post(task_cancel))
        .route("/tenants/{tenant_id}/jobs/{identity}/retry",post(task_retry))
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
        .route("/tenants/{tenant_id}/data/datasets",post(data_inventory))
        .route("/tenants/{tenant_id}/data/coverage",post(data_coverage))
        .route("/tenants/{tenant_id}/data/prepare-plan",post(data_prepare_plan))
        .route("/tenants/{tenant_id}/data/page",post(data_page))
        .route("/tenants/{tenant_id}/data/chart",post(data_chart))
        .route("/tenants/{tenant_id}/data/latest",post(data_latest))
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
async fn submit_job(
    State(state):State<AppState>,Path(tenant):Path<String>,Json(body):Json<Value>,
)->Result<(StatusCode,Json<Value>),AppError>{
    let kind=body["kind"].as_str().ok_or_else(||AppError::bad_request("任务类型无效。"))?;
    let params=body.get("parameters").ok_or_else(||AppError::bad_request("任务参数无效。"))?;
    let mut params=params.clone();
    if let Some(obj)=params.as_object_mut() {
        for (source,target) in [("start","start_ms"),("end","end_ms")] {
            if !obj.contains_key(target) {
                if let Some(value)=obj.get(source).and_then(Value::as_str)
                    && let Some(milliseconds)=parse_time_ms(value) {
                    obj.insert(target.to_string(),json!(milliseconds));
                }
            }
        }
    }
    let job=if kind=="backtest" {
        let queued=state.runtime.schedule_backtest_job(
            &tenant,&params,body["idempotency_key"].as_str()
        ).map_err(AppError::from)?;
        if queued["status"]=="queued" {
            spawn_backtest(state.runtime.clone(),tenant,queued["id"].as_str().unwrap_or("").into());
        }
        queued
    } else if kind=="data" {
        let queued=state.runtime.queue_data_job(&tenant,&params).map_err(AppError::from)?;
        if queued["status"]=="queued" {
            spawn_data_job(state.runtime.clone(),tenant,queued["id"].as_str().unwrap_or("").into());
        }
        queued
    } else {return Err(AppError::bad_request("任务类型无效。"));};
    Ok((StatusCode::ACCEPTED,Json(job)))
}
async fn task(
    State(state):State<AppState>,Path((tenant,id)):Path<(String,String)>,
)->Result<Json<Value>,AppError>{
    Ok(Json(state.runtime.data_job(&tenant,&id).map_err(AppError::from)?))
}
async fn task_result(
    State(state):State<AppState>,Path((tenant,id)):Path<(String,String)>,
)->Result<Json<Value>,AppError>{
    let task=state.runtime.data_job(&tenant,&id).map_err(AppError::from)?;
    if task["status"]=="completed" {return Ok(Json(task["result"].clone()));}
    if task["status"]=="failed" {return Err(AppError::conflict("任务失败，请查看任务记录。"));}
    Err(AppError::conflict("任务尚未完成，不能读取结果。"))
}
async fn task_cancel(
    State(state):State<AppState>,Path((tenant,id)):Path<(String,String)>,
)->Result<Json<Value>,AppError>{
    Ok(Json(state.runtime.cancel_data_job(&tenant,&id).map_err(AppError::from)?))
}
async fn task_retry(
    State(state):State<AppState>,Path((tenant,id)):Path<(String,String)>,
)->Result<(StatusCode,Json<Value>),AppError>{
    let before=state.runtime.data_job(&tenant,&id).map_err(AppError::from)?;
    let status=before["status"].as_str().unwrap_or("");
    if !matches!(status,"failed"|"interrupted") {
        return Err(AppError::conflict("任务未处于失败或中断状态。"));
    }
    if !state.runtime.ledger.transition_data_job(&tenant,&id,status,"queued",None)
        .map_err(|e|AppError::bad_request(e.to_string()))? {
        return Err(AppError::conflict("任务状态发生并发变化。"));
    }
    if before["action"]=="backtest"{
        spawn_backtest(state.runtime.clone(),tenant.clone(),id.clone());
    }else{
        spawn_data_job(state.runtime.clone(),tenant.clone(),id.clone());
    }
    Ok((StatusCode::ACCEPTED,Json(
        state.runtime.data_job(&tenant,&id).map_err(AppError::from)?
    )))
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
fn task_gate()->Arc<tokio::sync::Semaphore> {
    static GATE:OnceLock<Arc<tokio::sync::Semaphore>>=OnceLock::new();
    GATE.get_or_init(||Arc::new(tokio::sync::Semaphore::new(1))).clone()
}
fn spawn_data_job(runtime:Arc<seealpha_runtime::Runtime>,tenant:String,id:String) {
    tokio::spawn(async move {
        let Ok(_permit)=task_gate().acquire_owned().await else {return;};
        let _=runtime.execute_data_job(&tenant,&id).await;
    });
}
fn spawn_backtest(runtime:Arc<seealpha_runtime::Runtime>,tenant:String,id:String){
    tokio::spawn(async move {
        let Ok(_permit)=task_gate().acquire_owned().await else {return;};
        let _=tokio::task::spawn_blocking(move||{
            let _=runtime.execute_backtest_job(&tenant,&id);
        }).await;
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

// Read-side endpoints use ONLY the tenant's existing Rust catalog; no route
// here starts Binance sockets, downloads archives or modifies DataPolicy.
fn data_query_range(body:&Value)->Result<(&str,&str,&str,i64,i64,i64),AppError>{
    let product=body["product"].as_str().unwrap_or("futures_um");
    let symbol=body["symbol"].as_str()
        .ok_or_else(||AppError::bad_request("查询品种无效。"))?;
    let interval=body["interval"].as_str()
        .or_else(||body["intervals"].as_array()
            .and_then(|v|v.first()).and_then(Value::as_str))
        .unwrap_or("1m");
    if product=="options"{
        seealpha_market::options::parse_option_symbol(symbol)
            .map_err(|_|AppError::bad_request("期权合约无效。"))?;
        if !matches!(interval,"1m"|"5m"|"15m"){
            return Err(AppError::bad_request("期权原生历史周期不支持。"));
        }
    }else{
        let _=seealpha_market::Product::parse(product)
            .map_err(|_|AppError::bad_request("产品不支持原生历史读取。"))?;
        seealpha_market::check_symbol(symbol)
            .map_err(|_|AppError::bad_request("查询品种无效。"))?;
    }
    let step=seealpha_market::interval_ms(interval)
        .map_err(|_|AppError::bad_request("历史 K 线周期无效。"))?;
    let stamp=|key:&str,native:&str|->Result<i64,AppError>{
        body[native].as_i64().or_else(||body[key].as_str().and_then(parse_time_ms))
            .ok_or_else(||AppError::bad_request("历史数据时间无效。"))
    };
    let start=stamp("start","start_ms")?;
    let end=stamp("end","end_ms")?;
    let count=end.checked_sub(start)
        .filter(|d|*d>0 && *d<=step*5000)
        .ok_or_else(||AppError::bad_request("历史查询范围无效。"))?;
    if start<0||start%step!=0||end%step!=0||count<step{
        return Err(AppError::bad_request("历史查询必须对齐完整 K 线。"));
    }
    Ok((product,symbol,interval,start,end,step))
}
fn catalog_bar_json(bar:&seealpha_ledger::CatalogBar)->Value{
    json!({"open_time_ms":bar.open_time_ms,"open":bar.open,
        "high":bar.high,"low":bar.low,"close":bar.close,"volume":bar.volume,
        "is_closed":true})
}
async fn data_inventory(
    State(state):State<AppState>,Path(tenant):Path<String>,Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    state.runtime.require_active(&tenant).map_err(AppError::from)?;
    let symbols=body["symbols"].as_array().cloned().unwrap_or_default();
    if symbols.len()>200 {return Err(AppError::bad_request("查询品种数量超出范围。"));}
    let policies=state.runtime.ledger.policies(&tenant)
        .map_err(|e|AppError::bad_request(e.to_string()))?;
    let items=policies.into_iter().filter(|policy|{
        symbols.is_empty()||symbols.contains(&policy["symbol"])
    }).map(|policy|json!({"product":policy["product"],"symbol":policy["symbol"],
        "base_level":policy["base_level"],"data_ready":false,
        "status":"not_verified","catalog_backend":"sqlite"})).collect::<Vec<_>>();
    Ok(Json(json!({"items":items})))
}
async fn data_coverage(
    State(state):State<AppState>,Path(tenant):Path<String>,Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    state.runtime.require_active(&tenant).map_err(AppError::from)?;
    let (product,symbol,interval,start,end,step)=data_query_range(&body)?;
    let rows=state.runtime.ledger.catalog_bars(&tenant,product,symbol,interval,start,end,5000)
        .map_err(|e|AppError::bad_request(e.to_string()))?;
    let expected=(end-start)/step;
    let mut gaps=Vec::new();
    let mut cursor=start;
    for bar in &rows {
        while cursor<bar.open_time_ms {
            gaps.push(cursor);
            cursor+=step;
        }
        if bar.open_time_ms<cursor {
            return Err(AppError::conflict("历史时间戳重复或乱序。"));
        }
        cursor=cursor.saturating_add(step);
    }
    while cursor<end {
        gaps.push(cursor);
        cursor+=step;
    }
    Ok(Json(json!({"product":product,"symbol":symbol,"interval":interval,
        "start_ms":start,"end_ms":end,"expected_bars":expected,
        "present_bars":rows.len(),"missing_count":gaps.len(),
        "missing_open_times_ms":gaps,"complete":gaps.is_empty(),
        "quality":"unverified","backend":"native_sqlite",
        "message":"仅验证本地 SQLite K 线连续性，不等于交易所归档或 Tick 完整性。"})))
}
async fn data_prepare_plan(
    State(state):State<AppState>,Path(tenant):Path<String>,Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    // Merely a read-only plan: no implicit fetch, repair, deletion or order.
    let coverage=data_coverage(State(state),Path(tenant),Json(body)).await?;
    let mut result=coverage.0;
    result["action"]=json!("backfill");
    result["can_execute"]=json!(false);
    result["message"]=json!("请通过独立 data/control/job/start 显式执行受限原生历史补齐。");
    Ok(Json(result))
}
async fn data_page(
    State(state):State<AppState>,Path(tenant):Path<String>,Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    state.runtime.require_active(&tenant).map_err(AppError::from)?;
    if body["journal"]==true || body["cursor"].as_str()
        .is_some_and(|c|c.starts_with("journal.")){
        return Err(AppError::conflict("原生 Tick journal 分页尚未经过一致性验证。"));
    }
    let (product,symbol,interval,start,end,step)=data_query_range(&body)?;
    let limit=body["limit"].as_i64().unwrap_or(100).clamp(1,500);
    let cursor=body["cursor"].as_str().and_then(|v|v.parse::<i64>().ok())
        .unwrap_or(start);
    if cursor<start || cursor>end || (cursor-start)%step!=0 {
        return Err(AppError::bad_request("历史数据游标无效。"));
    }
    let bars=state.runtime.ledger.catalog_bars(
        &tenant,product,symbol,interval,cursor,end,limit+1
    ).map_err(|e|AppError::bad_request(e.to_string()))?;
    let more=bars.len()>limit as usize;
    let items=bars.iter().take(limit as usize).map(catalog_bar_json).collect::<Vec<_>>();
    let next=if more {items.last().and_then(|b|b["open_time_ms"].as_i64())
        .and_then(|v|v.checked_add(step)).map(|v|v.to_string())}else{None};
    Ok(Json(json!({"records":items,"cursor":next,"product":product,
        "symbol":symbol,"interval":interval,"quality":"unverified"})))
}
async fn data_latest(
    State(state):State<AppState>,Path(tenant):Path<String>,Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    state.runtime.require_active(&tenant).map_err(AppError::from)?;
    let mut result=serde_json::Map::new();
    let product=body["product"].as_str().unwrap_or("futures_um");
    let interval=body["interval"].as_str().unwrap_or("1m");
    let step=seealpha_market::interval_ms(interval)
        .map_err(|_|AppError::bad_request("周期无效。"))?;
    let end=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .ok().and_then(|d|i64::try_from(d.as_millis()).ok())
        .ok_or_else(||AppError::bad_request("时钟不可用。"))?;
    let start=end.saturating_sub(5000*step);
    for symbol in body["symbols"].as_array().cloned().unwrap_or_default() {
        let Some(symbol)=symbol.as_str() else {continue;};
        let rows=state.runtime.ledger.catalog_bars(
            &tenant,product,symbol,interval,start,end,5000
        ).map_err(|e|AppError::bad_request(e.to_string()))?;
        result.insert(symbol.to_string(),rows.last()
            .map(|bar|json!((bar.open_time_ms+step-1)*1_000_000))
            .unwrap_or(Value::Null));
    }
    Ok(Json(Value::Object(result)))
}
async fn data_chart(
    State(state):State<AppState>,Path(tenant):Path<String>,Json(body):Json<Value>,
)->Result<Json<Value>,AppError>{
    state.runtime.require_active(&tenant).map_err(AppError::from)?;
    let symbol=body["symbol"].as_str()
        .ok_or_else(||AppError::bad_request("品种无效。"))?;
    let product=body["product"].as_str().unwrap_or("futures_um");
    let interval=body["interval"].as_str().unwrap_or("1m");
    if product=="options" {
        seealpha_market::options::parse_option_symbol(symbol)
            .map_err(|_|AppError::bad_request("期权合约无效。"))?;
        if !matches!(interval,"1m"|"5m"|"15m") {
            return Err(AppError::bad_request("期权原生周期无效。"));
        }
    } else {
        seealpha_market::Product::parse(product).map_err(|_|AppError::bad_request("产品无效。"))?;
        seealpha_market::check_symbol(symbol)
            .map_err(|_|AppError::bad_request("品种无效。"))?;
    }
    let step=seealpha_market::interval_ms(interval)
        .map_err(|_|AppError::bad_request("K 线周期无效。"))?;
    let now=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .ok().and_then(|d|i64::try_from(d.as_millis()).ok())
        .ok_or_else(||AppError::bad_request("时钟不可用。"))?;
    let end=(now/step)*step;
    let start=end.saturating_sub(300*step);
    let bars=state.runtime.ledger.catalog_bars(
        &tenant,product,symbol,interval,start,end,300
    ).map_err(|e|AppError::bad_request(e.to_string()))?;
    let candles=bars.iter().map(|b|json!({
        "time":b.open_time_ms/1000,"open":b.open,"high":b.high,
        "low":b.low,"close":b.close,"volume":b.volume,"is_closed":true
    })).collect::<Vec<_>>();
    Ok(Json(json!({"symbol":symbol,"interval":interval,"candles":candles,
        "checked_at":now,"live":false,"quality":"unverified"})))
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
