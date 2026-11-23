//! Desired subscriptions are configuration only; never start collectors.
use crate::{AppError,AppState};
use axum::{Router,Json,extract::{Path,Query,State},routing::get};
use serde::Deserialize;
use serde_json::{Value,json};
use seealpha_catalog::{PipelineSubscription,validate_subscription};
pub(crate) fn routes()->Router<AppState>{
    Router::new().route("/pipeline/subscriptions",get(list))
        .route("/pipeline/subscriptions/{product}/{symbol}",axum::routing::put(upsert).delete(disable))
}
#[derive(Deserialize)]struct Scope{product:Option<String>}
async fn list(State(state):State<AppState>,Query(filter):Query<Scope>)->Result<Json<Value>,AppError>{
    let rows=state.control.subscriptions(filter.product.as_deref())
        .map_err(|e|AppError::bad_request(e.to_string()))?;
    Ok(Json(json!({"items":rows})))
}
async fn upsert(State(state):State<AppState>,Path((product,symbol)):Path<(String,String)>,
    Json(item):Json<PipelineSubscription>)->Result<Json<Value>,AppError>{
    if item.product!=product||item.symbol!=symbol{return Err(AppError::bad_request("path and subscription identity differ"));}
    validate_subscription(&item).map_err(|e|AppError::bad_request(e.to_string()))?;
    state.control.upsert_subscription(&item).map_err(|e|AppError::bad_request(e.to_string()))?;
    Ok(Json(json!({"subscription":item,"effect":"configuration_only","data_ready":false,"pending_reconciliation":true})))
}
async fn disable(State(state):State<AppState>,Path((product,symbol)):Path<(String,String)>)->Result<Json<Value>,AppError>{
    if !matches!(product.as_str(),"spot"|"futures_um"|"options"){return Err(AppError::bad_request("invalid product"));}
    if !state.control.disable_subscription(&product,&symbol)
        .map_err(|e|AppError::bad_request(e.to_string()))?{return Err(AppError::not_found("subscription not found"));}
    Ok(Json(json!({"disabled":true,"data_deleted":false})))
}
