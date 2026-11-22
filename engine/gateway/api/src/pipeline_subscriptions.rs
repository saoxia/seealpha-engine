//! Config-only control plane. Never starts a collector or deletes market data.
use crate::{AppError,AppState};
use axum::{Router,Json,extract::{Path,Query,State},routing::get};
use serde::Deserialize;
use serde_json::{Value,json};
use seealpha_catalog::{PipelineSubscription,validate_subscription};

pub(crate) fn routes()->Router<AppState>{
    Router::new()
      .route("/tenants/{tenant_id}/pipeline/subscriptions",get(list))
      .route("/tenants/{tenant_id}/pipeline/subscriptions/{product}/{symbol}",
          axum::routing::put(upsert).delete(disable))
}
#[derive(Deserialize)]
struct Scope{product:Option<String>}
async fn list(
    State(state):State<AppState>,Path(tenant):Path<String>,Query(filter):Query<Scope>,
)->Result<Json<Value>,AppError>{
    state.runtime.require_active(&tenant).map_err(AppError::from)?;
    let rows=state.runtime.control.subscriptions(&tenant,filter.product.as_deref())
        .map_err(|e|AppError::bad_request(e.to_string()))?;
    Ok(Json(json!({"items":rows})))
}
async fn upsert(
    State(state):State<AppState>,
    Path((tenant,product,symbol)):Path<(String,String,String)>,
    Json(item):Json<PipelineSubscription>,
)->Result<Json<Value>,AppError>{
    state.runtime.require_active(&tenant).map_err(AppError::from)?;
    if item.product!=product || item.symbol!=symbol {
        return Err(AppError::bad_request("path and subscription identity differ"));
    }
    validate_subscription(&item).map_err(|e|AppError::bad_request(e.to_string()))?;
    state.runtime.control.upsert_subscription(&tenant,&item)
        .map_err(|e|AppError::bad_request(e.to_string()))?;
    Ok(Json(json!({"subscription":item,"effect":"configuration_only",
        "data_ready":false,"pending_reconciliation":true})))
}
async fn disable(
    State(state):State<AppState>,
    Path((tenant,product,symbol)):Path<(String,String,String)>,
)->Result<Json<Value>,AppError>{
    state.runtime.require_active(&tenant).map_err(AppError::from)?;
    if !matches!(product.as_str(),"spot"|"futures_um"){
        return Err(AppError::bad_request("invalid product"));
    }
    if !state.runtime.control.disable_subscription(&tenant,&product,&symbol)
        .map_err(|e|AppError::bad_request(e.to_string()))?{
        return Err(AppError::not_found("subscription not found"));
    }
    Ok(Json(json!({"disabled":true,"data_deleted":false})))
}
