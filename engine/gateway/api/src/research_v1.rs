//! Read-only Research datasets and optional MCP, without task grants.
use crate::{AppError,AppState};
use axum::{Json,Router,extract::State,routing::get};
use serde_json::{Value,json};
#[path = "../../mcp/src/research.rs"]
mod mcp;
pub(super) fn routes(mcp_enabled:bool)->Router<AppState>{
    let mut routes=Router::new().route("/research/v1/datasets",get(datasets));
    if mcp_enabled{routes=routes.route("/research/mcp",axum::routing::post(mcp::handle));}
    routes
}
pub(super) fn available_datasets(state:&AppState)->Result<Value,AppError>{
    let inventory=crate::parity::dataset_inventory(state)?
        .ok_or_else(||AppError::conflict("Research Catalog 未挂载，无法确认数据集库存。"))?;
    let datasets=inventory.into_iter().filter_map(|mut record|{
        let id=format!("{}:{}:{}",record["product"].as_str()?,record["symbol"].as_str()?,record["level"].as_str()?);
        record["id"]=json!(id);Some(record)
    }).collect::<Vec<_>>();
    Ok(json!({"datasets":datasets}))
}
async fn datasets(State(state):State<AppState>)->Result<Json<Value>,AppError>{
    Ok(Json(available_datasets(&state)?))
}
